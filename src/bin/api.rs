use actix_web::{error, get, web, App, HttpRequest, HttpResponse, HttpServer, Responder};
use bento::chainweb_client::ChainwebClient;
use bento::db;
use bento::models::*;
use bento::repository::*;
use bigdecimal::BigDecimal;
use chrono::NaiveDateTime;
use dotenvy::dotenv;
use serde::Deserialize;
use std::collections::HashMap;
use std::env;
use std::time::Instant;

#[cfg(feature = "transactions")]
use actix_web::post;

#[cfg(feature = "transactions")]
#[derive(Deserialize)]
struct RequestKeys {
    request_keys: Vec<String>,
}

#[cfg(feature = "transactions")]
#[get("/tx/{request_key}")]
async fn tx(
    path: web::Path<String>,
    transactions: web::Data<TransactionsRepository>,
) -> actix_web::Result<impl Responder> {
    let request_key = path.into_inner();
    let req_key = request_key.clone();
    let tx: HashMap<String, Vec<Transaction>> =
        web::block(move || transactions.find_all_related(&vec![request_key]))
            .await?
            .map_err(error::ErrorInternalServerError)?;
    Ok(match tx.contains_key(&req_key) {
        false => HttpResponse::NotFound().body("Tx not found"),
        true => HttpResponse::Ok().json(tx.get(&req_key).unwrap()),
    })
}

#[cfg(feature = "transactions")]
#[post("/txs")]
async fn txs(
    body: web::Json<RequestKeys>,
    transactions: web::Data<TransactionsRepository>,
) -> actix_web::Result<impl Responder> {
    let result: HashMap<String, Vec<Transaction>> =
        web::block(move || transactions.find_all_related(&body.request_keys))
            .await?
            .map_err(error::ErrorInternalServerError)?;
    Ok(HttpResponse::Ok().json(result))
}

#[get("/balance/{account}")]
async fn get_balance(
    path: web::Path<String>,
    query: web::Query<HashMap<String, String>>,
    transfers: web::Data<TransfersRepository>,
) -> actix_web::Result<impl Responder> {
    let account = path.into_inner();
    let module = query.get("module").cloned();
    let all: HashMap<String, HashMap<i64, BigDecimal>> =
        web::block(move || transfers.get_balance(&account, module.as_deref()))
            .await?
            .map_err(error::ErrorInternalServerError)?;
    Ok(HttpResponse::Ok().json(all))
}

#[deprecated(note = "Use /transfers instead, this endpoint will be removed in the near future")]
#[get("/transfers/{account}/received")]
async fn received_transfers(
    path: web::Path<String>,
    request: HttpRequest,
    transfers: web::Data<TransfersRepository>,
) -> actix_web::Result<impl Responder> {
    let account = path.into_inner();
    let params = web::Query::<HashMap<String, i64>>::from_query(request.query_string()).unwrap();
    let min_height = params.get("min_height").copied();
    let before = Instant::now();
    let transfers: HashMap<String, Vec<Transfer>> =
        web::block(move || transfers.find_received(&account, min_height))
            .await?
            .map_err(error::ErrorInternalServerError)?;
    log::info!("Received transfers took {:?}", before.elapsed().as_millis());
    Ok(HttpResponse::Ok().json(transfers))
}

#[get("/transfers")]
async fn get_transfers(
    request: HttpRequest,
    transfers: web::Data<TransfersRepository>,
) -> actix_web::Result<impl Responder> {
    let params = web::Query::<HashMap<String, String>>::from_query(request.query_string()).unwrap();
    let from = params.get("from").map(|e| e.to_string());
    let to = params.get("to").map(|e| e.to_string());
    let min_height = match params.get("min_height").map(|h| h.parse::<i64>()) {
        Some(Ok(height)) => Some(height),
        Some(Err(_)) => return Ok(HttpResponse::BadRequest().body("Invalid min_height")),
        None => None,
    };
    let transfers = web::block(move || transfers.find(from, to, min_height))
        .await?
        .map_err(error::ErrorInternalServerError)?;
    Ok(HttpResponse::Ok().json(transfers))
}

#[derive(Deserialize)]
struct ActivityQuery {
    activity_type: Option<String>,
    module: Option<String>,
    start_date: Option<String>,
    end_date: Option<String>,
    limit: Option<i64>,
    offset: Option<i64>,
}

#[get("/account/{account}/activities")]
async fn get_account_activities(
    path: web::Path<String>,
    query: web::Query<ActivityQuery>,
    activities: web::Data<AccountActivitiesRepository>,
) -> actix_web::Result<impl Responder> {
    let account = path.into_inner();

    // Parse date parameters
    let start_date = query
        .start_date
        .as_ref()
        .and_then(|d| NaiveDateTime::parse_from_str(d, "%Y-%m-%dT%H:%M:%S").ok());
    let end_date = query
        .end_date
        .as_ref()
        .and_then(|d| NaiveDateTime::parse_from_str(d, "%Y-%m-%dT%H:%M:%S").ok());

    let results = web::block(move || {
        activities.find_by_account(
            &account,
            query.activity_type.as_deref(),
            query.module.as_deref(),
            start_date,
            end_date,
            query.limit,
            query.offset,
        )
    })
    .await?
    .map_err(error::ErrorInternalServerError)?;

    Ok(HttpResponse::Ok().json(results))
}

#[get("/health-check")]
async fn health_check(
    chainweb_client: web::Data<ChainwebClient>,
    blocks_repo: web::Data<BlocksRepository>,
) -> actix_web::Result<impl Responder> {
    // Get the latest cut from the blockchain node
    let cut = match chainweb_client.get_cut().await {
        Ok(cut) => cut,
        Err(_) => {
            return Ok(HttpResponse::BadRequest().body("Failed to get cut from blockchain node"))
        }
    };

    // Check each chain to see if our database is in sync
    for (chain_id, block_hash) in &cut.hashes {
        let chain_id_i64 = chain_id.0 as i64;
        let blockchain_height = block_hash.height as i64;

        // Get the min/max height blocks from our database for this chain
        let (_, max_block) = match web::block({
            let blocks_repo = blocks_repo.clone();
            move || blocks_repo.find_min_max_height_blocks(chain_id_i64)
        })
        .await?
        .map_err(error::ErrorInternalServerError)?
        {
            (min_block, max_block) => (min_block, max_block),
        };

        // Check if we have any blocks for this chain
        let db_height = match max_block {
            Some(block) => block.height,
            None => return Ok(HttpResponse::BadRequest().body("No blocks found in database")),
        };

        // If any chain is not in sync, return 400
        if blockchain_height - db_height > 2 {
            return Ok(HttpResponse::BadRequest().body(format!(
                "Chain {} not in sync: DB height {}, Blockchain height {}",
                chain_id_i64, db_height, blockchain_height
            )));
        }
    }

    // All chains are in sync
    Ok(HttpResponse::Ok().body("OK"))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));
    dotenv().ok();
    let port = env::var("API_PORT")
        .unwrap_or_else(|_| "80".to_string())
        .parse::<u16>()
        .expect("Invalid API_PORT");

    let pool = db::initialize_db_pool();
    let transactions = TransactionsRepository { pool: pool.clone() };
    let transfers = TransfersRepository { pool: pool.clone() };
    let blocks = BlocksRepository { pool: pool.clone() };
    let activities = AccountActivitiesRepository { pool: pool.clone() };

    HttpServer::new(move || {
        let chainweb_client = ChainwebClient::new();
        let mut app = App::new()
            .app_data(web::Data::new(transactions.clone()))
            .app_data(web::Data::new(transfers.clone()))
            .app_data(web::Data::new(blocks.clone()))
            .app_data(web::Data::new(activities.clone()))
            .app_data(web::Data::new(chainweb_client));

        // Conditionally add transaction endpoints (requires 'transactions' feature)
        #[cfg(feature = "transactions")]
        {
            app = app.service(tx).service(txs);
        }

        app.service(get_balance)
            .service(received_transfers)
            .service(get_transfers)
            .service(get_account_activities)
            .service(health_check)
    })
    .bind(("0.0.0.0", port))?
    .run()
    .await
}
