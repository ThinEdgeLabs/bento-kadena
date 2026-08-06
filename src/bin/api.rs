use actix_web::{error, get, web, App, HttpRequest, HttpResponse, HttpServer, Responder};
use bento::chainweb_client::ChainwebClient;
use bento::db;
use bento::models::*;
use bento::repository::*;
use bigdecimal::BigDecimal;
use chrono::{DateTime, NaiveDateTime};
use dotenvy::dotenv;
use serde::{Deserialize, Serialize};
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

const DEFAULT_ACTIVITY_LIMIT: i64 = 100;
const MAX_ACTIVITY_LIMIT: i64 = 1000;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ActivityQuery {
    activity_type: Option<String>,
    module: Option<String>,
    /// Unix microseconds
    start_time: Option<i64>,
    /// Unix microseconds
    end_time: Option<i64>,
    /// `next_cursor` from the previous response.
    cursor: Option<String>,
    limit: Option<i64>,
}

/// Where the previous page stopped: `(creation_time, id)` of its last row.
///
/// Two values rather than one because a timestamp alone is not a position — every transfer
/// in a block shares the block's `creation_time`, so it cannot express which rows were already returned.
/// `id` is unique and breaks the tie.
#[derive(Serialize, Deserialize)]
struct ActivityCursor {
    v: u8,
    /// `creation_time` in unix microseconds.
    t: i64,
    /// `id` of the last row.
    i: i64,
}

const CURSOR_VERSION: u8 = 1;

impl ActivityCursor {
    fn encode(&self) -> String {
        base64_url::encode(&serde_json::to_vec(self).expect("cursor is always serialisable"))
    }

    fn decode(raw: &str) -> Result<(NaiveDateTime, i64), String> {
        let bytes = base64_url::decode(raw).map_err(|_| "cursor is not valid base64")?;
        let cursor: ActivityCursor =
            serde_json::from_slice(&bytes).map_err(|_| "cursor is malformed")?;

        if cursor.v != CURSOR_VERSION {
            return Err(format!("unsupported cursor version {}", cursor.v));
        }

        let moment = micros_to_naive(cursor.t)?;
        Ok((moment, cursor.i))
    }
}

#[derive(Serialize)]
struct ActivitiesResponse {
    items: Vec<AccountActivity>,
    /// Absent once the feed is drained.
    #[serde(skip_serializing_if = "Option::is_none")]
    next_cursor: Option<String>,
}

/// Converts a unix-microsecond query parameter into the stored naive UTC value.
fn micros_to_naive(micros: i64) -> Result<NaiveDateTime, String> {
    DateTime::from_timestamp_micros(micros)
        .map(|moment| moment.naive_utc())
        .ok_or_else(|| format!("timestamp {micros} is outside the representable range"))
}

#[get("/account/{account}/activity")]
async fn get_account_activities(
    path: web::Path<String>,
    query: web::Query<ActivityQuery>,
    activities: web::Data<AccountActivitiesRepository>,
) -> actix_web::Result<impl Responder> {
    let account = path.into_inner();

    let start_time = match query.start_time.map(micros_to_naive).transpose() {
        Ok(parsed) => parsed,
        Err(message) => return Ok(HttpResponse::BadRequest().body(message)),
    };
    let end_time = match query.end_time.map(micros_to_naive).transpose() {
        Ok(parsed) => parsed,
        Err(message) => return Ok(HttpResponse::BadRequest().body(message)),
    };
    let before = match query
        .cursor
        .as_deref()
        .map(ActivityCursor::decode)
        .transpose()
    {
        Ok(parsed) => parsed,
        Err(message) => return Ok(HttpResponse::BadRequest().body(message)),
    };

    if let Some(limit) = query.limit {
        if limit <= 0 {
            return Ok(HttpResponse::BadRequest().body("limit must be positive"));
        }
    }
    let effective_limit = query
        .limit
        .unwrap_or(DEFAULT_ACTIVITY_LIMIT)
        .min(MAX_ACTIVITY_LIMIT);

    let results = web::block(move || {
        activities.find_by_account(
            &account,
            query.activity_type.as_deref(),
            query.module.as_deref(),
            start_time,
            end_time,
            before,
            Some(effective_limit),
        )
    })
    .await?
    .map_err(error::ErrorInternalServerError)?;

    // A short page means the feed is drained; a full one means there may be more.
    let next_cursor = (results.len() as i64 == effective_limit)
        .then(|| results.last())
        .flatten()
        .map(|last| {
            ActivityCursor {
                v: CURSOR_VERSION,
                t: last.creation_time.and_utc().timestamp_micros(),
                i: last.id,
            }
            .encode()
        });

    Ok(HttpResponse::Ok().json(ActivitiesResponse {
        items: results,
        next_cursor,
    }))
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

        app = app
            .service(get_balance)
            .service(received_transfers)
            .service(get_transfers)
            .service(get_account_activities)
            .service(health_check);

        #[cfg(feature = "transactions")]
        {
            app = app.service(tx).service(txs);
        }

        app
    })
    .bind(("0.0.0.0", port))?
    .run()
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn naive(raw: &str) -> NaiveDateTime {
        NaiveDateTime::parse_from_str(raw, "%Y-%m-%dT%H:%M:%S%.f").unwrap()
    }

    fn cursor(micros: i64, id: i64) -> String {
        ActivityCursor {
            v: CURSOR_VERSION,
            t: micros,
            i: id,
        }
        .encode()
    }

    #[test]
    fn converts_unix_micros_to_the_stored_value() {
        assert_eq!(
            micros_to_naive(1782564338730935).unwrap(),
            naive("2026-06-27T12:45:38.730935")
        );
        assert!(micros_to_naive(i64::MAX).is_err());
    }

    #[test]
    fn decoding_a_cursor_gives_back_the_exact_position() {
        let moment = naive("2026-06-27T12:45:38.730935");
        let encoded = cursor(moment.and_utc().timestamp_micros(), 42);

        let (decoded_time, decoded_id) = ActivityCursor::decode(&encoded).unwrap();
        assert_eq!(decoded_time, moment);
        assert_eq!(decoded_id, 42);
    }

    #[test]
    fn the_cursor_is_url_safe_and_unpadded() {
        let encoded = cursor(1782564338730935, 42);
        assert!(!encoded.contains('+') && !encoded.contains('/') && !encoded.contains('='));
    }

    #[test]
    fn two_rows_from_one_block_get_distinct_cursors() {
        // Same timestamp, different id
        let block_time = naive("2026-06-27T12:45:38.730935")
            .and_utc()
            .timestamp_micros();
        assert_ne!(cursor(block_time, 41), cursor(block_time, 42));
    }

    #[test]
    fn a_bad_cursor_is_rejected_rather_than_ignored() {
        assert!(ActivityCursor::decode("!!!not base64!!!").is_err());
        assert!(ActivityCursor::decode(&base64_url::encode(b"not json")).is_err());

        let future =
            base64_url::encode(&serde_json::to_vec(&ActivityCursor { v: 99, t: 1, i: 1 }).unwrap());
        assert!(ActivityCursor::decode(&future).is_err());
    }
}
