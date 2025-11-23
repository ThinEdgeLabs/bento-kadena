use bento::chainweb_client::ChainwebClient;
use bento::db;
use bento::gaps;
use bento::indexer::*;
use bento::repository::*;
use clap::{Parser, Subcommand};
use dotenvy::dotenv;

#[derive(Parser)]
/// By default new blocks are indexed as they are mined. For backfilling and filling gaps use the
/// subcommands.
struct IndexerCli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Backfill blocks
    Backfill {
        /// Starting block height (inclusive)
        #[arg(long)]
        start: Option<i64>,
        /// Ending block height (inclusive)
        #[arg(long)]
        end: Option<i64>,
        /// Specific chain ID to backfill (0-19). If not specified, backfills all chains
        #[arg(long)]
        chain: Option<i64>,
        /// Force re-indexing of existing blocks
        #[arg(long)]
        force: bool,
    },
    /// Index missed blocks
    Gaps,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));
    dotenv().ok();

    let pool = db::initialize_db_pool();
    db::run_migrations(&mut pool.get().unwrap()).unwrap();

    let blocks = BlocksRepository { pool: pool.clone() };
    let events = EventsRepository { pool: pool.clone() };
    let transactions = TransactionsRepository { pool: pool.clone() };
    let transfers_repo = TransfersRepository { pool: pool.clone() };
    let activities_repo = AccountActivitiesRepository { pool: pool.clone() };
    let chainweb_client = ChainwebClient::new();
    let indexer = Indexer {
        chainweb_client: &chainweb_client,
        blocks: blocks.clone(),
        events: events.clone(),
        transactions: transactions.clone(),
        transfers: transfers_repo.clone(),
        activities: activities_repo.clone(),
    };

    let args = IndexerCli::parse();
    match args.command {
        Some(Command::Backfill {
            start,
            end,
            chain,
            force,
        }) => {
            // Validate parameters
            if let (Some(start_height), Some(end_height)) = (start, end) {
                if start_height > end_height {
                    return Err(format!(
                        "Invalid range: start ({}) must be <= end ({})",
                        start_height, end_height
                    )
                    .into());
                }
            }

            if let Some(chain_id) = chain {
                if !(0..20).contains(&chain_id) {
                    return Err(format!(
                        "Invalid chain ID: {}. Must be between 0 and 19",
                        chain_id
                    )
                    .into());
                }
            }

            // Handle different backfill modes
            match (start, end, chain) {
                (Some(start_height), Some(end_height), Some(chain_id)) => {
                    log::info!(
                        "Backfilling chain {} from height {} to {}{}",
                        chain_id,
                        start_height,
                        end_height,
                        if force { " (force mode)" } else { "" }
                    );
                    indexer
                        .backfill_range(start_height, end_height, chain_id, force)
                        .await?;
                }
                (Some(start_height), Some(end_height), None) => {
                    log::info!(
                        "Backfilling all chains from height {} to {}{}",
                        start_height,
                        end_height,
                        if force { " (force mode)" } else { "" }
                    );
                    // Backfill all chains for the specified range
                    for chain_id in 0..20 {
                        log::info!("Backfilling chain {}...", chain_id);
                        indexer
                            .backfill_range(start_height, end_height, chain_id, force)
                            .await?;
                    }
                }
                (None, None, Some(_)) => {
                    return Err(
                        "Chain ID specified without start/end heights. Please specify --start and --end"
                            .into(),
                    );
                }
                (Some(_), None, _) | (None, Some(_), _) => {
                    return Err("Both --start and --end must be specified together".into());
                }
                (None, None, None) => {
                    log::info!("Backfilling all blocks...");
                    indexer.backfill().await?;
                }
            }
        }
        Some(Command::Gaps) => {
            log::info!("Filling gaps...");
            gaps::fill_gaps(&chainweb_client, &blocks, &indexer).await?;
        }
        None => {
            log::info!("Indexing new blocks...");
            indexer.index_new_blocks().await?;
        }
    }

    Ok(())
}
