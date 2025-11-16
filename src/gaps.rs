use std::vec;

use futures::{stream, StreamExt};

use crate::chainweb_client::{Bounds, ChainId, ChainwebClient, Hash};
use crate::indexer::Indexer;
use crate::models::Block;
use crate::repository::BlocksRepository;

pub async fn fill_gaps<'a>(
    chainweb_client: &ChainwebClient,
    blocks_repo: &BlocksRepository,
    indexer: &Indexer<'a>,
) -> Result<(), Box<dyn std::error::Error>> {
    let cut = chainweb_client.get_cut().await.unwrap();
    let gaps = cut
        .hashes
        .keys()
        .map(|chain| {
            let gaps = blocks_repo.find_gap_ranges(chain.0 as i64).unwrap();
            let missing_blocks = gaps
                .iter()
                .map(|gap| gap.1.height - gap.0.height - 1)
                .reduce(|acc, e| acc + e)
                .unwrap_or(0);
            log::info!("Chain {}, is missing {} blocks.", chain, missing_blocks);
            (chain, gaps)
        })
        .collect::<Vec<(&ChainId, Vec<(Block, Block)>)>>();

    for el in gaps {
        let (chain, gaps) = el;
        log::info!("Filling {} gaps for chain: {:?}", gaps.len(), chain);
        gaps.iter().for_each(|e| {
            log::info!(
                "Gap: {} - {}, size: {}",
                e.0.height,
                e.1.height,
                e.1.height - e.0.height - 1
            )
        });
        stream::iter(gaps)
            .map(|(lower_bound, upper_bound)| async move {
                indexer
                    .index_chain(
                        Bounds {
                            lower: vec![Hash(lower_bound.hash.clone())],
                            upper: vec![Hash(upper_bound.hash.clone())],
                        },
                        chain,
                        false,
                    )
                    .await
            })
            .buffer_unordered(4)
            .for_each(|result| {
                if let Err(e) = result {
                    log::error!("Error filling gap: {:?}", e);
                }
                async {}
            })
            .await;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;
    use crate::models::Block;
    use crate::repository::BlocksRepository;
    use chrono::Utc;
    use serial_test::serial;

    fn make_block(chain_id: i64, height: i64) -> Block {
        Block {
            chain_id,
            hash: format!("hash-{}", height),
            height,
            parent: "parent".to_string(),
            creation_time: Utc::now().naive_utc(),
        }
    }

    #[test]
    #[serial]
    fn test_find_gaps_returns_an_empty_vec_if_no_gaps_were_found() {
        dotenvy::from_filename(".env.test").ok();
        let pool = db::initialize_db_pool();
        let blocks = BlocksRepository { pool: pool.clone() };
        blocks.delete_all().unwrap();
        blocks
            .insert_batch(&vec![
                make_block(0, 0),
                make_block(0, 1),
                make_block(0, 2),
                make_block(0, 3),
                make_block(0, 4),
                make_block(0, 5),
            ])
            .unwrap();

        let chain = ChainId(0);
        assert!(blocks.find_gap_ranges(chain.0 as i64).unwrap().is_empty());
        blocks.delete_all().unwrap();
    }
    #[test]
    #[serial]
    fn test_find_gaps() {
        dotenvy::from_filename(".env.test").ok();
        let pool = db::initialize_db_pool();
        let blocks = BlocksRepository { pool: pool.clone() };
        blocks.delete_all().unwrap();
        blocks
            .insert_batch(&vec![
                make_block(0, 0),
                make_block(0, 1),
                make_block(0, 2),
                make_block(0, 4),
                make_block(0, 5),
                make_block(0, 9),
                make_block(0, 10),
            ])
            .unwrap();
        let gaps = blocks.find_gap_ranges(0);
        assert!(gaps.is_ok());
        let gaps_heights = gaps
            .unwrap()
            .iter()
            .map(|(a, b)| (a.height, b.height))
            .collect::<Vec<_>>();
        println!("{:?}", gaps_heights);
        assert!(gaps_heights == vec![(2, 4), (5, 9)]);
        blocks.delete_all().unwrap();
    }

    #[test]
    #[serial]
    fn test_find_gaps_in_range() {
        dotenvy::from_filename(".env.test").ok();
        let pool = db::initialize_db_pool();
        let blocks = BlocksRepository { pool: pool.clone() };
        blocks.delete_all().unwrap();
        blocks
            .insert_batch(&vec![
                make_block(0, 0),
                make_block(0, 1),
                make_block(0, 2),
                make_block(0, 4),
                make_block(0, 5),
                make_block(0, 6),
                make_block(0, 8),
                make_block(0, 10),
            ])
            .unwrap();

        let chain = ChainId(0);
        let gaps = blocks.find_gap_ranges(chain.0 as i64);
        let gaps_heights = gaps
            .unwrap()
            .iter()
            .map(|(a, b)| (a.height, b.height))
            .collect::<Vec<_>>();
        assert!(gaps_heights == vec![(2, 4), (6, 8), (8, 10)]);
        blocks.delete_all().unwrap();
    }
}
