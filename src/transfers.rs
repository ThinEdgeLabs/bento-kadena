use crate::chainweb_client::ChainwebClient;
use crate::db::DbError;
use crate::models::{Block, Event, Transfer};
use crate::repository::{BlocksRepository, EventsRepository, TransfersRepository};
use bigdecimal::BigDecimal;
use chrono::DateTime;
use std::collections::HashMap;
use std::str::FromStr;
use std::time::Instant;

pub async fn backfill(
    batch_size: i64,
    chainweb_client: &ChainwebClient,
    blocks_repository: &BlocksRepository,
    events_repository: &EventsRepository,
    transfers_repository: &TransfersRepository,
) -> Result<(), Box<dyn std::error::Error>> {
    let cut = chainweb_client.get_cut().await.unwrap();
    cut.hashes.iter().for_each(|e| {
        let chain_id = e.0 .0;
        log::info!("Backfilling transfers on chain {}...", chain_id);
        backfill_chain(
            chain_id as i64,
            batch_size,
            events_repository,
            blocks_repository,
            transfers_repository,
            None,
        )
        .unwrap();
    });
    Ok(())
}
/// Loop through events
/// Parse event
/// Check if event is a balance transfer
/// If it is, insert into transfers table
pub fn backfill_chain(
    chain_id: i64,
    batch_size: i64,
    events_repository: &EventsRepository,
    blocks_repository: &BlocksRepository,
    transfers_repository: &TransfersRepository,
    starting_max_height: Option<i64>,
) -> Result<(), DbError> {
    let min_height = 0;
    let mut max_height = match starting_max_height {
        Some(value) => value,
        None => events_repository.find_max_height(chain_id)?,
    };
    loop {
        if max_height <= min_height {
            break;
        }
        log::info!("Indexing transfers from height: {}", max_height);
        let before = Instant::now();
        let events =
            events_repository.find_by_range(max_height - batch_size, max_height, chain_id)?;
        log::info!(
            "Found {} events in {}ms",
            events.len(),
            before.elapsed().as_millis()
        );
        if events.is_empty() {
            max_height -= batch_size;
            continue;
        }
        let before = Instant::now();
        let blocks_hashes = events
            .iter()
            .map(|event| event.block.clone())
            .collect::<Vec<String>>();
        let blocks = &blocks_repository.find_by_hashes(&blocks_hashes)?;
        process_transfers(&events, blocks, transfers_repository)?;
        log::info!(
            "Processed {} events in {}ms",
            events.len(),
            before.elapsed().as_millis(),
        );
        max_height -= batch_size;
    }
    Ok(())
}

fn is_balance_transfer(event: &Event) -> bool {
    event.name == "TRANSFER"
}

pub fn process_transfers(
    events: &[Event],
    blocks: &[Block],
    repository: &TransfersRepository,
) -> Result<(), DbError> {
    let blocks_by_hash = blocks
        .iter()
        .map(|block| (block.hash.to_string(), block))
        .collect::<HashMap<String, &Block>>();
    let transfers = events
        .iter()
        .filter(|event| is_balance_transfer(event))
        .map(|event| make_transfer(event, blocks_by_hash[&event.block]))
        .collect::<Vec<Transfer>>();
    // Number of parameters in one SQL query is limited to 65535, so we need to split the inserts
    transfers.chunks(1000).for_each(|chunk| {
        repository.insert_batch(&chunk.to_vec()).unwrap();
    });
    Ok(())
}

fn make_transfer(event: &Event, block: &Block) -> Transfer {
    let sender = event.params[0].as_str().unwrap().to_string();
    let receiver = event.params[1].as_str().unwrap().to_string();
    let amount = match event.params[2].is_number() {
        true => BigDecimal::from_str(&event.params[2].to_string()).unwrap(),
        false => match event.params[2].is_object() {
            true => match &event.params[2].as_object().unwrap().get("decimal") {
                Some(number) => {
                    BigDecimal::from_str(number.as_str().unwrap()).unwrap_or(BigDecimal::from(0))
                }
                None => {
                    let number = &event.params[2]
                        .as_object()
                        .unwrap()
                        .get("int")
                        .unwrap()
                        .as_i64()
                        .unwrap();
                    BigDecimal::from(*number)
                }
            },
            false => BigDecimal::from(0),
        },
    };

    Transfer {
        amount,
        block: event.block.clone(),
        chain_id: event.chain_id,
        creation_time: DateTime::from_timestamp_micros(
            block.creation_time.and_utc().timestamp_micros(),
        )
        .unwrap()
        .naive_utc(),
        from_account: sender,
        height: event.height,
        idx: event.idx,
        module_hash: event.module_hash.clone(),
        module_name: event.module.clone(),
        request_key: event.request_key.clone(),
        to_account: receiver,
        pact_id: event.pact_id.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;
    use crate::models::Block;
    use crate::repository::BlocksRepository;
    use chrono::Utc;
    use rand::distr::{Alphanumeric, SampleString};
    use serial_test::serial;

    fn make_block(chain_id: i64, height: i64, hash: String) -> Block {
        Block {
            chain_id,
            hash,
            height,
            parent: "parent".to_string(),
            creation_time: Utc::now().naive_utc(),
        }
    }

    fn make_transfer_event(
        block: String,
        height: i64,
        idx: i64,
        chain_id: i64,
        from: String,
        to: String,
        amount: f64,
    ) -> Event {
        Event {
            block: block.clone(),
            chain_id,
            height,
            idx,
            module: "coin".to_string(),
            module_hash: "module-hash".to_string(),
            name: "TRANSFER".to_string(),
            params: serde_json::json!([from, to, amount]),
            param_text: "param-text".to_string(),
            qual_name: "coin.TRANSFER".to_string(),
            request_key: Alphanumeric.sample_string(&mut rand::rng(), 16),
            pact_id: None,
        }
    }

    #[test]
    #[serial]
    fn test_transfers_backfill() {
        dotenvy::from_filename(".env.test").ok();
        let pool = db::initialize_db_pool();
        let blocks_repository = BlocksRepository { pool: pool.clone() };
        let events_repository = EventsRepository { pool: pool.clone() };
        let transfers_repository = TransfersRepository { pool: pool.clone() };
        blocks_repository
            .insert_batch(&[
                make_block(0, 0, "block-0".to_string()),
                make_block(0, 1, "block-1".to_string()),
                make_block(0, 2, "block-2".to_string()),
            ])
            .unwrap();
        events_repository
            .insert_batch(&[
                make_transfer_event(
                    "block-0".to_string(),
                    0,
                    0,
                    0,
                    "bob".to_string(),
                    "alice".to_string(),
                    100.1,
                ),
                make_transfer_event(
                    "block-0".to_string(),
                    0,
                    1,
                    0,
                    "alice".to_string(),
                    "bob".to_string(),
                    10.0,
                ),
                make_transfer_event(
                    "block-2".to_string(),
                    2,
                    0,
                    0,
                    "alice".to_string(),
                    "bob".to_string(),
                    10.1,
                ),
                make_transfer_event(
                    "block-2".to_string(),
                    2,
                    1,
                    0,
                    "alice".to_string(),
                    "bob".to_string(),
                    5.5,
                ),
            ])
            .unwrap();
        backfill_chain(
            0,
            1,
            &events_repository,
            &blocks_repository,
            &transfers_repository,
            None,
        )
        .unwrap();

        let bob_incoming_transfers = transfers_repository
            .find(None, Some(String::from("bob")), None)
            .unwrap();
        assert!(bob_incoming_transfers.len() == 3);
        let alice_incoming_transfers = transfers_repository
            .find(None, Some(String::from("alice")), None)
            .unwrap();
        assert!(alice_incoming_transfers.len() == 1);

        events_repository.delete_all().unwrap();
        transfers_repository.delete_all().unwrap();
        blocks_repository.delete_all().unwrap();
    }

    #[test]
    fn test_make_transfer() {
        let event = Event {
            block: "block-hash".to_string(),
            chain_id: 0,
            height: 0,
            idx: 0,
            module: "coin".to_string(),
            module_hash: "module-hash".to_string(),
            name: "TRANSFER".to_string(),
            params: serde_json::json!(["bob", "alice", 100.12324354665567]),
            param_text: "param-text".to_string(),
            qual_name: "coin.TRANSFER".to_string(),
            request_key: "request-key".to_string(),
            pact_id: None,
        };
        let block = make_block(0, 0, "hash".to_string());
        let transfer = make_transfer(&event, &block);
        assert_eq!(
            transfer,
            Transfer {
                amount: BigDecimal::from_str("100.12324354665567").unwrap(),
                block: "block-hash".to_string(),
                chain_id: 0,
                creation_time: DateTime::from_timestamp_micros(
                    block.creation_time.and_utc().timestamp_micros()
                )
                .unwrap()
                .naive_utc(),
                from_account: "bob".to_string(),
                height: 0,
                idx: 0,
                module_hash: "module-hash".to_string(),
                module_name: "coin".to_string(),
                request_key: "request-key".to_string(),
                to_account: "alice".to_string(),
                pact_id: None
            }
        );

        let no_sender_event = Event {
            params: serde_json::json!(["", "alice", 10]),
            ..event.clone()
        };
        let transfer = make_transfer(&no_sender_event, &block);
        assert_eq!(
            transfer,
            Transfer {
                amount: BigDecimal::from_str("10").unwrap(),
                block: "block-hash".to_string(),
                chain_id: 0,
                creation_time: DateTime::from_timestamp_micros(
                    block.creation_time.and_utc().timestamp_micros()
                )
                .unwrap()
                .naive_utc(),
                from_account: "".to_string(),
                height: 0,
                idx: 0,
                module_hash: "module-hash".to_string(),
                module_name: "coin".to_string(),
                request_key: "request-key".to_string(),
                to_account: "alice".to_string(),
                pact_id: None
            }
        );
        let no_receiver_event = Event {
            params: serde_json::json!(["bob", "", 10]),
            ..event
        };
        let transfer = make_transfer(&no_receiver_event, &block);
        assert_eq!(
            transfer,
            Transfer {
                amount: BigDecimal::from_str("10").unwrap(),
                block: "block-hash".to_string(),
                chain_id: 0,
                creation_time: DateTime::from_timestamp_micros(
                    block.creation_time.and_utc().timestamp_micros()
                )
                .unwrap()
                .naive_utc(),
                from_account: "bob".to_string(),
                height: 0,
                idx: 0,
                module_hash: "module-hash".to_string(),
                module_name: "coin".to_string(),
                request_key: "request-key".to_string(),
                to_account: "".to_string(),
                pact_id: None
            }
        );
    }

    #[test]
    fn test_parse_transfer_event_decimal() {
        let event = Event {
            block: "block-hash".to_string(),
            chain_id: 0,
            height: 0,
            idx: 0,
            module: "coin".to_string(),
            module_hash: "module-hash".to_string(),
            name: "TRANSFER".to_string(),
            params: serde_json::json!(["bob", "alice", {"decimal": "22.230409400000000000000000"}]),
            param_text: "param-text".to_string(),
            qual_name: "coin.TRANSFER".to_string(),
            request_key: "request-key".to_string(),
            pact_id: None,
        };
        let block = make_block(0, 0, "hash".to_string());
        let transfer = make_transfer(&event, &block);
        assert!(transfer.amount == BigDecimal::from_str("22.230409400000000000000000").unwrap());
        let event = Event {
            block: "block-hash".to_string(),
            chain_id: 0,
            height: 0,
            idx: 0,
            module: "coin".to_string(),
            module_hash: "module-hash".to_string(),
            name: "TRANSFER".to_string(),
            params: serde_json::json!(["bob", "alice", {"int": 1}]),
            param_text: "param-text".to_string(),
            qual_name: "coin.TRANSFER".to_string(),
            request_key: "request-key".to_string(),
            pact_id: None,
        };
        let transfer = make_transfer(&event, &block);
        assert!(transfer.amount == BigDecimal::from(1));
    }

    #[test]
    /// This test is to make sure that if the amount is not a number, we default to 0
    fn test_make_transfer_when_event_has_string_as_amount() {
        let event = Event {
            block: "block-hash".to_string(),
            chain_id: 0,
            height: 0,
            idx: 0,
            module: "coin".to_string(),
            module_hash: "module-hash".to_string(),
            name: "TRANSFER".to_string(),
            params: serde_json::json!(["bob", "alice", "wrong-amount"]),
            param_text: "param-text".to_string(),
            qual_name: "coin.TRANSFER".to_string(),
            request_key: "request-key".to_string(),
            pact_id: None,
        };
        let block = make_block(0, 0, "hash".to_string());
        let transfer = make_transfer(&event, &block);
        assert!(transfer.amount == BigDecimal::from(0));
    }

    #[test]
    fn test_is_balance_transfer() {
        let event = Event {
            block: "block-hash".to_string(),
            chain_id: 0,
            height: 0,
            idx: 0,
            module: "coin".to_string(),
            module_hash: "module-hash".to_string(),
            name: "TRANSFER".to_string(),
            params: serde_json::json!(["bob", "alice", 100.1]),
            param_text: "param-text".to_string(),
            qual_name: "coin.TRANSFER".to_string(),
            request_key: "request-key".to_string(),
            pact_id: None,
        };
        assert!(is_balance_transfer(&event));
        let event = Event {
            name: "NOT_TRANSFER".to_string(),
            ..event
        };
        assert!(!is_balance_transfer(&event));
    }
}
