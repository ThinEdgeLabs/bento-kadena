use chrono::DateTime;
use core::panic;
use futures::stream;
use futures::StreamExt;
use std::collections::HashMap;
use std::error::Error;
use std::time::Instant;
use std::vec;

use super::chainweb_client::{
    tx_result::PactTransactionResult, BlockHeader, BlockPayload, Bounds, ChainId, Command, Cut,
    Hash, SignedTransaction,
};
use super::models::*;
use super::repository::*;
use crate::chainweb_client::ChainwebClient;
use crate::db::DbError;
use crate::event_filter::EventFilter;
use crate::transfers;

pub struct Indexer<'a> {
    pub chainweb_client: &'a ChainwebClient,
    pub blocks: BlocksRepository,
    pub events: EventsRepository,
    pub transactions: TransactionsRepository,
    pub transfers: TransfersRepository,
    pub activities: AccountActivitiesRepository,
    pub event_filter: EventFilter,
}

impl Indexer<'_> {
    pub async fn backfill(&self) -> Result<(), Box<dyn Error>> {
        let cut = self.chainweb_client.get_cut().await.unwrap();
        let bounds: Vec<(ChainId, Bounds)> = self.get_all_bounds(&cut);
        stream::iter(bounds)
            .map(|(chain, bounds)| async move { self.index_chain(bounds, &chain, false).await })
            .buffer_unordered(4)
            .collect::<Vec<Result<(), Box<dyn Error>>>>()
            .await;
        Ok(())
    }

    pub async fn backfill_range(
        &self,
        min_height: i64,
        max_height: i64,
        chain: i64,
        force_update: bool,
    ) -> Result<(), Box<dyn Error>> {
        let cut = self.chainweb_client.get_cut().await.unwrap();
        let latest_block_hash = cut
            .hashes
            .get(&ChainId(chain as u16))
            .unwrap()
            .hash
            .to_string();
        let bounds = Bounds {
            lower: vec![],
            upper: vec![Hash(latest_block_hash)],
        };
        let chain_id = ChainId(chain as u16);
        let range_low = self
            .chainweb_client
            .get_block_headers_branches(
                &chain_id,
                &bounds,
                &None,
                None,
                // The /chain/{chain}/header/branch endpoint returns blocks > min_height
                // not >= as the documentation states so we go one block back to make
                // sure we also get the block at min_height.
                Some((min_height - 1) as u64),
            )
            .await?;
        let range_high = self
            .chainweb_client
            .get_block_headers_branches(&chain_id, &bounds, &None, None, Some(max_height as u64))
            .await?;
        let bounds = Bounds {
            lower: vec![Hash(range_low.items.first().unwrap().hash.to_string())],
            upper: vec![Hash(range_high.items.first().unwrap().hash.to_string())],
        };
        self.index_chain(bounds, &chain_id, force_update).await?;
        Ok(())
    }

    pub async fn index_chain(
        &self,
        bounds: Bounds,
        chain: &ChainId,
        force_update: bool,
    ) -> Result<(), Box<dyn Error>> {
        log::info!("Indexing chain: {}, bounds: {:?}", chain.0, bounds);
        let mut next_bounds = bounds;
        loop {
            let before = Instant::now();
            let response = self
                .chainweb_client
                .get_block_headers_branches(chain, &next_bounds, &None, None, None)
                .await
                .unwrap();
            match response.items[..] {
                [] => return Ok(()),
                _ => {
                    log::info!(
                        "Chain {}: retrieved {} blocks, between heights {} and {}",
                        chain.0,
                        response.items.len(),
                        response.items.first().unwrap().height,
                        response.items.last().unwrap().height
                    );
                    let previous_bounds = next_bounds.clone();
                    next_bounds = Bounds {
                        upper: vec![Hash(response.items.last().unwrap().hash.to_string())],
                        ..next_bounds
                    };

                    if next_bounds == previous_bounds {
                        log::info!("Chain {}: fetched all blocks within given bounds.", chain.0);
                        return Ok(());
                    }
                }
            }
            self.process_headers(response.items, chain, force_update)
                .await?;
            log::info!(
                "Chain {}, elapsed time per batch: {:.2?}",
                chain.0,
                before.elapsed()
            );
        }
    }
    fn get_all_bounds(&self, cut: &Cut) -> Vec<(ChainId, Bounds)> {
        let mut bounds: Vec<(ChainId, Bounds)> = vec![];
        cut.hashes.iter().for_each(|(chain, last_block_hash)| {
            log::info!(
                "Chain: {}, current height: {}, last block hash: {}",
                chain.0,
                last_block_hash.height,
                last_block_hash.hash
            );
            match self
                .blocks
                .find_min_max_height_blocks(chain.0 as i64)
                .unwrap()
            {
                (Some(min_block), Some(max_block)) => {
                    bounds.push((
                        chain.clone(),
                        Bounds {
                            lower: vec![Hash(max_block.hash)],
                            upper: vec![Hash(last_block_hash.hash.to_string())],
                        },
                    ));
                    if min_block.height > 0 {
                        bounds.push((
                            chain.clone(),
                            Bounds {
                                lower: vec![],
                                upper: vec![Hash(min_block.hash)],
                            },
                        ));
                    }
                }
                (None, None) => bounds.push((
                    chain.clone(),
                    Bounds {
                        lower: vec![],
                        upper: vec![Hash(last_block_hash.hash.to_string())],
                    },
                )),
                _ => {}
            }
        });
        bounds
    }

    pub async fn process_headers(
        &self,
        headers: Vec<BlockHeader>,
        chain_id: &ChainId,
        force_update: bool,
    ) -> Result<(), Box<dyn Error>> {
        let payloads = self
            .chainweb_client
            .get_block_payload_batch(
                chain_id,
                headers
                    .iter()
                    .map(|e| e.payload_hash.as_str())
                    .collect::<Vec<&str>>(),
            )
            .await
            .unwrap();
        let blocks = self.build_blocks(&headers, &payloads);

        if force_update {
            blocks.iter().for_each(|block| {
                // Delete by height, not hash, to properly handle orphan blocks
                // (orphan blocks have different hashes at the same height)
                match self.blocks.delete_one(block.height, block.chain_id) {
                    Ok(deleted) => {
                        if deleted > 0 {
                            log::info!(
                                "Deleted existing block at height {} on chain {} (CASCADE will remove related data)",
                                block.height, block.chain_id
                            );
                        }
                    }
                    Err(e) => panic!(
                        "Error deleting block at height {} on chain {}: {:#?}",
                        block.height, block.chain_id, e
                    ),
                }
            });
        }

        match self.blocks.insert_batch(&blocks) {
            Ok(_) => {}
            Err(e) => panic!("Error inserting blocks: {:#?}", e),
        }

        let signed_txs_by_hash = get_signed_txs_from_payloads(&payloads);
        let request_keys: Vec<String> = signed_txs_by_hash.keys().map(|e| e.to_string()).collect();
        let tx_results = self
            .fetch_transactions_results(&request_keys[..], chain_id)
            .await?;
        let txs = get_transactions_from_payload(&signed_txs_by_hash, &tx_results, chain_id);

        if !txs.is_empty() {
            // OPTIONAL: Store transactions (when 'transactions' feature is enabled)
            #[cfg(feature = "transactions")]
            {
                match self.transactions.insert_batch(&txs) {
                    Ok(inserted) => log::info!("Inserted {} transactions", inserted),
                    Err(e) => panic!("Error inserting transactions: {:#?}", e),
                }
            }

            let events = get_events_from_txs(&tx_results, &signed_txs_by_hash);
            let filtered_events = self.event_filter.filter_events(events);

            if !filtered_events.is_empty() {
                // OPTIONAL: Store events (when 'events' feature is enabled)
                #[cfg(feature = "events")]
                {
                    match self.events.insert_batch(&filtered_events) {
                        Ok(inserted) => log::info!("Inserted {} events", inserted),
                        Err(e) => panic!("Error inserting events: {:#?}", e),
                    }
                }

                match transfers::process_transfers(&filtered_events, &blocks, &self.transfers) {
                    Ok(_) => {}
                    Err(e) => panic!("Error updating balances: {:#?}", e),
                }

                let activities =
                    crate::activities::process_account_activities(&filtered_events, &blocks);
                if !activities.is_empty() {
                    match self.activities.insert_batch(&activities) {
                        Ok(count) => log::info!("Inserted {} account activities", count),
                        Err(e) => {
                            log::error!("Error inserting account activities: {:#?}", e)
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn process_header(
        &self,
        header: &BlockHeader,
        chain_id: &ChainId,
    ) -> Result<(), Box<dyn Error>> {
        // Retry payload fetch up to 3 times
        const MAX_RETRIES: u32 = 3;
        let mut payloads = None;

        for attempt in 1..=MAX_RETRIES {
            match self
                .chainweb_client
                .get_block_payload_batch(chain_id, vec![header.payload_hash.as_str()])
                .await
            {
                Ok(p) if !p.is_empty() => {
                    payloads = Some(p);
                    break;
                }
                Ok(_) => {
                    log::warn!(
                        "Empty payload response (attempt {}/{}), payload hash: {}, height: {}, chain: {}",
                        attempt,
                        MAX_RETRIES,
                        header.payload_hash,
                        header.height,
                        chain_id.0
                    );
                }
                Err(e) => {
                    log::warn!(
                        "Failed to fetch payload (attempt {}/{}): {:#?}",
                        attempt,
                        MAX_RETRIES,
                        e
                    );
                }
            }

            if attempt < MAX_RETRIES {
                tokio::time::sleep(tokio::time::Duration::from_millis(500 * attempt as u64)).await;
            }
        }

        let payloads = payloads.ok_or_else(|| {
            format!(
                "Unable to retrieve payload after {} attempts, payload hash: {}, height: {}, chain: {}",
                MAX_RETRIES, header.payload_hash, header.height, chain_id.0
            )
        })?;

        let block = build_block(header, &payloads[0]);
        log::debug!(
            "Built block for chain {}, height {}, hash: {}",
            chain_id.0,
            header.height,
            block.hash
        );

        match self.save_block(&block) {
            Err(e) => {
                log::error!("Error saving block: {:#?}", e);
                return Err(e);
            }
            Ok(block) => block,
        };

        let signed_txs_by_hash = get_signed_txs_from_payload(&payloads[0]);
        let request_keys: Vec<String> = signed_txs_by_hash.keys().map(|e| e.to_string()).collect();

        if request_keys.is_empty() {
            log::debug!(
                "No transactions in block, chain {}, height {}",
                chain_id.0,
                header.height
            );
            return Ok(());
        }

        log::debug!(
            "Fetching {} transaction results for chain {}, height {}",
            request_keys.len(),
            chain_id.0,
            header.height
        );
        let before = Instant::now();
        let tx_results = self
            .fetch_transactions_results(&request_keys[..], chain_id)
            .await?;
        log::info!(
            "Fetched {} transaction results in {:.2?} for chain {}, height {}",
            tx_results.len(),
            before.elapsed(),
            chain_id.0,
            header.height
        );
        let txs = get_transactions_from_payload(&signed_txs_by_hash, &tx_results, chain_id);
        txs.iter().for_each(|tx| {
            if tx.block != block.hash {
                log::error!(
                    "Transaction block hash does not match header hash, tx: {:#?}",
                    tx,
                );
            }
        });
        // OPTIONAL: Store transactions (when 'transactions' feature is enabled)
        #[cfg(feature = "transactions")]
        {
            let txs = txs
                .into_iter()
                .filter(|tx| tx.block == block.hash)
                .collect::<Vec<Transaction>>();

            match self.transactions.insert_batch(&txs) {
                Ok(inserted) => {
                    if inserted > 0 {
                        log::info!("Inserted {} transactions", inserted)
                    }
                }
                Err(e) => panic!("Error inserting transactions: {:#?}", e),
            }
        }

        let events = get_events_from_txs(&tx_results, &signed_txs_by_hash);
        let events = events
            .into_iter()
            .filter(|e| e.block == block.hash)
            .collect::<Vec<Event>>();
        let filtered_events = self.event_filter.filter_events(events);

        if !filtered_events.is_empty() {
            // OPTIONAL: Store events (when 'events' feature is enabled)
            #[cfg(feature = "events")]
            {
                match self.events.insert_batch(&filtered_events) {
                    Ok(inserted) => {
                        if inserted > 0 {
                            log::info!("Inserted {} events", inserted);
                        }
                    }
                    Err(e) => panic!("Error inserting events: {:#?}", e),
                }
            }

            match transfers::process_transfers(&filtered_events, &[block.clone()], &self.transfers)
            {
                Ok(_) => {}
                Err(e) => panic!("Error updating balances: {:#?}", e),
            }

            let activities =
                crate::activities::process_account_activities(&filtered_events, &[block]);
            if !activities.is_empty() {
                match self.activities.insert_batch(&activities) {
                    Ok(count) => log::info!("Inserted {} account activities", count),
                    Err(e) => log::error!("Error inserting account activities: {:#?}", e),
                }
            }
        }
        Ok(())
    }

    pub async fn index_new_blocks(&self) -> Result<(), Box<dyn Error>> {
        use crate::chainweb_client::BlockHeaderEvent;
        use eventsource_client as es;
        use futures::stream::TryStreamExt;
        use tokio::time::{timeout, Duration};

        match self.chainweb_client.start_headers_stream() {
            Ok(stream) => {
                log::info!("Stream started");
                let mut stream = Box::pin(stream);
                const ACTIVITY_TIMEOUT: Duration = Duration::from_secs(300); // 5 minutes

                loop {
                    // Add timeout to detect stream inactivity
                    match timeout(ACTIVITY_TIMEOUT, stream.try_next()).await {
                        Ok(result) => match result {
                            Ok(Some(event)) => {
                                if let es::SSE::Event(ev) = event {
                                    if ev.event_type == "BlockHeader" {
                                        let block_header_event: BlockHeaderEvent =
                                            match serde_json::from_str(&ev.data) {
                                                Ok(e) => e,
                                                Err(e) => {
                                                    log::error!(
                                                        "Failed to parse block header event: {:#?}, data: {}",
                                                        e,
                                                        ev.data
                                                    );
                                                    continue;
                                                }
                                            };
                                        let chain_id = block_header_event.header.chain_id.clone();
                                        log::info!(
                                            "Chain {} header, height {} received",
                                            chain_id,
                                            block_header_event.header.height
                                        );
                                        match self
                                            .process_header(&block_header_event.header, &chain_id)
                                            .await
                                        {
                                            Ok(_) => {
                                                log::info!(
                                                    "Chain {} header, height {} processed",
                                                    chain_id,
                                                    block_header_event.header.height,
                                                );
                                            }
                                            Err(e) => {
                                                log::error!(
                                                    "Error processing header for chain {}, height {}: {:#?}",
                                                    chain_id,
                                                    block_header_event.header.height,
                                                    e
                                                );
                                                // Continue processing other blocks instead of failing
                                            }
                                        }
                                    }
                                }
                            }
                            Ok(None) => {
                                log::warn!("Headers stream ended unexpectedly");
                                return Err("Stream ended".into());
                            }
                            Err(e) => {
                                log::error!("Stream error: {:#?}", e);
                                return Err(Box::new(e));
                            }
                        },
                        Err(_) => {
                            log::error!(
                                "Stream activity timeout: no events received for {} seconds",
                                ACTIVITY_TIMEOUT.as_secs()
                            );
                            return Err(format!(
                                "No activity on stream for {} seconds",
                                ACTIVITY_TIMEOUT.as_secs()
                            )
                            .into());
                        }
                    }
                }
            }
            Err(e) => {
                log::error!("Failed to start headers stream: {:?}", e);
                Err(Box::new(e))
            }
        }
    }

    /// Builds the list of blocks from the given headers and payloads
    /// and inserts them in the database in a single transaction.
    fn build_blocks(&self, headers: &[BlockHeader], payloads: &[BlockPayload]) -> Vec<Block> {
        let headers_by_payload_hash = headers
            .iter()
            .map(|e| (e.payload_hash.clone(), e))
            .collect::<HashMap<String, &BlockHeader>>();
        let payloads_by_hash = payloads
            .iter()
            .map(|e| (e.payload_hash.clone(), e))
            .collect::<HashMap<String, &BlockPayload>>();
        headers_by_payload_hash
            .into_iter()
            .map(|(payload_hash, header)| {
                build_block(header, payloads_by_hash.get(&payload_hash).unwrap())
            })
            .collect::<Vec<Block>>()
    }

    /// Dealing with duplicate blocks (this only happens through the headers stream):
    /// - try to insert the block
    /// - if it fails, check if the block is already in the db
    /// - if it is, delete the block and associated data
    /// - insert the block again
    fn save_block(&self, block: &Block) -> Result<Block, DbError> {
        use diesel::result::DatabaseErrorKind;
        use diesel::result::Error::DatabaseError;
        match self.blocks.insert(block) {
            Ok(inserted_block) => Ok(inserted_block),
            Err(e) => match e.downcast_ref() {
                Some(DatabaseError(DatabaseErrorKind::UniqueViolation, _)) => {
                    log::info!("Block already exists");
                    let orphan = self
                        .blocks
                        .find_by_height(block.height, block.chain_id)
                        .unwrap()
                        .unwrap();
                    // Delete orphan block - CASCADE will automatically delete related records
                    self.blocks.delete_by_hash(&orphan.hash, orphan.chain_id)?;
                    self.blocks.insert(block)
                }
                _ => Err(e),
            },
        }
    }

    async fn fetch_transactions_results(
        &self,
        request_keys: &[String],
        chain: &ChainId,
    ) -> Result<Vec<PactTransactionResult>, Box<dyn Error>> {
        use futures::stream::StreamExt;

        let transactions_per_request = 10;
        let concurrent_requests = 4;
        let mut results: Vec<PactTransactionResult> = vec![];
        let mut errors = 0;

        let collected_results: Vec<_> =
            futures::stream::iter(request_keys.chunks(transactions_per_request))
                .map(|chunk| {
                    let chunk_vec = chunk.to_vec();
                    async move {
                        (
                            self.chainweb_client.poll(&chunk_vec, chain).await,
                            chunk_vec,
                        )
                    }
                })
                .buffer_unordered(concurrent_requests)
                .collect()
                .await;

        for (result, chunk) in collected_results {
            match result {
                Ok(tx_results) => {
                    results.append(
                        &mut tx_results
                            .into_values()
                            .collect::<Vec<PactTransactionResult>>(),
                    );
                }
                Err(e) => {
                    errors += 1;
                    log::error!(
                        "Failed to fetch transaction results for {} request keys on chain {}: {:#?}",
                        chunk.len(),
                        chain.0,
                        e
                    );
                }
            }
        }

        if errors > 0 {
            log::warn!(
                "Fetched {} transaction results with {} errors on chain {}",
                results.len(),
                errors,
                chain.0
            );
        }

        Ok(results)
    }
}

fn get_signed_txs_from_payload(payload: &BlockPayload) -> HashMap<String, SignedTransaction> {
    payload
        .transactions
        .iter()
        .map(|tx| {
            serde_json::from_slice::<SignedTransaction>(&base64_url::decode(&tx).unwrap()).unwrap()
        })
        .map(|tx| (tx.hash.clone(), tx))
        .collect::<HashMap<String, SignedTransaction>>()
}

fn get_signed_txs_from_payloads(payloads: &[BlockPayload]) -> HashMap<String, SignedTransaction> {
    payloads
        .iter()
        .map(get_signed_txs_from_payload)
        .filter(|e| !e.is_empty())
        .flatten()
        .collect::<HashMap<String, SignedTransaction>>()
}

fn build_block(header: &BlockHeader, _block_payload: &BlockPayload) -> Block {
    Block {
        chain_id: header.chain_id.0 as i64,
        hash: header.hash.clone(),
        height: header.height as i64,
        parent: header.parent.clone(),
        creation_time: DateTime::from_timestamp_micros(header.creation_time)
            .unwrap()
            .naive_utc(),
    }
}

fn get_transactions_from_payload(
    signed_txs: &HashMap<String, SignedTransaction>,
    tx_results: &[PactTransactionResult],
    chain_id: &ChainId,
) -> Vec<Transaction> {
    tx_results
        .iter()
        .map(|pact_result| {
            let signed_tx = signed_txs.get(&pact_result.request_key).unwrap();
            build_transaction(signed_tx, pact_result, chain_id)
        })
        .collect()
}

fn build_transaction(
    signed_tx: &SignedTransaction,
    pact_result: &PactTransactionResult,
    chain: &ChainId,
) -> Transaction {
    let continuation = pact_result.continuation.clone();
    let command = serde_json::from_str::<Command>(&signed_tx.cmd);
    match &command {
        Ok(_) => (),
        Err(e) => {
            log::info!("Error parsing command: {:#?}", signed_tx);
            panic!("{:#?}", e);
        }
    }
    let command = command.unwrap();

    Transaction {
        bad_result: pact_result.result.error.clone(),
        block: pact_result.metadata.block_hash.clone(),
        chain_id: chain.0 as i64,
        creation_time: DateTime::from_timestamp_micros(pact_result.metadata.block_time)
            .unwrap()
            .naive_utc(),
        continuation: pact_result.continuation.clone(),
        gas: pact_result.gas,
        gas_price: command.meta.gas_price,
        gas_limit: command.meta.gas_limit,
        good_result: pact_result.result.data.clone(),
        height: pact_result.metadata.block_height,
        logs: if pact_result.logs.is_empty() {
            None
        } else {
            Some(pact_result.logs.to_string())
        },
        metadata: Some(serde_json::to_value(&pact_result.metadata).unwrap()),
        nonce: command.nonce,
        pact_id: continuation
            .clone()
            .map(|e| e["pactId"].as_str().unwrap().to_string()),
        request_key: pact_result.request_key.to_string(),
        rollback: continuation
            .clone()
            .map(|e| e["stepHasRollback"].as_bool().unwrap()),
        sender: command.meta.sender,
        step: continuation.map(|e| e["step"].as_i64().unwrap()),
        ttl: command.meta.ttl as i64,
        tx_id: pact_result.tx_id,
    }
}

fn get_events_from_txs(
    tx_results: &[PactTransactionResult],
    signed_txs_by_hash: &HashMap<String, SignedTransaction>,
) -> Vec<Event> {
    tx_results
        .iter()
        .flat_map(|pact_result| {
            let signed_tx = signed_txs_by_hash.get(&pact_result.request_key).unwrap();
            build_events(signed_tx, pact_result)
        })
        .collect()
}

fn build_events(
    signed_tx: &SignedTransaction,
    pact_result: &PactTransactionResult,
) -> Vec<crate::models::Event> {
    let command = serde_json::from_str::<Command>(&signed_tx.cmd).unwrap();
    let mut events = vec![];
    if pact_result.events.is_some() {
        for (i, event) in pact_result.events.as_ref().unwrap().iter().enumerate() {
            let module = match &event.module.namespace {
                Some(namespace) => format!("{}.{}", namespace, event.module.name),
                None => event.module.name.to_string(),
            };
            let event = crate::models::Event {
                block: pact_result.metadata.block_hash.clone(),
                chain_id: command.meta.chain_id.parse().unwrap(),
                height: pact_result.metadata.block_height,
                idx: i as i64,
                module: module.to_string(),
                module_hash: event.module_hash.to_string(),
                name: event.name.clone(),
                params: event.params.clone(),
                param_text: event.params.to_string(),
                qual_name: format!("{}.{}", module, event.name),
                request_key: pact_result.request_key.to_string(),
                pact_id: pact_result
                    .continuation
                    .clone()
                    .map(|e| e["pactId"].as_str().unwrap().to_string()),
            };
            events.push(event);
        }
    }
    events
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        chainweb_client::{BlockPayload, Sig},
        db,
    };
    use bigdecimal::BigDecimal;
    use serial_test::serial;

    #[test]
    #[serial]
    fn test_save_block() {
        dotenvy::from_filename(".env.test").ok();
        let pool = db::initialize_db_pool();
        let client = ChainwebClient::new();
        let blocks = BlocksRepository { pool: pool.clone() };
        let events = EventsRepository { pool: pool.clone() };
        let transactions = TransactionsRepository { pool: pool.clone() };
        let transfers = TransfersRepository { pool: pool.clone() };
        let activities = AccountActivitiesRepository { pool: pool.clone() };

        let indexer = Indexer {
            chainweb_client: &client,
            blocks: blocks.clone(),
            events: events.clone(),
            transactions: transactions.clone(),
            transfers: transfers.clone(),
            activities: activities.clone(),
            event_filter: EventFilter::for_wallet(),
        };

        let orphan_header = BlockHeader {
            creation_time: 1688902875826238,
            parent: "mZ3SiegRI9qBY43T3B7VQ82jY40tSgU2E9A7ZGPvXhI".to_string(),
            height: 3882292,
            hash: "_6S6n6dhjGw-vVHwIyq8Ulk8VNSlADLchRJCJg4vclM".to_string(),
            chain_id: ChainId(14),
            payload_hash: "yRHdjMjoqIeqm8K7WW1c4A77jxi8qP__4x_BjgZoFgE".to_string(),
            weight: "2CiW41EoGzYIeAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".to_string(),
            epoch_start: 1688901280684376,
            feature_flags: BigDecimal::from(0),
            adjacents: HashMap::from([(
                ChainId(15),
                "Z_lSTY7KrOVMHPqKhMTUCy3v3YPnljKAg16N3CX5dP8".to_string(),
            )]),
            chainweb_version: "mainnet01".to_string(),
            target: "hvD3dR8UooHyvbpvuIKyu0eALPNztocLHAAAAAAAAAA".to_string(),
            nonce: "11077503293030185962".to_string(),
        };
        let payload = BlockPayload {
            miner_data: "eyJhY2NvdW50IjoiazplN2Y3MTMwZjM1OWZiMWY4Yzg3ODczYmY4NThhMGU5Y2JjM2MxMDU5ZjYyYWU3MTVlYzcyZTc2MGIwNTVlOWYzIiwicHJlZGljYXRlIjoia2V5cy1hbGwiLCJwdWJsaWMta2V5cyI6WyJlN2Y3MTMwZjM1OWZiMWY4Yzg3ODczYmY4NThhMGU5Y2JjM2MxMDU5ZjYyYWU3MTVlYzcyZTc2MGIwNTVlOWYzIl19".to_string(),
            outputs_hash: "WrjWEw4Gj-60kcBPY3HZKTT9Gyoh0ZnAjFrL65Fc3GU".to_string(),
            payload_hash: "yRHdjMjoqIeqm8K7WW1c4A77jxi8qP__4x_BjgZoFgE".to_string(),
            transactions: vec![],
            transactions_hash: "9yNSeh7rTW_j1ziKYyubdYUCefnO5K63d5RfPkHQXiM".to_string()
        };
        let chain_id = orphan_header.chain_id.0 as i64;
        let hash = orphan_header.hash.clone();
        let block = build_block(&orphan_header, &payload);
        indexer.save_block(&block).unwrap();
        let block = indexer
            .blocks
            .find_by_hash(&orphan_header.hash, chain_id)
            .unwrap();
        assert!(block.is_some());
        let header = BlockHeader {
            hash: "new_hash".to_string(),
            ..orphan_header
        };
        let block = build_block(&header, &payload);
        indexer.save_block(&block).unwrap();
        let block = indexer.blocks.find_by_hash("new_hash", chain_id).unwrap();
        assert!(block.is_some());
        let orphan_block = indexer.blocks.find_by_hash(&hash, chain_id).unwrap();
        assert!(orphan_block.is_none());
        transactions.delete_all().unwrap();
        events.delete_all().unwrap();
        blocks.delete_all().unwrap();
    }

    #[test]
    fn test_get_signed_txs_from_payloads() {
        let payload = BlockPayload {
            payload_hash: String::from("OGY90QgfrgHz33lhr3szDK-MtrZTcWKtDuqMjssIyHU"),
            transactions: vec![
                String::from("eyJoYXNoIjoiZ2FEX09aZEwzY0pLR2VsQzczbGFvQkRKaldKVGtzdGtraklBSUtPT3ExVSIsInNpZ3MiOlt7InNpZyI6IjMyOGFhNzZlOWYwNDA1NWU3YTBhNDczMTgwMzA3MjE1MDhmMjNhYzliMTQ2ODlhNmNlMGU2MGI2M2JlNDIyNmNhZmNiM2Q0MjExMzgzNDlhZTZhZGFkNDYxMGYzMDQ2MDA0MWRhNDBkZjIyZDQ2MTU0OTg5MjU2MDM1NWRmMTAyIn1dLCJjbWQiOiJ7XCJuZXR3b3JrSWRcIjpcIm1haW5uZXQwMVwiLFwicGF5bG9hZFwiOntcImV4ZWNcIjp7XCJkYXRhXCI6e1wia2V5c2V0XCI6e1wicHJlZFwiOlwia2V5cy1hbGxcIixcImtleXNcIjpbXCI1NmRmNzdiNTFhNWI2MTAwZGQyNWViN2I5Y2I1NWYzZDE5OTRmMjEzNjljYjU2NWNmOWQ5ZjdjMWQ2MzBkMWVmXCJdfX0sXCJjb2RlXCI6XCIoZnJlZS5yYWRpbzAyLmFkZC1yZWNlaXZlZCBcXFwiMzBhZTdiZmZmZWUzNDdlNlxcXCIgXFxcIlUyRnNkR1ZrWDEvOTZ6Y244TmhaM2loNGRSaHkwVGh2bTcyZG5sN0hLQUk9Ozs7OztxVFVjUkc1NFhXK3ZSdU8rdHRqK2lheE93b2pOU0l3Q1pDWHR1ZkpWRmZQRGJrVnZMYlk4ODVzRDBHWSs3cmxOalp5ZnByR2hXZlR0aEFPUDlicThJby81eXh1ODg4elBGWmRmUUQxbmdWcmswUnpoWjNBYzJIdEpYdEdCSlVLcjIxai9UNWQvL1dCVGdDbXRYSWkrR3ZxSDJOcmhxNlB1VlpteXZsVFlTUDg9XFxcIiApXCJ9fSxcInNpZ25lcnNcIjpbe1wicHViS2V5XCI6XCI1NmRmNzdiNTFhNWI2MTAwZGQyNWViN2I5Y2I1NWYzZDE5OTRmMjEzNjljYjU2NWNmOWQ5ZjdjMWQ2MzBkMWVmXCJ9XSxcIm1ldGFcIjp7XCJjcmVhdGlvblRpbWVcIjoxNjg3NjkxMzY1LFwidHRsXCI6Mjg4MDAsXCJnYXNMaW1pdFwiOjEwMDAsXCJjaGFpbklkXCI6XCIwXCIsXCJnYXNQcmljZVwiOjAuMDAwMDAxLFwic2VuZGVyXCI6XCJrOjU2ZGY3N2I1MWE1YjYxMDBkZDI1ZWI3YjljYjU1ZjNkMTk5NGYyMTM2OWNiNTY1Y2Y5ZDlmN2MxZDYzMGQxZWZcIn0sXCJub25jZVwiOlwiXFxcIjIwMjMtMDYtMjVUMTE6MDk6NDQuNjM1WlxcXCJcIn0ifQ"),
                String::from("eyJoYXNoIjoidGRac1BLMUtqRkV3bjNGbW0zdFRiNkRLNVh1bE4xcF9aTnpxMjRwdnhmdyIsInNpZ3MiOlt7InNpZyI6IjQzZjEyMTI0NjViZGJjNDFiZjAyMTZjMjZiYTMzMjgwNWZhMmFkNjE4YTIwZmU2NWJkNGVmYjU1OTkwMmFmNjliMGM4YmVkNDQwMjg3YzM0M2ZmZTM4ZWU2NmIzYmY2YTFiZDM3NmI1NzgxMDU1YjkyYTcxZmM2MTAzMDQ3NDBhIn1dLCJjbWQiOiJ7XCJuZXR3b3JrSWRcIjpcIm1haW5uZXQwMVwiLFwicGF5bG9hZFwiOntcImV4ZWNcIjp7XCJkYXRhXCI6e1wia2V5c2V0XCI6e1wicHJlZFwiOlwia2V5cy1hbGxcIixcImtleXNcIjpbXCI3YzUxZGQ2NjgxNjVkNWNkOGIwYTdhMTExNDFiYzFlYzk4MWYzZmVkMDA4ZjU1NGM2NzE3NGMwNGI4N2I3YTljXCJdfX0sXCJjb2RlXCI6XCIoZnJlZS5yYWRpbzAyLnVwZGF0ZS1zZW50IFxcXCJVMkZzZEdWa1gxOS9ET0xJaEF5VzBUemVLMGYzSDE0cXprWVY4cTdCUEhzPTs7Ozs7RkVoY0p4VFduT0hiaTFNZURCdVlpT2ZiaEZicXp6VUFzT1pHc21VcHQ2a0lsTUNkR0Y4b3J5MHhGZ0FmQmhuSVNITDBEZ2hzV1ZWNDZhRW1ZK2MzWC96dVNrL2VObld4RUNtUkdXNy9zekM3VlkrMng3RnhPVys5Y095cDBodFZ3NlN0N2t3VEFNTVpGQnVIMGJDUmxsZ2VmcGdSV2xTMlgrRFVEZG1keFFvPVxcXCIgKVwifX0sXCJzaWduZXJzXCI6W3tcInB1YktleVwiOlwiN2M1MWRkNjY4MTY1ZDVjZDhiMGE3YTExMTQxYmMxZWM5ODFmM2ZlZDAwOGY1NTRjNjcxNzRjMDRiODdiN2E5Y1wifV0sXCJtZXRhXCI6e1wiY3JlYXRpb25UaW1lXCI6MTY4NzY5MTM3MyxcInR0bFwiOjI4ODAwLFwiZ2FzTGltaXRcIjo3MDAwLFwiY2hhaW5JZFwiOlwiMFwiLFwiZ2FzUHJpY2VcIjowLjAwMDAwMSxcInNlbmRlclwiOlwiazo3YzUxZGQ2NjgxNjVkNWNkOGIwYTdhMTExNDFiYzFlYzk4MWYzZmVkMDA4ZjU1NGM2NzE3NGMwNGI4N2I3YTljXCJ9LFwibm9uY2VcIjpcIlxcXCIyMDIzLTA2LTI1VDExOjA5OjQ3Ljk0MFpcXFwiXCJ9In0"),
            ],
            transactions_hash: String::from("hKek4su-RzH18nLq9EuZjGa6k7cq-p-o4-pnyd2S85U"),
            outputs_hash: String::from("7aK26TiKVzvnsjXcL0h4iWg3r6_HBmPoqNpO-o5mYcQ"),
            miner_data: String::from("eyJhY2NvdW50IjoiYzUwYjlhY2I0OWNhMjVmNTkxOTNiOTViNGUwOGU1MmUyZWM4OWZhMWJmMzA4ZTY0MzZmMzlhNDBhYzJkYzRmMyIsInByZWRpY2F0ZSI6ImtleXMtYWxsIiwicHVibGljLWtleXMiOlsiYzUwYjlhY2I0OWNhMjVmNTkxOTNiOTViNGUwOGU1MmUyZWM4OWZhMWJmMzA4ZTY0MzZmMzlhNDBhYzJkYzRmMyJdfQ"),
        };
        let signed_txs = HashMap::from([
            (String::from("gaD_OZdL3cJKGelC73laoBDJjWJTkstkkjIAIKOOq1U"), SignedTransaction {
                cmd: String::from("{\"networkId\":\"mainnet01\",\"payload\":{\"exec\":{\"data\":{\"keyset\":{\"pred\":\"keys-all\",\"keys\":[\"56df77b51a5b6100dd25eb7b9cb55f3d1994f21369cb565cf9d9f7c1d630d1ef\"]}},\"code\":\"(free.radio02.add-received \\\"30ae7bfffee347e6\\\" \\\"U2FsdGVkX1/96zcn8NhZ3ih4dRhy0Thvm72dnl7HKAI=;;;;;qTUcRG54XW+vRuO+ttj+iaxOwojNSIwCZCXtufJVFfPDbkVvLbY885sD0GY+7rlNjZyfprGhWfTthAOP9bq8Io/5yxu888zPFZdfQD1ngVrk0RzhZ3Ac2HtJXtGBJUKr21j/T5d//WBTgCmtXIi+GvqH2Nrhq6PuVZmyvlTYSP8=\\\" )\"}},\"signers\":[{\"pubKey\":\"56df77b51a5b6100dd25eb7b9cb55f3d1994f21369cb565cf9d9f7c1d630d1ef\"}],\"meta\":{\"creationTime\":1687691365,\"ttl\":28800,\"gasLimit\":1000,\"chainId\":\"0\",\"gasPrice\":0.000001,\"sender\":\"k:56df77b51a5b6100dd25eb7b9cb55f3d1994f21369cb565cf9d9f7c1d630d1ef\"},\"nonce\":\"\\\"2023-06-25T11:09:44.635Z\\\"\"}"),
                hash: String::from("gaD_OZdL3cJKGelC73laoBDJjWJTkstkkjIAIKOOq1U"),
                sigs: vec![Sig { sig: String::from("328aa76e9f04055e7a0a47318030721508f23ac9b14689a6ce0e60b63be4226cafcb3d421138349ae6adad4610f30460041da40df22d461549892560355df102")}]
            }),
            (String::from("tdZsPK1KjFEwn3Fmm3tTb6DK5XulN1p_ZNzq24pvxfw"), SignedTransaction {
                cmd: String::from("{\"networkId\":\"mainnet01\",\"payload\":{\"exec\":{\"data\":{\"keyset\":{\"pred\":\"keys-all\",\"keys\":[\"7c51dd668165d5cd8b0a7a11141bc1ec981f3fed008f554c67174c04b87b7a9c\"]}},\"code\":\"(free.radio02.update-sent \\\"U2FsdGVkX19/DOLIhAyW0TzeK0f3H14qzkYV8q7BPHs=;;;;;FEhcJxTWnOHbi1MeDBuYiOfbhFbqzzUAsOZGsmUpt6kIlMCdGF8ory0xFgAfBhnISHL0DghsWVV46aEmY+c3X/zuSk/eNnWxECmRGW7/szC7VY+2x7FxOW+9cOyp0htVw6St7kwTAMMZFBuH0bCRllgefpgRWlS2X+DUDdmdxQo=\\\" )\"}},\"signers\":[{\"pubKey\":\"7c51dd668165d5cd8b0a7a11141bc1ec981f3fed008f554c67174c04b87b7a9c\"}],\"meta\":{\"creationTime\":1687691373,\"ttl\":28800,\"gasLimit\":7000,\"chainId\":\"0\",\"gasPrice\":0.000001,\"sender\":\"k:7c51dd668165d5cd8b0a7a11141bc1ec981f3fed008f554c67174c04b87b7a9c\"},\"nonce\":\"\\\"2023-06-25T11:09:47.940Z\\\"\"}"),
                hash: String::from("tdZsPK1KjFEwn3Fmm3tTb6DK5XulN1p_ZNzq24pvxfw"),
                sigs: vec![Sig { sig: String::from("43f1212465bdbc41bf0216c26ba332805fa2ad618a20fe65bd4efb559902af69b0c8bed440287c343ffe38ee66b3bf6a1bd376b5781055b92a71fc610304740a")}]
            }),
        ]);
        assert_eq!(get_signed_txs_from_payloads(&[payload]), signed_txs);
    }
}
