use std::collections::HashMap;
use std::vec;

use crate::db::DbError;

use super::db::DbPool;
use super::models::*;
use bigdecimal::BigDecimal;
use diesel::prelude::*;
use diesel::sql_types::BigInt;

#[derive(Clone)]
pub struct BlocksRepository {
    pub pool: DbPool,
}

#[derive(QueryableByName)]
struct GapBoundary {
    #[diesel(sql_type = BigInt)]
    gap_start: i64,
    #[diesel(sql_type = BigInt)]
    gap_end: i64,
}

impl BlocksRepository {
    pub fn find_by_hashes(&self, hashes: &[String]) -> Result<Vec<Block>, diesel::result::Error> {
        use crate::schema::blocks::dsl::{blocks, hash};
        let mut conn = self.pool.get().unwrap();
        let results = blocks
            .filter(hash.eq_any(hashes))
            .select(Block::as_select())
            .load::<Block>(&mut conn)?;
        Ok(results)
    }

    pub fn find_by_hash(
        &self,
        hash: &str,
        chain_id: i64,
    ) -> Result<Option<Block>, diesel::result::Error> {
        use crate::schema::blocks::dsl::{
            blocks as blocks_table, chain_id as chain_id_column, hash as hash_column,
        };
        let mut conn = self.pool.get().unwrap();
        let result = blocks_table
            .filter(hash_column.eq(hash))
            .filter(chain_id_column.eq(chain_id))
            .select(Block::as_select())
            .first::<Block>(&mut conn)
            .optional()?;
        Ok(result)
    }

    pub fn find_gap_ranges(&self, chain_id: i64) -> Result<Vec<(Block, Block)>, DbError> {
        use diesel::sql_query;
        use diesel::sql_types::BigInt;

        let mut conn = self.pool.get().unwrap();

        // First get min/max heights
        let (min_height, max_height): (Option<i64>, Option<i64>) = {
            use crate::schema::blocks::dsl::{
                blocks as blocks_table, chain_id as chain_id_column, height as height_column,
            };
            use diesel::dsl::{max, min};

            blocks_table
                .filter(chain_id_column.eq(chain_id))
                .select((min(height_column), max(height_column)))
                .first(&mut conn)?
        };

        if let (Some(_min), Some(_max)) = (min_height, max_height) {
            // Find gaps using SQL window functions - this identifies gap start/end points
            let gap_boundaries: Vec<GapBoundary> = sql_query(
                r#"
                WITH gaps AS (
                    SELECT
                        height,
                        LAG(height) OVER (ORDER BY height) as prev_height
                    FROM blocks
                    WHERE chain_id = $1
                    ORDER BY height
                ),
                gap_starts AS (
                    SELECT
                        prev_height + 1 as gap_start,
                        height - 1 as gap_end
                    FROM gaps
                    WHERE height - prev_height > 1
                )
                SELECT gap_start, gap_end FROM gap_starts
                "#,
            )
            .bind::<BigInt, _>(chain_id)
            .load(&mut conn)?;

            // Fetch the actual boundary blocks for each gap
            let mut result = Vec::new();
            for gap in gap_boundaries {
                let gap_start = gap.gap_start;
                let gap_end = gap.gap_end;
                // Get the block just before the gap (lower bound)
                if let Some(lower_block) = self.find_by_height(gap_start - 1, chain_id)? {
                    // Get the block just after the gap (upper bound)
                    if let Some(upper_block) = self.find_by_height(gap_end + 1, chain_id)? {
                        result.push((lower_block, upper_block));
                    }
                }
            }

            Ok(result)
        } else {
            Ok(vec![])
        }
    }

    pub fn find_by_height(&self, height: i64, chain_id: i64) -> Result<Option<Block>, DbError> {
        use crate::schema::blocks::dsl::{
            blocks as blocks_table, chain_id as chain_id_column, height as height_column,
        };
        let mut conn = self.pool.get().unwrap();
        let result = blocks_table
            .filter(height_column.eq(height))
            .filter(chain_id_column.eq(chain_id))
            .select(Block::as_select())
            .first::<Block>(&mut conn)
            .optional()?;
        Ok(result)
    }

    pub fn find_by_range(
        &self,
        min_height: i64,
        max_height: i64,
        chain_id: i64,
    ) -> Result<Vec<String>, DbError> {
        use crate::schema::blocks::dsl::{
            blocks as blocks_table, chain_id as chain_id_column, hash as hash_column,
            height as height_column,
        };
        let mut conn = self.pool.get().unwrap();
        let results = blocks_table
            .filter(height_column.ge(min_height))
            .filter(height_column.le(max_height))
            .filter(chain_id_column.eq(chain_id))
            .select(hash_column)
            .order(height_column.desc())
            .load::<String>(&mut conn)?;
        Ok(results)
    }

    pub fn find_min_max_height_blocks(
        &self,
        chain_id: i64,
    ) -> Result<(Option<Block>, Option<Block>), DbError> {
        use crate::schema::blocks::dsl::{
            blocks as blocks_table, chain_id as chain_id_column, height,
        };
        let mut conn = self.pool.get().unwrap();
        let query = blocks_table.filter(chain_id_column.eq(chain_id));
        let min_block = query
            .order_by(height.asc())
            .select(Block::as_select())
            .first::<Block>(&mut conn)
            .optional()?;
        let max_block = query
            .order_by(height.desc())
            .select(Block::as_select())
            .first::<Block>(&mut conn)
            .optional()?;
        Ok((min_block, max_block))
    }

    pub fn count(&self, chain_id: i64) -> Result<i64, DbError> {
        use crate::schema::blocks::dsl::{blocks, chain_id as chain_id_col, height};
        use diesel::dsl::count;
        let mut conn = self.pool.get().unwrap();
        let count = blocks
            .select(count(height))
            .filter(chain_id_col.eq(chain_id))
            .first(&mut conn)?;
        Ok(count)
    }

    pub fn insert(&self, block: &Block) -> Result<Block, DbError> {
        use crate::schema::blocks::dsl::*;
        let mut conn = self.pool.get().unwrap();
        let new_block = diesel::insert_into(blocks)
            .values(block)
            .returning(Block::as_returning())
            .get_result(&mut conn)?;
        Ok(new_block)
    }

    pub fn insert_batch(&self, blocks: &[Block]) -> Result<Vec<Block>, DbError> {
        use crate::schema::blocks::dsl::blocks as blocks_table;
        let mut conn = self.pool.get().unwrap();
        let inserted = diesel::insert_into(blocks_table)
            .values(blocks)
            .on_conflict_do_nothing()
            .returning(Block::as_returning())
            .get_results(&mut conn)?;
        Ok(inserted)
    }

    pub fn delete_all(&self) -> Result<usize, diesel::result::Error> {
        use crate::schema::blocks::dsl::*;
        let mut conn = self.pool.get().unwrap();
        let deleted = diesel::delete(blocks).execute(&mut conn)?;
        Ok(deleted)
    }

    #[allow(dead_code)]
    pub fn delete_one(&self, height: i64, chain_id: i64) -> Result<usize, DbError> {
        use crate::schema::blocks::dsl::{
            blocks as blocks_table, chain_id as chain_id_col, height as height_col,
        };
        let mut conn = self.pool.get().unwrap();
        let deleted = diesel::delete(
            blocks_table
                .filter(height_col.eq(height))
                .filter(chain_id_col.eq(chain_id)),
        )
        .execute(&mut conn)?;
        Ok(deleted)
    }

    pub fn delete_by_hash(&self, hash: &str, chain_id: i64) -> Result<usize, DbError> {
        use crate::schema::blocks::dsl::{
            blocks as blocks_table, chain_id as chain_id_col, hash as hash_col,
        };
        let mut conn = self.pool.get().unwrap();
        let deleted = diesel::delete(
            blocks_table
                .filter(hash_col.eq(hash))
                .filter(chain_id_col.eq(chain_id)),
        )
        .execute(&mut conn)?;
        Ok(deleted)
    }
}

#[derive(Clone)]
pub struct EventsRepository {
    pub pool: DbPool,
}

impl EventsRepository {
    #[allow(dead_code)]
    pub fn find_all(&self) -> Result<Vec<Event>, DbError> {
        use crate::schema::events::dsl::*;
        let mut conn = self.pool.get().unwrap();
        let results = events.select(Event::as_select()).load::<Event>(&mut conn)?;
        Ok(results)
    }

    pub fn find_max_height(&self, chain_id: i64) -> Result<i64, DbError> {
        use crate::schema::events::dsl::{chain_id as chain_id_col, events, height as height_col};
        let mut conn = self.pool.get().unwrap();
        let max_height = events
            .filter(chain_id_col.eq(chain_id))
            .select(diesel::dsl::max(height_col))
            .first::<Option<i64>>(&mut conn)?;
        Ok(max_height.unwrap_or(0))
    }

    pub fn find_by_range(
        &self,
        min_height: i64,
        max_height: i64,
        chain_id: i64,
    ) -> Result<Vec<Event>, DbError> {
        use crate::schema::events::dsl::{chain_id as chain_id_col, events, height as height_col};
        let mut conn = self.pool.get().unwrap();
        let results = events
            .filter(chain_id_col.eq(chain_id))
            .filter(height_col.ge(min_height))
            .filter(height_col.le(max_height))
            .select(Event::as_select())
            .order(height_col.asc())
            .load::<Event>(&mut conn)?;
        Ok(results)
    }

    #[allow(dead_code)]
    pub fn insert(&self, event: &Event) -> Result<Event, DbError> {
        use crate::schema::events::dsl::*;
        let mut conn = self.pool.get().unwrap();
        let new_event = diesel::insert_into(events)
            .values(event)
            .on_conflict((block, idx, request_key))
            .do_update()
            .set(event)
            .returning(Event::as_returning())
            .get_result(&mut conn)?;
        Ok(new_event)
    }

    pub fn insert_batch(&self, events: &[Event]) -> Result<usize, diesel::result::Error> {
        use crate::schema::events::dsl::events as events_table;
        let mut inserted = 0;
        let mut conn = self.pool.get().unwrap();
        for chunk in events.chunks(1000) {
            inserted += diesel::insert_into(events_table)
                .values(chunk)
                .on_conflict_do_nothing()
                .execute(&mut conn)?;
        }
        Ok(inserted)
    }

    #[allow(dead_code)]
    pub fn delete_all(&self) -> Result<usize, DbError> {
        use crate::schema::events::dsl::*;
        let mut conn = self.pool.get().unwrap();
        let deleted = diesel::delete(events).execute(&mut conn)?;
        Ok(deleted)
    }

    #[allow(dead_code)]
    pub fn delete_one(&self, block: &str, idx: i64, request_key: &str) -> Result<usize, DbError> {
        use crate::schema::events::dsl::{
            block as block_col, events, idx as idx_col, request_key as request_key_col,
        };
        let mut conn = self.pool.get().unwrap();
        let deleted = diesel::delete(
            events
                .filter(block_col.eq(block))
                .filter(idx_col.eq(idx))
                .filter(request_key_col.eq(request_key)),
        )
        .execute(&mut conn)?;
        Ok(deleted)
    }

    pub fn delete_all_by_block(&self, hash: &str) -> Result<usize, DbError> {
        use crate::schema::events::dsl::{block as block_col, events};
        let mut conn = self.pool.get().unwrap();
        let deleted = diesel::delete(events.filter(block_col.eq(hash))).execute(&mut conn)?;
        Ok(deleted)
    }
}

#[derive(Clone)]
pub struct TransactionsRepository {
    pub pool: DbPool,
}

impl TransactionsRepository {
    #[allow(dead_code)]
    pub fn find_all(&self) -> Result<Vec<Transaction>, DbError> {
        use crate::schema::transactions::dsl::*;
        let mut conn = self.pool.get().unwrap();
        let results = transactions
            .select(Transaction::as_select())
            .load::<Transaction>(&mut conn)?;
        Ok(results)
    }

    #[allow(dead_code)]
    pub fn find_by_request_key(
        &self,
        request_keys: &Vec<String>,
    ) -> Result<Vec<Transaction>, DbError> {
        use crate::schema::transactions::dsl::{
            request_key as request_key_column, transactions as transactions_table,
        };
        let mut conn = self.pool.get().unwrap();
        let result = transactions_table
            .filter(request_key_column.eq_any(request_keys))
            .select(Transaction::as_select())
            .load(&mut conn)?;
        Ok(result)
    }

    #[allow(dead_code)]
    pub fn find_all_related(
        &self,
        request_keys: &Vec<String>,
    ) -> Result<HashMap<String, Vec<Transaction>>, DbError> {
        match self.find_by_request_key(request_keys) {
            Ok(transactions) => {
                //TODO: Optimize this to avoid multiple queries
                let mut result = HashMap::new();
                for tx in transactions.iter() {
                    if tx.pact_id.is_some() {
                        match self.find_by_pact_id(&vec![tx.pact_id.clone().unwrap()]) {
                            Ok(multi_step_txs) => {
                                result.insert(tx.request_key.clone(), multi_step_txs);
                            }
                            Err(err) => return Err(err),
                        }
                    } else {
                        result.insert(tx.request_key.clone(), vec![tx.clone()]);
                    }
                }
                Ok(result)
            }
            Err(err) => Err(err),
        }
    }

    #[allow(dead_code)]
    pub fn find_by_pact_id(&self, pact_ids: &Vec<String>) -> Result<Vec<Transaction>, DbError> {
        use crate::schema::transactions::dsl::{
            pact_id as pact_id_column, transactions as transactions_table,
        };
        let mut conn = self.pool.get().unwrap();
        let result = transactions_table
            .filter(pact_id_column.eq_any(pact_ids))
            .select(Transaction::as_select())
            .load(&mut conn)?;
        Ok(result)
    }

    #[allow(dead_code)]
    pub fn insert(&self, transaction: &Transaction) -> Result<Transaction, DbError> {
        use crate::schema::transactions::dsl::*;
        let mut conn = self.pool.get().unwrap();
        let transaction = diesel::insert_into(transactions)
            .values(transaction)
            .returning(Transaction::as_returning())
            .get_result(&mut conn)?;
        Ok(transaction)
    }

    pub fn insert_batch(&self, transactions: &[Transaction]) -> Result<usize, DbError> {
        use crate::schema::transactions::dsl::transactions as transactions_table;
        let mut conn = self.pool.get().unwrap();
        let mut inserted = 0;
        for chunk in transactions.chunks(1000) {
            inserted += diesel::insert_into(transactions_table)
                .values(chunk)
                .on_conflict_do_nothing()
                .execute(&mut conn)?;
        }
        Ok(inserted)
    }

    #[allow(dead_code)]
    pub fn delete_all(&self) -> Result<usize, DbError> {
        use crate::schema::transactions::dsl::*;
        let mut conn = self.pool.get().unwrap();
        let deleted = diesel::delete(transactions).execute(&mut conn)?;
        Ok(deleted)
    }

    pub fn delete_all_by_block(&self, hash: &str) -> Result<usize, DbError> {
        use crate::schema::transactions::dsl::{block as block_col, transactions};
        let mut conn = self.pool.get().unwrap();
        let deleted = diesel::delete(transactions.filter(block_col.eq(hash))).execute(&mut conn)?;
        Ok(deleted)
    }

    #[allow(dead_code)]
    pub fn delete_one(&self, block: &str, request_key: &str) -> Result<usize, DbError> {
        use crate::schema::transactions::dsl::{
            block as block_column, request_key as request_key_column, transactions,
        };
        let mut conn = self.pool.get().unwrap();
        let deleted = diesel::delete(
            transactions
                .filter(block_column.eq(block))
                .filter(request_key_column.eq(request_key)),
        )
        .execute(&mut conn)?;
        Ok(deleted)
    }
}

#[derive(Clone)]
pub struct TransfersRepository {
    pub pool: DbPool,
}

impl TransfersRepository {
    pub fn get_balance(
        &self,
        account: &str,
        module_filter: Option<&str>,
    ) -> Result<HashMap<String, HashMap<i64, BigDecimal>>, DbError> {
        use diesel::sql_types::{BigInt, Nullable, Numeric, Text};

        let mut conn = self.pool.get().unwrap();

        #[derive(QueryableByName)]
        struct BalanceRow {
            #[diesel(sql_type = Text)]
            module_name: String,
            #[diesel(sql_type = BigInt)]
            chain_id: i64,
            #[diesel(sql_type = Nullable<Numeric>)]
            balance: Option<BigDecimal>,
        }

        // Use parameterized queries to prevent SQL injection
        let results = if let Some(module) = module_filter {
            diesel::sql_query(
                "SELECT
                    module_name,
                    chain_id,
                    COALESCE(SUM(CASE WHEN to_account = $1 THEN amount ELSE 0 END), 0) -
                    COALESCE(SUM(CASE WHEN from_account = $1 THEN amount ELSE 0 END), 0) as balance
                FROM transfers
                WHERE (from_account = $1 OR to_account = $1) AND module_name = $2
                GROUP BY module_name, chain_id
                HAVING COALESCE(SUM(CASE WHEN to_account = $1 THEN amount ELSE 0 END), 0) -
                       COALESCE(SUM(CASE WHEN from_account = $1 THEN amount ELSE 0 END), 0) != 0",
            )
            .bind::<Text, _>(account)
            .bind::<Text, _>(module)
            .load::<BalanceRow>(&mut conn)?
        } else {
            diesel::sql_query(
                "SELECT
                    module_name,
                    chain_id,
                    COALESCE(SUM(CASE WHEN to_account = $1 THEN amount ELSE 0 END), 0) -
                    COALESCE(SUM(CASE WHEN from_account = $1 THEN amount ELSE 0 END), 0) as balance
                FROM transfers
                WHERE (from_account = $1 OR to_account = $1)
                GROUP BY module_name, chain_id
                HAVING COALESCE(SUM(CASE WHEN to_account = $1 THEN amount ELSE 0 END), 0) -
                       COALESCE(SUM(CASE WHEN from_account = $1 THEN amount ELSE 0 END), 0) != 0",
            )
            .bind::<Text, _>(account)
            .load::<BalanceRow>(&mut conn)?
        };

        let mut balances_by_module: HashMap<String, HashMap<i64, BigDecimal>> = HashMap::new();
        for row in results {
            if let Some(balance) = row.balance {
                balances_by_module
                    .entry(row.module_name)
                    .or_insert_with(HashMap::new)
                    .insert(row.chain_id, balance);
            }
        }

        Ok(balances_by_module)
    }

    pub fn find(
        &self,
        from: Option<String>,
        to: Option<String>,
        min_height: Option<i64>,
    ) -> Result<Vec<Transfer>, DbError> {
        use crate::schema::transfers::dsl::{
            from_account as from_account_col, height as height_col, to_account as to_account_col,
            transfers,
        };
        let mut conn = self.pool.get().unwrap();
        let mut query = transfers.into_boxed();
        if let Some(from) = from {
            query = query.filter(from_account_col.eq(from));
        }
        if let Some(to) = to {
            query = query.filter(to_account_col.eq(to));
        }
        if let Some(min_height) = min_height {
            query = query.filter(height_col.ge(min_height));
        }
        let results: Vec<Transfer> = query
            .select(Transfer::as_select())
            .load::<Transfer>(&mut conn)?;
        Ok(results)
    }

    pub fn find_received(
        &self,
        to_account: &str,
        min_height: Option<i64>,
    ) -> Result<HashMap<String, Vec<Transfer>>, DbError> {
        use crate::schema::blocks::dsl::blocks;
        use crate::schema::transfers::dsl::{
            height as height_col, to_account as to_account_col, transfers,
        };
        use itertools::Itertools;
        let mut conn = self.pool.get().unwrap();
        let min_height = min_height.unwrap_or(0);
        let received_transfers: Vec<(Transfer, Block)> = transfers
            .inner_join(blocks)
            .filter(to_account_col.eq(to_account))
            .filter(height_col.ge(min_height))
            .select((Transfer::as_select(), Block::as_select()))
            .load::<(Transfer, Block)>(&mut conn)?;
        let multi_step_transfers_pact_ids = received_transfers
            .iter()
            .filter_map(|t| t.0.pact_id.clone())
            .collect::<Vec<String>>();
        let multi_step_transfers = self.find_by_pact_id(multi_step_transfers_pact_ids)?;
        let mut simple_transfers = received_transfers
            .iter()
            .filter(|e| e.0.pact_id.is_none())
            //FIXME: We're using the request_key as key, but it's not unique as there can be
            //multiple transfers with the same request_key
            .map(|e| (e.0.request_key.clone(), vec![e.0.clone()]))
            .collect::<HashMap<String, Vec<Transfer>>>();
        let multi_step_transfers = multi_step_transfers
            .iter()
            .filter(|t| t.from_account == to_account || t.to_account == to_account)
            .chunk_by(|t| t.pact_id.clone().unwrap());
        for (request_key, transfers_list) in &multi_step_transfers {
            simple_transfers.insert(request_key, transfers_list.cloned().collect_vec());
        }
        Ok(simple_transfers)
    }

    pub fn find_by_pact_id(&self, ids: Vec<String>) -> Result<Vec<Transfer>, DbError> {
        use crate::schema::transfers::dsl::{pact_id as pact_id_col, transfers};
        let mut conn = self.pool.get().unwrap();
        let results = transfers
            .filter(pact_id_col.eq_any(ids))
            .select(Transfer::as_select())
            .load(&mut conn)?;
        Ok(results)
    }

    pub fn insert(&self, transfer: &Transfer) -> Result<Transfer, DbError> {
        use crate::schema::transfers::dsl::*;
        let mut conn = self.pool.get().unwrap();
        let new_transfer = diesel::insert_into(transfers)
            .values(transfer)
            .on_conflict_do_nothing()
            .returning(Transfer::as_returning())
            .get_result(&mut conn)?;
        Ok(new_transfer)
    }

    pub fn insert_batch(&self, transfers: &Vec<Transfer>) -> Result<Vec<Transfer>, DbError> {
        use crate::schema::transfers::dsl::transfers as transfers_table;
        let mut conn = self.pool.get().unwrap();
        let inserted = diesel::insert_into(transfers_table)
            .values(transfers)
            .on_conflict_do_nothing()
            .returning(Transfer::as_returning())
            .get_results(&mut conn)?;
        Ok(inserted)
    }

    pub fn delete_all(&self) -> Result<usize, DbError> {
        use crate::schema::transfers::dsl::*;
        let mut conn = self.pool.get().unwrap();
        let deleted = diesel::delete(transfers).execute(&mut conn)?;
        Ok(deleted)
    }

    pub fn delete_all_by_block(&self, block: &str, chain_id: i64) -> Result<usize, DbError> {
        use crate::schema::transfers::dsl::{
            block as block_col, chain_id as chain_id_col, transfers,
        };
        let mut conn = self.pool.get().unwrap();
        let deleted = diesel::delete(
            transfers
                .filter(block_col.eq(block))
                .filter(chain_id_col.eq(chain_id)),
        )
        .execute(&mut conn)?;
        Ok(deleted)
    }
}
