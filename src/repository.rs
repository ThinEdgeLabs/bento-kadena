use std::collections::{HashMap, HashSet};
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
                    .or_default()
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

#[derive(Clone)]
pub struct AccountActivitiesRepository {
    pub pool: DbPool,
}

impl AccountActivitiesRepository {
    /// Newest first, paginated by cursor.
    ///
    /// `before` is a `(creation_time, id)` position: rows strictly before it in
    /// `(creation_time DESC, id DESC)` order. `id` is requied because
    /// every tx in a block shares the block's `creation_time`, so a timestamp alone
    /// cannot express which rows were already returned when the limit is lower than
    /// the number of txs with the same timestamp
    pub fn find_by_account(
        &self,
        account_param: &str,
        activity_type_param: Option<&str>,
        module_name_param: Option<&str>,
        start_time: Option<chrono::NaiveDateTime>,
        end_time: Option<chrono::NaiveDateTime>,
        before: Option<(chrono::NaiveDateTime, i64)>,
        limit: Option<i64>,
    ) -> Result<Vec<AccountActivity>, DbError> {
        use crate::schema::account_activities::dsl::*;

        let mut conn = self.pool.get().unwrap();
        let mut query = account_activities
            .filter(account.eq(account_param))
            .into_boxed();

        // Apply activity type filter
        if let Some(type_val) = activity_type_param {
            query = query.filter(activity_type.eq(type_val));
        }

        // Apply module name filter
        if let Some(module_val) = module_name_param {
            query = query.filter(module_name.eq(module_val));
        }

        if let Some(start) = start_time {
            query = query.filter(creation_time.ge(start));
        }
        if let Some(end) = end_time {
            query = query.filter(creation_time.le(end));
        }

        if let Some((before_time, before_id)) = before {
            query = query.filter(
                creation_time
                    .lt(before_time)
                    .or(creation_time.eq(before_time).and(id.lt(before_id))),
            );
        }

        query = query.order((creation_time.desc(), id.desc()));

        let limit_val = limit.unwrap_or(100).min(1000);
        query = query.limit(limit_val);

        let mut results = query.select(AccountActivity::as_select()).load(&mut conn)?;
        // Released before the completion query so serving a page never holds two connections.
        drop(conn);

        self.attach_xchain_completion(&mut results)?;
        Ok(results)
    }
}

#[derive(Default)]
struct XchainSteps {
    /// Distinct request keys seen for this pact. Two means both steps are indexed.
    steps: usize,
    /// Timestamp of the latest leg — the arrival time once both are present.
    latest: Option<chrono::NaiveDateTime>,
}

impl AccountActivitiesRepository {
    /// Steps for each pact id
    fn xchain_steps(&self, pact_ids: &[String]) -> Result<HashMap<String, XchainSteps>, DbError> {
        use crate::schema::account_activities::dsl::*;

        let mut conn = self.pool.get().unwrap();
        let rows: Vec<(Option<String>, String, chrono::NaiveDateTime)> = account_activities
            .filter(pact_id.eq_any(pact_ids))
            .select((pact_id, request_key, creation_time))
            .load(&mut conn)?;

        let mut by_pact: HashMap<String, (HashSet<String>, XchainSteps)> = HashMap::new();

        for (pact, key, moment) in rows {
            let Some(pact) = pact else { continue };
            let (keys, siblings) = by_pact.entry(pact).or_default();
            keys.insert(key);

            siblings.latest = Some(match siblings.latest {
                Some(existing) if existing >= moment => existing,
                _ => moment,
            });
        }

        Ok(by_pact
            .into_iter()
            .map(|(pact, (keys, mut siblings))| {
                siblings.steps = keys.len();
                (pact, siblings)
            })
            .collect())
    }

    /// Fills in `details.xchain.completed` and `completed_at`.
    fn attach_xchain_completion(&self, activities: &mut [AccountActivity]) -> Result<(), DbError> {
        let pact_ids = activities
            .iter()
            .filter(|activity| activity.details.get("xchain").is_some())
            .filter_map(|activity| activity.pact_id.clone())
            .collect::<std::collections::HashSet<String>>();

        if pact_ids.is_empty() {
            return Ok(());
        }

        let siblings_by_pact = self.xchain_steps(&pact_ids.into_iter().collect::<Vec<String>>())?;

        for activity in activities.iter_mut() {
            let siblings = activity
                .pact_id
                .as_ref()
                .and_then(|pact| siblings_by_pact.get(pact));

            let Some(siblings) = siblings else { continue };
            let completed = siblings.steps >= 2;

            let Some(xchain) = activity
                .details
                .get_mut("xchain")
                .and_then(|value| value.as_object_mut())
            else {
                continue;
            };

            xchain.insert("completed".to_string(), serde_json::json!(completed));
            if let (true, Some(landed)) = (completed, siblings.latest) {
                xchain.insert(
                    "completed_at".to_string(),
                    serde_json::json!(landed.and_utc().timestamp_micros()),
                );
            }
        }

        Ok(())
    }

    pub fn insert_batch(&self, activities: &[NewAccountActivity]) -> Result<usize, DbError> {
        use crate::schema::account_activities::dsl::account_activities as activities_table;

        let mut conn = self.pool.get().unwrap();
        let inserted = diesel::insert_into(activities_table)
            .values(activities)
            .on_conflict_do_nothing()
            .execute(&mut conn)?;
        Ok(inserted)
    }

    pub fn delete_all_by_block(&self, block_hash: &str, chain: i64) -> Result<usize, DbError> {
        use crate::schema::account_activities::dsl::{account_activities, block, chain_id};

        let mut conn = self.pool.get().unwrap();
        let deleted = diesel::delete(
            account_activities
                .filter(block.eq(block_hash))
                .filter(chain_id.eq(chain)),
        )
        .execute(&mut conn)?;
        Ok(deleted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;
    use chrono::NaiveDateTime;
    use serde_json::json;
    use serial_test::serial;

    // Mainnet pact EZyiBHNB_OWlYI-3bO4ROlhmKv5QCvWVCzPjLUn_axg: 1 KDA from chain 1 to chain 7.
    const ACCOUNT: &str = "k:3787e2f95b566189b6c0b53777f9aa1696370375e3c6a9e96892811c089e1992";
    const PACT_ID: &str = "EZyiBHNB_OWlYI-3bO4ROlhmKv5QCvWVCzPjLUn_axg";
    const LEG_ONE_BLOCK: &str = "xchain-leg-one-block";
    const LEG_TWO_BLOCK: &str = "xchain-leg-two-block";
    const LEG_TWO_REQUEST_KEY: &str = "s8Hs0Rn1SNQXQ9tZgLhqoLBAAf2WvHjnpQmL0hpP3Rw";

    fn moment(raw: &str) -> NaiveDateTime {
        NaiveDateTime::parse_from_str(raw, "%Y-%m-%dT%H:%M:%S%.f").unwrap()
    }

    fn burn_time() -> NaiveDateTime {
        moment("2026-06-27T14:02:00.123456")
    }

    /// Three minutes later, as cross-chain transfers go.
    fn mint_time() -> NaiveDateTime {
        moment("2026-06-27T14:05:12.654321")
    }

    fn block(chain_id: i64, hash: &str, creation_time: NaiveDateTime) -> Block {
        Block {
            chain_id,
            creation_time,
            hash: hash.to_string(),
            height: 7001001,
            parent: "parent".to_string(),
        }
    }

    fn leg(
        chain_id: i64,
        block_hash: &str,
        request_key: &str,
        creation_time: NaiveDateTime,
        direction: &str,
    ) -> NewAccountActivity {
        NewAccountActivity {
            account: ACCOUNT.to_string(),
            activity_type: "TRANSFER".to_string(),
            module_name: "coin".to_string(),
            chain_id,
            height: 7001001,
            block: block_hash.to_string(),
            request_key: request_key.to_string(),
            creation_time,
            details: json!({
                "direction": direction,
                "from": ACCOUNT,
                "to": ACCOUNT,
                "amount": "1",
                "xchain": { "source_chain": 1, "destination_chain": 7 },
            }),
            pact_id: Some(PACT_ID.to_string()),
            idx: 0,
        }
    }

    fn leg_one() -> NewAccountActivity {
        leg(1, LEG_ONE_BLOCK, PACT_ID, burn_time(), "sent")
    }

    fn leg_two() -> NewAccountActivity {
        leg(
            7,
            LEG_TWO_BLOCK,
            LEG_TWO_REQUEST_KEY,
            mint_time(),
            "received",
        )
    }

    /// A self-transfer's leg 2: written so completion resolves, typed out of the transfer feed.
    fn leg_two_receipt() -> NewAccountActivity {
        NewAccountActivity {
            activity_type: "XCHAIN_RECEIPT".to_string(),
            ..leg_two()
        }
    }

    fn setup() -> (BlocksRepository, AccountActivitiesRepository) {
        dotenvy::from_filename(".env.test").ok();
        let pool = db::initialize_db_pool();
        let blocks = BlocksRepository { pool: pool.clone() };
        let activities = AccountActivitiesRepository { pool };

        // Activities cascade with their block.
        blocks.delete_all().unwrap();
        blocks
            .insert_batch(&[
                block(1, LEG_ONE_BLOCK, burn_time()),
                block(7, LEG_TWO_BLOCK, mint_time()),
            ])
            .unwrap();

        (blocks, activities)
    }

    fn feed(activities: &AccountActivitiesRepository) -> Vec<AccountActivity> {
        activities
            .find_by_account(ACCOUNT, None, None, None, None, None, None)
            .unwrap()
    }

    #[test]
    #[serial]
    fn a_transfer_still_in_flight_is_not_complete() {
        let (blocks, activities) = setup();
        activities.insert_batch(&[leg_one()]).unwrap();

        let items = feed(&activities);

        assert_eq!(items.len(), 1);
        assert_eq!(items[0].details["xchain"]["completed"], json!(false));
        assert!(
            items[0].details["xchain"].get("completed_at").is_none(),
            "nothing has landed yet"
        );

        blocks.delete_all().unwrap();
    }

    #[test]
    #[serial]
    fn both_legs_report_complete_with_the_arrival_time() {
        let (blocks, activities) = setup();
        activities.insert_batch(&[leg_one(), leg_two()]).unwrap();

        let items = feed(&activities);

        assert_eq!(items.len(), 2);
        for item in &items {
            assert_eq!(
                item.details["xchain"]["completed"],
                json!(true),
                "the sender's row flips without being rewritten"
            );
            assert_eq!(
                item.details["xchain"]["completed_at"],
                json!(mint_time().and_utc().timestamp_micros()),
                "completion is when leg 2 landed"
            );
        }

        blocks.delete_all().unwrap();
    }

    #[test]
    #[serial]
    fn losing_leg_two_leaves_leg_one_in_flight_again() {
        let (blocks, activities) = setup();
        activities.insert_batch(&[leg_one(), leg_two()]).unwrap();

        // The orphan case: leg 2's block is replaced and its row goes with it.
        blocks.delete_by_hash(LEG_TWO_BLOCK, 7).unwrap();

        let items = feed(&activities);

        assert_eq!(items.len(), 1, "the burn is final and stays put");
        assert_eq!(items[0].details["xchain"]["completed"], json!(false));

        blocks.delete_all().unwrap();
    }

    #[test]
    #[serial]
    fn a_self_transfer_is_one_completed_entry_in_a_transfer_feed() {
        let (blocks, activities) = setup();
        activities
            .insert_batch(&[leg_one(), leg_two_receipt()])
            .unwrap();

        let items = activities
            .find_by_account(ACCOUNT, Some("TRANSFER"), None, None, None, None, None)
            .unwrap();

        assert_eq!(items.len(), 1, "the receipt is not a second entry");
        assert_eq!(
            items[0].chain_id, 1,
            "leg 1 survives — it is when the user acted"
        );
        assert_eq!(items[0].details["xchain"]["completed"], json!(true));
        assert_eq!(
            items[0].details["xchain"]["completed_at"],
            json!(mint_time().and_utc().timestamp_micros()),
            "the receipt still counts towards completion"
        );

        blocks.delete_all().unwrap();
    }

    #[test]
    #[serial]
    fn a_same_chain_transfer_gets_no_completion_state() {
        let (blocks, activities) = setup();
        let same_chain = NewAccountActivity {
            details: json!({
                "direction": "sent",
                "from": ACCOUNT,
                "to": "k:7be65ff759b17d601183de1ea8cbdb004d21bf2b4f29b005c83ca7911832f39b",
                "amount": "1",
            }),
            pact_id: None,
            ..leg_one()
        };
        activities.insert_batch(&[same_chain]).unwrap();

        let items = feed(&activities);

        assert_eq!(items.len(), 1);
        assert!(items[0].details.get("xchain").is_none());

        blocks.delete_all().unwrap();
    }

    #[test]
    #[serial]
    fn two_rows_from_the_same_leg_do_not_complete_the_transfer() {
        let (blocks, activities) = setup();
        // Same request key, so still one leg however many rows it wrote.
        let second_row = NewAccountActivity {
            idx: 1,
            ..leg_one()
        };
        activities.insert_batch(&[leg_one(), second_row]).unwrap();

        let items = feed(&activities);

        assert_eq!(items.len(), 2);
        for item in &items {
            assert_eq!(item.details["xchain"]["completed"], json!(false));
        }

        blocks.delete_all().unwrap();
    }

    /// Mainnet pact SwTyb_CHj9Tqq3wzseKxYp94cXeXEhSY-JSJBjaqO5I: 1e-12 free.wiza from chain 1
    /// to chain 2. The token doesn't emit events, only the Pact runtime's X_YIELD and
    /// X_RESUME show the transfer happened.
    #[test]
    #[serial]
    fn a_transfer_completes_without_any_event_from_the_token() {
        const WALLET: &str = "k:56923bf2fb287ac506330864497f29dff8a6843f051d35cb3ef43723259d8e11";
        const WIZA_PACT_ID: &str = "SwTyb_CHj9Tqq3wzseKxYp94cXeXEhSY-JSJBjaqO5I";
        const WIZA_LEG_ONE_BLOCK: &str = "NnAAm9ur73zGjXVeRKPo5nb8WdnVOVd5twLG8Ed3sTM";
        const WIZA_LEG_TWO_BLOCK: &str = "wiza-leg-two-block";
        const WIZA_LEG_TWO_REQUEST_KEY: &str = "m3w7apui-7BKOWJywe0LWR70E3JRc5CyzrF2scXd_4M";

        let (blocks, activities) = setup();
        let leg_blocks = [
            Block {
                height: 7308653,
                ..block(1, WIZA_LEG_ONE_BLOCK, burn_time())
            },
            Block {
                height: 7308660,
                ..block(2, WIZA_LEG_TWO_BLOCK, mint_time())
            },
        ];
        blocks.insert_batch(&leg_blocks).unwrap();

        let defpact_event = |chain_id: i64, block: &str, request_key: &str, name: &str| {
            let near_or_far = if name == "X_YIELD" { "2" } else { "1" };
            Event {
                block: block.to_string(),
                chain_id,
                height: 7308653,
                idx: 0,
                module: "pact".to_string(),
                module_hash: "hash".to_string(),
                name: name.to_string(),
                params: json!([
                    near_or_far,
                    "free.wiza.transfer-crosschain",
                    [WALLET, WALLET, { "pred": "keys-all", "keys": ["56923b"] }, "2", 1e-12],
                ]),
                param_text: String::new(),
                qual_name: format!("pact.{name}"),
                request_key: request_key.to_string(),
                pact_id: Some(WIZA_PACT_ID.to_string()),
            }
        };
        let events = [
            defpact_event(1, WIZA_LEG_ONE_BLOCK, WIZA_PACT_ID, "X_YIELD"),
            defpact_event(2, WIZA_LEG_TWO_BLOCK, WIZA_LEG_TWO_REQUEST_KEY, "X_RESUME"),
        ];
        let rows = crate::activities::process_account_activities(
            &events,
            &leg_blocks,
            &crate::activities::ActivityContext::with_wallet_swap_fee_account(
                &[],
                HashMap::new(),
                "k:unused".to_string(),
            ),
        );
        activities.insert_batch(&rows).unwrap();

        let items = activities
            .find_by_account(WALLET, Some("TRANSFER"), None, None, None, None, None)
            .unwrap();

        assert_eq!(items.len(), 1, "one entry for one transfer");
        assert_eq!(items[0].request_key, WIZA_PACT_ID, "the step-0 request key");
        assert_eq!(items[0].module_name, "free.wiza");
        assert_eq!(items[0].details["xchain"]["completed"], json!(true));
        assert_eq!(
            items[0].details["xchain"]["completed_at"],
            json!(mint_time().and_utc().timestamp_micros())
        );

        blocks.delete_all().unwrap();
    }
}
