use crate::db::DbError;
use crate::repository::*;
use chrono::NaiveDateTime;
use diesel::prelude::*;
use diesel::sql_types::Timestamptz;
use std::env;

pub struct DataCleanup<'a> {
    pub events: &'a EventsRepository,
    pub transactions: &'a TransactionsRepository,
    pub transfers: &'a TransfersRepository,
    pub activities: &'a AccountActivitiesRepository,
}

impl<'a> DataCleanup<'a> {
    pub fn delete_old_data(&self) -> Result<(), DbError> {
        let threshold_months = env::var("DELETE_OLD_DATA_THRESHOLD_MONTHS")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(6);

        log::info!(
            "Starting data cleanup. Deleting data older than {} months...",
            threshold_months
        );

        // Calculate the cutoff timestamp
        let cutoff_time = chrono::Utc::now()
            .naive_utc()
            - chrono::Duration::days(threshold_months * 30);

        log::info!("Cutoff timestamp: {}", cutoff_time);

        // Delete from events (all records)
        log::info!("Deleting all events...");
        let events_deleted = self.delete_all_events()?;
        log::info!("Deleted {} event records", events_deleted);

        // Delete from account_activities (records older than threshold)
        log::info!(
            "Deleting account_activities older than {}...",
            cutoff_time
        );
        let activities_deleted = self.delete_old_activities(cutoff_time)?;
        log::info!("Deleted {} account_activity records", activities_deleted);

        // Delete from transactions (records older than threshold)
        log::info!("Deleting transactions older than {}...", cutoff_time);
        let transactions_deleted = self.delete_old_transactions(cutoff_time)?;
        log::info!("Deleted {} transaction records", transactions_deleted);

        // Delete from transfers (records older than threshold)
        log::info!("Deleting transfers older than {}...", cutoff_time);
        let transfers_deleted = self.delete_old_transfers(cutoff_time)?;
        log::info!("Deleted {} transfer records", transfers_deleted);

        log::info!("Data cleanup completed successfully!");
        log::info!(
            "Total records deleted: {} (events: {}, activities: {}, transactions: {}, transfers: {})",
            events_deleted + activities_deleted + transactions_deleted + transfers_deleted,
            events_deleted,
            activities_deleted,
            transactions_deleted,
            transfers_deleted
        );

        Ok(())
    }

    fn delete_all_events(&self) -> Result<usize, DbError> {
        use crate::schema::events::dsl::*;
        let mut conn = self.events.pool.get().unwrap();
        let deleted = diesel::delete(events).execute(&mut conn)?;
        Ok(deleted)
    }

    fn delete_old_activities(&self, cutoff_time: NaiveDateTime) -> Result<usize, DbError> {
        use diesel::sql_query;

        let mut conn = self.activities.pool.get().unwrap();

        let deleted = sql_query(
            r#"
            DELETE FROM account_activities
            WHERE block IN (
                SELECT hash
                FROM blocks
                WHERE creation_time < $1
            )
            "#,
        )
        .bind::<Timestamptz, _>(cutoff_time)
        .execute(&mut conn)?;

        Ok(deleted)
    }

    fn delete_old_transactions(&self, cutoff_time: NaiveDateTime) -> Result<usize, DbError> {
        use diesel::sql_query;

        let mut conn = self.transactions.pool.get().unwrap();

        let deleted = sql_query(
            r#"
            DELETE FROM transactions
            WHERE block IN (
                SELECT hash
                FROM blocks
                WHERE creation_time < $1
            )
            "#,
        )
        .bind::<Timestamptz, _>(cutoff_time)
        .execute(&mut conn)?;

        Ok(deleted)
    }

    fn delete_old_transfers(&self, cutoff_time: NaiveDateTime) -> Result<usize, DbError> {
        use diesel::sql_query;

        let mut conn = self.transfers.pool.get().unwrap();

        let deleted = sql_query(
            r#"
            DELETE FROM transfers
            WHERE block IN (
                SELECT hash
                FROM blocks
                WHERE creation_time < $1
            )
            "#,
        )
        .bind::<Timestamptz, _>(cutoff_time)
        .execute(&mut conn)?;

        Ok(deleted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;
    use crate::models::*;
    use chrono::Utc;
    use serial_test::serial;

    fn make_block(chain_id: i64, height: i64, days_ago: i64) -> Block {
        Block {
            chain_id,
            hash: format!("hash-{}-{}", chain_id, height),
            height,
            parent: "parent".to_string(),
            creation_time: (Utc::now() - chrono::Duration::days(days_ago)).naive_utc(),
        }
    }

    fn make_event(block_hash: &str, chain_id: i64, height: i64, idx: i64) -> Event {
        Event {
            block: block_hash.to_string(),
            chain_id,
            height,
            idx,
            module: "test-module".to_string(),
            module_hash: "test-hash".to_string(),
            name: "test-event".to_string(),
            params: serde_json::json!({}),
            param_text: "{}".to_string(),
            qual_name: "test.event".to_string(),
            request_key: format!("rk-{}-{}-{}", block_hash, chain_id, idx),
            pact_id: None,
        }
    }

    #[test]
    #[serial]
    fn test_delete_all_events() {
        dotenvy::from_filename(".env.test").ok();
        let pool = db::initialize_db_pool();
        let blocks_repo = BlocksRepository { pool: pool.clone() };
        let events_repo = EventsRepository { pool: pool.clone() };
        let transactions_repo = TransactionsRepository { pool: pool.clone() };
        let transfers_repo = TransfersRepository { pool: pool.clone() };
        let activities_repo = AccountActivitiesRepository { pool: pool.clone() };

        // Clean up first
        events_repo.delete_all().unwrap();
        blocks_repo.delete_all().unwrap();

        // Insert test data
        let block = make_block(0, 1, 30);
        blocks_repo.insert(&block).unwrap();

        let event = make_event(&block.hash, 0, 1, 0);
        events_repo.insert(&event).unwrap();

        // Create cleanup instance
        let cleanup = DataCleanup {
            events: &events_repo,
            transactions: &transactions_repo,
            transfers: &transfers_repo,
            activities: &activities_repo,
        };

        // Delete all events
        let deleted = cleanup.delete_all_events().unwrap();
        assert_eq!(deleted, 1);

        // Verify deletion
        let remaining = events_repo.find_all().unwrap();
        assert_eq!(remaining.len(), 0);

        // Clean up
        blocks_repo.delete_all().unwrap();
    }

    #[test]
    #[serial]
    fn test_delete_old_data_respects_threshold() {
        dotenvy::from_filename(".env.test").ok();
        std::env::set_var("DELETE_OLD_DATA_THRESHOLD_MONTHS", "6");

        let pool = db::initialize_db_pool();
        let blocks_repo = BlocksRepository { pool: pool.clone() };
        let events_repo = EventsRepository { pool: pool.clone() };
        let transactions_repo = TransactionsRepository { pool: pool.clone() };
        let transfers_repo = TransfersRepository { pool: pool.clone() };
        let activities_repo = AccountActivitiesRepository { pool: pool.clone() };

        // Clean up first
        events_repo.delete_all().unwrap();
        blocks_repo.delete_all().unwrap();

        // Insert old and recent blocks
        let old_block = make_block(0, 1, 200); // 200 days old (> 6 months)
        let recent_block = make_block(0, 2, 30); // 30 days old (< 6 months)

        blocks_repo.insert(&old_block).unwrap();
        blocks_repo.insert(&recent_block).unwrap();

        // Insert events for both blocks
        let old_event = make_event(&old_block.hash, 0, 1, 0);
        let recent_event = make_event(&recent_block.hash, 0, 2, 0);

        events_repo.insert(&old_event).unwrap();
        events_repo.insert(&recent_event).unwrap();

        // Run cleanup
        let cleanup = DataCleanup {
            events: &events_repo,
            transactions: &transactions_repo,
            transfers: &transfers_repo,
            activities: &activities_repo,
        };

        cleanup.delete_old_data().unwrap();

        // Verify all events were deleted (as per requirements)
        let remaining_events = events_repo.find_all().unwrap();
        assert_eq!(remaining_events.len(), 0);

        // Clean up
        blocks_repo.delete_all().unwrap();
    }
}
