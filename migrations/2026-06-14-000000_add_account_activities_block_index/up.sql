-- Index the foreign-key column account_activities.block.
--
-- Built non-concurrently here so it runs inside Diesel's migration transaction.
-- On a large live table, build it first with:
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS account_activities_block_idx
--     ON account_activities (block);
-- after which this migration is a no-op (IF NOT EXISTS).
CREATE INDEX IF NOT EXISTS account_activities_block_idx
  ON account_activities (block);
