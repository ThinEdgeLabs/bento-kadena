-- Add indexes optimized for balance calculation queries

CREATE INDEX CONCURRENTLY IF NOT EXISTS transfers_from_acct_module_chain_idx
  ON transfers (from_account, module_name, chain_id);

CREATE INDEX CONCURRENTLY IF NOT EXISTS transfers_to_acct_module_chain_idx
  ON transfers (to_account, module_name, chain_id);
