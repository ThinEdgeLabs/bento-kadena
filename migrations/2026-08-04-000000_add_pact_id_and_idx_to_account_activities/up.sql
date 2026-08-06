-- Carry `pact_id` and `idx` from events onto account_activities.
--
-- pact_id ties together the two legs of a cross-chain transfer: a burn on the source chain
-- (empty to_account) and a mint on the destination (empty from_account), each with its own
-- request_key
--
-- idx distinguishes multiple events inside a single transaction. Without it
-- (request_key, chain_id) is not unique for a given account, so anything deduplicating by
-- transaction hash silently collapses two different activities into one.

ALTER TABLE account_activities
  ADD COLUMN IF NOT EXISTS pact_id VARCHAR,
  ADD COLUMN IF NOT EXISTS idx BIGINT;

-- Partial index: pact_id is NULL for ordinary same-chain transfers, which are the large
-- majority, so there is no reason to index them.
CREATE INDEX IF NOT EXISTS account_activities_pact_id_idx
  ON account_activities (pact_id)
  WHERE pact_id IS NOT NULL;
