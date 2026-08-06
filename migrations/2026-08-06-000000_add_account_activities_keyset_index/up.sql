-- Supports keyset pagination on (creation_time, id).
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS account_activities_account_keyset_idx
--     ON account_activities (account, creation_time DESC, id DESC);
CREATE INDEX IF NOT EXISTS account_activities_account_keyset_idx
  ON account_activities (account, creation_time DESC, id DESC);
