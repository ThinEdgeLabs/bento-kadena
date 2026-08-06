DROP INDEX IF EXISTS account_activities_pact_id_idx;

ALTER TABLE account_activities
  DROP COLUMN IF EXISTS idx,
  DROP COLUMN IF EXISTS pact_id;
