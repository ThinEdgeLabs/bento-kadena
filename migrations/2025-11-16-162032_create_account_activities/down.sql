-- This file should undo anything in `up.sql`

DROP INDEX IF EXISTS account_activities_chain_height_idx;
DROP INDEX IF EXISTS account_activities_account_module_time_idx;
DROP INDEX IF EXISTS account_activities_account_type_time_idx;
DROP INDEX IF EXISTS account_activities_account_time_idx;
DROP TABLE IF EXISTS account_activities;
