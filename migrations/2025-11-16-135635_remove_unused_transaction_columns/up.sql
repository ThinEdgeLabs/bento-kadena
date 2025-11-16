-- Remove unused columns from transactions table
-- These columns are not used by the application and can be safely removed

ALTER TABLE transactions DROP COLUMN IF EXISTS logs;
ALTER TABLE transactions DROP COLUMN IF EXISTS proof;
ALTER TABLE transactions DROP COLUMN IF EXISTS num_events;
ALTER TABLE transactions DROP COLUMN IF EXISTS metadata;
ALTER TABLE transactions DROP COLUMN IF EXISTS code;
ALTER TABLE transactions DROP COLUMN IF EXISTS data;
