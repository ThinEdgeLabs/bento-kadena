-- Add back logs and metadata columns to transactions table
-- These are needed for integration with other services

ALTER TABLE transactions ADD COLUMN logs character varying;
ALTER TABLE transactions ADD COLUMN metadata jsonb;
