-- This file should undo anything in `up.sql`
-- Restore the removed columns to transactions table

ALTER TABLE transactions ADD COLUMN logs character varying;
ALTER TABLE transactions ADD COLUMN proof character varying;
ALTER TABLE transactions ADD COLUMN num_events bigint;
ALTER TABLE transactions ADD COLUMN metadata jsonb;
ALTER TABLE transactions ADD COLUMN code character varying;
ALTER TABLE transactions ADD COLUMN data jsonb;
