-- Create account_activities table for tracking all types of account activities
-- This table provides a flexible, extensible structure for transaction history

CREATE TABLE account_activities (
    id BIGSERIAL PRIMARY KEY,
    account VARCHAR NOT NULL,
    activity_type VARCHAR NOT NULL,
    module_name VARCHAR NOT NULL,
    chain_id BIGINT NOT NULL,
    height BIGINT NOT NULL,
    block VARCHAR NOT NULL REFERENCES blocks(hash),
    request_key VARCHAR NOT NULL,
    creation_time TIMESTAMP WITH TIME ZONE NOT NULL,
    details JSONB NOT NULL
);

-- Index for querying activities by account, ordered by time (most common query)
CREATE INDEX account_activities_account_time_idx ON account_activities (account, creation_time DESC);

-- Index for filtering by account and activity type
CREATE INDEX account_activities_account_type_time_idx ON account_activities (account, activity_type, creation_time DESC);

-- Index for filtering by account and module, ordered by time
CREATE INDEX account_activities_account_module_time_idx ON account_activities (account, module_name, creation_time DESC);

-- Index for querying by chain and height (useful for syncing)
CREATE INDEX account_activities_chain_height_idx ON account_activities (chain_id, height);
