-- Revert CASCADE from foreign key constraints

-- Revert events foreign key
ALTER TABLE events DROP CONSTRAINT IF EXISTS events_block_fkey;
ALTER TABLE events ADD CONSTRAINT events_block_fkey
    FOREIGN KEY (block) REFERENCES blocks(hash);

-- Revert transactions foreign key
ALTER TABLE transactions DROP CONSTRAINT IF EXISTS transactions_block_fkey;
ALTER TABLE transactions ADD CONSTRAINT transactions_block_fkey
    FOREIGN KEY (block) REFERENCES blocks(hash);

-- Revert transfers foreign key
ALTER TABLE transfers DROP CONSTRAINT IF EXISTS transfers_block_fkey;
ALTER TABLE transfers ADD CONSTRAINT transfers_block_fkey
    FOREIGN KEY (block) REFERENCES blocks(hash);

-- Revert account_activities foreign key
ALTER TABLE account_activities DROP CONSTRAINT IF EXISTS account_activities_block_fkey;
ALTER TABLE account_activities ADD CONSTRAINT account_activities_block_fkey
    FOREIGN KEY (block) REFERENCES blocks(hash);
