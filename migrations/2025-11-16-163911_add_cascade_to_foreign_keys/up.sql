-- Add CASCADE to existing foreign key constraints
-- This allows automatic deletion of related records when a block is deleted

-- Drop and recreate events foreign key with CASCADE
ALTER TABLE events DROP CONSTRAINT IF EXISTS events_block_fkey;
ALTER TABLE events ADD CONSTRAINT events_block_fkey
    FOREIGN KEY (block) REFERENCES blocks(hash) ON DELETE CASCADE;

-- Drop and recreate transactions foreign key with CASCADE
ALTER TABLE transactions DROP CONSTRAINT IF EXISTS transactions_block_fkey;
ALTER TABLE transactions ADD CONSTRAINT transactions_block_fkey
    FOREIGN KEY (block) REFERENCES blocks(hash) ON DELETE CASCADE;

-- Drop and recreate transfers foreign key with CASCADE
ALTER TABLE transfers DROP CONSTRAINT IF EXISTS transfers_block_fkey;
ALTER TABLE transfers ADD CONSTRAINT transfers_block_fkey
    FOREIGN KEY (block) REFERENCES blocks(hash) ON DELETE CASCADE;

-- Drop and recreate account_activities foreign key with CASCADE
ALTER TABLE account_activities DROP CONSTRAINT IF EXISTS account_activities_block_fkey;
ALTER TABLE account_activities ADD CONSTRAINT account_activities_block_fkey
    FOREIGN KEY (block) REFERENCES blocks(hash) ON DELETE CASCADE;
