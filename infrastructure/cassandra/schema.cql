-- Create Keyspace
CREATE KEYSPACE IF NOT EXISTS graphgpt_conversations
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE graphgpt_conversations;

-- Conversations Table (Main Catalog)
CREATE TABLE IF NOT EXISTS conversations (
    conversation_id uuid,
    user_id uuid,
    title text,
    created_at timestamp,
    updated_at timestamp,
    status text,
    PRIMARY KEY (conversation_id)
);

-- Conversations by User Index Catalog
CREATE TABLE IF NOT EXISTS conversations_by_user (
    user_id uuid,
    updated_at timestamp,
    conversation_id uuid,
    title text,
    created_at timestamp,
    status text,
    PRIMARY KEY (user_id, updated_at, conversation_id)
) WITH CLUSTERING ORDER BY (updated_at DESC, conversation_id ASC);

-- Messages Table
CREATE TABLE IF NOT EXISTS messages_by_conversation (
    conversation_id uuid,
    message_id uuid,
    sender text,
    content text,
    created_at timestamp,
    status text,
    PRIMARY KEY (conversation_id, message_id)
) WITH CLUSTERING ORDER BY (message_id DESC);

-- Transactional Outbox Table
CREATE TABLE IF NOT EXISTS transactional_outbox (
    bucket int,
    event_id timeuuid,
    event_type text,
    payload text,
    published boolean,
    created_at timestamp,
    PRIMARY KEY (bucket, event_id)
) WITH CLUSTERING ORDER BY (event_id ASC);

-- Transactional Inbox Deduplication Table
CREATE TABLE IF NOT EXISTS inbox_events (
    event_id uuid,
    processed_at timestamp,
    PRIMARY KEY (event_id)
) WITH default_time_to_live = 604800;
