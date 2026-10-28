-- The keyspace is created by `make schema` from CASSANDRA_REPLICATION_STRATEGY / CASSANDRA_LOCAL_DC /
-- CASSANDRA_REPLICATION_FACTOR (python -m app.db.keyspace); it is no longer hard-coded here.
-- Existing clusters: see docs/cassandra_replication.md (ALTER KEYSPACE + nodetool repair).

USE graphgpt_conversations;

-- Conversations Table (Main Catalog)
CREATE TABLE IF NOT EXISTS conversations (
    conversation_id uuid,
    user_id uuid,
    title text,
    created_at timestamp,
    updated_at timestamp,
    status text,
    parent_conversation_id uuid,
    PRIMARY KEY (conversation_id)
);

-- Branch point (inclusive) in the parent; additive and idempotent (Cassandra >= 4.1).
ALTER TABLE conversations ADD IF NOT EXISTS branch_message_id uuid;

-- Conversations by User Index Catalog
CREATE TABLE IF NOT EXISTS conversations_by_user (
    user_id uuid,
    updated_at timestamp,
    conversation_id uuid,
    title text,
    created_at timestamp,
    status text,
    parent_conversation_id uuid,
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
    file_ids list<uuid>,
    citations text,
    PRIMARY KEY (conversation_id, message_id)
) WITH CLUSTERING ORDER BY (message_id DESC);

-- NOTE: file_ids was added after this table's initial deployment.
-- CREATE TABLE IF NOT EXISTS is a no-op against an already-existing table,
-- so a dev/prod keyspace created before this change needs the column added
-- explicitly (this repo has no formal migration tool, so run manually):
--   ALTER TABLE messages_by_conversation ADD file_ids list<uuid>;

-- NOTE: citations (JSON text, see app/services/citations.py) was added after the table's initial deployment.
-- Additive and nullable: old rows read as "no citations". Run once per keyspace BEFORE deploying the
-- conversation-service version that reads/writes it (the new code SELECTs this column):
--   ALTER TABLE messages_by_conversation ADD citations text;

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

-- User AI / application preferences (one row per user; this service is the
-- authoritative owner). Values are validated by the API; `version` backs
-- optimistic concurrency (conditional updates). Additive: apply with
-- `make schema` (CREATE TABLE IF NOT EXISTS is safe on an existing keyspace).
CREATE TABLE IF NOT EXISTS user_preferences (
    user_id uuid,
    default_mode text,
    default_tools list<text>,
    answer_length text,
    tone text,
    language text,
    personal_instructions text,
    citations text,
    document_context text,
    version int,
    updated_at timestamp,
    PRIMARY KEY (user_id)
);
