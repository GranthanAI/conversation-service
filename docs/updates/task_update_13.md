# Task Update 13 — File Attachment Persistence (`file_ids`)

This document logs closing the gap between what the `chat.message.created` Kafka event already claimed to carry and what was actually stored: `file_ids` (File Service `document_id`s attached to a message via "Ask Files" mode) was accepted by the request schema and forwarded into the outbound Kafka payload, but was never persisted in Cassandra, never read back on history, and never round-tripped through the Redis cache.

---

## 1. Background

Per Task Update 11 (§2.2), `CreateMessageRequest.file_ids` and `_build_chat_message_created_payload()`'s `file_ids` field already existed — the Kafka contract with llm-service was already correct. What was missing, confirmed by reading the actual `messages_by_conversation` CQL table and the `Message` dataclass directly (not assumed from the schema doc, which is stale for this service — Cassandra, not a relational store, is the real backing store): **no column, no field, no repository read/write path.** `file_ids` only ever existed transiently inside the JSON blob written to `transactional_outbox.payload` — a message's own attachments were unrecoverable the moment that outbox row was consumed, including on `GET /messages` history and on `regenerate()`, which had no way to look up what files the original prompt referenced.

The other three points of the requested plan — `POST /messages` accepting `file_ids`, the Kafka event carrying them, and no File Service calls anywhere in this service — were already true and needed no changes.

---

## 2. Changes

### 2.1 Cassandra Schema
`app/db/schema.cql`: added `file_ids list<uuid>` to `messages_by_conversation`. Since this repo has no migration tool and `CREATE TABLE IF NOT EXISTS` is a no-op against an already-existing table, a comment documents the manual step for any keyspace created before this change:
```sql
ALTER TABLE messages_by_conversation ADD file_ids list<uuid>;
```
Applied directly against the running `graphgpt-cassandra` container as part of verification (see §3.2) — confirmed via `DESCRIBE TABLE` that the column was genuinely absent beforehand.

### 2.2 Domain Model
`app/models/message.py`: `Message` dataclass gained `file_ids: List[UUID] = field(default_factory=list)`.

### 2.3 Repository
`app/repositories/message_repository.py`:
- `create_with_outbox()` now accepts `file_ids: Optional[List[UUID]] = None`, inserts it into the new column (same `LOGGED BATCH` as the outbox row — no change to the atomicity guarantee), and returns it on the constructed `Message`.
- `history()`'s two `SELECT` statements now fetch `file_ids`; a `null` column (rows written before this change, or assistant/system messages that never had attachments) maps to `[]`, not `None`.
- `create_message_direct()` (assistant placeholders, system summaries) intentionally left unchanged — those message kinds never carry attachments, and the dataclass's own default (`[]`) already covers it without a redundant parameter.

### 2.4 Cache (Redis, Cache-Aside)
`app/services/cache_service.py`: `get_last_50_messages()`/`set_last_50_messages()` now serialize/deserialize `file_ids`. Reads use `data.get("file_ids", [])` so entries already sitting in Redis from before this change (serialized without the key) still deserialize correctly rather than raising `KeyError`.

### 2.5 Service Layer
`app/services/message_service.py`:
- `send()`'s `file_ids` type tightened from `Optional[List[str]]` to `Optional[List[UUID]]` (matching the schema change below) and now passed through to `repo.create_with_outbox()` — previously accepted and forwarded only as far as the Kafka payload builder, never to the repository call.
- `_build_chat_message_created_payload()` now stringifies `file_ids` internally (`[str(fid) for fid in file_ids]`) rather than expecting pre-stringified input — `json.dumps` cannot serialize `UUID` objects directly, and this was a **live bug** that the previous `Optional[List[str]]` typing had been masking rather than preventing (the moment any caller passed real `UUID` objects instead of strings, `json.dumps(payload)` in `send()`/`regenerate()` would have raised `TypeError: Object of type UUID is not JSON serializable`).
- `regenerate()` now threads `prompt_msg.file_ids` into the new response's `chat.message.created` payload — previously always empty, since there was no way to recover the original prompt's attachments (the exact gap this task closes). A regenerated response is now grounded on the same documents as the original.

### 2.6 API Schema
`app/schemas/message.py`:
- `CreateMessageRequest.file_ids` tightened from `Optional[List[str]]` to `Optional[List[UUID]]` — Pydantic now validates/coerces at the request boundary instead of accepting arbitrary strings that would only fail later, deep in the Cassandra driver.
- `MessageResponse` gained `file_ids: List[UUID] = Field(default_factory=list)`, so it round-trips back to the client on every message-returning endpoint.

### 2.7 Router
`app/api/v1/messages.py`: the idempotency-replay path (`X-Idempotency-Key` cache hit) and the cached-response payload it's built from both gained `file_ids` — without this, a retried request with the same idempotency key would have silently dropped attachments from the replayed response.

---

## 3. Verification

### 3.1 Unit Tests
```bash
uv run python -m pytest tests/unit/ -q
```
```text
61 passed, 1 warning in 4.16s
```
4 new tests added to `tests/unit/test_services.py` (57 pre-existing tests unaffected):
- `test_message_service_send_with_file_ids` — `file_ids` reach both `repo.create_with_outbox` and the outbox JSON payload (as strings)
- `test_message_service_send_without_file_ids_defaults_to_empty` — no attachments still produces `file_ids: []` in the payload, not `null`/absent
- `test_message_service_regenerate_forwards_prompt_file_ids` — regeneration's new payload carries the original prompt's `file_ids`
- `test_message_service_history_cache_hit_with_file_ids` — a cache hit with `file_ids` present deserializes correctly (paired with an added assertion on the existing cache-hit test proving a *pre-existing* cached blob without the key still parses)

### 3.2 Real Infrastructure (not test containers)
Rather than only unit-testing against mocks, this was verified against the actual `graphgpt-cassandra` / `graphgpt-redis` containers (started, not created — pre-existing from prior sessions):

1. Confirmed via `cqlsh DESCRIBE TABLE` that the live `messages_by_conversation` table genuinely lacked `file_ids` before this change.
2. Applied the `ALTER TABLE ... ADD file_ids list<uuid>;` migration directly.
3. Ran the existing real API suite (`uv run python -m pytest tests/api/ -q`) — **5 passed**, no regressions.
4. Ran a full manual round trip via a throwaway script (`AsyncClient` against the real ASGI app, real Cassandra/Redis, not testcontainers): created a conversation, posted a message with two `file_ids`, and confirmed all of the following independently agree:
   - The `202` response body echoes both `file_ids`
   - `GET /messages` history (a genuine cache-miss DB read, not a cache hit) returns the same `file_ids`
   - A direct `cqlsh SELECT` against `messages_by_conversation` shows the column populated with the exact two UUIDs
   - A direct `cqlsh SELECT` against `transactional_outbox` shows the real Kafka-bound JSON payload's `"file_ids"` field populated with the same two UUIDs (as strings)
   - The assistant placeholder row created alongside it correctly shows `file_ids: null` (no attachments, as expected — `create_message_direct` was deliberately not changed)

Script deleted after use; no artifacts left in the repo.

---

## 4. What Was NOT Changed

- `POST /messages` accepting `file_ids`, and the Kafka event carrying them — already correct per Task Update 11, untouched here beyond the `List[str]` → `List[UUID]` tightening.
- No File Service HTTP/gRPC calls introduced anywhere — `file_ids` remain opaque UUIDs end-to-end; this service never validates them against File Service, matching the original plan's explicit "no File Service call required" requirement.
- `create_message_direct()` — assistant/system message creation path, unchanged (no attachment concept applies there).
- Transactional Outbox, idempotency (Inbox pattern) mechanics, Redis PubSub → SSE bridge — all unchanged; this was additive persistence for one field, not a redesign.
