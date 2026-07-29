# Task Update 10 — Conversation Branching & Lineage Implementation

This document logs implementation details, database queries, and validation tests for the **Neo4j Graph Synchronization** capabilities as specified in `docs/relational_hld.md`.

---

## 1. Database Schema Extension

We added the optional `parent_conversation_id` metadata column to the main conversation catalogs in Cassandra.

### 1.1 Schema Definition (`app/db/schema.cql`)
```sql
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

CREATE TABLE IF NOT EXISTS conversations_by_user (
    user_id uuid,
    updated_at timestamp,
    conversation_id uuid,
    title text,
    created_at timestamp,
    status text,
    parent_conversation_id uuid,
    PRIMARY KEY (user_id, updated_at, conversation_id)
);
```

---

## 2. API Validation Rules

When a request contains `parent_conversation_id`, the system enforces the following constraints before permitting conversation creation:
1. **Existence (404)**: Verifies that the parent conversation ID matches a valid record in the database.
2. **Access Control (403)**: Validates that the parent conversation belongs to the same authenticated user.
3. **Status Check (400)**: Rejects the request if the parent conversation has been deleted or archived (its status is not `active`).

These business rules are orchestrated inside `ConversationService.create` raising clean domain exceptions (`LookupError`, `PermissionError`, `ValueError`), which are captured at the router layer and mapped to standard HTTP status codes.

---

## 3. Self-Contained Domain Events

The `conversation.created` outbox event contract was extended to be self-contained, allowing the downstream Graph Service to process nodes and relationships without querying the REST API:

```json
{
  "event_id": "uuid",
  "event_type": "conversation.created",
  "event_version": 1,
  "conversation_id": "uuid",
  "parent_conversation_id": "uuid | null",
  "conversation_status": "ACTIVE",
  "user_id": "uuid",
  "created_at": "ISO-8601 timestamp",
  "trace_id": "uuid",
  "correlation_id": "uuid"
}
```

---

## 4. Verification

### 4.1 Unit & Integration Tests
All unit and integration tests for caching, Cassandra CRUD, and outbox workers have been verified.

### 4.2 End-to-End API Tests (`tests/api/test_crud_api.py`)
Added E2E test `test_conversation_branching_api` covering:
- Successful child branching (201 status code returning `parent_conversation_id`).
- Access violations across users (403 Forbidden).
- Missing parent identifiers (404 Not Found).
- Deleted parent nodes (400 Bad Request).

```bash
uv run python -m pytest tests/
```
```text
============================= test session starts =============================
platform win32 -- Python 3.12.3, pytest-9.1.1, pluggy-1.6.0
rootdir: C:\Users\hp\Desktop\Granthan\conversation-service
configfile: pyproject.toml
plugins: anyio-4.14.2, asyncio-1.4.0
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 64 items

tests\api\test_auth_api.py ..                                            [  3%]
tests\api\test_crud_api.py ..                                            [  6%]
tests\api\test_pagination_api.py .                                       [  7%]
tests\integration\test_cassandra_integration.py ..                       [ 10%]
tests\integration\test_grpc_integration.py .                             [ 12%]
tests\integration\test_kafka_integration.py .                            [ 14%]
tests\integration\test_redis_integration.py .                            [ 15%]
tests\unit\test_api.py ....                                              [ 21%]
tests\unit\test_cache.py ....                                            [ 28%]
tests\unit\test_consumers.py ...                                         [ 32%]
tests\unit\test_idempotency.py .....                                     [ 40%]
tests\unit\test_kafka.py ...                                             [ 45%]
tests\unit\test_pagination.py ...                                        [ 50%]
tests\unit\test_repositories.py ......                                   [ 59%]
tests\unit\test_security.py .........                                    [ 73%]
tests\unit\test_services.py .....                                        [ 81%]
tests\unit\test_stream.py ......                                         [ 90%]
tests\unit\test_workers.py ......                                        [100%]

======================= 64 passed, 5 warnings in 11.78s =======================
```
