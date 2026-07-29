# Task Update 8 — Background Workers Implementation

This document logs implementation details, database queries, and unit test validations for **Phase 19 — Background Workers**.

---

## 1. Component Specifications

### 1.1 Cleanup Worker (`app/workers/cleanup_worker.py`)
- **Event Trigger**: Listens on the `conversation.deleted` Kafka topic.
- **Cassandra Hard Purging**:
  - `CassandraConversationRepository.hard_delete`: Deletes from both `conversations` (by `conversation_id`) and `conversations_by_user` (by `user_id`, `updated_at`, `conversation_id` in a single `BEGIN BATCH ... APPLY BATCH` block).
  - `CassandraMessageRepository.delete_all_for_conversation`: Executes partition-level delete: `DELETE FROM messages_by_conversation WHERE conversation_id = ?`.
- **Cache Eviction**: Calls `delete_conversation` and `delete_last_50_messages` on `CacheService` to evict Redis records.

### 1.2 Retry Worker (`app/workers/retry_worker.py`)
- **Objective**: Acts as a safety net reconciliation loop for the Transactional Outbox.
- **Loop**: Polls all 32 Cassandra buckets every 30 seconds.
- **Filtering**: Filters for events with `published = false` older than 30 seconds.
- **Publish & Flag**: Re-publishes these stale events to Kafka and flags them as published in Cassandra.

### 1.3 Summary Worker (`app/workers/summary_worker.py`)
- **Event Trigger**: Consumes `conversation.summary.generated` from Kafka.
- **Deduplication**: Checks `CassandraInboxRepository` to filter duplicate updates.
- **Action**: Persists the summary system log to Cassandra and evicts the conversation history cache in Redis.

---

## 2. Code Modifications & Registering Daemons

All background processes are registered inside `app/main.py`'s application lifespan hooks:

```python
    app.state.cleanup_worker_task = asyncio.create_task(start_cleanup_worker())
    app.state.retry_worker_task = asyncio.create_task(start_retry_worker())
    app.state.summary_worker_task = asyncio.create_task(start_summary_worker())
```

Graceful cancellations are executed on shutdown:

```python
    app.state.cleanup_worker_task.cancel()
    app.state.retry_worker_task.cancel()
    app.state.summary_worker_task.cancel()
    await asyncio.gather(
        app.state.cleanup_worker_task,
        app.state.retry_worker_task,
        app.state.summary_worker_task,
        return_exceptions=True
    )
```

---

## 3. Verification

### 3.1 Unit Tests
Unit tests in `tests/unit/test_workers.py` verify cache evictions, outbox staleness thresholds, and summary inbox deduplications:

```bash
uv run python -m pytest tests/unit/
```
```text
============================= test session starts =============================
platform win32 -- Python 3.12.3, pytest-9.1.1, pluggy-1.6.0
rootdir: C:\Users\hp\Desktop\Granthan\conversation-service
configfile: pyproject.toml
plugins: anyio-4.14.2, asyncio-1.4.0
asyncio: mode=Mode.STRICT, debug=False
collected 51 items

tests\unit\test_api.py ....                                              [  7%]
tests\unit\test_cache.py ....                                            [ 15%]
tests\unit\test_consumers.py ...                                         [ 21%]
tests\unit\test_idempotency.py .....                                     [ 31%]
tests\unit\test_kafka.py ...                                             [ 37%]
tests\unit\test_pagination.py ...                                        [ 43%]
tests\unit\test_repositories.py ......                                   [ 54%]
tests\unit\test_security.py .........                                    [ 72%]
tests\unit\test_services.py .....                                        [ 82%]
tests\unit\test_stream.py ......                                         [ 94%]
tests\unit\test_workers.py ...                                           [100%]

======================== 51 passed, 1 warning in 3.57s ========================
```
