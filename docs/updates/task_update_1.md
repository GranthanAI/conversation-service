# Task Update 1 — Clean Architecture Foundation & Core Infrastructure Setup

This document records the completion of Phases 1 to 4 of the GraphGPT Conversation Service development lifecycle, complete with code snippets, configuration details, and architectural design justifications.

---

## 1. Phase 1 — Project Bootstrap

### 1.1 Directory Structure
We initialized a strict Clean Architecture layout under the `app/` folder. This separates request parsing, business use cases, connection pooling, database adapters, and serialization formats to ensure true separation of concerns.

### 1.2 Lifespan & Python 3.12 Compatibility Patch
To prevent the Cassandra Python driver from crashing on Python 3.12 (due to the removal of the standard library `asyncore` module), we dynamically registered an `asyncore` mock namespace in `app/main.py` and mapped the default connection class to `AsyncioConnection` in `app/db/cassandra.py` on startup:

```python
# --- app/main.py Python 3.12 Compatibility Patch ---
import sys
import types
asyncore_mock = types.ModuleType("asyncore")
class DummyDispatcher:
    pass
asyncore_mock.dispatcher = DummyDispatcher
sys.modules['asyncore'] = asyncore_mock
```

We configure connection lifecycles within FastAPI's async context manager:
```python
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup phase
    logger.info("Starting up FastAPI application lifespan context...")
    cassandra_manager.initialize()
    redis_manager.initialize()
    await kafka_manager.initialize()
    await grpc_manager.initialize()
    yield
    # Shutdown phase
    logger.info("Shutting down FastAPI application lifespan context...")
    cassandra_manager.close()
    await redis_manager.close()
    await kafka_manager.close()
    await grpc_manager.close()
```

### 1.3 Configuration Management (`app/core/config.py`)
Application settings load dynamically from the local `.env` variables file using Pydantic Settings:
```python
class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    ENVIRONMENT: str = "development"
    DEBUG: bool = True

    CASSANDRA_CONTACT_POINTS: str = "localhost"
    CASSANDRA_PORT: int = 9042
    CASSANDRA_KEYSPACE: str = "graphgpt_conversations"

    REDIS_URL: str = "redis://localhost:6379/0"
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_CONSUMER_GROUP: str = "conversation-service.chat-events.v1"
```

---

## 2. Phase 2 — Core Infrastructure

### 2.1 Separation of Concerns for Kafka
We split connections pooling from business event publishing to keep database details clean:
*   `app/db/kafka.py`: Houses raw producer/consumer sockets start/stop loops.
*   `app/clients/kafka_producer.py`: Exposes a clean `publish()` method that delegates queries to `kafka_manager`.
*   `app/events/topics.py`: Holds all topics constants.

### 2.2 Production-Grade Producer Config
We tuned the Kafka producer parameters for absolute durability and throughput:
```python
self.producer = AIOKafkaProducer(
    bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
    acks="all",                        # Confirm write only when in-sync replicas write it
    enable_idempotence=True,           # Prevents duplicate records on retry
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    compression_type="gzip",
    linger_ms=5,                       # Buffer events up to 5ms for batching efficiency
    max_batch_size=16384,              # 16KB batch size
    max_request_size=1048576           # 1MB limit
)
```

### 2.3 Metadata-Based Health Probe Check
To avoid relying on brittle checks like partition 0 readiness, the health checker queries broker metadata directly:
```python
async def check_health(self) -> bool:
    # ...
    try:
        await self.producer.client.fetch_all_metadata()
        await self.consumer._client.fetch_all_metadata()
        return True
    except Exception:
        return False
```

---

## 3. Phase 3 — Cassandra Schema

### 3.1 CQL Schema Table Definitions (`app/db/schema.cql`)
All core catalog and transaction outbox tables were declared:
```sql
CREATE KEYSPACE IF NOT EXISTS graphgpt_conversations
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Main lookup
CREATE TABLE conversations (
    conversation_id uuid PRIMARY KEY,
    user_id uuid,
    title text,
    created_at timestamp,
    updated_at timestamp,
    status text
);

-- User-sorted conversation list index
CREATE TABLE conversations_by_user (
    user_id uuid,
    updated_at timestamp,
    conversation_id uuid,
    title text,
    created_at timestamp,
    status text,
    PRIMARY KEY (user_id, updated_at, conversation_id)
) WITH CLUSTERING ORDER BY (updated_at DESC, conversation_id ASC);

-- Messages catalog (sorted latest-first)
CREATE TABLE messages_by_conversation (
    conversation_id uuid,
    message_id uuid,
    sender text,
    content text,
    created_at timestamp,
    status text,
    PRIMARY KEY (conversation_id, message_id)
) WITH CLUSTERING ORDER BY (message_id DESC);
```

### 3.2 Automated CRUD Validation (`scripts/test_crud.py`)
To satisfy the "manual CRUD verified" requirement, we wrote an automated test script that executes prepared statements:
```bash
uv run python scripts/test_crud.py
```
This executes sequential queries to insert, select, update, and delete entries, verifying that all schema definitions are valid and operational.

---

## 4. Phase 4 — Domain Models & Schemas

### 4.1 Chronological UUIDv7 Generator (`app/utils/helpers.py`)
We implemented a native, platform-independent UUIDv7 generator. We resolved a bit-packing byte offset bug to ensure that the generated strings correctly expose version `7` and variant `specified in RFC 4122`:

```python
def uuidv7() -> uuid.UUID:
    timestamp_ms = int(time.time() * 1000)
    rand_bytes = os.urandom(10)
    
    ts_bytes = timestamp_ms.to_bytes(6, byteorder='big')
    
    # Set version byte (index 6): set most significant 4 bits to 7
    v7_byte = (rand_bytes[0] & 0x0F) | 0x70
    
    # Set variant byte (index 8): set most significant 2 bits to 10 (binary)
    var_byte = (rand_bytes[1] & 0x3F) | 0x80
    
    uuid_bytes = (
        ts_bytes +
        bytes([v7_byte]) +
        bytes([rand_bytes[2]]) +  # index 7: random
        bytes([var_byte]) +       # index 8: variant
        rand_bytes[3:]            # index 9-15: random
    )
    return uuid.UUID(bytes=uuid_bytes)
```

### 4.2 Pydantic Validation Constraints
Validation schemas ensure semantically clean payloads:
*   **Conversation Title**: Constrained to `1–255` characters. Automatically strips outer whitespaces and rejects empty inputs.
*   **Message Content**: Constrained to `1–8000` characters, rejecting whitespace-only inputs.
*   **Pagination Response Envelope**: Implemented using Pydantic Generic classes:
    ```python
    T = TypeVar("T")
    class PaginationResponse(BaseModel, Generic[T]):
        items: List[T]
        next_cursor: Optional[str] = None
        limit: int
    ```
