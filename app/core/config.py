"""
Core Configuration File.
Loads application parameters and connection details from environment variables
using Pydantic Settings class representation.
"""

from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    """
    Pydantic settings model managing all runtime environment configurations.
    """
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    ENVIRONMENT: str = "development"
    DEBUG: bool = True

    # Cassandra settings
    CASSANDRA_CONTACT_POINTS: str = "localhost"
    CASSANDRA_PORT: int = 9042
    CASSANDRA_KEYSPACE: str = "graphgpt_conversations"
    CASSANDRA_USERNAME: Optional[str] = None
    CASSANDRA_PASSWORD: Optional[str] = None
    CASSANDRA_LOCAL_DC: str = "datacenter1"
    CASSANDRA_TIMEOUT_SECONDS: float = 5.0

    # PostgreSQL (authoritative store for user preferences and custom modes; Cassandra is not used for them).
    # Runtime role: DML only. Schema changes are applied separately with Alembic (`make db-migrate`), never at startup.
    DATABASE_URL: str = "postgresql+asyncpg://postgres:1234@localhost:5433/conversation_service_db"
    DB_POOL_SIZE: int = 5
    DB_MAX_OVERFLOW: int = 5
    DB_POOL_TIMEOUT_SECONDS: float = 5.0         # waiting for a free connection
    DB_CONNECT_TIMEOUT_SECONDS: float = 5.0
    DB_STATEMENT_TIMEOUT_MS: int = 3000          # server-side cap per statement
    DB_ECHO: bool = False

    # llm-service (owner of the live mode/tool registry): used to validate built-in mode ids and tool ids that are
    # saved as defaults. If it cannot be reached the save is accepted (the pipeline still validates at send time).
    LLM_SERVICE_URL: str = "http://localhost:8003"
    LLM_CATALOG_TTL_SECONDS: float = 60.0
    LLM_CATALOG_TIMEOUT_SECONDS: float = 1.5

    # Custom modes (user-owned, reusable instruction sets)
    CUSTOM_MODE_MAX_PER_USER: int = 20
    CUSTOM_MODE_NAME_MAX_CHARS: int = 60
    CUSTOM_MODE_DESCRIPTION_MAX_CHARS: int = 200
    CUSTOM_MODE_INSTRUCTIONS_MAX_CHARS: int = 4000

    # PostgreSQL (authoritative store for user preferences and custom modes; Cassandra is not used for them).
    # Runtime role: DML only. Schema changes are applied separately with Alembic (`make db-migrate`), never at startup.
    DATABASE_URL: str = "postgresql+asyncpg://postgres:1234@localhost:5433/conversation_service_db"
    DB_POOL_SIZE: int = 5
    DB_MAX_OVERFLOW: int = 5
    DB_POOL_TIMEOUT_SECONDS: float = 5.0         # waiting for a free connection
    DB_CONNECT_TIMEOUT_SECONDS: float = 5.0
    DB_STATEMENT_TIMEOUT_MS: int = 3000          # server-side cap per statement
    DB_ECHO: bool = False

    # Custom modes (user-owned, reusable instruction sets)
    CUSTOM_MODE_MAX_PER_USER: int = 20
    CUSTOM_MODE_NAME_MAX_CHARS: int = 60
    CUSTOM_MODE_DESCRIPTION_MAX_CHARS: int = 200
    CUSTOM_MODE_INSTRUCTIONS_MAX_CHARS: int = 4000

    # Redis settings
    REDIS_URL: str = "redis://localhost:6379/0"
    REDIS_NODES: str = "localhost:6379"
    REDIS_TIMEOUT_SECONDS: float = 2.0

    # Kafka settings
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_CONSUMER_GROUP: str = "conversation-service.chat-events.v1"

    # JWT Configs
    JWT_SECRET_KEY: str = "supersecretjwtkeyforauthservicelocaldvelopment12345"
    JWT_ALGORITHM: str = "HS256"
    JWT_ISSUER: Optional[str] = None
    JWT_AUDIENCE: Optional[str] = None

    # Outbox settings
    OUTBOX_POLL_INTERVAL_SECONDS: float = 0.25
    OUTBOX_RETRY_INTERVAL_SECONDS: float = 30.0
    OUTBOX_STALE_THRESHOLD_SECONDS: float = 30.0
    OUTBOX_BUCKETS: int = 32

    # Idempotency settings
    IDEMPOTENCY_TTL_SECONDS: int = 86400

    # Redis pool settings
    REDIS_MAX_CONNECTIONS: int = 100

    # Kafka producer performance settings
    KAFKA_LINGER_MS: int = 5
    KAFKA_MAX_BATCH_SIZE: int = 16384
    KAFKA_MAX_REQUEST_SIZE: int = 1048576

    # Stream & PubSub loop configs
    STREAM_OWNERSHIP_TTL_SECONDS: int = 60
    STREAM_HEARTBEAT_INTERVAL_SECONDS: float = 20.0
    STREAM_PUBSUB_TIMEOUT_SECONDS: float = 1.0
    STREAM_ERROR_SLEEP_SECONDS: float = 0.5
    # Replayable event log (Redis Stream per conversation, same Redis). Pub/Sub alone is
    # ephemeral; the log lets a client that connected late or reconnected recover the events
    # of an in-flight turn. Retention is deliberately short: persisted history is the
    # durable source of truth, this only bridges a turn's lifetime.
    STREAM_EVENT_LOG_MAXLEN: int = 2000
    STREAM_EVENT_LOG_TTL_SECONDS: int = 900
    # Upper bound for a resumable (message_id-scoped) stream connection.
    STREAM_MAX_CONNECTION_SECONDS: float = 600.0

    # Cache TTL configuration
    CACHE_TTL_SECONDS: int = 3600

    # Worker configuration
    SUMMARY_WORKER_ERROR_SLEEP_SECONDS: float = 1.0

    # Route limits & defaults
    CONVERSATION_LIST_DEFAULT_LIMIT: int = 20
    CONVERSATION_LIST_MIN_LIMIT: int = 1
    CONVERSATION_LIST_MAX_LIMIT: int = 100
    MESSAGE_HISTORY_DEFAULT_LIMIT: int = 50
    MESSAGE_HISTORY_MIN_LIMIT: int = 1
    MESSAGE_HISTORY_MAX_LIMIT: int = 100
    CACHE_HISTORY_LIMIT: int = 50

    # Distributed Outbox lock settings
    OUTBOX_LOCK_TTL_SECONDS: float = 10.0
    OUTBOX_LOCK_RENEW_INTERVAL_SECONDS: float = 3.0
    OUTBOX_WORKER_CONCURRENCY: int = 8

    # Pending Response Timeout settings (llm-service Kafka round-trip watchdog)
    PENDING_RESPONSE_TIMEOUT_SECONDS: int = 120
    # The Redis marker must outlive the deadline, otherwise the timeout worker
    # finds the key already expired and mistakes it for "response arrived".
    PENDING_RESPONSE_MARKER_GRACE_SECONDS: int = 300
    PENDING_TIMEOUT_WORKER_POLL_INTERVAL_SECONDS: float = 5.0

settings = Settings()
