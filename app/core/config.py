"""
Core Configuration File.
Loads application parameters and connection details from environment variables
using Pydantic Settings class representation.
"""

from typing import Optional
from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    """
    Pydantic settings model managing all runtime environment configurations.
    """
    model_config = SettingsConfigDict(hide_input_in_errors=True, env_file=".env", env_file_encoding="utf-8", extra="ignore")

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
    # Replication used when the keyspace is created (`make schema`). Production requires NetworkTopologyStrategy and
    # RF >= 3. RF unset = 3 in production, 1 elsewhere. Existing keyspaces are NOT altered; see docs/cassandra_replication.md.
    CASSANDRA_REPLICATION_STRATEGY: str = "NetworkTopologyStrategy"
    CASSANDRA_REPLICATION_FACTOR: Optional[int] = None
    CASSANDRA_CONSISTENCY_READ: str = "LOCAL_QUORUM"
    CASSANDRA_CONSISTENCY_WRITE: str = "LOCAL_QUORUM"
    CASSANDRA_SERIAL_CONSISTENCY: str = "LOCAL_SERIAL"  # for lightweight transactions (IF ...)

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

    # Artifact registry (Mermaid / Manim) and exports. Rendering itself stays in llm-service (isolated subprocess); this
    # service stores the verified MP4 / export archive in a PRIVATE bucket and serves it only to the owner.
    ARTIFACT_STORE_ENDPOINT: str = "localhost:9010"            # S3-compatible endpoint (the existing file-minio)
    ARTIFACT_STORE_ACCESS_KEY: str = "minioadmin"
    ARTIFACT_STORE_SECRET_KEY: str = "minioadmin123"
    ARTIFACT_STORE_BUCKET: str = "granthan-artifacts"
    ARTIFACT_STORE_SECURE: bool = False
    ARTIFACT_STORE_REGION: str = "us-east-1"
    ARTIFACT_WORKER_ENABLED: bool = True
    ARTIFACT_WORKER_INTERVAL_SECONDS: float = 1.5
    ARTIFACT_LEASE_SECONDS: float = 60.0                       # one advancer per artifact at a time
    ARTIFACT_MAX_AUTO_ATTEMPTS: int = 3                        # renders begun automatically per artifact
    ARTIFACT_MAX_MANUAL_RETRIES: int = 5
    ARTIFACT_MAX_ACTIVE_SECONDS: float = 900.0                 # a render stuck in one state this long is failed
    ARTIFACT_MAX_VIDEO_BYTES: int = 50 * 1024 * 1024
    ARTIFACT_MAX_MANIM_SPEC_CHARS: int = 4000
    ARTIFACT_MAX_MERMAID_CHARS: int = 8000
    ARTIFACT_MAX_PER_REGISTER_BATCH: int = 20
    ARTIFACT_RENDERER_VERSION: str = "manim-templates-1"       # fallback when llm-service cannot be asked
    ARTIFACT_RENDER_QUALITY: str = "l"
    ARTIFACT_LLM_TIMEOUT_SECONDS: float = 20.0

    # Exports
    # Windows dev only: folder holding Pango/GLib DLLs for WeasyPrint (e.g. C:/msys64/mingw64/bin). Empty on Linux/Docker.
    WEASYPRINT_DLL_DIRECTORIES: str = ""
    EXPORT_WORKER_ENABLED: bool = True
    EXPORT_WORKER_INTERVAL_SECONDS: float = 2.0
    EXPORT_LEASE_SECONDS: float = 300.0
    EXPORT_RETENTION_HOURS: float = 24.0
    EXPORT_MAX_CONVERSATIONS: int = 500
    EXPORT_MAX_MESSAGES_PER_CONVERSATION: int = 5000
    EXPORT_MAX_ARCHIVE_BYTES: int = 500 * 1024 * 1024
    EXPORT_INLINE_MAX_MESSAGES: int = 200                      # at or below this a single-conversation export runs inline
    EXPORT_MAX_ATTEMPTS: int = 3
    EXPORT_MAX_LIVE_PER_USER: int = 10

    # Conversation notes (user-authored, PostgreSQL). The database enforces a hard ceiling of 200,000 characters.
    NOTES_MAX_CHARS: int = 100_000

    # Edited copies of code/text blocks in AI answers (PostgreSQL); same 200,000-character database ceiling.
    SOURCE_EDIT_MAX_CHARS: int = 100_000
    SOURCE_EDIT_MAX_PER_CONVERSATION: int = 200

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

    @model_validator(mode="after")
    def _validate_notes_limits(self):
        if not 1 <= self.NOTES_MAX_CHARS <= 200_000:
            raise ValueError("NOTES_MAX_CHARS must be between 1 and 200000 (the database constraint)")
        if not 1 <= self.SOURCE_EDIT_MAX_CHARS <= 200_000:
            raise ValueError("SOURCE_EDIT_MAX_CHARS must be between 1 and 200000 (the database constraint)")
        return self

    @model_validator(mode="after")
    def _validate_cassandra_config(self):
        from app.db.keyspace import validate_cassandra_settings
        problems = validate_cassandra_settings(
            self.ENVIRONMENT, self.CASSANDRA_REPLICATION_STRATEGY, self.CASSANDRA_REPLICATION_FACTOR,
            self.CASSANDRA_KEYSPACE, self.CASSANDRA_LOCAL_DC, self.CASSANDRA_CONSISTENCY_READ,
            self.CASSANDRA_CONSISTENCY_WRITE, self.CASSANDRA_SERIAL_CONSISTENCY,
        )
        if problems:
            raise ValueError("Invalid Cassandra configuration: " + "; ".join(problems))
        return self

    @model_validator(mode="after")
    def _fail_fast_on_unsafe_production_config(self):
        """In production the service refuses to start with a development default or a weak secret (a known default
        would let anyone forge tokens). Development and test environments are unaffected. Secret VALUES are never
        included in the error message."""
        if self.ENVIRONMENT.strip().lower() not in ("production", "prod"):
            return self
        dev_values = {"supersecretjwtkeyforauthservicelocaldvelopment12345", "graphgpt-jwt-secret", "graphgpt-memory-secret", "changeme", "secret"}
        problems: list[str] = []

        def secret(name: str, value: str) -> None:
            if not value or value in dev_values or len(value) < 32:
                problems.append(f"{name} must be set to a unique secret of at least 32 characters")
        secret("JWT_SECRET_KEY", self.JWT_SECRET_KEY)
        if not self.JWT_ISSUER:
            problems.append("JWT_ISSUER must be set so token issuers are verified")
        if self.DEBUG:
            problems.append("DEBUG must be false")
        if problems:
            raise ValueError("Unsafe production configuration: " + "; ".join(problems))
        return self


settings = Settings()
