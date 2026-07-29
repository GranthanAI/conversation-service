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

    # Redis settings
    REDIS_URL: str = "redis://localhost:6379/0"
    REDIS_NODES: str = "localhost:6379"
    REDIS_TIMEOUT_SECONDS: float = 2.0

    # Kafka settings
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_CONSUMER_GROUP: str = "conversation-service.chat-events.v1"

    # gRPC settings
    LLM_SERVICE_GRPC_ENDPOINT: str = "localhost:50051"

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

    # gRPC client settings
    GRPC_RETRY_ATTEMPTS: int = 3
    GRPC_RETRY_BACKOFF_BASE: float = 1.0
    GRPC_CHUNK_TIMEOUT_SECONDS: float = 60.0
    GRPC_STREAM_TIMEOUT_SECONDS: float = 3600.0

    # Idempotency settings
    IDEMPOTENCY_TTL_SECONDS: int = 86400

settings = Settings()
