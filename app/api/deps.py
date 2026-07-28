"""
API Dependency Factory Methods.
Provides repository, service, and cache resource injections for FastAPI controllers.
"""

from fastapi import Depends

from app.repositories.conversation_repository import CassandraConversationRepository
from app.repositories.message_repository import CassandraMessageRepository
from app.repositories.outbox_repository import CassandraOutboxRepository
from app.repositories.inbox_repository import CassandraInboxRepository

from app.services.cache_service import CacheService
from app.services.conversation_service import ConversationService
from app.services.message_service import MessageService
from app.services.idempotency_service import IdempotencyService
from app.clients.kafka_producer import KafkaProducerClient, kafka_producer_client
from app.db.redis import redis_manager

# --- Repository Factory Injections ---

def get_conversation_repository() -> CassandraConversationRepository:
    """
    Factory method injecting the CassandraConversationRepository adapter.
    """
    return CassandraConversationRepository()

def get_message_repository() -> CassandraMessageRepository:
    """
    Factory method injecting the CassandraMessageRepository adapter.
    """
    return CassandraMessageRepository()

def get_outbox_repository() -> CassandraOutboxRepository:
    """
    Factory method injecting the CassandraOutboxRepository adapter.
    """
    return CassandraOutboxRepository()

def get_inbox_repository() -> CassandraInboxRepository:
    """
    Factory method injecting the CassandraInboxRepository adapter.
    """
    return CassandraInboxRepository()

# --- Cache Service Factory Injection ---

def get_cache_service() -> CacheService:
    """
    Factory method injecting the centralized CacheService.
    """
    return CacheService(redis_client=redis_manager.client)

def get_kafka_producer_client() -> KafkaProducerClient:
    """
    Factory method injecting the KafkaProducerClient singleton instance.
    """
    return kafka_producer_client

# --- Service Factory Injections ---

def get_conversation_service(
    repo: CassandraConversationRepository = Depends(get_conversation_repository),
    cache_service: CacheService = Depends(get_cache_service),
    producer_client: KafkaProducerClient = Depends(get_kafka_producer_client)
) -> ConversationService:
    """
    Factory method injecting ConversationService with repository and cache DI.
    """
    return ConversationService(repo=repo, cache_service=cache_service, producer_client=producer_client)

def get_message_service(
    repo: CassandraMessageRepository = Depends(get_message_repository),
    cache_service: CacheService = Depends(get_cache_service),
    producer_client: KafkaProducerClient = Depends(get_kafka_producer_client)
) -> MessageService:
    """
    Factory method injecting MessageService with repository and cache DI.
    """
    return MessageService(repo=repo, cache_service=cache_service, producer_client=producer_client)

def get_idempotency_service() -> IdempotencyService:
    """
    Factory method injecting the centralized IdempotencyService.
    """
    return IdempotencyService(redis_client=redis_manager.client)
