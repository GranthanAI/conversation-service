"""
API Dependency Factory Methods.
Provides repository, service, and cache resource injections for FastAPI controllers.
"""

from fastapi import Depends

from app.repositories.conversation_repository import CassandraConversationRepository
from app.repositories.message_repository import CassandraMessageRepository
from app.repositories.outbox_repository import CassandraOutboxRepository
from app.repositories.inbox_repository import CassandraInboxRepository
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.repositories.pg_repositories import CustomModeRepository, PreferencesRepository
from app.services.custom_mode_service import CustomModeService
from app.services.mode_catalog import mode_catalog
from app.services.branch_context_service import BranchContextService
from app.services.preferences_service import PreferencesService

from app.services.cache_service import CacheService
from app.services.conversation_service import ConversationService
from app.services.message_service import MessageService
from app.services.idempotency_service import IdempotencyService
from app.services.stream_service import StreamService
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

def get_preferences_repository(session: AsyncSession = Depends(get_db)) -> PreferencesRepository:
    """PostgreSQL preferences repository bound to the request-scoped session."""
    return PreferencesRepository(session)

def get_custom_mode_repository(session: AsyncSession = Depends(get_db)) -> CustomModeRepository:
    """PostgreSQL custom-mode repository bound to the request-scoped session."""
    return CustomModeRepository(session)

def get_preferences_service(
    repo: PreferencesRepository = Depends(get_preferences_repository),
    modes: CustomModeRepository = Depends(get_custom_mode_repository),
) -> PreferencesService:
    """Factory method injecting PreferencesService."""
    return PreferencesService(repo=repo, modes=modes, catalog=mode_catalog)

def get_custom_mode_service(
    modes: CustomModeRepository = Depends(get_custom_mode_repository),
) -> CustomModeService:
    return CustomModeService(modes)

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

def get_branch_context_service(
    conversations: ConversationService = Depends(get_conversation_service),
    message_repo: CassandraMessageRepository = Depends(get_message_repository),
) -> BranchContextService:
    return BranchContextService(conversations, message_repo)

def get_message_service(
    repo: CassandraMessageRepository = Depends(get_message_repository),
    cache_service: CacheService = Depends(get_cache_service),
    producer_client: KafkaProducerClient = Depends(get_kafka_producer_client),
    preferences_service: PreferencesService = Depends(get_preferences_service),
    branch_context: BranchContextService = Depends(get_branch_context_service),
) -> MessageService:
    """
    Factory method injecting MessageService with repository and cache DI.
    """
    return MessageService(
        repo=repo, cache_service=cache_service, producer_client=producer_client,
        preferences_service=preferences_service, branch_context=branch_context,
    )

def get_idempotency_service() -> IdempotencyService:
    """
    Factory method injecting the centralized IdempotencyService.
    """
    return IdempotencyService(redis_client=redis_manager.client)

def get_stream_service() -> StreamService:
    """
    Factory method injecting the StreamService.
    """
    return StreamService(redis_client=redis_manager.client)
