"""
API Dependency Factory Methods.
Provides Pydantic and repository resource injections for FastAPI controllers.
"""

from fastapi import Depends
from app.repositories.conversation_repository import CassandraConversationRepository
from app.repositories.message_repository import CassandraMessageRepository
from app.repositories.outbox_repository import CassandraOutboxRepository
from app.repositories.inbox_repository import CassandraInboxRepository

from app.services.conversation_service import ConversationService
from app.services.message_service import MessageService
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

# --- Service Factory Injections ---

def get_conversation_service(
    repo: CassandraConversationRepository = Depends(get_conversation_repository)
) -> ConversationService:
    """
    Factory method injecting ConversationService with repositories DI.
    """
    return ConversationService(repo=repo)

def get_message_service(
    repo: CassandraMessageRepository = Depends(get_message_repository)
) -> MessageService:
    """
    Factory method injecting MessageService with repositories and Redis pool DI.
    """
    return MessageService(repo=repo, redis_client=redis_manager.client)
