"""
API Dependency Factory Methods.
Provides Pydantic and repository resource injections for FastAPI controllers.
"""

from app.repositories.conversation_repository import CassandraConversationRepository
from app.repositories.message_repository import CassandraMessageRepository
from app.repositories.outbox_repository import CassandraOutboxRepository
from app.repositories.inbox_repository import CassandraInboxRepository

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
