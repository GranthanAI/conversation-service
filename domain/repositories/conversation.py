"""
Domain Interfaces File - Conversation Repository.
Defines the abstract interface (port) for conversation data access,
enforcing dependency inversion so the business logic depends only on interfaces.
"""

from abc import ABC, abstractmethod
from uuid import UUID
from datetime import datetime
from typing import List, Optional
from domain.entities.conversation import ConversationEntity

class IConversationRepository(ABC):
    """
    Abstract Base Class (Interface Port) for Conversation Data Access.
    """
    @abstractmethod
    async def create_conversation(
        self, conversation_id: UUID, user_id: UUID, title: str, created_at: datetime
    ) -> ConversationEntity:
        """
        Creates and persists a new conversation record in data stores.
        """
        pass

    @abstractmethod
    async def get_conversation(self, conversation_id: UUID) -> Optional[ConversationEntity]:
        """
        Retrieves a single conversation by its unique identifier.
        """
        pass

    @abstractmethod
    async def list_conversations_by_user(
        self, user_id: UUID, limit: int, cursor: Optional[datetime] = None
    ) -> List[ConversationEntity]:
        """
        Lists paginated conversation history owned by a specific user.
        """
        pass

    @abstractmethod
    async def update_conversation_status(self, conversation_id: UUID, status: str) -> None:
        """
        Updates the state lifecycle status flag of a conversation (e.g. archiving or soft deleting).
        """
        pass

    @abstractmethod
    async def update_conversation_title(self, conversation_id: UUID, title: str) -> None:
        """
        Updates the descriptive catalog title of an existing conversation.
        """
        pass
