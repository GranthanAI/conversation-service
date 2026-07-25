"""
Services File - Conversation Service.
Implements core use-cases orchestration for conversation creation, listing,
renaming, and soft deletion, coordinating the repository and caching components.
"""

import uuid
import logging
from uuid import UUID
from datetime import datetime
from typing import List, Optional, Tuple
from domain.entities.conversation import ConversationEntity
from domain.repositories.conversation import IConversationRepository
from domain.exceptions import NotFoundError, OwnershipError
from core.enums import ConversationStatus
from infrastructure.redis.conversation_cache import ConversationCache

logger = logging.getLogger(__name__)

class ConversationService:
    """
    Orchestration Service containing use-case logic implementations.
    """
    def __init__(self, repo: IConversationRepository, cache: ConversationCache):
        self.repo = repo
        self.cache = cache

    async def create_conversation(self, user_id: UUID, title: str) -> ConversationEntity:
        """
        Creates and registers a new conversation, populating the cache.
        """
        conversation_id = uuid.uuid4()
        created_at = datetime.utcnow()
        entity = await self.repo.create_conversation(
            conversation_id=conversation_id,
            user_id=user_id,
            title=title,
            created_at=created_at
        )
        
        # Populate cache
        await self.cache.set_conversation(entity)
        return entity

    async def get_conversation(self, conversation_id: UUID, user_id: UUID) -> ConversationEntity:
        """
        Retrieves a conversation by UUID, verifying user ownership and checking active status.
        """
        # Check cache first
        entity = await self.cache.get_conversation(conversation_id)
        if not entity:
            # Fallback to DB
            entity = await self.repo.get_conversation(conversation_id)
            if not entity:
                raise NotFoundError("Conversation not found")
            # Populate cache
            await self.cache.set_conversation(entity)

        # Enforce soft-deletion status check
        if entity.status == ConversationStatus.DELETED:
            raise NotFoundError("Conversation not found")

        # Verify ownership
        if entity.user_id != user_id:
            raise OwnershipError("User does not own this conversation")

        return entity

    async def list_conversations(
        self, user_id: UUID, limit: int = 20, cursor: Optional[str] = None
    ) -> Tuple[List[ConversationEntity], Optional[str]]:
        """
        Retrieves a page of conversations for a user, checking for page limits.
        """
        # Enforce max limit limits
        if limit > 100:
            limit = 100

        cursor_date = None
        if cursor:
            try:
                cursor_date = datetime.fromisoformat(cursor)
            except ValueError:
                pass

        entities = await self.repo.list_conversations_by_user(user_id, limit=limit + 1, cursor=cursor_date)
        
        has_next = len(entities) > limit
        if has_next:
            entities = entities[:limit]
            next_cursor = entities[-1].updated_at.isoformat()
        else:
            next_cursor = None

        # Filter out soft-deleted conversations
        active_entities = [e for e in entities if e.status != ConversationStatus.DELETED]
        return active_entities, next_cursor

    async def rename_conversation(self, conversation_id: UUID, user_id: UUID, title: str) -> ConversationEntity:
        """
        Renames a conversation and invalidates its cache records.
        """
        entity = await self.get_conversation(conversation_id, user_id)
        
        await self.repo.update_conversation_title(conversation_id, title)
        
        # Invalidate cache
        await self.cache.invalidate_conversation(conversation_id)
        
        # Fetch updated representation
        updated_entity = await self.repo.get_conversation(conversation_id)
        if updated_entity:
            await self.cache.set_conversation(updated_entity)
            return updated_entity
        
        entity.title = title
        entity.updated_at = datetime.utcnow()
        return entity

    async def delete_conversation(self, conversation_id: UUID, user_id: UUID) -> None:
        """
        Soft-deletes a conversation and removes cache mappings.
        """
        await self.get_conversation(conversation_id, user_id)
        
        # Soft delete flag
        await self.repo.update_conversation_status(conversation_id, ConversationStatus.DELETED.value)
        
        # Invalidate cache
        await self.cache.invalidate_conversation(conversation_id)
