"""
Conversation Service.
Coordinates atomic transactional batch writes for conversation entities and outbox events,
utilizing a centralized CacheService for Cache-Aside patterns.
"""

import json
import uuid
from typing import List, Optional
from datetime import datetime
from uuid import UUID

from app.repositories.conversation_repository import CassandraConversationRepository
from app.models.conversation import Conversation, ConversationStatus
from app.events.topics import KafkaTopics
from app.utils.helpers import uuidv7
from app.services.cache_service import CacheService

class ConversationService:
    """
    Service orchestrator handling conversational metadata pipelines.
    """
    def __init__(
        self,
        repo: CassandraConversationRepository,
        cache_service: Optional[CacheService] = None
    ):
        self.repo = repo
        self.cache = cache_service

    async def get(self, conversation_id: UUID) -> Optional[Conversation]:
        """
        Fetches conversation metadata using Read-Through Cache-Aside.
        """
        if self.cache:
            conv = await self.cache.get_conversation(conversation_id)
            if conv:
                return conv

        conv = self.repo.get(conversation_id)
        if conv and self.cache:
            await self.cache.set_conversation(conv)
        return conv

    async def create(self, user_id: UUID, title: str) -> Conversation:
        """
        Creates a conversation atomically alongside its outbox task and writes through to cache.
        """
        conversation_id = uuidv7()
        event_id = uuid.uuid1()
        
        payload = {
            "conversation_id": str(conversation_id),
            "user_id": str(user_id),
            "title": title,
            "status": "active"
        }
        
        conv = self.repo.create_with_outbox(
            conversation_id=conversation_id,
            user_id=user_id,
            title=title,
            status="active",
            event_id=event_id,
            event_type=KafkaTopics.CONVERSATION_CREATED,
            outbox_payload=json.dumps(payload)
        )
        
        # Write-through update
        if self.cache:
            await self.cache.set_conversation(conv)
        return conv

    async def rename(self, conversation_id: UUID, new_title: str) -> Optional[Conversation]:
        """
        Renames a conversation title atomically alongside its outbox task and writes through to cache.
        """
        conv = await self.get(conversation_id)
        if not conv:
            return None

        event_id = uuid.uuid1()
        payload = {
            "conversation_id": str(conversation_id),
            "user_id": str(conv.user_id),
            "title": new_title,
            "status": str(conv.status)
        }
        
        updated = self.repo.update_with_outbox(
            conversation_id=conversation_id,
            title=new_title,
            status=conv.status,
            event_id=event_id,
            event_type=KafkaTopics.CONVERSATION_UPDATED,
            outbox_payload=json.dumps(payload)
        )
        
        if updated and self.cache:
            await self.cache.set_conversation(updated)
        return updated

    async def archive(self, conversation_id: UUID) -> Optional[Conversation]:
        """
        Archives a conversation atomically alongside its outbox task and writes through to cache.
        """
        conv = await self.get(conversation_id)
        if not conv:
            return None

        event_id = uuid.uuid1()
        payload = {
            "conversation_id": str(conversation_id),
            "user_id": str(conv.user_id),
            "title": conv.title,
            "status": "archived"
        }
        
        archived = self.repo.update_with_outbox(
            conversation_id=conversation_id,
            title=conv.title,
            status="archived",
            event_id=event_id,
            event_type=KafkaTopics.CONVERSATION_UPDATED,
            outbox_payload=json.dumps(payload)
        )
        
        if archived and self.cache:
            await self.cache.set_conversation(archived)
        return archived

    async def delete(self, conversation_id: UUID) -> bool:
        """
        Soft-deletes a conversation atomically alongside its outbox task and invalidates cache.
        """
        conv = await self.get(conversation_id)
        if not conv:
            return False

        event_id = uuid.uuid1()
        payload = {
            "conversation_id": str(conversation_id),
            "user_id": str(conv.user_id),
            "title": conv.title,
            "status": "deleted"
        }
        
        success = self.repo.delete_with_outbox(
            conversation_id=conversation_id,
            event_id=event_id,
            event_type=KafkaTopics.CONVERSATION_DELETED,
            outbox_payload=json.dumps(payload)
        )
        
        if success and self.cache:
            await self.cache.delete_conversation(conversation_id)
        return success

    def list(self, user_id: UUID, limit: int = 20, cursor: Optional[datetime] = None) -> List[Conversation]:
        """
        Lists user conversations. No cache lookup is performed for list feeds due to sorting/cursors.
        """
        return self.repo.list(user_id, limit, cursor)
