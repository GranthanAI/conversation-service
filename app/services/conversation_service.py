"""
Conversation Service.
Coordinates atomic transactional batch writes for conversation entities and outbox events,
integrating Cache-Aside caching and direct Kafka event publishing.
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
from app.clients.kafka_producer import KafkaProducerClient

class ConversationService:
    """
    Service orchestrator handling conversational metadata pipelines.
    """
    def __init__(
        self,
        repo: CassandraConversationRepository,
        cache_service: Optional[CacheService] = None,
        producer_client: Optional[KafkaProducerClient] = None
    ):
        self.repo = repo
        self.cache = cache_service
        self.producer = producer_client

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
        Creates a conversation atomically alongside its outbox task,
        writes through to cache, and publishes directly to Kafka.
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
            
        # Direct Kafka publish
        if self.producer:
            await self.producer.publish(
                topic=KafkaTopics.CONVERSATION_CREATED,
                key=str(conversation_id),
                value=payload
            )
            
        return conv

    async def rename(self, conversation_id: UUID, new_title: str) -> Optional[Conversation]:
        """
        Renames a conversation title atomically, updates cache, and publishes directly to Kafka.
        """
        conv = await self.get(conversation_id)
        if not conv:
            return None

        event_id = uuid.uuid1()
        payload = {
            "conversation_id": str(conversation_id),
            "user_id": str(conv.user_id),
            "title": new_title,
            "status": conv.status.value
        }
        
        updated = self.repo.update_with_outbox(
            conversation_id=conversation_id,
            title=new_title,
            status=conv.status,
            event_id=event_id,
            event_type=KafkaTopics.CONVERSATION_UPDATED,
            outbox_payload=json.dumps(payload)
        )
        
        if updated:
            if self.cache:
                await self.cache.set_conversation(updated)
            if self.producer:
                await self.producer.publish(
                    topic=KafkaTopics.CONVERSATION_UPDATED,
                    key=str(conversation_id),
                    value=payload
                )
        return updated

    async def archive(self, conversation_id: UUID) -> Optional[Conversation]:
        """
        Archives a conversation atomically, updates cache, and publishes directly to Kafka.
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
        
        if archived:
            if self.cache:
                await self.cache.set_conversation(archived)
            if self.producer:
                await self.producer.publish(
                    topic=KafkaTopics.CONVERSATION_UPDATED,
                    key=str(conversation_id),
                    value=payload
                )
        return archived

    async def delete(self, conversation_id: UUID) -> bool:
        """
        Soft-deletes a conversation atomically, invalidates cache, and publishes directly to Kafka.
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
        
        if success:
            if self.cache:
                await self.cache.delete_conversation(conversation_id)
            if self.producer:
                await self.producer.publish(
                    topic=KafkaTopics.CONVERSATION_DELETED,
                    key=str(conversation_id),
                    value=payload
                )
        return success

    def list(self, user_id: UUID, limit: int = 20, cursor: Optional[datetime] = None) -> List[Conversation]:
        """
        Lists user conversations.
        """
        return self.repo.list(user_id, limit, cursor)
