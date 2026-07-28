"""
Message Service.
Coordinates Cache-Aside caching and database operations atomically using batch writes,
leveraging a centralized CacheService.
"""

import json
import uuid
from typing import List, Optional
from datetime import datetime, timezone
from uuid import UUID

from app.repositories.message_repository import CassandraMessageRepository
from app.models.message import Message, MessageStatus
from app.events.topics import KafkaTopics
from app.core.logging import logger
from app.services.cache_service import CacheService

class MessageService:
    """
    Service orchestrator handling conversational message logs and streaming triggers atomically.
    """
    def __init__(
        self,
        repo: CassandraMessageRepository,
        cache_service: Optional[CacheService] = None
    ):
        self.repo = repo
        self.cache = cache_service

    async def _invalidate_cache(self, conversation_id: UUID):
        """
        Deletes the cached message history list from Redis via CacheService.
        """
        if self.cache:
            await self.cache.delete_last_50_messages(conversation_id)

    async def send(self, conversation_id: UUID, message_id: UUID, sender: str, content: str) -> Message:
        """
        Persists a message atomically with its outbox event inside a single Cassandra batch.
        """
        event_id = uuid.uuid1()
        payload = {
            "conversation_id": str(conversation_id),
            "message_id": str(message_id),
            "sender": sender,
            "content": content,
            "status": "sent"
        }
        
        msg = self.repo.create_with_outbox(
            conversation_id=conversation_id,
            message_id=message_id,
            sender=sender,
            content=content,
            status="sent",
            event_id=event_id,
            event_type=KafkaTopics.CHAT_MESSAGE_CREATED,
            outbox_payload=json.dumps(payload)
        )
        
        # Invalidate history cache
        await self._invalidate_cache(conversation_id)
        
        return msg

    async def history(self, conversation_id: UUID, limit: int = 50, cursor: Optional[UUID] = None) -> List[Message]:
        """
        Returns message history page using Cache-Aside (only for the first page).
        """
        if cursor is None and self.cache:
            messages = await self.cache.get_last_50_messages(conversation_id, limit)
            if messages is not None:
                return messages

        logger.info("Message history cache miss. Fetching from database...", conversation_id=conversation_id)
        messages = self.repo.history(conversation_id, limit, cursor)

        if cursor is None and self.cache and messages:
            await self.cache.set_last_50_messages(conversation_id, messages)

        return messages

    async def delete(self, conversation_id: UUID, message_id: UUID) -> bool:
        """
        Soft-deletes a message and invalidates cache.
        """
        success = self.repo.delete(conversation_id, message_id)
        if success:
            await self._invalidate_cache(conversation_id)
            return True
        return False

    async def regenerate(self, conversation_id: UUID, message_id: UUID) -> Optional[Message]:
        """
        Soft-deletes the target assistant message and atomically stages regeneration outbox task.
        """
        history_msgs = self.repo.history(conversation_id, limit=50)
        
        target_msg = None
        prompt_msg = None
        
        for i, m in enumerate(history_msgs):
            if m.message_id == message_id:
                target_msg = m
                if i + 1 < len(history_msgs):
                    prompt_msg = history_msgs[i + 1]
                break

        if not target_msg:
            return None

        # Calculate outbox params for new assistant streaming response
        event_id = uuid.uuid1()
        payload = {
            "conversation_id": str(conversation_id),
            "message_id": str(message_id),
            "sender": "assistant",
            "content": "",
            "prompt_content": prompt_msg.content if prompt_msg else "",
            "status": "pending"
        }

        # Stage regeneration atomically by soft deleting the old response and executing batch outbox stage
        if target_msg.sender == "assistant":
            # Soft delete old response
            self.repo.delete(conversation_id, message_id)

        # Create new pending message atomic with outbox
        new_msg_id = uuid.uuid1() # generate a new response task ID
        self.repo.create_with_outbox(
            conversation_id=conversation_id,
            message_id=new_msg_id,
            sender="assistant",
            content="",
            status="pending",
            event_id=event_id,
            event_type=KafkaTopics.CHAT_MESSAGE_CREATED,
            outbox_payload=json.dumps(payload)
        )

        await self._invalidate_cache(conversation_id)
        return target_msg
