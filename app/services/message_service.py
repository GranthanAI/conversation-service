"""
Message Service.
Coordinates Cache-Aside caching and database operations atomically using batch writes.
"""

import json
import uuid
from typing import List, Optional
from datetime import datetime, timezone
from uuid import UUID
import redis.asyncio as aioredis
from app.repositories.message_repository import CassandraMessageRepository
from app.models.message import Message, MessageStatus
from app.events.topics import KafkaTopics
from app.core.logging import logger

class MessageService:
    """
    Service orchestrator handling conversational message logs and streaming triggers atomically.
    """
    def __init__(
        self,
        repo: CassandraMessageRepository,
        redis_client: Optional[aioredis.Redis] = None
    ):
        self.repo = repo
        self.redis = redis_client

    async def _invalidate_cache(self, conversation_id: UUID):
        """
        Deletes the cached message history list from Redis.
        """
        if self.redis:
            try:
                cache_key = f"conversation:{conversation_id}:last50"
                await self.redis.delete(cache_key)
            except Exception as e:
                logger.warning("Failed to invalidate Redis cache", error=str(e))

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
        cache_key = f"conversation:{conversation_id}:last50"
        
        if cursor is None and self.redis:
            try:
                cached_items = await self.redis.lrange(cache_key, 0, limit - 1)
                if cached_items:
                    logger.info("Message history cache hit", conversation_id=conversation_id)
                    messages = []
                    for item in cached_items:
                        data = json.loads(item)
                        messages.append(Message(
                            conversation_id=UUID(data["conversation_id"]),
                            message_id=UUID(data["message_id"]),
                            sender=data["sender"],
                            content=data["content"],
                            created_at=datetime.fromisoformat(data["created_at"]),
                            status=MessageStatus(data["status"])
                        ))
                    return messages
            except Exception as e:
                logger.warning("Failed to read from Redis cache", error=str(e))

        logger.info("Message history cache miss. Fetching from database...", conversation_id=conversation_id)
        messages = self.repo.history(conversation_id, limit, cursor)

        if cursor is None and self.redis and messages:
            try:
                serialized_msgs = []
                for m in messages:
                    serialized_msgs.append(json.dumps({
                        "conversation_id": str(m.conversation_id),
                        "message_id": str(m.message_id),
                        "sender": m.sender,
                        "content": m.content,
                        "created_at": m.created_at.isoformat(),
                        "status": m.status.value
                    }))
                await self.redis.rpush(cache_key, *serialized_msgs)
                await self.redis.expire(cache_key, 3600)
            except Exception as e:
                logger.warning("Failed to populate Redis cache", error=str(e))

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
