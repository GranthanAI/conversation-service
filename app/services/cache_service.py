"""
Cache Service.
Provides centralized management of Redis Cache-Aside read/write operations
for Conversations and Messages domain models.
"""

import json
from datetime import datetime
from typing import List, Optional
from uuid import UUID
import redis.asyncio as aioredis

from app.models.conversation import Conversation, ConversationStatus
from app.models.message import Message, MessageStatus
from app.core.logging import logger

class CacheService:
    """
    Handles serialization, key patterns, TTL defaults, and exceptions for Redis operations.
    """
    def __init__(self, redis_client: Optional[aioredis.Redis] = None):
        self.redis = redis_client

    def _get_conversation_key(self, conversation_id: UUID) -> str:
        return f"conversation:{conversation_id}"

    def _get_message_list_key(self, conversation_id: UUID) -> str:
        return f"conversation:{conversation_id}:last50"

    # --- Conversation Metadata Cache Operations ---

    async def get_conversation(self, conversation_id: UUID) -> Optional[Conversation]:
        """
        Reads conversation metadata from Redis Hash.
        """
        if not self.redis:
            return None
        try:
            cache_key = self._get_conversation_key(conversation_id)
            cached_data = await self.redis.hgetall(cache_key)
            if cached_data:
                logger.info("Conversation metadata cache hit", conversation_id=conversation_id)
                return Conversation(
                    conversation_id=UUID(cached_data["conversation_id"]),
                    user_id=UUID(cached_data["user_id"]),
                    title=cached_data["title"],
                    created_at=datetime.fromisoformat(cached_data["created_at"]),
                    updated_at=datetime.fromisoformat(cached_data["updated_at"]),
                    status=ConversationStatus(cached_data["status"])
                )
        except Exception as e:
            logger.warning("Failed to read conversation from Redis cache", error=str(e))
        return None

    async def set_conversation(self, conv: Conversation) -> None:
        """
        Saves conversation metadata as Redis Hash with 1-hour TTL.
        """
        if not self.redis:
            return
        try:
            cache_key = self._get_conversation_key(conv.conversation_id)
            mapping = {
                "conversation_id": str(conv.conversation_id),
                "user_id": str(conv.user_id),
                "title": conv.title,
                "created_at": conv.created_at.isoformat(),
                "updated_at": conv.updated_at.isoformat(),
                "status": conv.status.value
            }
            await self.redis.hset(cache_key, mapping=mapping)
            await self.redis.expire(cache_key, 3600)
        except Exception as e:
            logger.warning("Failed to write conversation to Redis cache", error=str(e))

    async def delete_conversation(self, conversation_id: UUID) -> None:
        """
        Evicts conversation metadata key.
        """
        if not self.redis:
            return
        try:
            cache_key = self._get_conversation_key(conversation_id)
            await self.redis.delete(cache_key)
        except Exception as e:
            logger.warning("Failed to invalidate conversation Redis cache", error=str(e))

    # --- Message List Cache Operations ---

    async def get_last_50_messages(self, conversation_id: UUID, limit: int = 50) -> Optional[List[Message]]:
        """
        Reads message history list from Redis List.
        """
        if not self.redis:
            return None
        try:
            cache_key = self._get_message_list_key(conversation_id)
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
            logger.warning("Failed to read messages from Redis cache", error=str(e))
        return None

    async def set_last_50_messages(self, conversation_id: UUID, messages: List[Message]) -> None:
        """
        Saves message list in Redis with 1-hour TTL.
        """
        if not self.redis or not messages:
            return
        try:
            cache_key = self._get_message_list_key(conversation_id)
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
            # Cap list length implicitly or overwrite
            await self.redis.delete(cache_key)
            await self.redis.rpush(cache_key, *serialized_msgs)
            await self.redis.expire(cache_key, 3600)
        except Exception as e:
            logger.warning("Failed to write messages to Redis cache", error=str(e))

    async def delete_last_50_messages(self, conversation_id: UUID) -> None:
        """
        Evicts message history list key.
        """
        if not self.redis:
            return
        try:
            cache_key = self._get_message_list_key(conversation_id)
            await self.redis.delete(cache_key)
        except Exception as e:
            logger.warning("Failed to invalidate message Redis cache", error=str(e))
