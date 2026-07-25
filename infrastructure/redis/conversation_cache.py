"""
Infrastructure Redis Cache - Conversation.
Implements the concrete cache manager for conversations, persisting and invalidating
conversation hash mappings in Redis with configured TTL rules.
"""

import json
import logging
from uuid import UUID
from typing import Optional
from datetime import datetime
from domain.entities.conversation import ConversationEntity
from core.enums import ConversationStatus
from infrastructure.redis.client import RedisClientManager

logger = logging.getLogger(__name__)

class ConversationCache:
    """
    Handles caching behaviors for Conversation objects using Redis hash mappings.
    """
    def __init__(self, client_manager: RedisClientManager, ttl_seconds: int = 3600):
        self.manager = client_manager
        self.ttl = ttl_seconds

    def _key(self, conversation_id: UUID) -> str:
        """
        Builds standard cache key mapping string for a conversation.
        """
        return f"conversation:{conversation_id}"

    async def get_conversation(self, conversation_id: UUID) -> Optional[ConversationEntity]:
        """
        Retrieves a cached conversation entity. Returns None on cache miss.
        """
        if not self.manager.client:
            return None
        key = self._key(conversation_id)
        try:
            data = await self.manager.client.hgetall(key)
            if not data:
                return None
            
            return ConversationEntity(
                conversation_id=UUID(data["conversation_id"]),
                user_id=UUID(data["user_id"]),
                title=data["title"],
                created_at=datetime.fromisoformat(data["created_at"]),
                updated_at=datetime.fromisoformat(data["updated_at"]),
                status=ConversationStatus(data["status"])
            )
        except Exception as e:
            logger.error(f"Redis get_conversation failed: {e}")
            return None

    async def set_conversation(self, entity: ConversationEntity) -> None:
        """
        Saves a conversation entity as a Redis hash mapping, applying key TTLs.
        """
        if not self.manager.client:
            return
        key = self._key(entity.conversation_id)
        try:
            mapping = {
                "conversation_id": str(entity.conversation_id),
                "user_id": str(entity.user_id),
                "title": entity.title,
                "created_at": entity.created_at.isoformat(),
                "updated_at": entity.updated_at.isoformat(),
                "status": entity.status.value
            }
            async with self.manager.client.pipeline(transaction=True) as pipe:
                pipe.hset(key, mapping=mapping)
                pipe.expire(key, self.ttl)
                await pipe.execute()
        except Exception as e:
            logger.error(f"Redis set_conversation failed: {e}")

    async def invalidate_conversation(self, conversation_id: UUID) -> None:
        """
        Deletes a conversation from Redis cache immediately.
        """
        if not self.manager.client:
            return
        key = self._key(conversation_id)
        try:
            await self.manager.client.delete(key)
        except Exception as e:
            logger.error(f"Redis invalidate_conversation failed: {e}")
