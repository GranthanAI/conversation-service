"""
Cache Dependency Provider.
Instantiates the Redis cluster client manager and conversation caching helpers
using configurations imported from the central Settings model.
"""

from fastapi import Depends
from core.config import settings
from infrastructure.redis.client import RedisClientManager
from infrastructure.redis.conversation_cache import ConversationCache

# Singleton client manager using actual environment configuration settings
redis_manager = RedisClientManager(
    redis_url=settings.REDIS_URL,
    redis_nodes=settings.REDIS_NODES,
    timeout=settings.REDIS_TIMEOUT_SECONDS
)

def get_redis_manager() -> RedisClientManager:
    """
    Returns the initialized Redis client manager singleton.
    """
    return redis_manager

def get_conversation_cache(
    manager: RedisClientManager = Depends(get_redis_manager)
) -> ConversationCache:
    """
    Dependency provider creating instances of ConversationCache adapters.
    """
    return ConversationCache(client_manager=manager)
