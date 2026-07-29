"""
Redis Client Connection Manager.
Manages connection pooling lifecycle to Redis instances.
"""

from typing import Optional
import redis.asyncio as aioredis
from app.core.config import settings
from app.core.logging import logger

class RedisClientManager:
    """
    Client manager for initiating and holding the Redis connection pool.
    """
    def __init__(self):
        self.client: Optional[aioredis.Redis] = None

    def initialize(self):
        """
        Builds the active connection pool.
        """
        try:
            logger.info("Initializing Redis connection pool...", url=settings.REDIS_URL)
            self.client = aioredis.from_url(
                settings.REDIS_URL,
                decode_responses=True,
                socket_timeout=settings.REDIS_TIMEOUT_SECONDS,
                max_connections=settings.REDIS_MAX_CONNECTIONS
            )
            logger.info("Redis client pool initialized.")
        except Exception as e:
            logger.error("Failed to connect to Redis pool", error=str(e))
            self.client = None

    async def check_health(self) -> bool:
        """
        Executes standard PING command.
        """
        if not self.client:
            return False
        try:
            res = await self.client.ping()
            return res is True
        except Exception as e:
            logger.warning("Redis health probe check failed", error=str(e))
            return False

    async def close(self):
        """
        Closes connection sockets pool.
        """
        if self.client:
            try:
                await self.client.close()
                logger.info("Redis client closed successfully.")
            except Exception as e:
                logger.error("Error closing Redis connection pool", error=str(e))

redis_manager = RedisClientManager()
