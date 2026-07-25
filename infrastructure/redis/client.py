"""
Infrastructure Redis Client.
Manages connections to Redis and Redis Cluster configurations, handling client
initialization, socket timeouts, ping validations, and shutdown cleanup.
"""

import logging
from typing import Optional
from redis.asyncio.cluster import RedisCluster
from redis.asyncio import Redis

logger = logging.getLogger(__name__)

class RedisClientManager:
    """
    Manages connections to Redis nodes or Redis Cluster configurations.
    """
    def __init__(self, redis_url: str, redis_nodes: str, timeout: float = 2.0):
        self.redis_url = redis_url
        self.redis_nodes = redis_nodes
        self.timeout = timeout
        self.client: Optional[Redis] = None
        self.is_cluster = False

    async def initialize(self) -> None:
        """
        Initializes client connections based on node configuration counts.
        """
        try:
            # Check if cluster mode is desired by nodes configurations
            if self.redis_nodes and len(self.redis_nodes.split(",")) > 1:
                nodes = self.redis_nodes.split(",")
                startup_nodes = [{"host": n.split(":")[0], "port": int(n.split(":")[1])} for n in nodes]
                self.client = RedisCluster(
                    startup_nodes=startup_nodes,
                    decode_responses=True,
                    socket_timeout=self.timeout
                )
                self.is_cluster = True
                logger.info("Connected to Redis Cluster.")
            else:
                self.client = Redis.from_url(
                    self.redis_url,
                    decode_responses=True,
                    socket_timeout=self.timeout
                )
                self.is_cluster = False
                logger.info(f"Connected to Redis single node at {self.redis_url}.")
            await self.client.ping()
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            self.client = None

    async def close(self) -> None:
        """
        Closes any active Redis client connections.
        """
        if self.client:
            await self.client.close()
            logger.info("Closed Redis connection.")
