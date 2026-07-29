"""
Idempotency Service.
Provides HTTP-level idempotency lock management using atomic Redis SETNX.
"""

import json
from typing import Optional, Dict, Any
import redis.asyncio as aioredis
from app.core.logging import logger

from app.core.config import settings

class IdempotencyService:
    """
    Manages client request idempotency locks and cached responses in Redis.
    """
    def __init__(self, redis_client: Optional[aioredis.Redis] = None):
        self.redis = redis_client
        self.ttl = settings.IDEMPOTENCY_TTL_SECONDS  # configured in settings

    def _get_key(self, key: str) -> str:
        return f"idempotency:{key}"

    async def claim_key(self, key: str) -> Optional[str]:
        """
        Attempts to claim the idempotency key using SETNX (SET with NX=True).
        If the key is new: claims it with value "processing", returns None.
        If the key already exists: returns its current value ("processing" or cached JSON response).
        """
        if not self.redis:
            return None
        
        cache_key = self._get_key(key)
        try:
            # Set key to "processing" if not exists, with 24h TTL
            acquired = await self.redis.set(cache_key, "processing", nx=True, ex=self.ttl)
            if acquired:
                logger.info("Idempotency lock acquired", key=key)
                return None
            
            # Lock already exists, fetch the value
            current_value = await self.redis.get(cache_key)
            logger.info("Idempotency match found", key=key, value=current_value)
            return current_value
        except Exception as e:
            logger.warning("Failed to check/claim idempotency key in Redis", key=key, error=str(e))
            return None

    async def save_response(self, key: str, response_payload: Dict[str, Any]) -> None:
        """
        Overwrites the "processing" lock placeholder with the completed request response payload.
        """
        if not self.redis:
            return
        
        cache_key = self._get_key(key)
        try:
            payload_str = json.dumps(response_payload)
            await self.redis.set(cache_key, payload_str, ex=self.ttl)
            logger.info("Idempotency response cached", key=key)
        except Exception as e:
            logger.warning("Failed to save idempotency response in Redis", key=key, error=str(e))

    async def remove_lock(self, key: str) -> None:
        """
        Deletes the idempotency lock from Redis. Typically called on request processing failure.
        """
        if not self.redis:
            return
        
        cache_key = self._get_key(key)
        try:
            await self.redis.delete(cache_key)
            logger.info("Idempotency lock evicted due to failure", key=key)
        except Exception as e:
            logger.warning("Failed to remove idempotency lock in Redis", key=key, error=str(e))
