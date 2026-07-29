"""
Distributed Outbox Worker Daemon.
Uses Redis lease locks to partition buckets processing across worker replica instances.
"""

import asyncio
import json
import uuid
from typing import Set, Optional
import redis.asyncio as aioredis

from app.core.logging import logger
from app.repositories.outbox_repository import CassandraOutboxRepository
from app.clients.kafka_producer import kafka_producer_client
from app.core.config import settings
from app.db.redis import redis_manager

class DistributedOutboxWorker:
    def __init__(self, redis_client: Optional[aioredis.Redis] = None, worker_id: Optional[str] = None):
        self.redis = redis_client or redis_manager.client
        self.worker_id = worker_id or f"outbox_worker_{uuid.uuid4().hex[:8]}"
        self.repo = CassandraOutboxRepository()
        self.locked_buckets: Set[int] = set()
        self.running = False
        self.renew_task: Optional[asyncio.Task] = None

    def _get_lock_key(self, bucket: int) -> str:
        return f"outbox:lock:bucket:{bucket}"

    async def acquire_lock(self, bucket: int) -> bool:
        if not self.redis:
            return False
        key = self._get_lock_key(bucket)
        try:
            acquired = await self.redis.set(
                key,
                self.worker_id,
                nx=True,
                ex=int(settings.OUTBOX_LOCK_TTL_SECONDS)
            )
            if acquired:
                self.locked_buckets.add(bucket)
                logger.info(
                    "Outbox bucket lock acquired successfully",
                    bucket=bucket,
                    worker_id=self.worker_id,
                    ttl=settings.OUTBOX_LOCK_TTL_SECONDS
                )
                return True
        except Exception as e:
            logger.warning("Failed to acquire outbox bucket lock", bucket=bucket, error=str(e))
        return False

    async def renew_locks(self):
        while self.running:
            try:
                await asyncio.sleep(settings.OUTBOX_LOCK_RENEW_INTERVAL_SECONDS)
                if not self.redis or not self.locked_buckets:
                    continue
                
                # Renew all currently locked buckets
                for bucket in list(self.locked_buckets):
                    key = self._get_lock_key(bucket)
                    try:
                        # Verify we still own it before renewing
                        current_owner = await self.redis.get(key)
                        if current_owner == self.worker_id:
                            await self.redis.expire(key, int(settings.OUTBOX_LOCK_TTL_SECONDS))
                        else:
                            # Lost lock somehow
                            self.locked_buckets.discard(bucket)
                            logger.warning("Lost outbox bucket lock lease, discarded from active list", bucket=bucket)
                    except Exception as e:
                        logger.warning("Failed to renew outbox bucket lock lease", bucket=bucket, error=str(e))
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in outbox lock renewal loop", error=str(e))

    async def release_lock(self, bucket: int):
        if not self.redis:
            return
        key = self._get_lock_key(bucket)
        try:
            current_owner = await self.redis.get(key)
            if current_owner == self.worker_id:
                await self.redis.delete(key)
                self.locked_buckets.discard(bucket)
                logger.info("Outbox bucket lock released", bucket=bucket, worker_id=self.worker_id)
        except Exception as e:
            logger.warning("Failed to release outbox bucket lock", bucket=bucket, error=str(e))

    async def process_bucket(self, bucket: int):
        try:
            # Poll limit 200 events
            unpublished_events = self.repo.fetch_unpublished(bucket, limit=200)
            if not unpublished_events:
                return
            
            for event in unpublished_events:
                try:
                    if isinstance(event.payload, str):
                        payload = json.loads(event.payload)
                    else:
                        payload = event.payload
                    
                    conv_id = payload.get("conversation_id") or str(event.event_id)
                    
                    await kafka_producer_client.publish(
                        topic=event.event_type,
                        key=conv_id,
                        value=payload
                    )
                    
                    self.repo.mark_published(event.bucket, event.event_id)
                except Exception as e:
                    logger.error(
                        "Outbox worker failed to process event inside locked bucket. Skipping.",
                        event_id=str(event.event_id),
                        bucket=bucket,
                        error=str(e)
                    )
                    break
        except Exception as e:
            logger.error("Outbox worker failed to fetch from bucket", bucket=bucket, error=str(e))

    async def start(self):
        logger.info("Starting Distributed Outbox Worker...", worker_id=self.worker_id)
        self.running = True
        self.renew_task = asyncio.create_task(self.renew_locks())
        
        # Concurrency semaphore
        sem = asyncio.Semaphore(settings.OUTBOX_WORKER_CONCURRENCY)
        
        try:
            while self.running:
                # We attempt to check/claim buckets concurrently
                tasks = []
                for bucket in range(settings.OUTBOX_BUCKETS):
                    async def process_with_lease(b=bucket):
                        # Attempt to acquire lock if not already owned by us
                        if b in self.locked_buckets:
                            async with sem:
                                await self.process_bucket(b)
                        elif await self.acquire_lock(b):
                            async with sem:
                                await self.process_bucket(b)
                    
                    tasks.append(process_with_lease())
                
                await asyncio.gather(*tasks)
                await asyncio.sleep(settings.OUTBOX_POLL_INTERVAL_SECONDS)
        except asyncio.CancelledError:
            logger.info("Distributed Outbox Worker received cancellation signal.")
        finally:
            self.running = False
            if self.renew_task:
                self.renew_task.cancel()
                try:
                    await self.renew_task
                except asyncio.CancelledError:
                    pass
            # Release all our locked buckets
            for bucket in list(self.locked_buckets):
                await self.release_lock(bucket)

# Global helper function to match lifecycle manager hooks
_global_worker: Optional[DistributedOutboxWorker] = None

async def start_outbox_worker():
    global _global_worker
    _global_worker = DistributedOutboxWorker()
    await _global_worker.start()
