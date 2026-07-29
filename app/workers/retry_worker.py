"""
Distributed Outbox Retry & Reconciliation Background Worker.
Uses Redis lease locks to partition outbox retry reconciliations across worker replica instances.
"""

import asyncio
import json
import uuid
from datetime import datetime, timezone
from typing import Set, Optional
import redis.asyncio as aioredis

from app.core.logging import logger
from app.repositories.outbox_repository import CassandraOutboxRepository
from app.clients.kafka_producer import kafka_producer_client
from app.core.config import settings
from app.db.redis import redis_manager

class DistributedRetryWorker:
    def __init__(self, redis_client: Optional[aioredis.Redis] = None, worker_id: Optional[str] = None):
        self.redis = redis_client or redis_manager.client
        self.worker_id = worker_id or f"retry_worker_{uuid.uuid4().hex[:8]}"
        self.repo = CassandraOutboxRepository()
        self.locked_buckets: Set[int] = set()
        self.running = False
        self.renew_task: Optional[asyncio.Task] = None

    def _get_lock_key(self, bucket: int) -> str:
        return f"outbox:retry:lock:bucket:{bucket}"

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
                    "Retry worker bucket lock acquired successfully",
                    bucket=bucket,
                    worker_id=self.worker_id,
                    ttl=settings.OUTBOX_LOCK_TTL_SECONDS
                )
                return True
        except Exception as e:
            logger.warning("Failed to acquire retry worker bucket lock", bucket=bucket, error=str(e))
        return False

    async def renew_locks(self):
        while self.running:
            try:
                await asyncio.sleep(settings.OUTBOX_LOCK_RENEW_INTERVAL_SECONDS)
                if not self.redis or not self.locked_buckets:
                    continue
                
                for bucket in list(self.locked_buckets):
                    key = self._get_lock_key(bucket)
                    try:
                        current_owner = await self.redis.get(key)
                        if current_owner == self.worker_id:
                            await self.redis.expire(key, int(settings.OUTBOX_LOCK_TTL_SECONDS))
                        else:
                            self.locked_buckets.discard(bucket)
                            logger.warning("Lost retry worker bucket lock lease", bucket=bucket)
                    except Exception as e:
                        logger.warning("Failed to renew retry worker bucket lock lease", bucket=bucket, error=str(e))
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in retry worker lock renewal loop", error=str(e))

    async def release_lock(self, bucket: int):
        if not self.redis:
            return
        key = self._get_lock_key(bucket)
        try:
            current_owner = await self.redis.get(key)
            if current_owner == self.worker_id:
                await self.redis.delete(key)
                self.locked_buckets.discard(bucket)
                logger.info("Retry worker bucket lock released", bucket=bucket, worker_id=self.worker_id)
        except Exception as e:
            logger.warning("Failed to release retry worker bucket lock", bucket=bucket, error=str(e))

    async def process_bucket(self, bucket: int, now: datetime):
        try:
            unpublished_events = self.repo.fetch_unpublished(bucket, limit=100)
            if not unpublished_events:
                return
                
            for event in unpublished_events:
                event_time = event.created_at
                if event_time.tzinfo is None:
                    event_time = event_time.replace(tzinfo=timezone.utc)
                    
                age_seconds = (now - event_time).total_seconds()
                if age_seconds < settings.OUTBOX_STALE_THRESHOLD_SECONDS:
                    continue
                    
                logger.warning(
                    "Found stale unpublished event in outbox. Retrying publish...",
                    event_id=str(event.event_id),
                    event_type=event.event_type,
                    age_seconds=age_seconds
                )
                
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
                    logger.info("Successfully reconciled stale event.", event_id=str(event.event_id))
                except Exception as e:
                    logger.error(
                        "Failed to reconcile stale outbox event.",
                        event_id=str(event.event_id),
                        error=str(e)
                    )
        except Exception as e:
            logger.error("Retry worker failed to scan bucket", bucket=bucket, error=str(e))

    async def start(self):
        logger.info("Starting Distributed Outbox Retry Worker...", worker_id=self.worker_id)
        self.running = True
        self.renew_task = asyncio.create_task(self.renew_locks())
        
        sem = asyncio.Semaphore(settings.OUTBOX_WORKER_CONCURRENCY)
        
        try:
            while self.running:
                logger.info("Reconciliation loop running: scanning for stale outbox events...")
                now = datetime.now(timezone.utc)
                tasks = []
                for bucket in range(settings.OUTBOX_BUCKETS):
                    async def process_with_lease(b=bucket):
                        if b in self.locked_buckets:
                            async with sem:
                                await self.process_bucket(b, now)
                        elif await self.acquire_lock(b):
                            async with sem:
                                await self.process_bucket(b, now)
                    tasks.append(process_with_lease())
                
                await asyncio.gather(*tasks)
                await asyncio.sleep(settings.OUTBOX_RETRY_INTERVAL_SECONDS)
        except asyncio.CancelledError:
            logger.info("Distributed Outbox Retry Worker received cancellation signal.")
        finally:
            self.running = False
            if self.renew_task:
                self.renew_task.cancel()
                try:
                    await self.renew_task
                except asyncio.CancelledError:
                    pass
            for bucket in list(self.locked_buckets):
                await self.release_lock(bucket)

# Global helper function to match lifecycle manager hooks
_global_retry_worker: Optional[DistributedRetryWorker] = None

async def start_retry_worker():
    global _global_retry_worker
    _global_retry_worker = DistributedRetryWorker()
    await _global_retry_worker.start()
