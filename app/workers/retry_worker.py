"""
Outbox Retry & Reconciliation Background Worker.
Periodically scans for stale, unpublished outbox events and retries publishing them.
"""

import asyncio
import json
from datetime import datetime, timezone
from app.core.logging import logger
from app.repositories.outbox_repository import CassandraOutboxRepository
from app.clients.kafka_producer import kafka_producer_client

async def start_retry_worker():
    """
    Background worker loop that runs every 30 seconds.
    Finds outbox events older than 30 seconds that are still unpublished,
    and retries publishing them to Kafka brokers.
    """
    logger.info("Starting background Outbox Retry Worker...")
    repo = CassandraOutboxRepository()
    
    poll_interval_seconds = 30.0
    stale_threshold_seconds = 30.0
    
    try:
        while True:
            logger.info("Reconciliation loop running: scanning for stale outbox events...")
            now = datetime.now(timezone.utc)
            
            for bucket in range(32):
                try:
                    unpublished_events = repo.fetch_unpublished(bucket, limit=100)
                    if not unpublished_events:
                        continue
                        
                    for event in unpublished_events:
                        # Determine event age
                        event_time = event.created_at
                        if event_time.tzinfo is None:
                            event_time = event_time.replace(tzinfo=timezone.utc)
                            
                        age_seconds = (now - event_time).total_seconds()
                        if age_seconds < stale_threshold_seconds:
                            # Skip newly created events to prevent overlap with OutboxWorker
                            continue
                            
                        logger.warning(
                            "Found stale unpublished event in outbox. Retrying publish...",
                            event_id=str(event.event_id),
                            event_type=event.event_type,
                            age_seconds=age_seconds
                        )
                        
                        try:
                            # Parse JSON payload
                            if isinstance(event.payload, str):
                                payload = json.loads(event.payload)
                            else:
                                payload = event.payload
                                
                            # Partition key: always conversation_id
                            conv_id = payload.get("conversation_id")
                            if not conv_id:
                                conv_id = str(event.event_id)
                                
                            # Re-publish to Kafka
                            await kafka_producer_client.publish(
                                topic=event.event_type,
                                key=conv_id,
                                value=payload
                            )
                            
                            # Mark published in Cassandra
                            repo.mark_published(event.bucket, event.event_id)
                            logger.info("Successfully reconciled stale event.", event_id=str(event.event_id))
                            
                        except Exception as e:
                            logger.error(
                                "Failed to reconcile stale outbox event.",
                                event_id=str(event.event_id),
                                error=str(e)
                            )
                            
                except Exception as e:
                    logger.error("Outbox retry worker failed to scan bucket", bucket=bucket, error=str(e))
            
            await asyncio.sleep(poll_interval_seconds)
            
    except asyncio.CancelledError:
        logger.info("Outbox Retry Worker background loop shutdown signal received.")
    except Exception as e:
        logger.error("Fatal error in background Outbox Retry Worker loop", error=str(e))
