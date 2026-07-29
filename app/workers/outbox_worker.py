"""
Outbox Worker Daemon.
Periodically polls the Cassandra transactional outbox table and publishes events to Kafka,
guaranteeing at-least-once delivery semantics.
"""

import asyncio
import json
from app.core.logging import logger
from app.repositories.outbox_repository import CassandraOutboxRepository
from app.clients.kafka_producer import kafka_producer_client

from app.core.config import settings

async def start_outbox_worker():
    """
    Poller task looping across all partition buckets.
    Reads unpublished events, attempts Kafka publishes, and updates status in Cassandra.
    """
    logger.info("Starting background Outbox Worker loop...")
    repo = CassandraOutboxRepository()
    
    poll_interval_seconds = settings.OUTBOX_POLL_INTERVAL_SECONDS
    
    try:
        while True:
            for bucket in range(settings.OUTBOX_BUCKETS):
                try:
                    unpublished_events = repo.fetch_unpublished(bucket, limit=200)
                    if not unpublished_events:
                        continue
                    
                    for event in unpublished_events:
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

                            # Publish to Kafka
                            await kafka_producer_client.publish(
                                topic=event.event_type,
                                key=conv_id,
                                value=payload
                            )
                            
                            # Mark published in Cassandra
                            repo.mark_published(event.bucket, event.event_id)
                            
                        except Exception as e:
                            logger.error(
                                "Failed to process outbox event. Leaving unpublished for retry.",
                                event_id=str(event.event_id),
                                error=str(e)
                            )
                            # Break the bucket loop to avoid infinite error loop on down services
                            break
                except Exception as e:
                    logger.error("Outbox worker failed to fetch from bucket", bucket=bucket, error=str(e))
                    break
            
            await asyncio.sleep(poll_interval_seconds)
            
    except asyncio.CancelledError:
        logger.info("Outbox worker background loop shutdown signal received.")
    except Exception as e:
        logger.error("Fatal error in background Outbox Worker loop", error=str(e))
