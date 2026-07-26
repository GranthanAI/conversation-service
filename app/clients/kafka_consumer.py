"""
Kafka Event Consumer Wrapper.
Contains polling logic loops and listener handlers.
"""

from typing import Callable, Awaitable
from app.db.kafka import kafka_manager
from app.core.logging import logger

class KafkaConsumerClient:
    """
    Wrapper around the underlying AIOKafkaConsumer client to listen to events.
    """
    def __init__(self):
        self.manager = kafka_manager

    async def start_polling(self, handler: Callable[[str, bytes, bytes], Awaitable[None]]):
        """
        Polls records from consumer topics in a loop and invokes the dispatcher callback.
        """
        if not self.manager.consumer:
            logger.error("Kafka Consumer is not initialized. Cannot start polling.")
            raise RuntimeError("Kafka Consumer not running")

        logger.info("Starting Kafka Consumer polling loop...")
        try:
            async for msg in self.manager.consumer:
                logger.debug("Received event from Kafka topic", topic=msg.topic, partition=msg.partition, offset=msg.offset)
                try:
                    await handler(msg.topic, msg.key, msg.value)
                    # Manually commit offset as per LLD §11
                    await self.manager.consumer.commit()
                except Exception as handler_err:
                    logger.error("Error executing handler for Kafka event", topic=msg.topic, error=str(handler_err))
                    # Do not commit offset to trigger reprocessing
        except Exception as loop_err:
            logger.critical("Kafka Consumer polling loop crashed", error=str(loop_err))

kafka_consumer_client = KafkaConsumerClient()
