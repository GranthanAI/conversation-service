"""
Kafka Consumer Loop.
Subscribes to inbound event topics and routes them to service-level handlers,
guaranteeing idempotency and at-least-once delivery semantics using the Inbox pattern.
"""

import asyncio
import json
import uuid
from uuid import UUID

from app.db.kafka import kafka_manager
from app.events.topics import KafkaTopics
from app.core.logging import logger

from app.repositories.inbox_repository import CassandraInboxRepository
from app.repositories.conversation_repository import CassandraConversationRepository
from app.repositories.message_repository import CassandraMessageRepository
from app.services.cache_service import CacheService
from app.services.conversation_service import ConversationService
from app.services.message_service import MessageService
from app.services.stream_service import StreamService
from app.db.redis import redis_manager

# Namespace used to derive deterministic UUIDs for inbox deduplication of
# flat (envelope-less) llm-service events, which carry no event_id of their own.
_LLM_EVENT_NAMESPACE = uuid.UUID("6f9c9e1e-6b39-4e2a-9f2a-8f0b6e6b8b3a")


def _derive_event_id(*parts: str) -> UUID:
    """Derives a stable UUID5 inbox key from string parts (e.g. message_id, sequence_number)."""
    return uuid.uuid5(_LLM_EVENT_NAMESPACE, ":".join(parts))


async def start_kafka_consumer():
    """
    Spawns the async polling consumer loop, routing events to service handlers.
    Commit offsets only after handler updates successfully complete.
    """
    consumer = kafka_manager.consumer
    if not consumer:
        logger.error("Kafka Consumer connection not active. Skipping polling loop.")
        return

    logger.info("Starting background Kafka consumer loop...")

    # Instantiate services and repos
    inbox_repo = CassandraInboxRepository()
    conv_repo = CassandraConversationRepository()
    msg_repo = CassandraMessageRepository()
    cache_service = CacheService(redis_client=redis_manager.client)

    conv_service = ConversationService(repo=conv_repo, cache_service=cache_service)
    msg_service = MessageService(repo=msg_repo, cache_service=cache_service)
    stream_service = StreamService(redis_client=redis_manager.client)

    try:
        async for msg in consumer:
            try:
                # 1. Parse JSON payload
                payload_str = msg.value.decode("utf-8") if isinstance(msg.value, bytes) else msg.value

                if isinstance(payload_str, dict):
                    event = payload_str
                else:
                    event = json.loads(payload_str)

                # 2. Route by topic. chat.response.* events from llm-service are flat
                # (no envelope, no top-level event_id) — handle them via dedicated
                # branches with a derived inbox key. Everything else uses the
                # canonical envelope shape.
                if msg.topic == KafkaTopics.CHAT_RESPONSE_CHUNK:
                    await _handle_chat_response_chunk(event, inbox_repo, stream_service)
                    await consumer.commit()
                    continue

                if msg.topic == KafkaTopics.CHAT_RESPONSE_GENERATED:
                    await _handle_chat_response_generated(
                        event, inbox_repo, msg_service, stream_service
                    )
                    await consumer.commit()
                    continue

                # 3. Canonical envelope-shaped events (e.g. conversation.title.generated)
                event_id_str = event.get("event_id")
                event_type = event.get("event_type")
                inner_payload = event.get("payload", {})

                if not event_id_str:
                    logger.warning("Consumed Kafka message without event_id envelope. Skipping.", topic=msg.topic)
                    await consumer.commit()
                    continue

                event_id = UUID(event_id_str)

                if inbox_repo.exists(event_id):
                    logger.info("Kafka event already processed (deduplication hit). Skipping.", event_id=event_id_str)
                    await consumer.commit()
                    continue

                logger.info("Received Kafka event for consumption", topic=msg.topic, event_id=event_id_str)

                if msg.topic == KafkaTopics.TITLE_GENERATED:
                    conversation_id = UUID(inner_payload["conversation_id"])
                    title = inner_payload["title"]
                    await conv_service.set_title(conversation_id, title)
                    logger.info("Processed conversation title update from Kafka", conversation_id=str(conversation_id))

                inbox_repo.save(event_id)
                await consumer.commit()

            except Exception as e:
                logger.error("Failed to process consumed Kafka message", topic=msg.topic, error=str(e))
                # Do not commit to allow recovery or retry of this partition offset
                await asyncio.sleep(1) # throttled retry wait
    except asyncio.CancelledError:
        logger.info("Kafka consumer background loop shutdown signal received.")
    except Exception as e:
        logger.error("Fatal error in background Kafka consumer loop", error=str(e))


async def _handle_chat_response_chunk(
    event: dict,
    inbox_repo: CassandraInboxRepository,
    stream_service: StreamService,
) -> None:
    """
    Handles a flat chat.response.chunk event published by llm-service.
    Forwards the chunk to Redis PubSub so the SSE stream can deliver it.
    """
    message_id = event.get("message_id")
    conversation_id_str = event.get("conversation_id")
    sequence_number = event.get("sequence_number")

    if not message_id or not conversation_id_str:
        logger.warning("chat.response.chunk missing required fields. Skipping.", event=event)
        return

    dedup_id = _derive_event_id("chat.response.chunk", str(message_id), str(sequence_number))
    if inbox_repo.exists(dedup_id):
        logger.info("Duplicate chat.response.chunk skipped via inbox dedup.", message_id=message_id)
        return

    conversation_id = UUID(conversation_id_str)
    await stream_service.publish_token(conversation_id, {
        "conversation_id": conversation_id_str,
        "message_id": message_id,
        "sender": "assistant",
        "content": event.get("content", ""),
        "sequence_number": sequence_number,
        # Never final: only chat.response.generated ends a turn. It carries
        # the complete text and citations, and can arrive after the last
        # chunk, so flagging the last chunk final made clients stop early.
        "is_final": False,
    })

    inbox_repo.save(dedup_id)


async def _handle_chat_response_generated(
    event: dict,
    inbox_repo: CassandraInboxRepository,
    msg_service: MessageService,
    stream_service: StreamService,
) -> None:
    """
    Handles a flat chat.response.generated event published by llm-service.
    On success, persists the final assistant message to Cassandra. On error,
    marks the message failed and pushes a terminal SSE error event.
    """
    message_id = event.get("request_message_id") or event.get("message_id")
    conversation_id_str = event.get("conversation_id")

    if not message_id or not conversation_id_str:
        logger.warning("chat.response.generated missing required fields. Skipping.", event=event)
        return

    dedup_id = _derive_event_id("chat.response.generated", str(message_id))
    if inbox_repo.exists(dedup_id):
        logger.info("Duplicate chat.response.generated skipped via inbox dedup.", message_id=message_id)
        return

    conversation_id = UUID(conversation_id_str)
    status = event.get("status", "success")
    # Terminal event received: the turn is answered (successfully or not), so the
    # timeout worker must not fire for it later.
    await msg_service.clear_pending_response(UUID(message_id))

    if status == "success":
        full_content = event.get("full_content") or ""
        await msg_service.finalize_assistant_message(conversation_id, UUID(message_id), full_content)
        await stream_service.publish_token(conversation_id, {
            "conversation_id": conversation_id_str,
            "message_id": message_id,
            "sender": "assistant",
            "content": "",
            "is_final": True,
            "status": "completed",
            # Retrieval sources (file/page/section) for grounded answers; not
            # persisted with the message, so only the live stream carries them.
            "citations": event.get("citations") or [],
            # Chunks and the final event travel on separate topics, so the final
            # event can overtake the last chunks; clients should replace their
            # streamed buffer with this complete text.
            "full_content": full_content,
        })
        logger.info("Processed successful chat.response.generated from Kafka", message_id=message_id)
    else:
        error_message = event.get("error_message") or "Generation failed."
        msg_service.repo.create_message_direct(
            conversation_id=conversation_id,
            message_id=UUID(message_id),
            sender="assistant",
            content=error_message,
            status="failed",
        )
        await msg_service._invalidate_cache(conversation_id)
        await stream_service.publish_token(conversation_id, {
            "conversation_id": conversation_id_str,
            "message_id": message_id,
            "sender": "assistant",
            "content": error_message,
            "is_final": True,
            "status": "failed",
        })
        logger.warning("Processed error chat.response.generated from Kafka", message_id=message_id, error=error_message)

    inbox_repo.save(dedup_id)
