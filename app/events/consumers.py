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
from app.core.config import settings

from app.repositories.inbox_repository import CassandraInboxRepository
from app.repositories.conversation_repository import CassandraConversationRepository
from app.repositories.message_repository import CassandraMessageRepository
from app.services.cache_service import CacheService
from app.services.citations import sanitize_citations
from app.services.conversation_service import ConversationService
from app.services.entitlements_service import build_entitlements_service
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
    entitlements = build_entitlements_service()

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

                if msg.topic == KafkaTopics.CHAT_RESPONSE_CANCELLED:
                    await _handle_chat_response_cancelled(event, inbox_repo, msg_service)
                    await consumer.commit()
                    continue

                if msg.topic == KafkaTopics.USAGE_THRESHOLD_REACHED:
                    await _handle_usage_threshold(event, inbox_repo, stream_service)
                    await consumer.commit()
                    continue

                if msg.topic in (KafkaTopics.ENTITLEMENTS_CHANGED, KafkaTopics.USER_DELETED):
                    await _handle_entitlements_event(msg.topic, event, inbox_repo, entitlements)
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


async def record_user_deleted(user_id: UUID, session_factory=None) -> None:
    """Idempotent: a replay (or a second user.deleted event) keeps the FIRST deleted_at and purge_after."""
    from datetime import datetime, timedelta, timezone
    from sqlalchemy.dialects.postgresql import insert
    from app.db.session import get_session_factory
    from app.models.sql_models import UserDeletion

    now = datetime.now(timezone.utc)
    async with (session_factory or get_session_factory())() as db:
        await db.execute(insert(UserDeletion).values(
            user_id=user_id, deleted_at=now, purge_after=now + timedelta(days=settings.USER_DATA_RETENTION_DAYS)
        ).on_conflict_do_nothing(index_elements=["user_id"]))
        await db.commit()
    logger.info("user_deleted_scheduled", user_id=str(user_id))


async def _handle_entitlements_event(topic: str, event: dict, inbox_repo, entitlements) -> None:
    """entitlements.changed (billing) and user.deleted (auth): drop the user's cached entitlements (both the 60 s key
    and the 1 h stale key), deduplicated through the inbox on event_id. Deleting the user's conversations on
    user.deleted is NOT done here (out of scope for P6)."""
    event_id_raw, user_id_raw = event.get("event_id"), event.get("user_id")
    try:
        event_id, user_id = UUID(str(event_id_raw)), UUID(str(user_id_raw))
    except ValueError:
        logger.warning("Entitlement event without a valid event_id/user_id. Skipping.", topic=topic)
        return
    if inbox_repo.exists(event_id):
        logger.info("Duplicate entitlement event skipped via inbox dedup.", event_id=str(event_id), topic=topic)
        return
    await entitlements.invalidate(user_id)
    if topic == KafkaTopics.USER_DELETED:
        await record_user_deleted(user_id)              # before the inbox mark: a failure here is retried, not lost
        from app.db.redis import redis_manager as _r
        await StreamService(redis_client=_r.client).close_user_streams(user_id)
    inbox_repo.save(event_id)
    logger.info("entitlements_invalidated", topic=topic, user_id=str(user_id))


async def _handle_chat_response_cancelled(event: dict, inbox_repo, msg_service) -> None:
    """chat.response.cancelled (llm-service 2.2): carries no text, so nothing is finalized here (the pending watchdog
    still settles the turn). It only records which model was answering, on the existing assistant row."""
    response_id = _response_message_id(event, "chat.response.cancelled")
    conversation_id_str = event.get("conversation_id")
    if not response_id or not conversation_id_str:
        logger.warning("chat.response.cancelled without response_message_id or conversation_id. Ignoring.",
                       request_message_id=event.get("message_id"))
        return
    dedup_id = _derive_event_id("chat.response.cancelled", str(response_id))
    if inbox_repo.exists(dedup_id):
        logger.info("Duplicate chat.response.cancelled skipped via inbox dedup.", message_id=str(response_id))
        return
    await msg_service.record_answering_model(
        UUID(conversation_id_str), response_id, event.get("model"), event.get("model_group"))
    inbox_repo.save(dedup_id)


async def _handle_usage_threshold(event: dict, inbox_repo, stream_service) -> None:
    """usage.threshold_reached (billing): push an SSE `notice` {notice_key, metric, used, limit, resets_at} to the
    user's open streams. With no open stream it is dropped (a notice is only useful live)."""
    body = event.get("payload") if isinstance(event.get("payload"), dict) else event
    try:
        user_id = UUID(str(body.get("user_id")))
    except ValueError:
        logger.warning("usage.threshold_reached without a valid user_id. Skipping.")
        return
    metric, percent = body.get("limit"), body.get("percent")
    try:
        event_id = UUID(str(event.get("event_id") or body.get("event_id")))
    except ValueError:      # billing's payload may not carry an event_id: one notice per user, limit, percent and period
        event_id = _derive_event_id("usage.threshold_reached", str(user_id), str(metric), str(percent),
                                    str(body.get("resets_at")))
    if inbox_repo.exists(event_id):
        logger.info("Duplicate usage.threshold_reached skipped via inbox dedup.", event_id=str(event_id))
        return
    delivered = await stream_service.publish_user_notice(user_id, {
        "notice_key": f"usage.threshold_{percent}",
        "metric": metric,
        "used": body.get("used"),
        "limit": body.get("allowed"),
        "resets_at": body.get("resets_at"),
    })
    inbox_repo.save(event_id)
    logger.info("usage_notice_pushed", user_id=str(user_id), metric=metric, percent=percent, streams=delivered)


def _response_message_id(event: dict, kind: str):
    """Parses the assistant message id from a llm-service response event.

    Returns None - and the caller ignores the event - when it is absent or not a UUID.
    There is deliberately NO fallback to `message_id` / `request_message_id`: those are
    the request's identity (the user's message), and writing the reply under them would
    overwrite the user's own row. An ignored turn is failed cleanly by the pending
    timeout instead (this is the rollout path for events produced by llm-service < 2.1)."""
    raw = event.get("response_message_id")
    try:
        return UUID(str(raw)) if raw else None
    except ValueError:
        logger.error(f"{kind} has an invalid response_message_id; ignoring.", response_message_id=raw)
        return None


from app.utils.helpers import uuid7_age_ms  # noqa: E402


async def _handle_chat_response_chunk(
    event: dict,
    inbox_repo: CassandraInboxRepository,
    stream_service: StreamService,
) -> None:
    """
    Handles a flat chat.response.chunk event published by llm-service.
    Forwards the chunk to Redis PubSub so the SSE stream can deliver it.

    SSE `message_id` is the ASSISTANT message (response_message_id); the request's
    id travels separately as `request_message_id`.
    """
    response_id = _response_message_id(event, "chat.response.chunk")
    conversation_id_str = event.get("conversation_id")
    sequence_number = event.get("sequence_number")

    if not response_id or not conversation_id_str:
        logger.warning(
            "chat.response.chunk without response_message_id or conversation_id. Ignoring.",
            request_message_id=event.get("message_id"), conversation_id=conversation_id_str,
        )
        return

    dedup_id = _derive_event_id("chat.response.chunk", str(response_id), str(sequence_number))
    if inbox_repo.exists(dedup_id):
        logger.info("Duplicate chat.response.chunk skipped via inbox dedup.", message_id=str(response_id))
        return

    conversation_id = UUID(conversation_id_str)
    if sequence_number in (0, 1):
        logger.info(
            "turn_timing", stage="first_chunk_published", conversation_id=conversation_id_str,
            response_message_id=str(response_id), request_message_id=event.get("message_id"),
            since_accept_ms=uuid7_age_ms(response_id),
        )
    await stream_service.publish_token(conversation_id, {
        "conversation_id": conversation_id_str,
        "message_id": str(response_id),
        "request_message_id": event.get("message_id"),
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

    The reply is written to, and the timeout marker cleared for, the ASSISTANT message
    id (response_message_id) that send()/regenerate() allocated - never the request id.
    """
    response_id = _response_message_id(event, "chat.response.generated")
    request_message_id = event.get("request_message_id") or event.get("message_id")
    conversation_id_str = event.get("conversation_id")

    if not response_id or not conversation_id_str:
        logger.error(
            "chat.response.generated without response_message_id or conversation_id "
            "(legacy llm-service < 2.1?). Ignoring; the pending timeout will fail this turn.",
            request_message_id=request_message_id, conversation_id=conversation_id_str,
        )
        return

    dedup_id = _derive_event_id("chat.response.generated", str(response_id))
    if inbox_repo.exists(dedup_id):
        logger.info("Duplicate chat.response.generated skipped via inbox dedup.", message_id=str(response_id))
        return
    # llm-service delivers at-least-once; after a restart the same request can arrive again, possibly with a NEW
    # event_id. The request is the identity: once a request's answer is finished, any later terminal event for it is a
    # duplicate (it must not create another message or overwrite the completed one).
    request_done_id = _derive_event_id("request.completed", str(request_message_id)) if request_message_id else None
    if request_done_id is not None and inbox_repo.exists(request_done_id):
        logger.info("Duplicate terminal event for an already answered request ignored.",
                    request_message_id=request_message_id, message_id=str(response_id))
        return

    conversation_id = UUID(conversation_id_str)
    status = event.get("status", "success")
    logger.info(
        "turn_timing", stage="final_received", status=status, conversation_id=conversation_id_str,
        response_message_id=str(response_id), request_message_id=request_message_id,
        since_accept_ms=uuid7_age_ms(response_id),
    )
    base_payload = {
        "conversation_id": conversation_id_str,
        "message_id": str(response_id),
        "request_message_id": request_message_id,
        "sender": "assistant",
        "is_final": True,
    }

    if status == "success":
        if msg_service.is_completed_reply(UUID(conversation_id_str), response_id):
            logger.info("Terminal event for an already completed reply ignored.", message_id=str(response_id))
            await msg_service.clear_pending_response(response_id)
            inbox_repo.save(dedup_id)
            if request_done_id is not None:
                inbox_repo.save(request_done_id)
            return
        full_content = event.get("full_content") or ""
        model, model_group = event.get("model") or None, event.get("model_group") or None
        # Persisted with the message (the durable record); the SSE event below is only live delivery.
        citations = sanitize_citations(event.get("citations"))
        extra = {k: v for k, v in (("model", model), ("model_group", model_group)) if v}    # 2.1 events carry neither
        if citations:
            written = await msg_service.finalize_assistant_message(
                conversation_id, response_id, full_content, citations=citations, **extra)
        else:
            written = await msg_service.finalize_assistant_message(conversation_id, response_id, full_content, **extra)
        if written is None:
            logger.error("chat.response.generated refused: response_message_id belongs to a non-assistant message.",
                         message_id=str(response_id), conversation_id=conversation_id_str)
            return
        await msg_service.clear_pending_response(response_id)
        await stream_service.publish_token(conversation_id, {
            **base_payload,
            "content": "",
            "status": "completed",
            # Retrieval sources (file/page/section) for grounded answers. Same validated list that was
            # persisted above, so live and reloaded views agree.
            "citations": citations,
            # Chunks and the final event travel on separate topics, so the final
            # event can overtake the last chunks; clients should replace their
            # streamed buffer with this complete text.
            "full_content": full_content,
            # Which catalog model answered (llm-service 2.2). Absent for older events.
            **extra,
        })
        if request_done_id is not None:
            inbox_repo.save(request_done_id)
        logger.info("Processed successful chat.response.generated from Kafka", message_id=str(response_id))
    else:
        error_message = event.get("error_message") or "Generation failed."
        written = await msg_service.fail_assistant_message(conversation_id, response_id, error_message)
        if written is None:
            logger.error("chat.response.generated (error) refused: response_message_id belongs to a non-assistant message.",
                         message_id=str(response_id), conversation_id=conversation_id_str)
            return
        await msg_service.clear_pending_response(response_id)
        await stream_service.publish_token(conversation_id, {
            **base_payload,
            "content": error_message,
            "status": "failed",
        })
        logger.warning("Processed error chat.response.generated from Kafka", message_id=str(response_id), error=error_message)

    inbox_repo.save(dedup_id)
