"""
Message Service.
Coordinates Cache-Aside caching, database operations, and direct Kafka event publishing.
"""

import json
import uuid
from typing import List, Optional
from datetime import datetime, timezone
from uuid import UUID

from app.repositories.message_repository import CassandraMessageRepository
from app.models.message import Message, MessageStatus
from app.events.topics import KafkaTopics
from app.core.logging import logger
from app.services.cache_service import CacheService
from app.clients.kafka_producer import KafkaProducerClient
from app.core.config import settings
from app.middleware.correlation import get_correlation_id
from app.utils.helpers import uuidv7, uuidv7_after
from app.services.preferences_service import PreferencesService, ResolvedMessagePreferences


class PendingResponseRegistrationError(Exception):
    """The Redis timeout marker for a pending assistant message could not be registered.

    A request must not be dispatched without timeout protection, otherwise a lost
    generation would leave the client waiting forever, so the send is aborted and the
    placeholder removed."""


# Bounds for the history attached to each request event.
HISTORY_EVENT_MAX_MESSAGES = 10
HISTORY_EVENT_MAX_MESSAGE_CHARS = 2000
HISTORY_EVENT_MAX_CHARS = 8000


class MessageService:
    """
    Service orchestrator handling conversational message logs and streaming triggers.
    """
    def __init__(
        self,
        repo: CassandraMessageRepository,
        cache_service: Optional[CacheService] = None,
        producer_client: Optional[KafkaProducerClient] = None,
        preferences_service: Optional["PreferencesService"] = None,
        branch_context=None,
    ):
        self.repo = repo
        self.cache = cache_service
        self.producer = producer_client
        # Optional: without it messages carry no saved defaults (built-in behaviour).
        self.preferences = preferences_service
        # Optional: resolves what a branch inherits from its ancestors (see BranchContextService).
        self.branch_context = branch_context

    async def _resolve_preferences(
        self, user_id: Optional[UUID], mode: Optional[str], selected_tools: Optional[List[str]]
    ) -> ResolvedMessagePreferences:
        if self.preferences is None:
            return ResolvedMessagePreferences(mode=mode, selected_tools=selected_tools,
                                              defaults_applied={"mode": False, "tools": False})
        return await self.preferences.resolve_for_message(user_id, mode, selected_tools)

    async def _invalidate_cache(self, conversation_id: UUID):
        """
        Deletes the cached message history list from Redis via CacheService.
        """
        if self.cache:
            await self.cache.delete_last_50_messages(conversation_id)

    def _build_chat_message_created_payload(
        self,
        conversation_id: UUID,
        message_id: UUID,
        user_id: Optional[UUID],
        content: str,
        mode: Optional[str] = None,
        selected_tools: Optional[List[str]] = None,
        file_ids: Optional[List[UUID]] = None,
        response_message_id: Optional[UUID] = None,
        metadata: Optional[dict] = None,
        history: Optional[List[dict]] = None,
        preferences: Optional[dict] = None,
        defaults_applied: Optional[dict] = None,
        ancestors: Optional[List[dict]] = None,
        custom_mode: Optional[dict] = None,
        include_diagrams: bool = False,
    ) -> dict:
        """
        Builds the flat chat.message.created payload matching llm-service's
        ChatMessageCreatedEvent (app/models/request.py) exactly - no envelope
        wrapper, since llm-service's Kafka consumer deserializes the raw
        message body directly into that Pydantic model.

        Identity contract (schema_version 2.1):
          message_id            request / correlation id. For a send this is the user's
                                message id; for a regeneration it is a fresh request id.
          response_message_id   the pending ASSISTANT message this request will fill,
                                allocated here before dispatch. llm-service echoes it
                                unchanged on every chunk / final / error event.
        file_ids are the attached File Service document_ids.

        Preferences (schema_version 2.2, additive; older consumers ignore them):
          mode_hint / selected_tools   already resolved: the message's explicit choice, or
                                       the user's saved default when the message had none.
          defaults_applied             {"mode": bool, "tools": bool}: which of the two came
                                       from saved defaults (llm-service skips an unavailable
                                       default instead of rejecting the message).
          preferences                  generation preferences (length, tone, language,
                                       personal instructions, citations) at send time.
        """
        now = datetime.now(timezone.utc).isoformat()
        correlation_id = get_correlation_id()
        return {
            "event_type": "chat.message.created",
            "schema_version": "2.3",
            "message_id": str(message_id),
            "response_message_id": str(response_message_id) if response_message_id else None,
            "conversation_id": str(conversation_id),
            "user_id": str(user_id) if user_id else "",
            "content": content,
            "mode_hint": mode,
            "selected_tools": selected_tools or [],
            # Per-message opt-in (additive, schema 2.3): offer diagram blocks in any mode and nudge the model to use one if it helps.
            "include_diagrams": bool(include_diagrams),
            "file_ids": [str(fid) for fid in file_ids] if file_ids else [],
            # Recent turns before this request (oldest first), so llm-service has the conversation even when
            # the memory service is slow or down. Optional and additive: older consumers ignore it.
            "history": history or [],
            # Branch lineage with boundaries (root first): lets other services apply the same branch point to
            # summaries. Empty for a root conversation.
            "ancestors": ancestors or [],
            # Trusted, immutable definition of the custom mode this request runs on (resolved here from
            # PostgreSQL, owner-scoped). mode_hint is then the BUILT-IN path it executes on ("default"/"smart").
            # llm-service never resolves custom modes itself and ignores any client-supplied instruction text.
            "custom_mode": custom_mode,
            "preferences": preferences or {},
            "defaults_applied": defaults_applied or {"mode": False, "tools": False},
            "metadata": metadata or {},
            "trace_context": {"traceparent": correlation_id, "tracestate": ""},
            "timestamp": now,
        }

    async def send(
        self,
        conversation_id: UUID,
        message_id: UUID,
        sender: str,
        content: str,
        user_id: Optional[UUID] = None,
        mode: Optional[str] = None,
        selected_tools: Optional[List[str]] = None,
        file_ids: Optional[List[UUID]] = None,
        include_diagrams: bool = False,
    ) -> Message:
        """
        Persists a message atomically (message + outbox event in one LOGGED BATCH) and
        returns it. Response generation happens asynchronously when llm-service
        publishes chat.response.chunk / chat.response.generated back over Kafka.

        For a user message the assistant reply's identity is fixed FIRST, and ordering
        guarantees that before the outbox row can be published:
          1. the response id is allocated (always sorts after the user message),
          2. the pending assistant row exists under that id,
          3. the Redis timeout marker is registered under that same id,
          4. the request event carries that id.
        Cassandra and Redis do not share a transaction, so each step has an explicit
        compensation: if a later step fails, the earlier ones are undone (placeholder
        soft-deleted, marker removed) and the error is raised to the caller.
        """
        response_message_id = uuidv7_after(message_id) if sender == "user" else None
        event_id = uuid.uuid1()
        # Read BEFORE this turn's rows exist, so the history is strictly what came earlier.
        history = await self._recent_history(conversation_id) if sender == "user" else []
        ancestors: List[dict] = []
        if sender == "user" and self.branch_context is not None:
            history, ancestors = await self._branch_event_context(conversation_id, history)
        resolved = await self._resolve_preferences(user_id, mode, selected_tools)
        payload = self._build_chat_message_created_payload(
            conversation_id, message_id, user_id, content, resolved.mode, resolved.selected_tools, file_ids,
            response_message_id=response_message_id,
            history=history,
            preferences=resolved.generation,
            defaults_applied=resolved.defaults_applied,
            ancestors=ancestors,
            custom_mode=resolved.custom_mode.model_dump(mode="json") if resolved.custom_mode else None,
            include_diagrams=include_diagrams,
        )

        if response_message_id:
            await self._open_pending_response(conversation_id, response_message_id)

        try:
            msg = self.repo.create_with_outbox(
                conversation_id=conversation_id,
                message_id=message_id,
                sender=sender,
                content=content,
                status="sent",
                event_id=event_id,
                event_type=KafkaTopics.CHAT_MESSAGE_CREATED,
                outbox_payload=json.dumps(payload),
                file_ids=file_ids
            )
        except Exception:
            if response_message_id:
                await self._discard_pending_response(conversation_id, response_message_id)
            raise

        # Invalidate history cache
        await self._invalidate_cache(conversation_id)
        msg.response_message_id = response_message_id
        return msg

    async def _recent_history(self, conversation_id: UUID) -> List[dict]:
        """Recent completed turns for the request event (best effort: a history read failure must never
        block sending a message)."""
        try:
            msgs = await self.history(conversation_id, limit=HISTORY_EVENT_MAX_MESSAGES * 2)
        except Exception as e:
            logger.warning("Could not read history for the request event", conversation_id=str(conversation_id), error=str(e))
            return []
        return self._format_history(msgs)

    async def _with_inherited_history(self, conversation_id: UUID, own: List[dict]) -> List[dict]:
        history, _ancestors = await self._branch_event_context(conversation_id, own)
        return history

    async def _branch_event_context(self, conversation_id: UUID, own: List[dict]) -> tuple:
        """(history, ancestors) for the request event. History = what this branch inherits from its ancestors
        (up to each branch point, marked `inherited` with its source conversation) followed by its own turns;
        ancestors = the branch lineage with boundaries, so other services can apply the same boundaries to
        summaries. Best effort: never blocks sending."""
        try:
            conv = await self.branch_context.conversations.get(conversation_id)
            if conv is None or not conv.parent_conversation_id:
                return own, []
            inherited, ancestors = await self.branch_context.event_context(conv)
        except Exception as e:
            logger.warning("Could not attach inherited history", conversation_id=str(conversation_id), error=str(e))
            return own, []
        marked = [{"role": h["role"], "content": h["content"], "inherited": True,
                   "source_conversation_id": h["conversation_id"]} for h in inherited]
        return marked + own, ancestors

    @staticmethod
    def _format_history(messages_newest_first: List[Message]) -> List[dict]:
        """Newest-first rows -> oldest-first [{role, content, message_id}], keeping only finished user/assistant text.
        Pending placeholders, failed replies and system/summary rows are skipped; each message and the
        total are size-capped so the event stays small."""
        picked: List[dict] = []
        budget = HISTORY_EVENT_MAX_CHARS
        for m in messages_newest_first:
            if m.sender not in ("user", "assistant"):
                continue
            status = getattr(m.status, "value", m.status)
            text = (m.content or "").strip()
            if status != "sent" or not text:
                continue
            text = text[:HISTORY_EVENT_MAX_MESSAGE_CHARS]
            if budget - len(text) < 0 and picked:
                break
            budget -= len(text)
            # message_id is additive: lets the LLM service recognise the same turn arriving from memory-service too.
            picked.append({"role": m.sender, "content": text, "message_id": str(m.message_id)})
            if len(picked) >= HISTORY_EVENT_MAX_MESSAGES:
                break
        picked.reverse()
        return picked

    async def _open_pending_response(self, conversation_id: UUID, response_message_id: UUID) -> None:
        """Creates the pending assistant row and registers its timeout marker (in that
        order). If the marker cannot be registered the row is removed again."""
        self.repo.create_message_direct(
            conversation_id=conversation_id,
            message_id=response_message_id,
            sender="assistant",
            content="",
            status="pending",
        )
        try:
            await self._register_pending_response(conversation_id, response_message_id)
        except PendingResponseRegistrationError:
            self._soft_delete_placeholder(conversation_id, response_message_id)
            raise

    async def _discard_pending_response(self, conversation_id: UUID, response_message_id: UUID) -> None:
        """Compensation for a send that failed after the placeholder/marker were created."""
        self._soft_delete_placeholder(conversation_id, response_message_id)
        await self.clear_pending_response(response_message_id)

    def _soft_delete_placeholder(self, conversation_id: UUID, response_message_id: UUID) -> None:
        try:
            self.repo.delete(conversation_id, response_message_id)
        except Exception as e:  # compensation is best effort; never mask the original error
            logger.error("Failed to remove pending assistant placeholder",
                         message_id=str(response_message_id), error=str(e))

    async def _register_pending_response(self, conversation_id: UUID, message_id: UUID) -> None:
        """
        Records a pending-response marker in Redis so the PendingTimeoutWorker can
        fail a message that never receives a chat.response.generated event.

        `message_id` is the ASSISTANT response id. Raises
        PendingResponseRegistrationError when the marker cannot be written: dispatching
        a request without timeout protection could strand the client indefinitely.
        """
        if not self.cache or not self.cache.redis:
            raise PendingResponseRegistrationError("Redis is not available for pending-response tracking")
        try:
            key = f"pending_response:{message_id}"
            await self.cache.redis.set(
                key,
                str(conversation_id),
                ex=settings.PENDING_RESPONSE_TIMEOUT_SECONDS + settings.PENDING_RESPONSE_MARKER_GRACE_SECONDS,
            )
            await self.cache.redis.zadd(
                "pending_response:zset",
                {str(message_id): datetime.now(timezone.utc).timestamp() + settings.PENDING_RESPONSE_TIMEOUT_SECONDS},
            )
        except Exception as e:
            logger.warning("Failed to register pending-response timeout marker", message_id=str(message_id), error=str(e))
            # Do not leave a half-registered marker behind.
            await self.clear_pending_response(message_id)
            raise PendingResponseRegistrationError(str(e)) from e

    async def clear_pending_response(self, message_id: UUID) -> None:
        """Removes the pending-response marker once a terminal response (success
        or error) has been handled, so the timeout worker never fails a message
        that was already answered."""
        try:
            await self.cache.redis.delete(f"pending_response:{message_id}")
            await self.cache.redis.zrem("pending_response:zset", str(message_id))
        except Exception as e:
            logger.warning("Failed to clear pending-response marker", message_id=str(message_id), error=str(e))

    async def history(self, conversation_id: UUID, limit: int = settings.MESSAGE_HISTORY_DEFAULT_LIMIT, cursor: Optional[UUID] = None) -> List[Message]:
        """
        Returns message history page using Cache-Aside.
        """
        if cursor is None and self.cache:
            messages = await self.cache.get_last_50_messages(conversation_id, limit)
            if messages is not None:
                return messages

        logger.info("Message history cache miss. Fetching from database...", conversation_id=conversation_id)
        messages = self.repo.history(conversation_id, limit, cursor)

        if cursor is None and self.cache and messages:
            await self.cache.set_last_50_messages(conversation_id, messages)

        return messages

    async def delete(self, conversation_id: UUID, message_id: UUID) -> bool:
        """
        Soft-deletes a message and invalidates cache.
        """
        success = self.repo.delete(conversation_id, message_id)
        if success:
            await self._invalidate_cache(conversation_id)
            return True
        return False

    async def regenerate(self, conversation_id: UUID, message_id: UUID, user_id: Optional[UUID] = None) -> Optional[Message]:
        """
        Regenerates an assistant message.

        Identity: a regeneration is a NEW request, so it gets a fresh request/correlation
        id (llm-service de-duplicates on it, so reusing the original would drop the
        event) and a fresh assistant response id that sorts after the prompt. The
        original prompt row, its id and its attached file_ids are untouched; the prompt
        id is recorded in the event metadata for tracing.

        Ordering matches send(): pending row + timeout marker first, then the outbox
        event; the previous assistant answer is soft-deleted only after the new request
        is staged, so a failed regeneration never costs the user their existing answer.
        """
        history_msgs = self.repo.history(conversation_id, limit=settings.CACHE_HISTORY_LIMIT)

        target_msg = None
        prompt_msg = None
        older_than_prompt: List[Message] = []

        for i, m in enumerate(history_msgs):
            if m.message_id == message_id:
                target_msg = m
                if i + 1 < len(history_msgs):
                    prompt_msg = history_msgs[i + 1]
                    older_than_prompt = history_msgs[i + 2:]
                break

        if not target_msg:
            return None

        request_id = uuidv7()
        response_message_id = uuidv7_after(prompt_msg.message_id if prompt_msg else target_msg.message_id)
        prompt_content = prompt_msg.content if prompt_msg else ""
        prompt_user_id = getattr(prompt_msg, "user_id", None)

        # Carries the original prompt's file_ids forward so a regenerated response is
        # re-derived against the same attached documents.
        owner_id = user_id or prompt_user_id
        # A regeneration carries no explicit mode/tool choice: the saved defaults apply.
        resolved = await self._resolve_preferences(owner_id, None, None)
        payload = self._build_chat_message_created_payload(
            conversation_id, request_id, owner_id, prompt_content,
            mode=resolved.mode,
            selected_tools=resolved.selected_tools,
            preferences=resolved.generation,
            defaults_applied=resolved.defaults_applied,
            file_ids=prompt_msg.file_ids if prompt_msg else None,
            response_message_id=response_message_id,
            history=self._format_history(older_than_prompt),
            metadata={
                "regeneration": True,
                "prompt_message_id": str(prompt_msg.message_id) if prompt_msg else None,
                "replaces_message_id": str(message_id),
            },
        )

        await self._open_pending_response(conversation_id, response_message_id)
        try:
            self.repo.create_with_outbox(
                conversation_id=conversation_id,
                message_id=response_message_id,
                sender="assistant",
                content="",
                status="pending",
                event_id=uuid.uuid1(),
                event_type=KafkaTopics.CHAT_MESSAGE_CREATED,
                outbox_payload=json.dumps(payload),
            )
        except Exception:
            await self._discard_pending_response(conversation_id, response_message_id)
            raise

        if target_msg.sender == "assistant":
            try:
                self.repo.delete(conversation_id, message_id)  # soft delete the replaced answer
            except Exception as e:
                logger.error("Failed to soft-delete replaced assistant message",
                             message_id=str(message_id), error=str(e))

        await self._invalidate_cache(conversation_id)
        target_msg.response_message_id = response_message_id
        return target_msg

    def _is_writable_assistant_row(self, conversation_id: UUID, message_id: UUID) -> bool:
        """False if `message_id` already belongs to a non-assistant (user/system) row.

        The table key is (conversation_id, message_id), so writing assistant output
        under a user message's id would silently replace the user's message. This is the
        last line of defence if an upstream ever sends a wrong response id."""
        existing = self.repo.get_message(conversation_id, message_id)
        if existing is not None and existing.sender != "assistant":
            logger.error(
                "Refusing to write assistant output over a non-assistant message",
                conversation_id=str(conversation_id), message_id=str(message_id), existing_sender=existing.sender,
            )
            return False
        return True

    async def finalize_assistant_message(
        self,
        conversation_id: UUID,
        message_id: UUID,
        content: str,
        citations: Optional[List[dict]] = None,
    ) -> Optional[Message]:
        """
        Saves the completed assistant message (with its source citations, if any) and evicts the history cache. `message_id`
        is the ASSISTANT response id. Returns None (and writes nothing) if that id
        belongs to a user message.
        """
        if not self._is_writable_assistant_row(conversation_id, message_id):
            return None
        msg = self.repo.create_message_direct(
            conversation_id=conversation_id,
            message_id=message_id,
            sender="assistant",
            content=content,
            status="sent",
            **({"citations": citations} if citations else {}),
        )
        await self._invalidate_cache(conversation_id)
        return msg

    async def fail_assistant_message(
        self,
        conversation_id: UUID,
        message_id: UUID,
        error_message: str
    ) -> Optional[Message]:
        """Marks the assistant message failed (same user-row guard as finalize)."""
        if not self._is_writable_assistant_row(conversation_id, message_id):
            return None
        existing = self.repo.get_message(conversation_id, message_id)
        if existing is not None and existing.sender == "assistant" and existing.status == "sent" and existing.content:
            # A real reply is already stored; a late failure event must never overwrite it.
            logger.warning("Ignoring late failure for an already-completed reply", message_id=str(message_id))
            return existing
        msg = self.repo.create_message_direct(
            conversation_id=conversation_id,
            message_id=message_id,
            sender="assistant",
            content=error_message,
            status="failed",
        )
        await self._invalidate_cache(conversation_id)
        return msg

    async def attach_summary(
        self,
        conversation_id: UUID,
        summary: str
    ) -> Message:
        """
        Attaches the summary by saving a special system summary message to Cassandra and evicting the cache.
        """
        summary_msg_id = uuid.uuid4()
        msg = self.repo.create_message_direct(
            conversation_id=conversation_id,
            message_id=summary_msg_id,
            sender="system",
            content=f"Summary: {summary}",
            status="sent"
        )
        await self._invalidate_cache(conversation_id)
        return msg

