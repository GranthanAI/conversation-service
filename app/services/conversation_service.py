"""
Conversation Service.
Coordinates atomic transactional batch writes for conversation entities and outbox events,
integrating Cache-Aside caching and direct Kafka event publishing.
"""

import json
import uuid
from typing import List, Optional
from datetime import datetime, timedelta, timezone
from uuid import UUID

from app.repositories.conversation_repository import CassandraConversationRepository
from app.models.conversation import Conversation, ConversationStatus
from app.events.topics import KafkaTopics
from app.events.producers import build_event_envelope
from app.utils.helpers import uuidv7
from app.services.cache_service import CacheService
from app.clients.kafka_producer import KafkaProducerClient
from app.core.config import settings

class TemporaryConversationError(Exception):
    """The operation is not allowed on a temporary chat (branch from it, edit, share): HTTP 409 temporary_conversation."""


class ConversationService:
    """
    Service orchestrator handling conversational metadata pipelines.
    """
    def __init__(
        self,
        repo: CassandraConversationRepository,
        cache_service: Optional[CacheService] = None,
        producer_client: Optional[KafkaProducerClient] = None
    ):
        self.repo = repo
        self.cache = cache_service
        self.producer = producer_client

    async def get(self, conversation_id: UUID) -> Optional[Conversation]:
        """
        Fetches conversation metadata using Read-Through Cache-Aside.
        """
        if self.cache:
            conv = await self.cache.get_conversation(conversation_id)
            if conv:
                return conv

        conv = self.repo.get(conversation_id)
        if conv and self.cache:
            await self.cache.set_conversation(conv)
        return conv

    async def create(self, user_id: UUID, title: str, parent_conversation_id: Optional[UUID] = None,
                     branch_message_id: Optional[UUID] = None, message_repo=None,
                     plan_limits=None, temporary: bool = False,
                     edited_from_message_id: Optional[UUID] = None) -> Conversation:
        """
        Creates a conversation atomically alongside its outbox task,
        writes through to cache, and publishes directly to Kafka.

        temporary=True: a temporary chat (no parent allowed). It is written to `conversations` only (never to
        conversations_by_user) with expires_at = now + TEMPORARY_CHAT_TTL_HOURS; conversation.created is still emitted
        with payload.temporary = true. The caller registers it with TemporaryService for the expiry sweep.
        edited_from_message_id: set when this conversation was created by editing a message.
        """
        if temporary and parent_conversation_id:
            raise ValueError("A temporary chat cannot be a branch.")
        # Validate parent lineage if specified
        if parent_conversation_id:
            parent_conv = await self.get(parent_conversation_id)
            if not parent_conv:
                raise LookupError("Parent conversation not found.")
            if parent_conv.status != ConversationStatus.ACTIVE:
                raise ValueError("Parent conversation is not active.")
            if parent_conv.user_id != user_id:
                raise PermissionError("Access to parent conversation denied.")
            if parent_conv.temporary:
                raise TemporaryConversationError("A temporary chat cannot be branched.")
            if plan_limits is not None:
                await plan_limits.check_new_branch(user_id, parent_conversation_id)     # 403 plan_limit
            # Branch point: an explicit message must belong to the parent; otherwise the parent's latest
            # message right now is frozen as the boundary, so later parent messages never leak in.
            if message_repo is not None:
                if branch_message_id is not None:
                    if message_repo.get_message(parent_conversation_id, branch_message_id) is None:
                        raise ValueError("branch_message_id is not a message of the parent conversation.")
                else:
                    latest = message_repo.history(parent_conversation_id, limit=1)
                    branch_message_id = latest[0].message_id if latest else None
        else:
            branch_message_id = None

        conversation_id = uuidv7()
        event_id = uuid.uuid1()
        now = datetime.now(timezone.utc)
        expires_at = now + timedelta(hours=settings.TEMPORARY_CHAT_TTL_HOURS) if temporary else None

        envelope = build_event_envelope(
            event_type=KafkaTopics.CONVERSATION_CREATED,
            payload={
                "conversation_id":        str(conversation_id),
                "user_id":                str(user_id),
                "title":                  title,
                "status":                 "ACTIVE",
                "parent_conversation_id": str(parent_conversation_id) if parent_conversation_id else None,
                "branch_message_id":      str(branch_message_id) if branch_message_id else None,
                "created_at":             now.isoformat(),
                # graph-service skips temporary conversations; edited_from_message_id is stored on the Conversation node
                "temporary":              bool(temporary),
                "edited_from_message_id": str(edited_from_message_id) if edited_from_message_id else None,
            },
            causation_id=str(event_id),
        )
        # Overwrite event_id so it matches the one we store in Cassandra
        envelope["event_id"] = str(event_id)

        conv = self.repo.create_with_outbox(
            conversation_id=conversation_id,
            user_id=user_id,
            title=title,
            status="active",
            event_id=event_id,
            event_type=KafkaTopics.CONVERSATION_CREATED,
            outbox_payload=json.dumps(envelope),
            parent_conversation_id=parent_conversation_id,
            branch_message_id=branch_message_id,
            temporary=temporary,
            expires_at=expires_at,
            edited_from_message_id=edited_from_message_id,
        )
        
        # Write-through update
        if self.cache:
            await self.cache.set_conversation(conv)
            
        return conv

    async def rename(self, conversation_id: UUID, new_title: str) -> Optional[Conversation]:
        """
        Renames a conversation title atomically, updates cache, and schedules outbox task.
        """
        conv = await self.get(conversation_id)
        if not conv:
            return None

        event_id = uuid.uuid1()
        envelope = build_event_envelope(
            event_type=KafkaTopics.CONVERSATION_UPDATED,
            payload={
                "conversation_id": str(conversation_id),
                "user_id":         str(conv.user_id),
                "title":           new_title,
                "status":          conv.status.value,
            },
            causation_id=str(event_id),
        )
        envelope["event_id"] = str(event_id)

        updated = self.repo.update_with_outbox(
            conversation_id=conversation_id,
            title=new_title,
            status=conv.status,
            event_id=event_id,
            event_type=KafkaTopics.CONVERSATION_UPDATED,
            outbox_payload=json.dumps(envelope)
        )
        
        if updated and self.cache:
            await self.cache.set_conversation(updated)
        return updated

    async def archive(self, conversation_id: UUID) -> Optional[Conversation]:
        """
        Archives a conversation atomically, updates cache, and schedules outbox task.
        """
        conv = await self.get(conversation_id)
        if not conv:
            return None

        event_id = uuid.uuid1()
        envelope = build_event_envelope(
            event_type=KafkaTopics.CONVERSATION_UPDATED,
            payload={
                "conversation_id": str(conversation_id),
                "user_id":         str(conv.user_id),
                "title":           conv.title,
                "status":          "archived",
            },
            causation_id=str(event_id),
        )
        envelope["event_id"] = str(event_id)

        archived = self.repo.update_with_outbox(
            conversation_id=conversation_id,
            title=conv.title,
            status="archived",
            event_id=event_id,
            event_type=KafkaTopics.CONVERSATION_UPDATED,
            outbox_payload=json.dumps(envelope)
        )
        
        if archived and self.cache:
            await self.cache.set_conversation(archived)
        return archived

    async def delete(self, conversation_id: UUID) -> bool:
        """
        Soft-deletes a conversation atomically, invalidates cache, and schedules outbox task.
        """
        conv = await self.get(conversation_id)
        if not conv:
            return False

        event_id = uuid.uuid1()
        envelope = build_event_envelope(
            event_type=KafkaTopics.CONVERSATION_DELETED,
            payload={
                "conversation_id": str(conversation_id),
                "user_id":         str(conv.user_id),
                "title":           conv.title,
                "status":          "deleted",
            },
            causation_id=str(event_id),
        )
        envelope["event_id"] = str(event_id)

        success = self.repo.delete_with_outbox(
            conversation_id=conversation_id,
            event_id=event_id,
            event_type=KafkaTopics.CONVERSATION_DELETED,
            outbox_payload=json.dumps(envelope)
        )
        
        if success and self.cache:
            await self.cache.delete_conversation(conversation_id)
        return success

    def list(self, user_id: UUID, limit: int = settings.CONVERSATION_LIST_DEFAULT_LIMIT, cursor: Optional[datetime] = None) -> List[Conversation]:
        """
        Lists user conversations.
        """
        return self.repo.list(user_id, limit, cursor)

    async def set_title(self, conversation_id: UUID, title: str) -> bool:
        """
        Directly sets the title of a conversation in Cassandra and evicts its cache key.
        """
        success = self.repo.update_title(conversation_id, title)
        if success and self.cache:
            await self.cache.delete_conversation(conversation_id)
        return success
