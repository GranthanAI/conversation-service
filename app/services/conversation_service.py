"""
Conversation Service.
Coordinates atomic transactional batch writes for conversation entities and outbox events.
"""

import json
import uuid
from typing import List, Optional
from datetime import datetime
from uuid import UUID
from app.repositories.conversation_repository import CassandraConversationRepository
from app.models.conversation import Conversation
from app.events.topics import KafkaTopics
from app.utils.helpers import uuidv7

class ConversationService:
    """
    Service orchestrator handling conversational metadata pipelines atomically.
    """
    def __init__(self, repo: CassandraConversationRepository):
        self.repo = repo

    def create(self, user_id: UUID, title: str) -> Conversation:
        """
        Creates a conversation atomically alongside its outbox task in a single batch.
        """
        conversation_id = uuidv7()
        event_id = uuid.uuid1()
        
        payload = {
            "conversation_id": str(conversation_id),
            "user_id": str(user_id),
            "title": title,
            "status": "active"
        }
        
        return self.repo.create_with_outbox(
            conversation_id=conversation_id,
            user_id=user_id,
            title=title,
            status="active",
            event_id=event_id,
            event_type=KafkaTopics.CONVERSATION_CREATED,
            outbox_payload=json.dumps(payload)
        )

    def rename(self, conversation_id: UUID, new_title: str) -> Optional[Conversation]:
        """
        Renames a conversation title atomically alongside its outbox task in a single batch.
        """
        conv = self.repo.get(conversation_id)
        if not conv:
            return None

        event_id = uuid.uuid1()
        payload = {
            "conversation_id": str(conversation_id),
            "user_id": str(conv.user_id),
            "title": new_title,
            "status": str(conv.status)
        }
        
        return self.repo.update_with_outbox(
            conversation_id=conversation_id,
            title=new_title,
            status=conv.status,
            event_id=event_id,
            event_type=KafkaTopics.CONVERSATION_UPDATED,
            outbox_payload=json.dumps(payload)
        )

    def archive(self, conversation_id: UUID) -> Optional[Conversation]:
        """
        Archives a conversation atomically alongside its outbox task in a single batch.
        """
        conv = self.repo.get(conversation_id)
        if not conv:
            return None

        event_id = uuid.uuid1()
        payload = {
            "conversation_id": str(conversation_id),
            "user_id": str(conv.user_id),
            "title": conv.title,
            "status": "archived"
        }
        
        return self.repo.update_with_outbox(
            conversation_id=conversation_id,
            title=conv.title,
            status="archived",
            event_id=event_id,
            event_type=KafkaTopics.CONVERSATION_UPDATED,
            outbox_payload=json.dumps(payload)
        )

    def delete(self, conversation_id: UUID) -> bool:
        """
        Soft-deletes a conversation atomically alongside its outbox task in a single batch.
        """
        conv = self.repo.get(conversation_id)
        if not conv:
            return False

        event_id = uuid.uuid1()
        payload = {
            "conversation_id": str(conversation_id),
            "user_id": str(conv.user_id),
            "title": conv.title,
            "status": "deleted"
        }
        
        return self.repo.delete_with_outbox(
            conversation_id=conversation_id,
            event_id=event_id,
            event_type=KafkaTopics.CONVERSATION_DELETED,
            outbox_payload=json.dumps(payload)
        )

    def list(self, user_id: UUID, limit: int = 20, cursor: Optional[datetime] = None) -> List[Conversation]:
        """
        Lists user conversations.
        """
        return self.repo.list(user_id, limit, cursor)
