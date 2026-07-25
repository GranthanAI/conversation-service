"""
Domain Entities File - Conversation.
Contains the plain Python dataclass representation of the Conversation entity,
representing the core entity state independent of database implementations.
"""

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID
from core.enums import ConversationStatus

@dataclass
class ConversationEntity:
    """
    Core Domain Dataclass representing a Conversation.
    """
    conversation_id: UUID
    user_id: UUID
    title: str
    created_at: datetime
    updated_at: datetime
    status: ConversationStatus
