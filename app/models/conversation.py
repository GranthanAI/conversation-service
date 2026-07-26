"""
Conversation Domain Entity.
Defines python dataclasses mapping conversation states.
"""

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID
from enum import Enum

class ConversationStatus(str, Enum):
    """
    Taxonomy codes matching active state lifecycle of conversations.
    """
    ACTIVE = "active"
    ARCHIVED = "archived"
    DELETED = "deleted"

@dataclass
class Conversation:
    """
    Plain python model representing conversational context logs.
    """
    conversation_id: UUID
    user_id: UUID
    title: str
    created_at: datetime
    updated_at: datetime
    status: ConversationStatus = ConversationStatus.ACTIVE
