"""
Message Domain Entity.
Defines message schemas containing user requests and LLM replies.
"""

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID
from enum import Enum

class MessageStatus(str, Enum):
    """
    Message state codes.
    """
    PENDING = "pending"
    SENT = "sent"
    FAILED = "failed"

@dataclass
class Message:
    """
    Plain python model representing conversational message events.
    """
    conversation_id: UUID
    message_id: UUID
    sender: str  # "user" or "assistant"
    content: str
    created_at: datetime
    status: MessageStatus = MessageStatus.PENDING
