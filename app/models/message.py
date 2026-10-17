"""
Message Domain Entity.
Defines message schemas containing user requests and LLM replies.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional
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
    file_ids: List[UUID] = field(default_factory=list)  # attached File Service document_ids, "Ask Files" mode
    # Set on the object returned by send()/regenerate(): the pending assistant message
    # the reply will be written to and streamed under. Not a stored column.
    response_message_id: Optional[UUID] = None
