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
    # Source references of a final assistant answer (see app/services/citations.py); [] for older messages.
    citations: List[dict] = field(default_factory=list)
    # Catalog model id + group that answered (assistant messages from llm-service 2.2 events); None for older rows.
    model: Optional[str] = None
    model_group: Optional[str] = None
    # Follow-up questions generated after an answer (chat.response.suggestions); [] when none.
    suggestions: List[str] = field(default_factory=list)
    # Assistant rows: the answer call used provider reasoning ("think longer").
    think: bool = False
    # The caller's own thumbs rating, attached when serving history (not stored on the row): {rating, reason} or None.
    feedback: Optional[dict] = None
    # Set on the object returned by send()/regenerate(): the pending assistant message
    # the reply will be written to and streamed under. Not a stored column.
    response_message_id: Optional[UUID] = None
    # Set on the object returned by send()/regenerate() when billing is on (BILLING_ENFORCEMENT != off). Not stored.
    billing: Optional[dict] = None
    defaults_applied: Optional[dict] = None
