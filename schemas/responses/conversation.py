"""
Schemas Responses File - Conversation.
Defines Pydantic response models for serializing conversation attributes
and paginated collections to clients.
"""

from pydantic import BaseModel
from datetime import datetime
from uuid import UUID
from typing import List, Optional
from core.enums import ConversationStatus

class ConversationResponse(BaseModel):
    """
    Pydantic schema representing conversation attributes returned to clients.
    """
    conversation_id: UUID
    user_id: UUID
    title: str
    created_at: datetime
    updated_at: datetime
    status: ConversationStatus

    class Config:
        from_attributes = True

class ConversationListResponse(BaseModel):
    """
    Pydantic schema representing a paginated list of conversations.
    """
    conversations: List[ConversationResponse]
    next_cursor: Optional[str] = None
