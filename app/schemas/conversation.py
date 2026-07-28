"""
Conversation Pydantic Schemas.
Request validation forms and serialization DTOs for Conversation endpoints.
"""

from typing import List, Optional
from datetime import datetime
from uuid import UUID
from pydantic import BaseModel, Field, field_validator

class CreateConversationRequest(BaseModel):
    """
    Form model for conversation creation.
    """
    title: str = Field(..., description="Initial title for conversation catalog", min_length=1, max_length=255)

    @field_validator("title")
    def validate_title(cls, v: str) -> str:
        trimmed = v.strip()
        if not trimmed:
            raise ValueError("Conversation title cannot be empty or whitespace.")
        return trimmed

class RenameConversationRequest(BaseModel):
    """
    Form model for renaming conversation.
    """
    title: str = Field(..., description="New title for conversation catalog", min_length=1, max_length=255)

    @field_validator("title")
    def validate_title(cls, v: str) -> str:
        trimmed = v.strip()
        if not trimmed:
            raise ValueError("Conversation title cannot be empty or whitespace.")
        return trimmed

class ConversationResponse(BaseModel):
    """
    Serialized DTO for conversation entity responses.
    """
    conversation_id: UUID
    user_id: UUID
    title: str
    created_at: datetime
    updated_at: datetime
    status: str

    model_config = {"from_attributes": True}

class ConversationListResponse(BaseModel):
    """
    Paginated response container for user conversation catalogs.
    """
    items: List[ConversationResponse]
    next_cursor: Optional[str] = None
    has_more: bool = False
