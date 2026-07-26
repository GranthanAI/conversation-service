"""
Conversation Schema DTOs.
Validates HTTP request bodies and serializes conversation records.
"""

from datetime import datetime
from uuid import UUID
from pydantic import BaseModel, Field, field_validator
from app.models.conversation import ConversationStatus

class CreateConversationRequest(BaseModel):
    """
    Validates conversation creation input parameters.
    """
    title: str = Field(..., min_length=1, max_length=255, description="Initial conversation title")

    @field_validator("title")
    @classmethod
    def clean_title(cls, v: str) -> str:
        cleaned = v.strip()
        if not cleaned:
            raise ValueError("title cannot be empty or whitespace only")
        return cleaned

class RenameConversationRequest(BaseModel):
    """
    Validates conversation rename input parameters.
    """
    title: str = Field(..., min_length=1, max_length=255, description="New conversation title")

    @field_validator("title")
    @classmethod
    def clean_title(cls, v: str) -> str:
        cleaned = v.strip()
        if not cleaned:
            raise ValueError("title cannot be empty or whitespace only")
        return cleaned

class ConversationResponse(BaseModel):
    """
    Serializes a single conversation details response record.
    """
    conversation_id: UUID
    user_id: UUID
    title: str
    created_at: datetime
    updated_at: datetime
    status: ConversationStatus

    class Config:
        from_attributes = True
