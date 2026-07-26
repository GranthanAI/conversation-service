"""
Message Schema DTOs.
Validates HTTP request bodies and serializes message exchange logs.
"""

from datetime import datetime
from uuid import UUID
from pydantic import BaseModel, Field, field_validator
from app.models.message import MessageStatus

class CreateMessageRequest(BaseModel):
    """
    Validates message submission input parameters.
    """
    content: str = Field(..., min_length=1, max_length=8000, description="Text content of the message")

    @field_validator("content")
    @classmethod
    def clean_content(cls, v: str) -> str:
        cleaned = v.strip()
        if not cleaned:
            raise ValueError("content cannot be empty or whitespace only")
        return cleaned

class MessageResponse(BaseModel):
    """
    Serializes a single message record.
    """
    conversation_id: UUID
    message_id: UUID
    sender: str
    content: str
    created_at: datetime
    status: MessageStatus

    class Config:
        from_attributes = True
