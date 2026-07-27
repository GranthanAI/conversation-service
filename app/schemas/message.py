"""
Message Pydantic Schemas.
Request validation forms and response DTOs for Message endpoints.
"""

from typing import List, Optional
from datetime import datetime
from uuid import UUID
from pydantic import BaseModel, Field, field_validator

class CreateMessageRequest(BaseModel):
    """
    Form model for message creation.
    """
    content: str = Field(..., description="Message text content", min_length=1, max_length=8000)

    @field_validator("content")

    def validate_content(cls, v: str) -> str:
        trimmed = v.strip()
        if not trimmed:
            raise ValueError("Message content cannot be empty or whitespace.")
        return trimmed

class MessageResponse(BaseModel):
    """
    Serialized DTO for message entity responses.
    """
    message_id: UUID
    conversation_id: UUID
    sender: str
    content: str
    created_at: datetime
    status: str

    class Config:
        from_attributes = True

class MessageListResponse(BaseModel):
    """
    Paginated response container for message history.
    """
    items: List[MessageResponse]
    next_cursor: Optional[UUID] = None
