"""
Message Pydantic Schemas.
Request validation forms and response DTOs for Message endpoints.
"""

from typing import List, Optional
from datetime import datetime
from uuid import UUID
import re

from pydantic import BaseModel, Field, field_validator

# Tool ids are registry names (e.g. web_search). Shape-checked here; whether a tool exists and is
# enabled is decided by llm-service (the registry owner) before anything runs.
_TOOL_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,63}$")
MAX_SELECTED_TOOLS = 5  # must equal llm-service app/tools/selection.py MAX_SELECTED_TOOLS

from app.models.custom_modes import parse_custom_ref

class CreateMessageRequest(BaseModel):
    """
    Form model for message creation.
    """
    content: str = Field(..., description="Message text content", min_length=1, max_length=8000)
    mode: Optional[str] = Field(None, description="Requested generation mode, e.g. 'smart', 'default'")
    selected_tools: Optional[List[str]] = Field(None, description="Explicitly selected optional tools")
    include_diagrams: bool = Field(False, description="Ask the model to include a Mermaid/Manim diagram when it would help")
    file_ids: Optional[List[UUID]] = Field(
        None, description="Attached File Service document_ids for Ask Files mode"
    )

    @field_validator("content")
    def validate_content(cls, v: str) -> str:
        trimmed = v.strip()
        if not trimmed:
            raise ValueError("Message content cannot be empty or whitespace.")
        return trimmed

    @field_validator("mode")
    def validate_mode(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        v = v.strip()
        # Shape only. A built-in id is validated by llm-service (which owns the mode registry); a
        # `custom:<uuid>` reference is validated against PostgreSQL (owner-scoped) when the message is sent.
        if v.lower().startswith("custom:"):
            if parse_custom_ref(v) is None:
                raise ValueError("Invalid custom mode reference.")
            return v.lower()
        if v and not re.fullmatch(r"[A-Za-z0-9_-]{1,32}", v):
            raise ValueError("Invalid mode.")
        return v or None

    @field_validator("selected_tools")
    def validate_selected_tools(cls, v: Optional[List[str]]) -> Optional[List[str]]:
        if v is None:
            return v
        cleaned: List[str] = []
        for raw in v:
            name = raw.strip()
            if not _TOOL_ID.match(name):
                raise ValueError(f"Invalid tool id: {raw[:64]!r}")
            if name not in cleaned:  # a tool is selected at most once
                cleaned.append(name)
        if len(cleaned) > MAX_SELECTED_TOOLS:
            raise ValueError(f"At most {MAX_SELECTED_TOOLS} tools can be selected.")
        return cleaned

class CitationOut(BaseModel):
    """One persisted source reference. Mirrors the frontend `Citation`; see app/services/citations.py."""
    file_id: str
    source_name: str = ""
    page_number: Optional[int] = None
    section: Optional[str] = None
    start_seconds: Optional[int] = None
    end_seconds: Optional[int] = None


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
    file_ids: List[UUID] = Field(default_factory=list)
    # Sources of an assistant answer (document pages / video time ranges); [] for user messages and for
    # answers saved before citations were stored.
    citations: List[CitationOut] = Field(default_factory=list)
    # Present on POST .../messages and .../regenerate: the assistant message that the
    # reply streams under (SSE `message_id`). Distinct from `message_id`.
    response_message_id: Optional[UUID] = None

    model_config = {"from_attributes": True}

class MessageListResponse(BaseModel):
    """
    Paginated response container for message history.
    """
    items: List[MessageResponse]
    next_cursor: Optional[str] = None
    has_more: bool = False
