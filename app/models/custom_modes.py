"""
Custom modes: API schemas, validation and the mode-reference convention.

A custom mode is a user-owned, reusable instruction set that runs on one of the EXISTING execution paths
(`direct` = the standard generation path, `smart` = the Smart workflow). It is configuration, not code: it cannot
define graphs, grant tools or change safety rules. Instructions are treated downstream as user-provided guidance.

Mode reference convention (used in `mode` of a message and `default_mode` of preferences):
    built-in mode:   "tutor", "code", ...      (validated against llm-service's live mode registry)
    custom mode:     "custom:<uuid>"           (validated against PostgreSQL, owner-scoped)
"""

import re
import unicodedata
from datetime import datetime
from enum import Enum
from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.core.config import settings

CUSTOM_PREFIX = "custom:"
_CUSTOM_REF = re.compile(r"^custom:([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})$")


class ExecutionStyle(str, Enum):
    DIRECT = "direct"
    SMART = "smart"


class ModeStatus(str, Enum):
    ACTIVE = "active"
    ARCHIVED = "archived"


def parse_custom_ref(value: Optional[str]) -> Optional[UUID]:
    """The UUID of a `custom:<uuid>` reference, else None (not a custom reference)."""
    m = _CUSTOM_REF.match((value or "").strip())
    return UUID(m.group(1)) if m else None


def is_custom_ref(value: Optional[str]) -> bool:
    return (value or "").strip().lower().startswith(CUSTOM_PREFIX)


def custom_ref(mode_id: UUID) -> str:
    return f"{CUSTOM_PREFIX}{mode_id}"


def normalize_name(name: str) -> str:
    """Case-insensitive, whitespace-collapsed key used for per-owner uniqueness."""
    return " ".join(unicodedata.normalize("NFKC", name).split()).casefold()


def _clean_text(value: str, *, multiline: bool) -> str:
    """Strips control characters (keeping newlines/tabs for multi-line text) and surrounding space."""
    allowed = "\n\t" if multiline else ""
    return "".join(ch for ch in value if ch.isprintable() or ch in allowed).strip()


def _name(v: str) -> str:
    cleaned = " ".join(_clean_text(v, multiline=False).split())
    if not cleaned:
        raise ValueError("Name cannot be empty.")
    if len(cleaned) > settings.CUSTOM_MODE_NAME_MAX_CHARS:
        raise ValueError(f"Name is limited to {settings.CUSTOM_MODE_NAME_MAX_CHARS} characters.")
    return cleaned


def _description(v: str) -> str:
    cleaned = " ".join(_clean_text(v or "", multiline=False).split())
    if len(cleaned) > settings.CUSTOM_MODE_DESCRIPTION_MAX_CHARS:
        raise ValueError(f"Description is limited to {settings.CUSTOM_MODE_DESCRIPTION_MAX_CHARS} characters.")
    return cleaned


def _instructions(v: str) -> str:
    cleaned = _clean_text(v or "", multiline=True)
    if not cleaned:
        raise ValueError("Instructions cannot be empty.")
    if len(cleaned) > settings.CUSTOM_MODE_INSTRUCTIONS_MAX_CHARS:
        raise ValueError(f"Instructions are limited to {settings.CUSTOM_MODE_INSTRUCTIONS_MAX_CHARS} characters.")
    return cleaned


class CustomModeCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")   # an owner id (or any unknown field) in the body is rejected

    name: str
    description: str = ""
    instructions: str
    execution_style: ExecutionStyle = ExecutionStyle.DIRECT

    @field_validator("name")
    @classmethod
    def _n(cls, v):
        return _name(v)

    @field_validator("description")
    @classmethod
    def _d(cls, v):
        return _description(v)

    @field_validator("instructions")
    @classmethod
    def _i(cls, v):
        return _instructions(v)


class CustomModeUpdate(BaseModel):
    """PATCH body: only the fields sent change. `version` (from the last read) turns the write into a
    compare-and-set: a stale version is rejected with 409 instead of overwriting a newer edit."""

    model_config = ConfigDict(extra="forbid")

    name: Optional[str] = None
    description: Optional[str] = None
    instructions: Optional[str] = None
    execution_style: Optional[ExecutionStyle] = None
    version: Optional[int] = Field(None, ge=1)

    @field_validator("name")
    @classmethod
    def _n(cls, v):
        return None if v is None else _name(v)

    @field_validator("description")
    @classmethod
    def _d(cls, v):
        return None if v is None else _description(v)

    @field_validator("instructions")
    @classmethod
    def _i(cls, v):
        return None if v is None else _instructions(v)

    def changes(self) -> dict:
        return {k: v for k, v in self.model_dump(exclude={"version"}).items() if v is not None}


class CustomModeResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    mode_id: UUID
    mode_ref: str                       # "custom:<uuid>": what to send as `mode` / `default_mode`
    name: str
    description: str
    instructions: str
    execution_style: ExecutionStyle
    status: ModeStatus
    version: int
    created_at: datetime
    updated_at: datetime
    archived_at: Optional[datetime] = None

    @classmethod
    def from_record(cls, r) -> "CustomModeResponse":
        return cls(mode_id=r.mode_id, mode_ref=custom_ref(r.mode_id), name=r.name, description=r.description,
                   instructions=r.instructions, execution_style=r.execution_style, status=r.status,
                   version=r.version, created_at=r.created_at, updated_at=r.updated_at, archived_at=r.archived_at)


class CustomModeList(BaseModel):
    items: List[CustomModeResponse]
    total: int
    limit: int
    offset: int
    max_per_user: int
    limits: dict = Field(default_factory=dict)      # {name, description, instructions}: max characters (server config)


class CustomModeSnapshot(BaseModel):
    """The immutable definition that travels with a request (event contract). Execution is deterministic:
    replays and in-flight requests use exactly this version even if the mode is edited or archived later."""

    mode_id: str
    version: int
    name: str
    execution_style: ExecutionStyle
    instructions: str

    @classmethod
    def from_record(cls, r) -> "CustomModeSnapshot":
        return cls(mode_id=str(r.mode_id), version=r.version, name=r.name,
                   execution_style=r.execution_style, instructions=r.instructions)
