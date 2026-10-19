"""PostgreSQL models: user preferences and custom modes (the only authoritative store for both)."""

import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import (
    ARRAY, CheckConstraint, DateTime, ForeignKey, Index, Integer, String, Text, UniqueConstraint, func, text,
)
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class CustomModeRecord(Base):
    """A user-owned reusable mode. Soft-deleted (status='archived') so saved defaults, historical requests and audit
    records can still refer to it."""

    __tablename__ = "custom_modes"

    mode_id: Mapped[uuid.UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    owner_user_id: Mapped[uuid.UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    name_normalized: Mapped[str] = mapped_column(String(120), nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False, server_default="")
    instructions: Mapped[str] = mapped_column(Text, nullable=False)
    execution_style: Mapped[str] = mapped_column(String(16), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, server_default="active")
    version: Mapped[int] = mapped_column(Integer, nullable=False, server_default="1")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    archived_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        CheckConstraint("length(btrim(name)) > 0", name="ck_custom_modes_name_not_blank"),
        CheckConstraint("length(btrim(instructions)) > 0", name="ck_custom_modes_instructions_not_blank"),
        CheckConstraint("execution_style IN ('direct', 'smart')", name="ck_custom_modes_execution_style"),
        CheckConstraint("status IN ('active', 'archived')", name="ck_custom_modes_status"),
        CheckConstraint("version >= 1", name="ck_custom_modes_version_positive"),
        # one ACTIVE mode per (owner, case-insensitive name); archived modes free the name again
        Index("uq_custom_modes_owner_active_name", "owner_user_id", "name_normalized", unique=True,
              postgresql_where=text("status = 'active'")),
        Index("ix_custom_modes_owner_status_updated", "owner_user_id", "status", "updated_at"),
    )


class UserPreferenceRecord(Base):
    """One row per user. No row = built-in defaults (the API fills them in)."""

    __tablename__ = "user_preferences"

    user_id: Mapped[uuid.UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    # Built-in mode id, or NULL for Auto. A custom default is stored as a real foreign key instead.
    default_mode: Mapped[Optional[str]] = mapped_column(String(40), nullable=True)
    default_custom_mode_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("custom_modes.mode_id", ondelete="SET NULL"), nullable=True)
    default_tools: Mapped[list[str]] = mapped_column(ARRAY(String(64)), nullable=False, server_default=text("'{}'"))
    answer_length: Mapped[str] = mapped_column(String(16), nullable=False, server_default="balanced")
    tone: Mapped[str] = mapped_column(String(16), nullable=False, server_default="professional")
    language: Mapped[str] = mapped_column(String(16), nullable=False, server_default="auto")
    personal_instructions: Mapped[str] = mapped_column(Text, nullable=False, server_default="")
    citations: Mapped[str] = mapped_column(String(16), nullable=False, server_default="show")
    document_context: Mapped[str] = mapped_column(String(24), nullable=False, server_default="conversation")
    version: Mapped[int] = mapped_column(Integer, nullable=False, server_default="1")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    __table_args__ = (
        CheckConstraint("version >= 1", name="ck_user_preferences_version_positive"),
        CheckConstraint("answer_length IN ('concise', 'balanced', 'detailed')", name="ck_user_preferences_answer_length"),
        CheckConstraint("tone IN ('professional', 'friendly', 'simple')", name="ck_user_preferences_tone"),
        CheckConstraint("citations IN ('show', 'hide')", name="ck_user_preferences_citations"),
        CheckConstraint("document_context IN ('conversation', 'attached_only')", name="ck_user_preferences_document_context"),
        CheckConstraint("char_length(personal_instructions) <= 1000", name="ck_user_preferences_instructions_len"),
        CheckConstraint("default_mode IS NULL OR default_custom_mode_id IS NULL", name="ck_user_preferences_one_default_mode"),
    )
