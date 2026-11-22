"""PostgreSQL models: user preferences and custom modes (the only authoritative store for both)."""

import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import (
    ARRAY, BigInteger, Boolean, Date, LargeBinary, SmallInteger, CheckConstraint, DateTime, ForeignKey, Index, Integer, String, Text, UniqueConstraint, func, text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID as PGUUID
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
    default_model: Mapped[str] = mapped_column(String(64), nullable=False, server_default="auto")
    answer_length: Mapped[str] = mapped_column(String(16), nullable=False, server_default="balanced")
    tone: Mapped[str] = mapped_column(String(16), nullable=False, server_default="professional")
    language: Mapped[str] = mapped_column(String(16), nullable=False, server_default="auto")
    personal_instructions: Mapped[str] = mapped_column(Text, nullable=False, server_default="")
    citations: Mapped[str] = mapped_column(String(16), nullable=False, server_default="show")
    document_context: Mapped[str] = mapped_column(String(24), nullable=False, server_default="conversation")
    suggest_followups: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))
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


class UserDeletion(Base):
    """user.deleted: the user's routes answer 404 at once; their data is purged after `purge_after` (USER_DATA_RETENTION_DAYS)."""

    __tablename__ = "user_deletions"

    user_id: Mapped[uuid.UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    deleted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    purge_after: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    purged_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)


class ConversationMessageCounter(Base):
    """USER messages sent in a conversation's own turns (not inherited branch history); regenerate adds nothing.
    Created lazily from one Cassandra count the first time a limit needs it, then incremented in the send transaction."""

    __tablename__ = "conversation_message_counters"

    conversation_id: Mapped[uuid.UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    user_message_count: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    __table_args__ = (CheckConstraint("user_message_count >= 0", name="ck_conv_msg_counters_nonneg"),)


class TreeDocument(Base):
    """Distinct File Service document ids attached anywhere in a conversation tree (root_id = the tree's root)."""

    __tablename__ = "tree_documents"

    root_id: Mapped[uuid.UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    document_id: Mapped[uuid.UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class MessageFeedback(Base):
    """A user's thumbs up/down on one assistant message (one row per user and message)."""

    __tablename__ = "message_feedback"

    user_id: Mapped[uuid.UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    message_id: Mapped[uuid.UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    conversation_id: Mapped[uuid.UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    rating: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    reason: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    comment: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    model: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    model_group: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    __table_args__ = (
        CheckConstraint("rating IN (-1, 1)", name="ck_message_feedback_rating"),
        Index("ix_message_feedback_rating_created", "rating", "created_at"),
    )


class MessageEdit(Base):
    """A branch created by editing a message: which message was edited and where the edited version went."""

    __tablename__ = "message_edits"

    branch_conversation_id: Mapped[uuid.UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    original_message_id: Mapped[uuid.UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    original_conversation_id: Mapped[uuid.UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    user_id: Mapped[uuid.UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    __table_args__ = (Index("ix_message_edits_original_message", "original_message_id"),)


class SharedLink(Base):
    """A public read-only snapshot. Only sha256(token) is stored; the token is shown once at creation."""

    __tablename__ = "shared_links"

    share_id: Mapped[uuid.UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    user_id: Mapped[uuid.UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    conversation_id: Mapped[uuid.UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    scope: Mapped[str] = mapped_column(String(16), nullable=False)
    token_hash: Mapped[bytes] = mapped_column(LargeBinary, nullable=False, unique=True)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    object_key: Mapped[str] = mapped_column(Text, nullable=False)
    message_count: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    refreshed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    revoked_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    view_count: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default="0")

    __table_args__ = (Index("ix_shared_links_user_created", "user_id", "created_at"),)


class TemporaryConversation(Base):
    """Bookkeeping for temporary chats (not in conversations_by_user, so the expiry worker lists them here)."""

    __tablename__ = "temporary_conversations"

    conversation_id: Mapped[uuid.UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    user_id: Mapped[uuid.UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    deleted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index("ix_temporary_conversations_due", "expires_at", postgresql_where=text("deleted_at IS NULL")),
        Index("ix_temporary_conversations_user", "user_id"),
    )


class ThinkUsageDaily(Base):
    """Think-longer answers a user has started per IST day (the daily plan limit)."""

    __tablename__ = "think_usage_daily"

    user_id: Mapped[uuid.UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    day: Mapped[datetime] = mapped_column(Date, primary_key=True)
    count: Mapped[int] = mapped_column(Integer, nullable=False)

    __table_args__ = (CheckConstraint("count >= 0", name="ck_think_usage_daily_nonneg"),)
