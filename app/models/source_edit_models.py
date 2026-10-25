"""PostgreSQL model: a user's edited copy of one source block (code, text or Markdown) of one assistant message.

The assistant message (Cassandra) is never modified. An edit is a separate, owner-scoped, versioned row that points at the
message and a stable block id; `original_sha256` fingerprints the immutable original so the UI can tell which block an
edit belongs to and detect a mismatch instead of applying an edit to different text. The artifact registry was not reused:
it is a content-addressed, shareable-render store (fingerprint-unique, status lifecycle, object storage), whereas an edit is
mutable, per-user and versioned.

Like notes, `conversation_id`/`message_id` point at the Cassandra catalog and cannot be foreign keys; the API checks
ownership against the authoritative conversation record on every request.
"""

import uuid
from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, Integer, String, Text, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base

SOURCE_EDIT_DB_MAX_CHARS = 200_000
SOURCE_FORMATS = ("code", "text", "markdown")


class SourceEditRecord(Base):
    __tablename__ = "source_edits"

    id: Mapped[uuid.UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    owner_id: Mapped[uuid.UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    conversation_id: Mapped[uuid.UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    message_id: Mapped[uuid.UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    block_id: Mapped[str] = mapped_column(String(80), nullable=False)
    language: Mapped[str] = mapped_column(String(32), nullable=False, server_default="")
    source_format: Mapped[str] = mapped_column(String(16), nullable=False)
    original_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False, server_default="")
    version: Mapped[int] = mapped_column(Integer, nullable=False, server_default="1")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    __table_args__ = (
        UniqueConstraint("owner_id", "conversation_id", "message_id", "block_id", name="uq_source_edits_block"),
        CheckConstraint("version >= 1", name="ck_source_edits_version_positive"),
        CheckConstraint(f"char_length(content) <= {SOURCE_EDIT_DB_MAX_CHARS}", name="ck_source_edits_content_len"),
        CheckConstraint("source_format IN ('code','text','markdown')", name="ck_source_edits_format"),
    )
