"""PostgreSQL model: a user's personal Notes document for one conversation.

Conversation Service owns it (PostgreSQL already holds preferences, custom modes, artifacts and exports). `conversation_id`
points at the Cassandra conversation catalog, which cannot be a foreign key; ownership and "is this conversation still
active" are therefore checked by the API against the authoritative conversation record on EVERY request.

Notes are user-authored content only: they are never sent to the Memory Service, Kafka, a prompt or an index.
"""

import uuid
from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, Integer, Text, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base

# Hard ceiling enforced by the database. The API limit (settings.NOTES_MAX_CHARS) is lower and is what users meet.
NOTES_DB_MAX_CHARS = 200_000


class ConversationNotesRecord(Base):
    __tablename__ = "conversation_notes"

    id: Mapped[uuid.UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    owner_id: Mapped[uuid.UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    conversation_id: Mapped[uuid.UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False, server_default="")
    # Optimistic concurrency: starts at 1 on the first save, +1 on every change. "No document yet" is version 0 on the API.
    version: Mapped[int] = mapped_column(Integer, nullable=False, server_default="1")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    __table_args__ = (
        # one document per user per conversation (also the lookup index: every query filters on both columns)
        UniqueConstraint("owner_id", "conversation_id", name="uq_conversation_notes_owner_conversation"),
        CheckConstraint("version >= 1", name="ck_conversation_notes_version_positive"),
        CheckConstraint(f"char_length(content) <= {NOTES_DB_MAX_CHARS}", name="ck_conversation_notes_content_len"),
    )
