"""PostgreSQL models: the artifact registry (Mermaid diagrams, Manim animations) and export jobs.

conversation-service owns these tables. `conversation_id` / `source_message_id` point at Cassandra data, which cannot be
a foreign key; ownership of the hierarchy stays in the conversation catalog and is resolved at read time (the registry
never copies parent/child links).
"""

import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import (
    BigInteger, CheckConstraint, DateTime, ForeignKey, Index, Integer, String, Text, UniqueConstraint, func, text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base

ARTIFACT_TYPES = ("mermaid", "manim")
ARTIFACT_STATUSES = ("pending", "queued", "rendering", "uploading", "ready", "failed")
EXPORT_STATUSES = ("pending", "processing", "ready", "failed", "expired")


class ArtifactRecord(Base):
    """One logical artifact, unique per (owner, type, fingerprint). The same animation referenced by many messages,
    sidebar views and exports is ONE row (ArtifactRefRecord holds the references)."""

    __tablename__ = "artifacts"

    artifact_id: Mapped[uuid.UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    owner_user_id: Mapped[uuid.UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    artifact_type: Mapped[str] = mapped_column(String(16), nullable=False)
    title: Mapped[str] = mapped_column(String(200), nullable=False, server_default="")
    description: Mapped[str] = mapped_column(Text, nullable=False, server_default="")
    fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    fingerprint_version: Mapped[int] = mapped_column(Integer, nullable=False, server_default="1")
    spec_text: Mapped[str] = mapped_column(Text, nullable=False)           # canonical Mermaid source / Manim spec JSON
    status: Mapped[str] = mapped_column(String(16), nullable=False, server_default="pending")
    render_job_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    object_key: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    thumbnail_key: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    mime_type: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    size_bytes: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    checksum_sha256: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    renderer_version: Mapped[str] = mapped_column(String(64), nullable=False, server_default="")
    failure_code: Mapped[Optional[str]] = mapped_column(String(48), nullable=True)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")   # automatic renders begun
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")    # explicit user retries
    lease_until: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    origin_conversation_id: Mapped[uuid.UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    origin_message_id: Mapped[Optional[uuid.UUID]] = mapped_column(PGUUID(as_uuid=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    __table_args__ = (
        CheckConstraint("artifact_type IN ('mermaid', 'manim')", name="ck_artifacts_type"),
        CheckConstraint("status IN ('pending', 'queued', 'rendering', 'uploading', 'ready', 'failed')",
                        name="ck_artifacts_status"),
        CheckConstraint("attempt_count >= 0", name="ck_artifacts_attempts"),
        CheckConstraint("char_length(spec_text) <= 20000", name="ck_artifacts_spec_len"),
        CheckConstraint("char_length(fingerprint) = 64", name="ck_artifacts_fingerprint_len"),
        # a ready video must be backed by a verified object; a ready diagram needs no object
        CheckConstraint(
            "artifact_type <> 'manim' OR status <> 'ready' OR "
            "(object_key IS NOT NULL AND checksum_sha256 IS NOT NULL AND size_bytes > 0)",
            name="ck_artifacts_ready_manim_has_object"),
        # the deduplication scope: one artifact per owner + type + fingerprint (never across users)
        UniqueConstraint("owner_user_id", "artifact_type", "fingerprint", name="uq_artifacts_owner_type_fingerprint"),
        Index("ix_artifacts_owner_created", "owner_user_id", "created_at"),
        Index("ix_artifacts_active", "status", "lease_until",
              postgresql_where=text("status NOT IN ('ready', 'failed')")),
    )


class ArtifactRefRecord(Base):
    """Where an artifact appears: (conversation, message, block). The hierarchy is NOT stored here."""

    __tablename__ = "artifact_refs"

    artifact_id: Mapped[uuid.UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("artifacts.artifact_id", ondelete="CASCADE"), primary_key=True)
    conversation_id: Mapped[uuid.UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    source_message_id: Mapped[uuid.UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    block_id: Mapped[str] = mapped_column(String(64), primary_key=True, server_default="")
    owner_user_id: Mapped[uuid.UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    __table_args__ = (
        Index("ix_artifact_refs_conversation", "conversation_id", "owner_user_id"),
    )


class ExportJobRecord(Base):
    """A conversation / hierarchy export. `request_key` hashes the owner, scope, options AND the source snapshot, so an
    unchanged source reuses a live export and a changed one never serves stale content."""

    __tablename__ = "export_jobs"

    job_id: Mapped[uuid.UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    owner_user_id: Mapped[uuid.UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    conversation_id: Mapped[uuid.UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    scope: Mapped[str] = mapped_column(String(16), nullable=False)             # conversation | hierarchy
    format: Mapped[str] = mapped_column(String(16), nullable=False)            # markdown | pdf
    include_media: Mapped[bool] = mapped_column(nullable=False, server_default=text("false"))
    request_key: Mapped[str] = mapped_column(String(64), nullable=False)
    snapshot: Mapped[dict] = mapped_column(JSONB, nullable=False)              # {conversation_id: latest_message_id|null}
    status: Mapped[str] = mapped_column(String(16), nullable=False, server_default="pending")
    progress_done: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    progress_total: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    object_key: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    filename: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    content_type: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    size_bytes: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    checksum_sha256: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    failure_code: Mapped[Optional[str]] = mapped_column(String(48), nullable=True)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    lease_until: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    __table_args__ = (
        CheckConstraint("scope IN ('conversation', 'hierarchy')", name="ck_export_jobs_scope"),
        CheckConstraint("format IN ('markdown', 'pdf')", name="ck_export_jobs_format"),
        CheckConstraint("status IN ('pending', 'processing', 'ready', 'failed', 'expired')",
                        name="ck_export_jobs_status"),
        CheckConstraint("progress_done >= 0 AND progress_total >= 0", name="ck_export_jobs_progress"),
        # at most one LIVE job per request (a failed/expired one can be requested again)
        Index("uq_export_jobs_live_request", "owner_user_id", "request_key", unique=True,
              postgresql_where=text("status IN ('pending', 'processing', 'ready')")),
        Index("ix_export_jobs_owner_created", "owner_user_id", "created_at"),
        Index("ix_export_jobs_active", "status", "lease_until",
              postgresql_where=text("status IN ('pending', 'processing')")),
        Index("ix_export_jobs_expiry", "expires_at", postgresql_where=text("status = 'ready'")),
    )
