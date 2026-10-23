"""artifact registry and export jobs

Revision ID: 0002
Revises: 0001
Create Date: 2026-10-23

Adds the registry that makes Mermaid / Manim artifacts durable and de-duplicated (unique per owner + type +
fingerprint), the references that say where they appear, and the export-job table. Purely additive: nothing in 0001 is
touched. Rollback (`alembic downgrade 0001`) drops only these three tables; objects in the artifact bucket are not
removed by a downgrade (the reconciler/retention job owns object cleanup).
"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None

UUID = postgresql.UUID(as_uuid=True)


def upgrade() -> None:
    op.create_table(
        "artifacts",
        sa.Column("artifact_id", UUID, primary_key=True),
        sa.Column("owner_user_id", UUID, nullable=False),
        sa.Column("artifact_type", sa.String(16), nullable=False),
        sa.Column("title", sa.String(200), nullable=False, server_default=""),
        sa.Column("description", sa.Text(), nullable=False, server_default=""),
        sa.Column("fingerprint", sa.String(64), nullable=False),
        sa.Column("fingerprint_version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("spec_text", sa.Text(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False, server_default="pending"),
        sa.Column("render_job_id", sa.String(64), nullable=True),
        sa.Column("object_key", sa.String(255), nullable=True),
        sa.Column("thumbnail_key", sa.String(255), nullable=True),
        sa.Column("mime_type", sa.String(64), nullable=True),
        sa.Column("size_bytes", sa.BigInteger(), nullable=True),
        sa.Column("checksum_sha256", sa.String(64), nullable=True),
        sa.Column("renderer_version", sa.String(64), nullable=False, server_default=""),
        sa.Column("failure_code", sa.String(48), nullable=True),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("retry_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("lease_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("origin_conversation_id", UUID, nullable=False),
        sa.Column("origin_message_id", UUID, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("artifact_type IN ('mermaid', 'manim')", name="ck_artifacts_type"),
        sa.CheckConstraint("status IN ('pending', 'queued', 'rendering', 'uploading', 'ready', 'failed')",
                           name="ck_artifacts_status"),
        sa.CheckConstraint("attempt_count >= 0", name="ck_artifacts_attempts"),
        sa.CheckConstraint("char_length(spec_text) <= 20000", name="ck_artifacts_spec_len"),
        sa.CheckConstraint("char_length(fingerprint) = 64", name="ck_artifacts_fingerprint_len"),
        sa.CheckConstraint(
            "artifact_type <> 'manim' OR status <> 'ready' OR "
            "(object_key IS NOT NULL AND checksum_sha256 IS NOT NULL AND size_bytes > 0)",
            name="ck_artifacts_ready_manim_has_object"),
        sa.UniqueConstraint("owner_user_id", "artifact_type", "fingerprint", name="uq_artifacts_owner_type_fingerprint"),
    )
    op.create_index("ix_artifacts_owner_created", "artifacts", ["owner_user_id", "created_at"])
    op.create_index("ix_artifacts_active", "artifacts", ["status", "lease_until"],
                    postgresql_where=sa.text("status NOT IN ('ready', 'failed')"))

    op.create_table(
        "artifact_refs",
        sa.Column("artifact_id", UUID, sa.ForeignKey("artifacts.artifact_id", ondelete="CASCADE"), primary_key=True),
        sa.Column("conversation_id", UUID, primary_key=True),
        sa.Column("source_message_id", UUID, primary_key=True),
        sa.Column("block_id", sa.String(64), primary_key=True, server_default=""),
        sa.Column("owner_user_id", UUID, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_artifact_refs_conversation", "artifact_refs", ["conversation_id", "owner_user_id"])

    op.create_table(
        "export_jobs",
        sa.Column("job_id", UUID, primary_key=True),
        sa.Column("owner_user_id", UUID, nullable=False),
        sa.Column("conversation_id", UUID, nullable=False),
        sa.Column("scope", sa.String(16), nullable=False),
        sa.Column("format", sa.String(16), nullable=False),
        sa.Column("include_media", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("request_key", sa.String(64), nullable=False),
        sa.Column("snapshot", postgresql.JSONB(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False, server_default="pending"),
        sa.Column("progress_done", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("progress_total", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("object_key", sa.String(255), nullable=True),
        sa.Column("filename", sa.String(200), nullable=True),
        sa.Column("content_type", sa.String(64), nullable=True),
        sa.Column("size_bytes", sa.BigInteger(), nullable=True),
        sa.Column("checksum_sha256", sa.String(64), nullable=True),
        sa.Column("failure_code", sa.String(48), nullable=True),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("lease_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("scope IN ('conversation', 'hierarchy')", name="ck_export_jobs_scope"),
        sa.CheckConstraint("format IN ('markdown', 'pdf')", name="ck_export_jobs_format"),
        sa.CheckConstraint("status IN ('pending', 'processing', 'ready', 'failed', 'expired')",
                           name="ck_export_jobs_status"),
        sa.CheckConstraint("progress_done >= 0 AND progress_total >= 0", name="ck_export_jobs_progress"),
    )
    op.create_index("uq_export_jobs_live_request", "export_jobs", ["owner_user_id", "request_key"], unique=True,
                    postgresql_where=sa.text("status IN ('pending', 'processing', 'ready')"))
    op.create_index("ix_export_jobs_owner_created", "export_jobs", ["owner_user_id", "created_at"])
    op.create_index("ix_export_jobs_active", "export_jobs", ["status", "lease_until"],
                    postgresql_where=sa.text("status IN ('pending', 'processing')"))
    op.create_index("ix_export_jobs_expiry", "export_jobs", ["expires_at"],
                    postgresql_where=sa.text("status = 'ready'"))


def downgrade() -> None:
    op.drop_table("export_jobs")
    op.drop_table("artifact_refs")
    op.drop_table("artifacts")
