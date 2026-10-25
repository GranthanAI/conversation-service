"""source edits

Revision ID: 0004
Revises: 0003
Create Date: 2026-10-24

Adds `source_edits`: a user's edited copy of one code/text/Markdown block of an assistant message, with an
optimistic-concurrency `version`. Purely additive (no existing table or row is touched, no backfill). Rollback
(`alembic downgrade 0003`) drops only this table; users' edits would be lost, the original AI messages are unaffected.
"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "source_edits",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("owner_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("conversation_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("message_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("block_id", sa.String(80), nullable=False),
        sa.Column("language", sa.String(32), nullable=False, server_default=""),
        sa.Column("source_format", sa.String(16), nullable=False),
        sa.Column("original_sha256", sa.String(64), nullable=False),
        sa.Column("content", sa.Text(), nullable=False, server_default=""),
        sa.Column("version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("owner_id", "conversation_id", "message_id", "block_id", name="uq_source_edits_block"),
        sa.CheckConstraint("version >= 1", name="ck_source_edits_version_positive"),
        sa.CheckConstraint("char_length(content) <= 200000", name="ck_source_edits_content_len"),
        sa.CheckConstraint("source_format IN ('code','text','markdown')", name="ck_source_edits_format"),
    )


def downgrade() -> None:
    op.drop_table("source_edits")
