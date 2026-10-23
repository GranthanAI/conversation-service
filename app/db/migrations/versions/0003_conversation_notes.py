"""conversation notes

Revision ID: 0003
Revises: 0002
Create Date: 2026-10-24

Adds `conversation_notes`: one Markdown/plain-text document per (owner, conversation) with an optimistic-concurrency
`version`. Purely additive: no existing table or row is touched and no backfill is needed (a conversation without a row
simply has empty notes). Rollback (`alembic downgrade 0002`) drops only this table; users' notes would be lost on rollback,
so export them first if the feature has been used.
"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "conversation_notes",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("owner_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("conversation_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("content", sa.Text(), nullable=False, server_default=""),
        sa.Column("version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("owner_id", "conversation_id", name="uq_conversation_notes_owner_conversation"),
        sa.CheckConstraint("version >= 1", name="ck_conversation_notes_version_positive"),
        sa.CheckConstraint("char_length(content) <= 200000", name="ck_conversation_notes_content_len"),
    )


def downgrade() -> None:
    op.drop_table("conversation_notes")
