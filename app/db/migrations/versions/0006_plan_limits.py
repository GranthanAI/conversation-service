"""plan limits: per-conversation message counters and per-tree documents

Revision ID: 0006
Revises: 0005
Create Date: 2026-11-10

Adds `conversation_message_counters` (user messages per conversation, initialised lazily from one Cassandra count) and
`tree_documents` (distinct document ids per conversation tree). Purely additive; no backfill. Rollback
(`alembic downgrade 0005`) drops both tables; the counters are rebuilt lazily, tree documents restart from empty.
"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "conversation_message_counters",
        sa.Column("conversation_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("user_message_count", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("user_message_count >= 0", name="ck_conv_msg_counters_nonneg"),
    )
    op.create_table(
        "tree_documents",
        sa.Column("root_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("document_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )


def downgrade() -> None:
    op.drop_table("tree_documents")
    op.drop_table("conversation_message_counters")
