"""quick wins: message feedback, message edits, shared links, temporary chats, think usage, suggest_followups

Revision ID: 0008
Revises: 0007
Create Date: 2026-11-22

Purely additive. Rollback drops the five tables and the preferences column.
"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0008"
down_revision = "0007"
branch_labels = None
depends_on = None

UUID = postgresql.UUID(as_uuid=True)


def upgrade() -> None:
    op.create_table(
        "message_feedback",
        sa.Column("user_id", UUID, primary_key=True),
        sa.Column("message_id", UUID, primary_key=True),
        sa.Column("conversation_id", UUID, nullable=False),
        sa.Column("rating", sa.SmallInteger, nullable=False),
        sa.Column("reason", sa.String(32), nullable=True),
        sa.Column("comment", sa.Text, nullable=True),
        sa.Column("model", sa.String(64), nullable=True),
        sa.Column("model_group", sa.String(16), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("rating IN (-1, 1)", name="ck_message_feedback_rating"),
    )
    op.create_index("ix_message_feedback_rating_created", "message_feedback", ["rating", sa.text("created_at DESC")])

    op.create_table(
        "message_edits",
        sa.Column("branch_conversation_id", UUID, primary_key=True),
        sa.Column("original_message_id", UUID, nullable=False),
        sa.Column("original_conversation_id", UUID, nullable=False),
        sa.Column("user_id", UUID, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_message_edits_original_message", "message_edits", ["original_message_id"])

    op.create_table(
        "shared_links",
        sa.Column("share_id", UUID, primary_key=True),
        sa.Column("user_id", UUID, nullable=False),
        sa.Column("conversation_id", UUID, nullable=False),
        sa.Column("scope", sa.String(16), nullable=False),
        sa.Column("token_hash", sa.LargeBinary, nullable=False, unique=True),
        sa.Column("title", sa.Text, nullable=False),
        sa.Column("object_key", sa.Text, nullable=False),
        sa.Column("message_count", sa.Integer, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("refreshed_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("view_count", sa.BigInteger, nullable=False, server_default="0"),
        sa.CheckConstraint("scope IN ('conversation', 'hierarchy')", name="ck_shared_links_scope"),
    )
    op.create_index("ix_shared_links_user_created", "shared_links", ["user_id", sa.text("created_at DESC")])
    op.create_index("ix_shared_links_conversation", "shared_links", ["conversation_id"])

    op.create_table(
        "temporary_conversations",
        sa.Column("conversation_id", UUID, primary_key=True),
        sa.Column("user_id", UUID, nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_temporary_conversations_due", "temporary_conversations", ["expires_at"],
                    postgresql_where=sa.text("deleted_at IS NULL"))
    op.create_index("ix_temporary_conversations_user", "temporary_conversations", ["user_id"])

    op.create_table(
        "think_usage_daily",
        sa.Column("user_id", UUID, primary_key=True),
        sa.Column("day", sa.Date, primary_key=True),
        sa.Column("count", sa.Integer, nullable=False),
        sa.CheckConstraint("count >= 0", name="ck_think_usage_daily_nonneg"),
    )

    op.add_column("user_preferences",
                  sa.Column("suggest_followups", sa.Boolean, nullable=False, server_default=sa.text("true")))


def downgrade() -> None:
    op.drop_column("user_preferences", "suggest_followups")
    op.drop_table("think_usage_daily")
    op.drop_index("ix_temporary_conversations_user", table_name="temporary_conversations")
    op.drop_index("ix_temporary_conversations_due", table_name="temporary_conversations")
    op.drop_table("temporary_conversations")
    op.drop_index("ix_shared_links_conversation", table_name="shared_links")
    op.drop_index("ix_shared_links_user_created", table_name="shared_links")
    op.drop_table("shared_links")
    op.drop_index("ix_message_edits_original_message", table_name="message_edits")
    op.drop_table("message_edits")
    op.drop_index("ix_message_feedback_rating_created", table_name="message_feedback")
    op.drop_table("message_feedback")
