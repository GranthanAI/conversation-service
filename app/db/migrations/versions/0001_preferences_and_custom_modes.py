"""user preferences and custom modes

Revision ID: 0001
Revises:
Create Date: 2026-10-19

Creates the PostgreSQL tables that replace the Cassandra `user_preferences` table (legacy data is copied with
scripts/migrate_preferences_to_postgres.py; the Cassandra table is left untouched).
"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "custom_modes",
        sa.Column("mode_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("owner_user_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("name", sa.String(120), nullable=False),
        sa.Column("name_normalized", sa.String(120), nullable=False),
        sa.Column("description", sa.Text(), nullable=False, server_default=""),
        sa.Column("instructions", sa.Text(), nullable=False),
        sa.Column("execution_style", sa.String(16), nullable=False),
        sa.Column("status", sa.String(16), nullable=False, server_default="active"),
        sa.Column("version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("archived_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("length(btrim(name)) > 0", name="ck_custom_modes_name_not_blank"),
        sa.CheckConstraint("length(btrim(instructions)) > 0", name="ck_custom_modes_instructions_not_blank"),
        sa.CheckConstraint("execution_style IN ('direct', 'smart')", name="ck_custom_modes_execution_style"),
        sa.CheckConstraint("status IN ('active', 'archived')", name="ck_custom_modes_status"),
        sa.CheckConstraint("version >= 1", name="ck_custom_modes_version_positive"),
    )
    op.create_index("uq_custom_modes_owner_active_name", "custom_modes", ["owner_user_id", "name_normalized"],
                    unique=True, postgresql_where=sa.text("status = 'active'"))
    op.create_index("ix_custom_modes_owner_status_updated", "custom_modes", ["owner_user_id", "status", "updated_at"])

    op.create_table(
        "user_preferences",
        sa.Column("user_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("default_mode", sa.String(40), nullable=True),
        sa.Column("default_custom_mode_id", postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("custom_modes.mode_id", ondelete="SET NULL"), nullable=True),
        sa.Column("default_tools", postgresql.ARRAY(sa.String(64)), nullable=False, server_default=sa.text("'{}'")),
        sa.Column("answer_length", sa.String(16), nullable=False, server_default="balanced"),
        sa.Column("tone", sa.String(16), nullable=False, server_default="professional"),
        sa.Column("language", sa.String(16), nullable=False, server_default="auto"),
        sa.Column("personal_instructions", sa.Text(), nullable=False, server_default=""),
        sa.Column("citations", sa.String(16), nullable=False, server_default="show"),
        sa.Column("document_context", sa.String(24), nullable=False, server_default="conversation"),
        sa.Column("version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("version >= 1", name="ck_user_preferences_version_positive"),
        sa.CheckConstraint("answer_length IN ('concise', 'balanced', 'detailed')", name="ck_user_preferences_answer_length"),
        sa.CheckConstraint("tone IN ('professional', 'friendly', 'simple')", name="ck_user_preferences_tone"),
        sa.CheckConstraint("citations IN ('show', 'hide')", name="ck_user_preferences_citations"),
        sa.CheckConstraint("document_context IN ('conversation', 'attached_only')", name="ck_user_preferences_document_context"),
        sa.CheckConstraint("char_length(personal_instructions) <= 1000", name="ck_user_preferences_instructions_len"),
        sa.CheckConstraint("default_mode IS NULL OR default_custom_mode_id IS NULL", name="ck_user_preferences_one_default_mode"),
    )


def downgrade() -> None:
    op.drop_table("user_preferences")
    op.drop_index("ix_custom_modes_owner_status_updated", table_name="custom_modes")
    op.drop_index("uq_custom_modes_owner_active_name", table_name="custom_modes")
    op.drop_table("custom_modes")
