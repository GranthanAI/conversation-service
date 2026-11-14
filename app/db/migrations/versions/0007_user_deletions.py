"""user_deletions: user.deleted bookkeeping

Revision ID: 0007
Revises: 0006
Create Date: 2026-11-14

One row per deleted account: the user's routes answer 404 from `deleted_at`, and the purge worker removes their data after
`purge_after` (USER_DATA_RETENTION_DAYS, default 30), then sets `purged_at`. Purely additive; rollback drops the table.
"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0007"
down_revision = "0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "user_deletions",
        sa.Column("user_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("purge_after", sa.DateTime(timezone=True), nullable=False),
        sa.Column("purged_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_table("user_deletions")
