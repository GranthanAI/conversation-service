"""preferences: default_model

Revision ID: 0005
Revises: 0004
Create Date: 2026-11-10

Adds `user_preferences.default_model` ("auto" or a catalog model id). Purely additive: existing rows read as "auto"
through the server default, no backfill. Rollback (`alembic downgrade 0004`) drops only the column; users' saved model
defaults would be lost, nothing else is touched.
"""
import sqlalchemy as sa
from alembic import op

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("user_preferences", sa.Column("default_model", sa.String(64), nullable=False, server_default="auto"))


def downgrade() -> None:
    op.drop_column("user_preferences", "default_model")
