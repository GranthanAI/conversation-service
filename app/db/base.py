from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Declarative base for the PostgreSQL models."""


# Imported so Alembic's env.py only needs Base.metadata to see every table.
from app.models import artifact_models, notes_models, sql_models  # noqa: E402,F401
