"""PostgreSQL engine (SQLAlchemy 2 async). One engine per process, bounded pool, per-statement timeout."""

from typing import Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from app.core.config import settings

_engine: Optional[AsyncEngine] = None


def get_engine() -> AsyncEngine:
    global _engine
    if _engine is None:
        _engine = create_async_engine(
            settings.DATABASE_URL,
            echo=settings.DB_ECHO,
            pool_size=settings.DB_POOL_SIZE,
            max_overflow=settings.DB_MAX_OVERFLOW,
            pool_timeout=settings.DB_POOL_TIMEOUT_SECONDS,
            pool_pre_ping=True,
            connect_args={
                "timeout": settings.DB_CONNECT_TIMEOUT_SECONDS,
                "server_settings": {"statement_timeout": str(settings.DB_STATEMENT_TIMEOUT_MS)},
            },
        )
    return _engine


async def check_database() -> bool:
    """Readiness probe: can a connection be opened and a trivial query run?"""
    try:
        async with get_engine().connect() as conn:
            await conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


def pool_stats() -> dict:
    pool = get_engine().pool
    return {"size": pool.size(), "checked_out": pool.checkedout(), "overflow": pool.overflow()}


async def dispose_engine() -> None:
    global _engine
    if _engine is not None:
        await _engine.dispose()
        _engine = None
