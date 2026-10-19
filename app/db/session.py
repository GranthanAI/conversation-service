from collections.abc import AsyncGenerator
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.postgres import get_engine

_session_factory: Optional[async_sessionmaker[AsyncSession]] = None


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    global _session_factory
    if _session_factory is None:
        _session_factory = async_sessionmaker(bind=get_engine(), expire_on_commit=False, autoflush=False)
    return _session_factory


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Request-scoped session. Repositories only flush; this dependency commits on success and rolls back on any
    exception, so a failed request never leaves a half-applied change."""
    async with get_session_factory()() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
