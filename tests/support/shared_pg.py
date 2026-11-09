"""A throwaway database inside the SHARED dev Postgres (no new containers): created with a unique name, migrated with
the real Alembic chain, dropped at the end. Skips cleanly when the shared Postgres is not reachable."""

import asyncio
import os
import uuid

import pytest

ADMIN_URL = os.environ.get("TEST_PG_ADMIN_URL", "postgresql://postgres:1234@127.0.0.1:5433/postgres")
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


async def _admin(sql: str):
    import asyncpg
    conn = await asyncpg.connect(ADMIN_URL, timeout=3)
    try:
        await conn.execute(sql)
    finally:
        await conn.close()


def _alembic_cfg(url: str):
    from alembic.config import Config
    cfg = Config(os.path.join(ROOT, "alembic.ini"))
    cfg.set_main_option("script_location", os.path.join(ROOT, "app", "db", "migrations"))
    cfg.set_main_option("sqlalchemy.url", url)
    return cfg


def alembic_upgrade(url: str, revision: str = "head") -> None:
    from alembic import command
    command.upgrade(_alembic_cfg(url), revision)


def alembic_downgrade(url: str, revision: str) -> None:
    from alembic import command
    command.downgrade(_alembic_cfg(url), revision)


@pytest.fixture(scope="session")
def migrated_pg_url():
    name = f"p6_test_{uuid.uuid4().hex[:10]}"
    try:
        asyncio.run(_admin(f'CREATE DATABASE "{name}"'))
    except Exception as e:  # shared Postgres not reachable
        pytest.skip(f"shared Postgres not reachable: {type(e).__name__}")
    url = ADMIN_URL.replace("postgresql://", "postgresql+asyncpg://").rsplit("/", 1)[0] + f"/{name}"
    try:
        alembic_upgrade(url)
        yield url
    finally:
        asyncio.run(_admin(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
