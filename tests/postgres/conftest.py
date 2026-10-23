"""PostgreSQL integration fixtures: a throwaway Postgres container, migrated with the REAL Alembic chain
(app/db/migrations), so these tests exercise the schema production uses (not create_all)."""

import os

import pytest
import pytest_asyncio
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from testcontainers.community.postgres import PostgresContainer

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture(scope="session")
def postgres_container():
    with PostgresContainer("postgres:16-alpine") as container:
        yield container


@pytest.fixture(scope="session")
def database_url(postgres_container) -> str:
    return postgres_container.get_connection_url().replace("postgresql+psycopg2://", "postgresql+asyncpg://")


@pytest.fixture(scope="session")
def migrated_url(database_url) -> str:
    cfg = Config(os.path.join(ROOT, "alembic.ini"))
    cfg.set_main_option("script_location", os.path.join(ROOT, "app", "db", "migrations"))
    cfg.set_main_option("sqlalchemy.url", database_url)
    command.upgrade(cfg, "head")
    return database_url


@pytest_asyncio.fixture
async def engine(migrated_url):
    eng = create_async_engine(migrated_url, pool_size=10, max_overflow=10)
    async with eng.begin() as conn:
        await conn.execute(text("TRUNCATE user_preferences, custom_modes, artifact_refs, artifacts, export_jobs, conversation_notes CASCADE"))
    yield eng
    await eng.dispose()


@pytest.fixture
def session_factory(engine):
    return async_sessionmaker(bind=engine, expire_on_commit=False, autoflush=False)
