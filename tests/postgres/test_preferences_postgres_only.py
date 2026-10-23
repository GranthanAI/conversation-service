"""Audit tests for "persistent preferences live in PostgreSQL only".

Evidence, not assumptions:
  * a write is visible in the table through raw SQL AND through a brand-new engine/session (a fresh request or session);
  * every preference/custom-mode code path runs with tripwires on Cassandra, Redis and Kafka: any use fails the test;
  * the preference modules do not import those clients at all;
  * partial updates by different callers do not overwrite each other's fields, and users cannot touch each other's rows.
(The Cassandra -> PostgreSQL data migration is covered by test_migration.py: all fields, idempotent re-run, newer data kept.)
"""

import ast
import asyncio
import pathlib
import uuid

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.db.cassandra import cassandra_manager
from app.db.kafka import kafka_manager
from app.db.redis import redis_manager
from tests.postgres.test_api_and_resolution import ALICE, BOB, build_app, create, token

import httpx

pytestmark = pytest.mark.asyncio
APP = pathlib.Path(__file__).resolve().parents[2] / "app"
PREFERENCE_MODULES = ["api/v1/preferences.py", "api/v1/custom_modes.py", "services/preferences_service.py",
                      "services/custom_mode_service.py", "repositories/pg_repositories.py", "models/preferences.py",
                      "models/custom_modes.py", "models/sql_models.py"]
FORBIDDEN_IMPORTS = ("cassandra", "redis", "aiokafka", "app.db.cassandra", "app.db.redis", "app.db.kafka",
                     "app.clients.redis_client", "app.clients.kafka_producer", "app.repositories.conversation_repository",
                     "app.repositories.message_repository", "app.services.cache_service")


class Tripwire:
    def __getattr__(self, name):
        raise AssertionError(f"a preference code path touched a non-PostgreSQL store ({name})")

    def __call__(self, *a, **k):
        raise AssertionError("a preference code path touched a non-PostgreSQL store")


@pytest_asyncio.fixture
async def client(session_factory, monkeypatch):
    monkeypatch.setattr(cassandra_manager, "session", Tripwire())
    monkeypatch.setattr(redis_manager, "client", Tripwire())
    monkeypatch.setattr(kafka_manager, "producer", Tripwire())
    transport = httpx.ASGITransport(app=build_app(session_factory))
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        yield c


async def test_static_guard_preference_modules_do_not_import_other_stores():
    for rel in PREFERENCE_MODULES:
        tree = ast.parse((APP / rel).read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            names = [a.name for a in node.names] if isinstance(node, ast.Import) else \
                    [node.module or ""] if isinstance(node, ast.ImportFrom) else []
            for n in names:
                assert not any(n == f or n.startswith(f + ".") for f in FORBIDDEN_IMPORTS), f"{rel} imports {n}"


async def test_every_preference_and_custom_mode_operation_runs_with_other_stores_booby_trapped(client):
    h = token(ALICE)
    mode = await create(client, ALICE)
    assert (await client.get("/v1/preferences", headers=h)).status_code == 200
    assert (await client.patch("/v1/preferences", json={"tone": "friendly", "default_mode": mode["mode_ref"]}, headers=h)).status_code == 200
    assert (await client.patch("/v1/preferences", json={"answer_length": "detailed"}, headers=h)).status_code == 200
    assert (await client.patch(f"/v1/custom-modes/{mode['mode_id']}", json={"description": "x"}, headers=h)).status_code == 200
    assert (await client.delete(f"/v1/custom-modes/{mode['mode_id']}", headers=h)).status_code in (200, 202, 204)
    assert (await client.delete("/v1/preferences", headers=h)).status_code == 200


async def test_a_write_is_in_the_table_and_a_brand_new_session_reads_it_back(client, migrated_url):
    h = token(ALICE)
    saved = await client.patch("/v1/preferences", json={"tone": "friendly", "answer_length": "concise", "language": "de",
                                                "citations": "hide"}, headers=h)
    assert saved.status_code == 200, saved.text
    engine = create_async_engine(migrated_url)              # an independent connection pool = a new process/session
    try:
        async with engine.connect() as conn:
            row = (await conn.execute(text("SELECT tone, answer_length, language, citations, version "
                                           "FROM user_preferences WHERE user_id = :u"), {"u": ALICE})).one()
        assert (row.tone, row.answer_length, row.language, row.citations) == \
               ("friendly", "concise", "de", "hide") and row.version == 1
        fresh_client = httpx.AsyncClient(transport=httpx.ASGITransport(app=build_app(
            async_sessionmaker(bind=engine, expire_on_commit=False, autoflush=False))), base_url="http://t")
        async with fresh_client:
            again = (await fresh_client.get("/v1/preferences", headers=h)).json()
        assert again["tone"] == "friendly" and again["language"] == "de" and again["version"] == 1
    finally:
        await engine.dispose()


async def test_defaults_are_virtual_until_the_first_write_and_reset_removes_the_row(client, session_factory):
    h = token(ALICE)
    async with session_factory() as s:
        assert (await s.execute(text("SELECT count(*) FROM user_preferences"))).scalar_one() == 0
    assert (await client.get("/v1/preferences", headers=h)).json()["tone"] == "professional"       # a read never writes
    async with session_factory() as s:
        assert (await s.execute(text("SELECT count(*) FROM user_preferences"))).scalar_one() == 0
    await client.patch("/v1/preferences", json={"tone": "simple"}, headers=h)
    async with session_factory() as s:
        assert (await s.execute(text("SELECT count(*) FROM user_preferences"))).scalar_one() == 1
    await client.delete("/v1/preferences", headers=h)
    async with session_factory() as s:
        assert (await s.execute(text("SELECT count(*) FROM user_preferences"))).scalar_one() == 0


async def test_concurrent_partial_updates_never_silently_overwrite_each_other(client):
    """Each write is an atomic compare-and-set on top of the newest document. Under heavy contention a request may be
    refused with an explicit 409 (its bounded retries ran out) but an ACCEPTED write is never lost, and a refused one
    succeeds when repeated."""
    h = token(ALICE)
    patches = [{"tone": "friendly"}, {"answer_length": "detailed"}, {"language": "fr"}, {"citations": "hide"},
               {"document_context": "attached_only"}, {"personal_instructions": "be brief"}]
    results = await asyncio.gather(*[client.patch("/v1/preferences", json=p, headers=h) for p in patches])
    assert {r.status_code for r in results} <= {200, 409}
    for patch, r in zip(patches, results):
        if r.status_code == 409:
            assert (await client.patch("/v1/preferences", json=patch, headers=h)).status_code == 200
    final = (await client.get("/v1/preferences", headers=h)).json()
    assert (final["tone"], final["answer_length"], final["language"], final["citations"], final["document_context"],
            final["personal_instructions"]) == ("friendly", "detailed", "fr", "hide", "attached_only", "be brief")


async def test_one_users_writes_never_touch_or_reveal_another_users_row(client, session_factory):
    await client.patch("/v1/preferences", json={"tone": "friendly"}, headers=token(ALICE))
    await client.patch("/v1/preferences", json={"tone": "simple", "language": "es"}, headers=token(BOB))
    await client.delete("/v1/preferences", headers=token(BOB))
    alice = (await client.get("/v1/preferences", headers=token(ALICE))).json()
    assert alice["tone"] == "friendly" and alice["language"] == "auto"
    async with session_factory() as s:
        users = [r[0] for r in (await s.execute(text("SELECT user_id FROM user_preferences"))).all()]
    assert users == [ALICE]
    spoof = await client.patch("/v1/preferences", json={"tone": "simple", "user_id": str(ALICE)}, headers=token(BOB))
    assert spoof.status_code == 422                                              # an owner field is not accepted
    assert (await client.get("/v1/preferences", headers=token(ALICE))).json()["tone"] == "friendly"
