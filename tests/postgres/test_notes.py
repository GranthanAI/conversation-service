"""Conversation notes against real PostgreSQL (migrated with the real Alembic chain): persistence, ownership, soft delete,
branch isolation, optimistic concurrency and failure behaviour. Conversations come from an in-memory stand-in for the
Cassandra-backed ConversationService (the authoritative record the API must consult on every request)."""

import asyncio
import time
import uuid
from unittest.mock import MagicMock

import httpx
import jwt
import pytest
import pytest_asyncio
from fastapi import FastAPI
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.deps import get_conversation_service, get_notes_repository
from app.api.v1 import notes as notes_api
from app.core.config import settings
from app.db.errors import register_postgres_handlers
from app.db.session import get_db
from app.models.conversation import ConversationStatus
from tests.postgres.test_exports import FakeConversations

ALICE, BOB = uuid.uuid4(), uuid.uuid4()
aio = pytest.mark.asyncio


def auth(user):
    t = jwt.encode({"sub": str(user), "email": "u@example.com", "exp": int(time.time()) + 600},
                   settings.JWT_SECRET_KEY, algorithm=settings.JWT_ALGORITHM)
    return {"Authorization": f"Bearer {t}"}


def build_app(factory, convs) -> FastAPI:
    app = FastAPI()

    async def db():
        async with factory() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    app.dependency_overrides[get_db] = db
    app.dependency_overrides[get_conversation_service] = lambda: convs
    register_postgres_handlers(app)
    app.include_router(notes_api.router, prefix="/v1/conversations")
    return app


@pytest.fixture
def convs():
    return FakeConversations()


@pytest_asyncio.fixture
async def client(session_factory, convs):
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=build_app(session_factory, convs)), base_url="http://t") as c:
        yield c


def url(conv) -> str:
    return f"/v1/conversations/{conv.conversation_id}/notes"


async def rows(session_factory) -> int:
    async with session_factory() as s:
        return (await s.execute(text("SELECT count(*) FROM conversation_notes"))).scalar_one()


# ------------------------------------------------------------------------------------------ authentication

@aio
async def test_both_endpoints_require_authentication(client, convs):
    c = convs.add("Chat", user=ALICE)
    assert (await client.get(url(c))).status_code == 401
    assert (await client.put(url(c), json={"content": "x", "version": 0})).status_code == 401


# ------------------------------------------------------------------------------------------ create / read / update

@aio
async def test_a_missing_document_reads_as_empty_and_creates_nothing(client, convs, session_factory):
    c = convs.add("Chat", user=ALICE)
    r = await client.get(url(c), headers=auth(ALICE))
    assert r.status_code == 200
    assert r.json() == {"conversation_id": str(c.conversation_id), "content": "", "version": 0, "updated_at": None,
                        "max_chars": settings.NOTES_MAX_CHARS}
    assert await rows(session_factory) == 0                     # a read never writes


@aio
async def test_first_save_creates_the_document_and_it_survives_a_new_session(client, convs, migrated_url):
    c = convs.add("Chat", user=ALICE)
    saved = await client.put(url(c), json={"content": "# Plan\n- [ ] ship notes", "version": 0}, headers=auth(ALICE))
    assert saved.status_code == 200 and saved.json()["version"] == 1 and saved.json()["updated_at"]
    engine = create_async_engine(migrated_url)                  # an independent pool = a restart / another process
    try:
        factory = async_sessionmaker(bind=engine, expire_on_commit=False, autoflush=False)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=build_app(factory, convs)), base_url="http://t") as fresh:
            again = (await fresh.get(url(c), headers=auth(ALICE))).json()
    finally:
        await engine.dispose()
    assert again["content"] == "# Plan\n- [ ] ship notes" and again["version"] == 1


@aio
async def test_saving_empty_notes_is_allowed_and_is_a_real_document(client, convs, session_factory):
    c = convs.add("Chat", user=ALICE)
    r = await client.put(url(c), json={"content": "", "version": 0}, headers=auth(ALICE))
    assert r.status_code == 200 and r.json()["version"] == 1 and r.json()["content"] == ""
    assert await rows(session_factory) == 1


@aio
async def test_updates_bump_the_version_and_keep_unicode_markdown_exactly(client, convs):
    c = convs.add("Chat", user=ALICE)
    text_v1 = "## Tasks\n\n- [x] डेटा\n- [ ] 日本語\n\n```py\nprint('hi')\n```\n"
    v1 = (await client.put(url(c), json={"content": text_v1, "version": 0}, headers=auth(ALICE))).json()
    v2 = (await client.put(url(c), json={"content": text_v1 + "more", "version": v1["version"]}, headers=auth(ALICE))).json()
    assert (v1["version"], v2["version"]) == (1, 2) and v2["content"] == text_v1 + "more"
    assert (await client.get(url(c), headers=auth(ALICE))).json()["content"] == text_v1 + "more"


@aio
async def test_saving_unchanged_content_writes_nothing(client, convs, session_factory):
    c = convs.add("Chat", user=ALICE)
    first = (await client.put(url(c), json={"content": "same", "version": 0}, headers=auth(ALICE))).json()
    again = await client.put(url(c), json={"content": "same", "version": first["version"]}, headers=auth(ALICE))
    assert again.status_code == 200 and again.json()["version"] == first["version"]
    assert again.json()["updated_at"] == first["updated_at"]                      # no write happened


@aio
async def test_the_unique_constraint_allows_one_document_per_owner_and_conversation(client, convs, session_factory):
    c = convs.add("Chat", user=ALICE)
    await client.put(url(c), json={"content": "a", "version": 0}, headers=auth(ALICE))
    async with session_factory() as s:
        with pytest.raises(IntegrityError):
            await s.execute(text("INSERT INTO conversation_notes (id, owner_id, conversation_id, content) VALUES (:i,:o,:c,'dup')"),
                            {"i": uuid.uuid4(), "o": ALICE, "c": c.conversation_id})
            await s.commit()


# ------------------------------------------------------------------------------------------ optimistic concurrency

@aio
async def test_a_stale_version_is_a_409_carrying_the_current_document_and_overwrites_nothing(client, convs):
    c = convs.add("Chat", user=ALICE)
    v1 = (await client.put(url(c), json={"content": "one", "version": 0}, headers=auth(ALICE))).json()
    await client.put(url(c), json={"content": "two", "version": v1["version"]}, headers=auth(ALICE))
    stale = await client.put(url(c), json={"content": "old tab's text", "version": v1["version"]}, headers=auth(ALICE))
    assert stale.status_code == 409
    detail = stale.json()["detail"]
    assert detail["code"] == "version_conflict" and detail["current"]["content"] == "two" and detail["current"]["version"] == 2
    assert (await client.get(url(c), headers=auth(ALICE))).json()["content"] == "two"        # untouched


@aio
async def test_creating_over_an_existing_document_is_a_conflict_unless_the_text_is_identical(client, convs):
    c = convs.add("Chat", user=ALICE)
    await client.put(url(c), json={"content": "first tab", "version": 0}, headers=auth(ALICE))
    other = await client.put(url(c), json={"content": "second tab", "version": 0}, headers=auth(ALICE))
    assert other.status_code == 409 and other.json()["detail"]["current"]["content"] == "first tab"
    same = await client.put(url(c), json={"content": "first tab", "version": 0}, headers=auth(ALICE))
    assert same.status_code == 200 and same.json()["version"] == 1


@aio
async def test_a_stale_version_against_a_document_that_does_not_exist_is_a_conflict(client, convs):
    c = convs.add("Chat", user=ALICE)
    r = await client.put(url(c), json={"content": "x", "version": 5}, headers=auth(ALICE))
    assert r.status_code == 409 and r.json()["detail"]["current"]["version"] == 0


@aio
async def test_concurrent_saves_from_the_same_version_have_exactly_one_winner(client, convs, session_factory):
    c = convs.add("Chat", user=ALICE)
    base = (await client.put(url(c), json={"content": "base", "version": 0}, headers=auth(ALICE))).json()
    results = await asyncio.gather(*[
        client.put(url(c), json={"content": f"edit {i}", "version": base["version"]}, headers=auth(ALICE)) for i in range(8)])
    assert sorted(r.status_code for r in results) == [200] + [409] * 7
    winner = next(r for r in results if r.status_code == 200).json()
    final = (await client.get(url(c), headers=auth(ALICE))).json()
    assert final["version"] == 2 and final["content"] == winner["content"]
    for loser in (r for r in results if r.status_code == 409):                         # every loser can still see the winner
        assert loser.json()["detail"]["current"]["content"] == winner["content"]


@aio
async def test_concurrent_first_saves_create_one_row(client, convs, session_factory):
    c = convs.add("Chat", user=ALICE)
    results = await asyncio.gather(*[
        client.put(url(c), json={"content": f"first {i}", "version": 0}, headers=auth(ALICE)) for i in range(6)])
    assert sorted(r.status_code for r in results) == [200] + [409] * 5
    assert await rows(session_factory) == 1


# ------------------------------------------------------------------------------------------ ownership / lifecycle

@aio
async def test_another_users_conversation_is_a_404_for_read_and_write_and_nothing_is_created(client, convs, session_factory):
    c = convs.add("Alice's", user=ALICE)
    await client.put(url(c), json={"content": "private", "version": 0}, headers=auth(ALICE))
    before = await rows(session_factory)
    assert (await client.get(url(c), headers=auth(BOB))).status_code == 404
    put = await client.put(url(c), json={"content": "hijack", "version": 0}, headers=auth(BOB))
    assert put.status_code == 404 and "private" not in put.text
    assert await rows(session_factory) == before
    assert (await client.get(url(c), headers=auth(ALICE))).json()["content"] == "private"


@aio
async def test_a_missing_conversation_is_indistinguishable_from_someone_elses(client, convs):
    ghost = type("C", (), {"conversation_id": uuid.uuid4()})
    mine = convs.add("Alice's", user=ALICE)
    foreign = (await client.get(url(mine), headers=auth(BOB))).json()
    missing = (await client.get(url(ghost), headers=auth(ALICE))).json()
    assert foreign == missing == {"detail": "Conversation not found"}


@aio
async def test_a_soft_deleted_conversations_notes_are_not_reachable(client, convs, session_factory):
    c = convs.add("Doomed", user=ALICE)
    await client.put(url(c), json={"content": "secret", "version": 0}, headers=auth(ALICE))
    c.status = ConversationStatus.DELETED                                              # what the delete flow sets
    for call in (client.get(url(c), headers=auth(ALICE)), client.put(url(c), json={"content": "x", "version": 1}, headers=auth(ALICE))):
        r = await call
        assert r.status_code == 404 and "secret" not in r.text


@aio
async def test_an_archived_conversation_keeps_its_notes(client, convs):
    c = convs.add("Old", user=ALICE)
    await client.put(url(c), json={"content": "keep", "version": 0}, headers=auth(ALICE))
    c.status = ConversationStatus.ARCHIVED
    assert (await client.get(url(c), headers=auth(ALICE))).json()["content"] == "keep"


# ------------------------------------------------------------------------------------------ branches

@aio
async def test_parent_child_and_sibling_notes_are_independent(client, convs):
    parent = convs.add("Parent", user=ALICE)
    child = convs.add("Child", user=ALICE, parent=parent.conversation_id)
    sibling = convs.add("Sibling", user=ALICE, parent=parent.conversation_id)
    await client.put(url(parent), json={"content": "parent notes", "version": 0}, headers=auth(ALICE))
    assert (await client.get(url(child), headers=auth(ALICE))).json()["content"] == ""          # nothing inherited
    await client.put(url(child), json={"content": "child notes", "version": 0}, headers=auth(ALICE))
    assert (await client.get(url(parent), headers=auth(ALICE))).json()["content"] == "parent notes"   # parent untouched
    assert (await client.get(url(sibling), headers=auth(ALICE))).json()["content"] == ""             # sibling never sees it
    assert (await client.get(url(child), headers=auth(ALICE))).json()["content"] == "child notes"


# ------------------------------------------------------------------------------------------ validation

@aio
@pytest.mark.parametrize("body", [
    {"content": "x"},                                       # version is required: no blind overwrite
    {"content": "x", "version": -1},
    {"content": "x", "version": "1"},
    {"content": "x", "version": True},
    {"content": "x", "version": 1.5},
    {"version": 0},
    {"content": 5, "version": 0},
    {"content": "x", "version": 0, "owner_id": str(BOB)},   # identity is never taken from the body
    {"content": "has\x00nul", "version": 0},
])
async def test_invalid_bodies_are_rejected_without_writing(client, convs, session_factory, body):
    c = convs.add("Chat", user=ALICE)
    assert (await client.put(url(c), json=body, headers=auth(ALICE))).status_code == 422
    assert await rows(session_factory) == 0


@aio
async def test_the_size_limit_is_enforced_at_the_boundary(client, convs):
    c = convs.add("Chat", user=ALICE)
    limit = settings.NOTES_MAX_CHARS
    ok = await client.put(url(c), json={"content": "a" * limit, "version": 0}, headers=auth(ALICE))
    assert ok.status_code == 200
    too_big = await client.put(url(c), json={"content": "a" * (limit + 1), "version": 1}, headers=auth(ALICE))
    assert too_big.status_code == 422
    assert len((await client.get(url(c), headers=auth(ALICE))).json()["content"]) == limit


# ------------------------------------------------------------------------------------------ failures

@aio
async def test_a_postgres_outage_is_a_controlled_503_without_driver_text_and_a_retry_succeeds(session_factory, convs):
    c = convs.add("Chat", user=ALICE)
    app = build_app(session_factory, convs)

    class Down:
        async def get(self, *a, **k):
            raise ConnectionError("password authentication failed for user postgres")

        save = get

    app.dependency_overrides[get_notes_repository] = lambda: Down()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as client:
        r = await client.put(url(c), json={"content": "draft", "version": 0}, headers=auth(ALICE))
        assert r.status_code == 503 and r.json()["detail"]["code"] == "storage_unavailable"
        assert "password" not in r.text and "postgres" not in r.text.lower() and "draft" not in r.text
        del app.dependency_overrides[get_notes_repository]                               # the database is back
        retry = await client.put(url(c), json={"content": "draft", "version": 0}, headers=auth(ALICE))
        assert retry.status_code == 200 and retry.json()["content"] == "draft"


@aio
async def test_if_the_conversation_catalog_is_down_nothing_is_served_or_written(session_factory, convs, monkeypatch):
    c = convs.add("Chat", user=ALICE)
    app = build_app(session_factory, convs)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as client:
        await client.put(url(c), json={"content": "stored", "version": 0}, headers=auth(ALICE))

        async def broken(_cid):
            raise RuntimeError("cassandra: NoHostAvailable")

        monkeypatch.setattr(convs, "get", broken)
        for call in (client.get(url(c), headers=auth(ALICE)), client.put(url(c), json={"content": "x", "version": 1}, headers=auth(ALICE))):
            r = await call
            assert r.status_code == 503 and r.json()["detail"]["code"] == "conversation_unavailable"
            assert "stored" not in r.text and "cassandra" not in r.text.lower()


@aio
async def test_note_contents_never_reach_the_logs(client, convs, monkeypatch):
    log = MagicMock()
    monkeypatch.setattr(notes_api, "logger", log)
    c = convs.add("Chat", user=ALICE)
    secret = "TOP-SECRET-NOTE-TEXT"
    await client.put(url(c), json={"content": secret, "version": 0}, headers=auth(ALICE))
    await client.get(url(c), headers=auth(ALICE))
    await client.put(url(c), json={"content": secret + "2", "version": 0}, headers=auth(ALICE))      # a conflict
    assert log.info.called
    assert secret not in repr(log.mock_calls)
