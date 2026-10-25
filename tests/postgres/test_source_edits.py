"""Source edits (edited copies of code/text/Markdown blocks) against real PostgreSQL migrated with the real Alembic chain:
persistence, exact source preservation, optimistic concurrency, idempotent retry, owner isolation, deleted conversations,
catalog outage, limits, and migration upgrade/downgrade."""

import asyncio
import hashlib
import os
import uuid

import httpx
import pytest
import pytest_asyncio
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine

from app.api.v1 import source_edits as api
from app.core.config import settings
from app.models.conversation import ConversationStatus
from tests.postgres.test_exports import FakeConversations
from tests.postgres.test_notes import ALICE, BOB, aio, auth
from fastapi import FastAPI
from app.api.deps import get_conversation_service
from app.db.errors import register_postgres_handlers
from app.db.session import get_db

ORIGINAL = "def f():\n\treturn 'héllo ✓ 日本語'\n"
SHA = hashlib.sha256(ORIGINAL.encode()).hexdigest()
MSG = uuid.uuid4()
BLOCK = "0-L3-abcdef12"


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
    app.include_router(api.router, prefix="/v1/conversations")
    return app


@pytest.fixture
def convs():
    return FakeConversations()


@pytest_asyncio.fixture
async def client(session_factory, convs):
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=build_app(session_factory, convs)), base_url="http://t") as c:
        yield c


def base(conv) -> str:
    return f"/v1/conversations/{conv.conversation_id}/source-edits"


def one(conv, msg=MSG, block=BLOCK) -> str:
    return f"{base(conv)}/{msg}/{block}"


def body(content, version=0, **kw):
    return {"content": content, "version": version, "language": "python", "source_format": "code", "original_sha256": SHA, **kw}


async def count(factory) -> int:
    async with factory() as s:
        return (await s.execute(text("SELECT count(*) FROM source_edits"))).scalar_one()


@aio
async def test_every_endpoint_requires_authentication(client, convs):
    c = convs.add("Chat", user=ALICE)
    assert (await client.get(base(c))).status_code == 401
    assert (await client.put(one(c), json=body("x"))).status_code == 401
    assert (await client.delete(one(c) + "?version=1")).status_code == 401


@aio
async def test_empty_list_and_save_then_reload_returns_the_exact_source(client, convs, session_factory):
    c = convs.add("Chat", user=ALICE)
    assert (await client.get(base(c), headers=auth(ALICE))).json()["edits"] == []
    edited = "def f():\r\n\treturn 'héllo ✓ 日本語 🚀'  \r\n\r\n"           # CRLF, tab, trailing spaces, emoji, trailing blank line
    saved = await client.put(one(c), json=body(edited), headers=auth(ALICE))
    assert saved.status_code == 200 and saved.json()["version"] == 1 and saved.json()["content"] == edited
    again = (await client.get(base(c), headers=auth(ALICE))).json()["edits"]
    assert len(again) == 1 and again[0]["content"] == edited and again[0]["original_sha256"] == SHA
    assert again[0]["message_id"] == str(MSG) and again[0]["block_id"] == BLOCK and again[0]["language"] == "python"
    assert await count(session_factory) == 1


@aio
async def test_saves_bump_the_version_and_a_stale_version_is_a_409_carrying_the_current_edit(client, convs):
    c = convs.add("Chat", user=ALICE)
    v1 = (await client.put(one(c), json=body("a"), headers=auth(ALICE))).json()
    v2 = (await client.put(one(c), json=body("b", 1), headers=auth(ALICE))).json()
    assert (v1["version"], v2["version"]) == (1, 2)
    stale = await client.put(one(c), json=body("c", 1), headers=auth(ALICE))        # "another tab" still on version 1
    assert stale.status_code == 409
    d = stale.json()["detail"]
    assert d["code"] == "version_conflict" and d["current"]["content"] == "b" and d["current"]["version"] == 2
    assert (await client.get(base(c), headers=auth(ALICE))).json()["edits"][0]["content"] == "b"   # nothing overwritten


@aio
async def test_creating_with_version_zero_twice_conflicts_instead_of_overwriting(client, convs):
    c = convs.add("Chat", user=ALICE)
    assert (await client.put(one(c), json=body("first"), headers=auth(ALICE))).status_code == 200
    second = await client.put(one(c), json=body("second"), headers=auth(ALICE))
    assert second.status_code == 409 and second.json()["detail"]["current"]["content"] == "first"


@aio
async def test_retrying_a_save_whose_response_was_lost_is_idempotent(client, convs, session_factory):
    c = convs.add("Chat", user=ALICE)
    first = (await client.put(one(c), json=body("x = 1"), headers=auth(ALICE))).json()
    retry_create = await client.put(one(c), json=body("x = 1"), headers=auth(ALICE))           # same request again
    assert retry_create.status_code == 200 and retry_create.json()["version"] == 1 and await count(session_factory) == 1
    upd = (await client.put(one(c), json=body("x = 2", 1), headers=auth(ALICE))).json()
    retry_update = await client.put(one(c), json=body("x = 2", 1), headers=auth(ALICE))         # lost response, retried
    assert retry_update.status_code == 200 and retry_update.json()["version"] == upd["version"] == 2
    assert retry_update.json()["updated_at"] == upd["updated_at"] and first["version"] == 1


@aio
async def test_concurrent_saves_with_the_same_version_have_exactly_one_winner(client, convs, session_factory):
    c = convs.add("Chat", user=ALICE)
    await client.put(one(c), json=body("base"), headers=auth(ALICE))
    results = await asyncio.gather(*[client.put(one(c), json=body(f"writer {i}", 1), headers=auth(ALICE)) for i in range(8)])
    assert sorted(r.status_code for r in results) == [200] + [409] * 7
    stored = (await client.get(base(c), headers=auth(ALICE))).json()["edits"][0]
    assert stored["version"] == 2 and stored["content"].startswith("writer ")


@aio
async def test_discard_removes_the_edit_checks_the_version_and_is_idempotent(client, convs, session_factory):
    c = convs.add("Chat", user=ALICE)
    await client.put(one(c), json=body("a"), headers=auth(ALICE))
    await client.put(one(c), json=body("b", 1), headers=auth(ALICE))
    stale = await client.delete(one(c) + "?version=1", headers=auth(ALICE))
    assert stale.status_code == 409 and stale.json()["detail"]["current"]["version"] == 2 and await count(session_factory) == 1
    assert (await client.delete(one(c) + "?version=2", headers=auth(ALICE))).status_code == 204
    assert await count(session_factory) == 0
    assert (await client.delete(one(c) + "?version=2", headers=auth(ALICE))).status_code == 204   # retry: still fine
    assert (await client.delete(one(c), headers=auth(ALICE))).status_code == 422                  # version is required


@aio
async def test_update_after_the_edit_was_discarded_elsewhere_is_a_conflict_without_current(client, convs):
    c = convs.add("Chat", user=ALICE)
    await client.put(one(c), json=body("a"), headers=auth(ALICE))
    await client.delete(one(c) + "?version=1", headers=auth(ALICE))
    r = await client.put(one(c), json=body("b", 1), headers=auth(ALICE))
    assert r.status_code == 409 and r.json()["detail"]["current"] is None


@aio
async def test_blocks_and_messages_are_independent(client, convs):
    c = convs.add("Chat", user=ALICE)
    other_msg = uuid.uuid4()
    await client.put(one(c), json=body("block A"), headers=auth(ALICE))
    await client.put(one(c, block="0-L9-ffff0000"), json=body("block B"), headers=auth(ALICE))
    await client.put(one(c, msg=other_msg), json=body("other message"), headers=auth(ALICE))
    all_ = (await client.get(base(c), headers=auth(ALICE))).json()["edits"]
    assert sorted(e["content"] for e in all_) == ["block A", "block B", "other message"]
    only = (await client.get(base(c) + f"?message_id={other_msg}", headers=auth(ALICE))).json()["edits"]
    assert [e["content"] for e in only] == ["other message"]


@aio
async def test_another_user_cannot_read_write_or_discard_and_the_owner_is_never_taken_from_the_body(client, convs, session_factory):
    c = convs.add("Chat", user=ALICE)
    await client.put(one(c), json=body("alice secret"), headers=auth(ALICE))
    assert (await client.get(base(c), headers=auth(BOB))).status_code == 404
    assert (await client.put(one(c), json=body("bob", 1), headers=auth(BOB))).status_code == 404
    assert (await client.delete(one(c) + "?version=1", headers=auth(BOB))).status_code == 404
    spoof = await client.put(one(c), json={**body("x", 1), "owner_id": str(ALICE)}, headers=auth(BOB))
    assert spoof.status_code in (404, 422)
    # same ids, but Bob's own conversation: he sees nothing of Alice's, and writing creates a separate row
    bobs = convs.add("Bob chat", user=BOB)
    assert (await client.get(base(bobs), headers=auth(BOB))).json()["edits"] == []
    assert (await client.get(base(c), headers=auth(ALICE))).json()["edits"][0]["content"] == "alice secret"
    assert await count(session_factory) == 1


@aio
async def test_deleted_missing_and_archived_conversations(client, convs):
    c = convs.add("Chat", user=ALICE)
    await client.put(one(c), json=body("kept"), headers=auth(ALICE))
    c.status = ConversationStatus.ARCHIVED
    assert (await client.get(base(c), headers=auth(ALICE))).json()["edits"][0]["content"] == "kept"
    c.status = ConversationStatus.DELETED
    assert (await client.get(base(c), headers=auth(ALICE))).status_code == 404
    assert (await client.put(one(c), json=body("x", 1), headers=auth(ALICE))).status_code == 404
    assert (await client.get(f"/v1/conversations/{uuid.uuid4()}/source-edits", headers=auth(ALICE))).status_code == 404


@aio
async def test_catalog_outage_is_503_and_never_grants_access(client, convs):
    c = convs.add("Chat", user=ALICE)

    async def boom(cid):
        raise RuntimeError("cassandra: NoHostAvailable")

    convs.get = boom
    r = await client.get(base(c), headers=auth(ALICE))
    assert r.status_code == 503 and r.json()["detail"]["code"] == "conversation_unavailable" and "cassandra" not in r.text.lower().replace("conversation_unavailable", "")


@aio
async def test_validation_rejects_bad_input_without_writing(client, convs, session_factory):
    c = convs.add("Chat", user=ALICE)
    h = auth(ALICE)
    assert (await client.put(one(c), json=body("a\x00b"), headers=h)).status_code == 422                  # NUL
    assert (await client.put(one(c), json=body("x" * (settings.SOURCE_EDIT_MAX_CHARS + 1)), headers=h)).status_code == 422
    assert (await client.put(one(c), json=body("x", source_format="html"), headers=h)).status_code == 422
    assert (await client.put(one(c), json=body("x", language="../../etc"), headers=h)).status_code == 422
    assert (await client.put(one(c), json=body("x", original_sha256="zz"), headers=h)).status_code == 422
    assert (await client.put(one(c), json={**body("x"), "extra": 1}, headers=h)).status_code == 422
    assert (await client.put(one(c), json=body("x", version="0"), headers=h)).status_code == 422          # strict int
    assert (await client.put(f"{base(c)}/{MSG}/bad%20id", json=body("x"), headers=h)).status_code == 422
    assert (await client.put(f"{base(c)}/not-a-uuid/{BLOCK}", json=body("x"), headers=h)).status_code == 422
    assert await count(session_factory) == 0


@aio
async def test_per_conversation_limit(client, convs, monkeypatch):
    monkeypatch.setattr(settings, "SOURCE_EDIT_MAX_PER_CONVERSATION", 2)
    c = convs.add("Chat", user=ALICE)
    h = auth(ALICE)
    for i in range(2):
        assert (await client.put(one(c, block=f"b{i}"), json=body("x"), headers=h)).status_code == 200
    r = await client.put(one(c, block="b2"), json=body("x"), headers=h)
    assert r.status_code == 422 and r.json()["detail"]["code"] == "too_many_edits"
    assert (await client.put(one(c, block="b0"), json=body("y", 1), headers=h)).status_code == 200       # updating still works


@aio
async def test_empty_source_is_a_valid_edit_and_markdown_text_formats_are_kept(client, convs):
    c = convs.add("Chat", user=ALICE)
    md = await client.put(one(c, block="response"), json=body("# Title\n\n- [ ] a\n", language="markdown", source_format="markdown"),
                          headers=auth(ALICE))
    assert md.status_code == 200 and md.json()["source_format"] == "markdown"
    empty = await client.put(one(c, block="e"), json=body("", source_format="text", language=""), headers=auth(ALICE))
    assert empty.status_code == 200 and empty.json()["content"] == ""


@aio
async def test_database_constraints_hold_even_without_the_api(session_factory):
    async with session_factory() as s:
        row = {"id": uuid.uuid4(), "o": ALICE, "c": uuid.uuid4(), "m": MSG, "b": "b"}
        ins = text("INSERT INTO source_edits (id, owner_id, conversation_id, message_id, block_id, source_format, original_sha256, content) "
                   "VALUES (:id, :o, :c, :m, :b, :f, 'x', 'y')")
        with pytest.raises(IntegrityError):
            await s.execute(ins, {**row, "f": "html"})
        await s.rollback()
        await s.execute(ins, {**row, "f": "code"})
        with pytest.raises(IntegrityError):                                      # unique per (owner, conversation, message, block)
            await s.execute(ins, {**row, "id": uuid.uuid4(), "f": "code"})


def test_migration_upgrades_from_0003_with_existing_data_and_downgrades_cleanly(database_url):
    """In its own throwaway database: upgrade to 0003, add notes data, upgrade to 0004 (data untouched), downgrade, upgrade."""
    admin_url = database_url
    database_url = database_url.rsplit("/", 1)[0] + "/migration_0004"
    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    cfg = Config(os.path.join(root, "alembic.ini"))
    cfg.set_main_option("script_location", os.path.join(root, "app", "db", "migrations"))
    cfg.set_main_option("sqlalchemy.url", database_url)

    async def run(sql, params=None, fetch=False):
        eng = create_async_engine(database_url)
        try:
            async with eng.begin() as conn:
                res = await conn.execute(text(sql), params or {})
                return res.all() if fetch else None
        finally:
            await eng.dispose()

    async def create_db():
        eng = create_async_engine(admin_url, isolation_level="AUTOCOMMIT")
        try:
            async with eng.connect() as conn:
                await conn.execute(text("DROP DATABASE IF EXISTS migration_0004"))
                await conn.execute(text("CREATE DATABASE migration_0004"))
        finally:
            await eng.dispose()

    asyncio.run(create_db())
    command.upgrade(cfg, "0003")
    asyncio.run(run("INSERT INTO conversation_notes (id, owner_id, conversation_id, content) VALUES (:i, :o, :c, 'keep me')",
                    {"i": uuid.uuid4(), "o": ALICE, "c": uuid.uuid4()}))
    command.upgrade(cfg, "0004")
    assert asyncio.run(run("SELECT content FROM conversation_notes", fetch=True))[0][0] == "keep me"
    assert asyncio.run(run("SELECT count(*) FROM source_edits", fetch=True))[0][0] == 0
    command.downgrade(cfg, "0003")
    tables = {r[0] for r in asyncio.run(run("SELECT tablename FROM pg_tables WHERE schemaname='public'", fetch=True))}
    assert "source_edits" not in tables and "conversation_notes" in tables
    command.upgrade(cfg, "head")                                                 # and forward again
    assert "source_edits" in {r[0] for r in asyncio.run(run("SELECT tablename FROM pg_tables WHERE schemaname='public'", fetch=True))}
