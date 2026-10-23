"""HTTP layer for artifacts and exports: real JWT verification, real PostgreSQL, in-memory storage and conversation stand-ins."""

import asyncio
import time
import uuid

import httpx
import jwt
import pytest
import pytest_asyncio
from fastapi import FastAPI

from app.api.deps import get_conversation_service, get_message_repository
from app.api.v1 import artifacts as artifacts_api
from app.api.v1 import exports as exports_api
from app.artifacts.runtime import get_artifact_service
from app.artifacts.service import ArtifactService
from app.artifacts.storage import MemoryObjectStore
from app.core.config import settings
from app.exports.service import ExportService
from tests.postgres.fakes import FakeBackend, make_mp4
from tests.postgres.test_exports import FakeConversations, FakeMessages, MANIM_BLOCK, SPEC

pytestmark = pytest.mark.asyncio
ALICE, BOB = uuid.uuid4(), uuid.uuid4()


def auth(user):
    t = jwt.encode({"sub": str(user), "email": "u@example.com", "exp": int(time.time()) + 600},
                   settings.JWT_SECRET_KEY, algorithm=settings.JWT_ALGORITHM)
    return {"Authorization": f"Bearer {t}"}


@pytest.fixture
def world(session_factory, monkeypatch):
    store, backend = MemoryObjectStore(), FakeBackend()
    convs, msgs = FakeConversations(), FakeMessages()
    artifacts = ArtifactService(session_factory, store, backend)
    exporter = ExportService(session_factory, store, convs, msgs, artifacts)
    app = FastAPI()
    app.include_router(artifacts_api.router, prefix="/v1/artifacts")
    app.include_router(exports_api.router, prefix="/v1/exports")
    app.dependency_overrides[get_artifact_service] = lambda: artifacts
    app.dependency_overrides[get_conversation_service] = lambda: convs
    app.dependency_overrides[get_message_repository] = lambda: msgs
    app.dependency_overrides[exports_api.get_export_service] = lambda: exporter
    monkeypatch.setattr(artifacts_api, "get_object_store", lambda: store)
    monkeypatch.setattr(exports_api, "get_object_store", lambda: store)
    return type("W", (), dict(app=app, store=store, backend=backend, convs=convs, msgs=msgs, artifacts=artifacts, exporter=exporter))


@pytest_asyncio.fixture
async def client(world):
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=world.app), base_url="http://t") as c:
        yield c


async def drain():
    pending = list(artifacts_api._background | exports_api._background)
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)


def body(conv, msg, kind="manim", content=None, block="b1"):
    return {"conversation_id": str(conv.conversation_id), "source_message_id": str(msg.message_id), "type": kind,
            "content": content if content is not None else (SPEC if kind == "manim" else "graph TD\n  A-->B"), "block_id": block}


async def test_every_endpoint_requires_authentication(client):
    rid = uuid.uuid4()
    for method, url in [("post", "/v1/artifacts"), ("get", "/v1/artifacts?conversation_id=" + str(rid)),
                        ("get", f"/v1/artifacts/{rid}"), ("get", f"/v1/artifacts/{rid}/media"),
                        ("post", f"/v1/artifacts/{rid}/retry"), ("post", "/v1/exports"), ("get", f"/v1/exports/{rid}"),
                        ("get", f"/v1/exports/{rid}/download")]:
        assert (await getattr(client, method)(url)).status_code == 401, url


async def test_register_is_idempotent_and_never_renders_on_a_second_open(client, world):
    conv = world.convs.add("Chat", user=ALICE)
    msg = world.msgs.add(conv.conversation_id, "assistant", MANIM_BLOCK)
    r1 = await client.post("/v1/artifacts", json=body(conv, msg), headers=auth(ALICE))
    assert r1.status_code == 201 and r1.json()["status"] == "pending"
    await drain()
    for _ in range(4):                                         # the chat is reopened four more times
        again = await client.post("/v1/artifacts", json=body(conv, msg), headers=auth(ALICE))
        assert again.status_code == 200 and again.json()["artifact_id"] == r1.json()["artifact_id"]
    await drain()
    got = (await client.get(f"/v1/artifacts/{r1.json()['artifact_id']}", headers=auth(ALICE))).json()
    assert got["status"] == "ready" and got["ready"] and got["has_media"] and "source" not in {k for k, v in got.items() if v}
    assert len(world.backend.submits) == 1 and world.store.puts_ending('.mp4') == 1


async def test_register_validates_ownership_message_and_content(client, world):
    conv = world.convs.add("Chat", user=ALICE)
    msg = world.msgs.add(conv.conversation_id, "assistant", "x")
    bob_conv = world.convs.add("Bob", user=BOB)
    bob_msg = world.msgs.add(bob_conv.conversation_id, "assistant", "x")
    assert (await client.post("/v1/artifacts", json=body(bob_conv, bob_msg), headers=auth(ALICE))).status_code == 404
    ghost = type("M", (), {"message_id": uuid.uuid4()})
    assert (await client.post("/v1/artifacts", json=body(conv, ghost), headers=auth(ALICE))).status_code == 404
    assert (await client.post("/v1/artifacts", json=body(conv, msg, content={"template": "t", "evil": 1}), headers=auth(ALICE))).status_code == 422
    assert (await client.post("/v1/artifacts", json=body(conv, msg, kind="mermaid", content="   "), headers=auth(ALICE))).status_code == 422
    assert (await client.post("/v1/artifacts", json={**body(conv, msg), "owner_user_id": str(BOB)}, headers=auth(ALICE))).status_code == 422
    assert (await client.post("/v1/artifacts", json={**body(conv, msg), "type": "gif"}, headers=auth(ALICE))).status_code == 422


async def test_mermaid_is_persisted_and_returned_with_its_source_without_any_render(client, world):
    conv = world.convs.add("Chat", user=ALICE)
    msg = world.msgs.add(conv.conversation_id, "assistant", "x")
    r = await client.post("/v1/artifacts", json=body(conv, msg, kind="mermaid"), headers=auth(ALICE))
    j = r.json()
    assert r.status_code == 201 and j["status"] == "ready" and j["source"] == "graph TD\n  A-->B" and not j["has_media"]
    assert world.backend.submits == []


async def test_other_users_get_404_for_metadata_media_and_retry(client, world):
    conv = world.convs.add("Chat", user=ALICE)
    msg = world.msgs.add(conv.conversation_id, "assistant", MANIM_BLOCK)
    a = (await client.post("/v1/artifacts", json=body(conv, msg), headers=auth(ALICE))).json()
    await drain()
    for method, url in [("get", f"/v1/artifacts/{a['artifact_id']}"), ("get", f"/v1/artifacts/{a['artifact_id']}/media"),
                        ("post", f"/v1/artifacts/{a['artifact_id']}/retry")]:
        assert (await getattr(client, method)(url, headers=auth(BOB))).status_code == 404
    assert (await client.get(f"/v1/artifacts?conversation_id={conv.conversation_id}", headers=auth(BOB))).status_code == 404


async def test_media_streams_the_stored_video_and_download_forces_attachment(client, world):
    conv = world.convs.add("Chat", user=ALICE)
    msg = world.msgs.add(conv.conversation_id, "assistant", MANIM_BLOCK)
    a = (await client.post("/v1/artifacts", json=body(conv, msg), headers=auth(ALICE))).json()
    await drain()
    r = await client.get(f"/v1/artifacts/{a['artifact_id']}/media", headers=auth(ALICE))
    assert r.status_code == 200 and r.content == world.backend.media and r.headers["content-type"] == "video/mp4"
    assert r.headers["x-content-type-options"] == "nosniff" and "attachment" not in r.headers.get("content-disposition", "")
    d = await client.get(f"/v1/artifacts/{a['artifact_id']}/media?download=1", headers=auth(ALICE))
    assert d.headers["content-disposition"].startswith('attachment; filename="Binary-search.mp4"')


async def test_media_is_404_until_ready_and_a_vanished_object_triggers_one_reconciliation(client, world):
    world.backend.auto_ready = False
    conv = world.convs.add("Chat", user=ALICE)
    msg = world.msgs.add(conv.conversation_id, "assistant", MANIM_BLOCK)
    a = (await client.post("/v1/artifacts", json=body(conv, msg), headers=auth(ALICE))).json()
    await drain()
    assert (await client.get(f"/v1/artifacts/{a['artifact_id']}/media", headers=auth(ALICE))).status_code == 404
    world.backend.states = {k: "ready" for k in world.backend.states}
    await world.artifacts.run_once()
    assert (await client.get(f"/v1/artifacts/{a['artifact_id']}", headers=auth(ALICE))).json()["status"] == "ready"
    world.store.objects.clear()                                                  # the object disappears
    assert (await client.get(f"/v1/artifacts/{a['artifact_id']}/media", headers=auth(ALICE))).status_code == 404
    await drain()
    assert (await client.get(f"/v1/artifacts/{a['artifact_id']}/media", headers=auth(ALICE))).status_code == 200


async def test_failed_artifact_exposes_a_safe_message_and_can_be_retried_once(client, world):
    world.backend.reject_with = "render_error"
    conv = world.convs.add("Chat", user=ALICE)
    msg = world.msgs.add(conv.conversation_id, "assistant", MANIM_BLOCK)
    a = (await client.post("/v1/artifacts", json=body(conv, msg), headers=auth(ALICE))).json()
    await drain()
    failed = (await client.get(f"/v1/artifacts/{a['artifact_id']}", headers=auth(ALICE))).json()
    assert failed["status"] == "failed" and failed["retryable"] and failed["message"] == "The animation could not be rendered."
    assert "spec" not in failed and "object_key" not in failed
    world.backend.reject_with = None
    r1 = await client.post(f"/v1/artifacts/{a['artifact_id']}/retry", headers=auth(ALICE))
    r2 = await client.post(f"/v1/artifacts/{a['artifact_id']}/retry", headers=auth(ALICE))
    assert r1.status_code == r2.status_code == 200 and r1.json()["status"] == "pending"
    await drain()
    assert (await client.get(f"/v1/artifacts/{a['artifact_id']}", headers=auth(ALICE))).json()["status"] == "ready"
    assert len(world.backend.submits) == 1


async def test_sidebar_listing_scopes_and_groups_by_originating_conversation(client, world):
    root = world.convs.add("Root", user=ALICE)
    child = world.convs.add("Child", user=ALICE, parent=root.conversation_id)
    grand = world.convs.add("Grand", user=ALICE, parent=child.conversation_id)
    sibling = world.convs.add("Sibling", user=ALICE, parent=root.conversation_id)
    other = world.convs.add("Other root", user=ALICE)
    spec2 = {**SPEC, "title": "Other"}
    for conv, kind, content in [(root, "mermaid", "graph TD\n  R-->S"), (child, "manim", SPEC), (grand, "mermaid", "graph TD\n  G-->H"),
                                (sibling, "manim", spec2), (other, "mermaid", "graph TD\n  O-->P")]:
        m = world.msgs.add(conv.conversation_id, "assistant", "x")
        assert (await client.post("/v1/artifacts", json=body(conv, m, kind, content), headers=auth(ALICE))).status_code == 201
    await drain()

    def titles(j):
        return {g["title"]: [a["type"] for a in g["artifacts"]] for g in j["groups"]}

    root_view = (await client.get(f"/v1/artifacts?conversation_id={root.conversation_id}", headers=auth(ALICE))).json()
    assert root_view["scope"] == "hierarchy" and root_view["is_root"] and root_view["total"] == 4
    assert set(titles(root_view)) == {"Root", "Child", "Grand", "Sibling"}                    # whole tree, not "Other root"
    assert [g["title"] for g in root_view["groups"]] == ["Root", "Child", "Grand", "Sibling"]   # tree order
    assert root_view["groups"][0]["is_current"]

    child_view = (await client.get(f"/v1/artifacts?conversation_id={child.conversation_id}", headers=auth(ALICE))).json()
    assert child_view["scope"] == "self" and not child_view["is_root"] and set(titles(child_view)) == {"Child"}

    expanded = (await client.get(f"/v1/artifacts?conversation_id={child.conversation_id}&scope=hierarchy", headers=auth(ALICE))).json()
    assert set(titles(expanded)) == {"Root", "Child", "Grand", "Sibling"} and expanded["root_conversation_id"] == str(root.conversation_id)

    only_manim = (await client.get(f"/v1/artifacts?conversation_id={root.conversation_id}&type=manim", headers=auth(ALICE))).json()
    assert only_manim["total"] == 2
    found = (await client.get(f"/v1/artifacts?conversation_id={root.conversation_id}&q=other", headers=auth(ALICE))).json()
    assert found["total"] == 1


async def test_sidebar_never_leaks_artifacts_of_deleted_conversations_or_other_users(client, world):
    root = world.convs.add("Root", user=ALICE)
    child = world.convs.add("Doomed child", user=ALICE, parent=root.conversation_id)
    m = world.msgs.add(child.conversation_id, "assistant", "x")
    await client.post("/v1/artifacts", json=body(child, m, "mermaid"), headers=auth(ALICE))
    bob_conv = world.convs.add("Bob", user=BOB)
    bm = world.msgs.add(bob_conv.conversation_id, "assistant", "x")
    await client.post("/v1/artifacts", json=body(bob_conv, bm, "mermaid"), headers=auth(BOB))
    assert (await client.get(f"/v1/artifacts?conversation_id={root.conversation_id}", headers=auth(ALICE))).json()["total"] == 1
    world.convs.delete(child.conversation_id)                                    # conversation deleted
    after = (await client.get(f"/v1/artifacts?conversation_id={root.conversation_id}", headers=auth(ALICE))).json()
    assert after["total"] == 0 and after["groups"] == []


async def test_export_api_flow_status_download_and_isolation(client, world):
    conv = world.convs.add("Exportable", user=ALICE)
    world.msgs.add(conv.conversation_id, "user", "hello")
    world.msgs.add(conv.conversation_id, "assistant", "world")
    r = await client.post("/v1/exports", json={"conversation_id": str(conv.conversation_id)}, headers=auth(ALICE))
    j = r.json()
    assert r.status_code == 202 and j["status"] == "ready" and j["filename"].endswith(".md") and j["download_ready"]   # small: inline
    again = await client.post("/v1/exports", json={"conversation_id": str(conv.conversation_id)}, headers=auth(ALICE))
    assert again.status_code == 200 and again.json()["job_id"] == j["job_id"]
    status_ = (await client.get(f"/v1/exports/{j['job_id']}", headers=auth(ALICE))).json()
    assert status_["progress"] == {"done": 1, "total": 1}
    dl = await client.get(f"/v1/exports/{j['job_id']}/download", headers=auth(ALICE))
    assert dl.status_code == 200 and b"## User" in dl.content and "attachment" in dl.headers["content-disposition"]
    assert dl.headers["cache-control"] == "private, no-store"
    assert (await client.get(f"/v1/exports/{j['job_id']}", headers=auth(BOB))).status_code == 404
    assert (await client.get(f"/v1/exports/{j['job_id']}/download", headers=auth(BOB))).status_code == 404
    assert (await client.post("/v1/exports", json={"conversation_id": str(conv.conversation_id)}, headers=auth(BOB))).status_code == 404


async def test_hierarchy_export_is_asynchronous_and_download_waits_until_ready(client, world):
    root = world.convs.add("Root", user=ALICE)
    child = world.convs.add("Kid", user=ALICE, parent=root.conversation_id)
    for c in (root, child):
        world.msgs.add(c.conversation_id, "user", f"hi {c.title}")
    r = await client.post("/v1/exports", json={"conversation_id": str(child.conversation_id), "scope": "hierarchy",
                                              "format": "markdown", "include_media": True}, headers=auth(ALICE))
    j = r.json()
    assert r.status_code == 202 and j["scope"] == "hierarchy" and j["include_media"]
    await drain()
    ready = (await client.get(f"/v1/exports/{j['job_id']}", headers=auth(ALICE))).json()
    assert ready["status"] == "ready" and ready["filename"].endswith(".zip") and ready["progress"]["total"] == 2
    dl = await client.get(f"/v1/exports/{j['job_id']}/download", headers=auth(ALICE))
    assert dl.status_code == 200 and dl.content[:2] == b"PK"


async def test_expired_exports_cannot_be_downloaded(client, world, session_factory):
    from sqlalchemy import text
    conv = world.convs.add("Old", user=ALICE)
    world.msgs.add(conv.conversation_id, "user", "hi")
    j = (await client.post("/v1/exports", json={"conversation_id": str(conv.conversation_id)}, headers=auth(ALICE))).json()
    async with session_factory() as s, s.begin():
        await s.execute(text("UPDATE export_jobs SET expires_at = now() - interval '1 second'"))
    assert (await client.get(f"/v1/exports/{j['job_id']}/download", headers=auth(ALICE))).status_code == 404
    assert (await client.get(f"/v1/exports/{j['job_id']}", headers=auth(ALICE))).json()["status"] == "expired"


async def test_export_request_validation(client, world):
    conv = world.convs.add("Chat", user=ALICE)
    for bad in [{"conversation_id": str(conv.conversation_id), "format": "docx"},
                {"conversation_id": str(conv.conversation_id), "scope": "everything"},
                {"conversation_id": "not-a-uuid"}, {"conversation_id": str(conv.conversation_id), "owner_user_id": str(BOB)}]:
        assert (await client.post("/v1/exports", json=bad, headers=auth(ALICE))).status_code == 422


async def test_thumbnail_endpoint_serves_the_preview_only_to_the_owner(client, world):
    conv = world.convs.add("Chat", user=ALICE)
    msg = world.msgs.add(conv.conversation_id, "assistant", MANIM_BLOCK)
    a = (await client.post("/v1/artifacts", json=body(conv, msg), headers=auth(ALICE))).json()
    await drain()
    meta = (await client.get(f"/v1/artifacts/{a['artifact_id']}", headers=auth(ALICE))).json()
    assert meta["has_thumbnail"] is True
    ok = await client.get(f"/v1/artifacts/{a['artifact_id']}/thumbnail", headers=auth(ALICE))
    assert ok.status_code == 200 and ok.headers["content-type"] == "image/png" and ok.content == world.backend.thumbnail
    assert (await client.get(f"/v1/artifacts/{a['artifact_id']}/thumbnail", headers=auth(BOB))).status_code == 404
