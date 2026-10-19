"""Custom-mode and preferences HTTP API (real JWT verification, real PostgreSQL) plus message-time resolution."""

import time
import uuid

import httpx
import jwt
import pytest
import pytest_asyncio
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.deps import get_custom_mode_service, get_preferences_service  # noqa: F401
from app.api.v1.custom_modes import router as custom_modes_router
from app.api.v1.preferences import router as preferences_router
from app.core.config import settings
from app.db.errors import register_postgres_handlers
from app.db.session import get_db
from app.repositories.pg_repositories import CustomModeRepository, PreferencesRepository
from app.services.custom_mode_service import CustomModeService, CustomModeUnavailableError
from app.services.mode_catalog import Catalog, ModeCatalog
from app.services.preferences_service import PreferencesService

pytestmark = pytest.mark.asyncio

ALICE, BOB = uuid.uuid4(), uuid.uuid4()


def token(user: uuid.UUID) -> dict:
    t = jwt.encode({"sub": str(user), "email": "u@example.com", "exp": int(time.time()) + 600},
                   settings.JWT_SECRET_KEY, algorithm=settings.JWT_ALGORITHM)
    return {"Authorization": f"Bearer {t}"}


def build_app(factory) -> FastAPI:
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

    register_postgres_handlers(app)
    app.include_router(custom_modes_router, prefix="/v1/custom-modes")
    app.include_router(preferences_router, prefix="/v1/preferences")
    return app


@pytest_asyncio.fixture
async def client(session_factory):
    transport = httpx.ASGITransport(app=build_app(session_factory))
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        yield c


BODY = {"name": "Code reviewer", "description": "Strict reviews", "instructions": "Review code strictly.", "execution_style": "direct"}


async def create(client, user=ALICE, **over):
    r = await client.post("/v1/custom-modes", json={**BODY, **over}, headers=token(user))
    assert r.status_code == 201, r.text
    return r.json()


# ------------------------------------------------------------------ authentication and ownership


async def test_every_endpoint_requires_authentication(client):
    for method, url in [("post", "/v1/custom-modes"), ("get", "/v1/custom-modes"), ("get", f"/v1/custom-modes/{uuid.uuid4()}"),
                        ("patch", f"/v1/custom-modes/{uuid.uuid4()}"), ("delete", f"/v1/custom-modes/{uuid.uuid4()}"),
                        ("get", "/v1/preferences"), ("patch", "/v1/preferences"), ("delete", "/v1/preferences")]:
        kwargs = {"json": BODY} if method in ("post", "patch") else {}
        assert (await getattr(client, method)(url, **kwargs)).status_code in (401, 403), (method, url)


async def test_the_owner_comes_from_the_token_never_from_the_body(client):
    r = await client.post("/v1/custom-modes", json={**BODY, "owner_user_id": str(BOB)}, headers=token(ALICE))
    assert r.status_code == 422                                           # unknown field rejected
    mode = await create(client, ALICE)
    assert "owner_user_id" not in mode
    assert (await client.get("/v1/custom-modes", headers=token(BOB))).json()["items"] == []


async def test_another_users_mode_is_indistinguishable_from_a_missing_one(client):
    mode = await create(client, ALICE)
    url = f"/v1/custom-modes/{mode['mode_id']}"
    missing = await client.get(f"/v1/custom-modes/{uuid.uuid4()}", headers=token(BOB))
    for r in (await client.get(url, headers=token(BOB)), await client.patch(url, json={"name": "x"}, headers=token(BOB)),
              await client.delete(url, headers=token(BOB))):
        assert r.status_code == 404 and r.json() == missing.json()
    assert (await client.get(url, headers=token(ALICE))).json()["name"] == "Code reviewer"       # untouched


# ------------------------------------------------------------------ CRUD and validation


async def test_crud_lifecycle(client):
    mode = await create(client)
    assert mode["version"] == 1 and mode["status"] == "active" and mode["mode_ref"] == f"custom:{mode['mode_id']}"
    h = token(ALICE)
    patched = await client.patch(f"/v1/custom-modes/{mode['mode_id']}", json={"instructions": "Be gentle.", "version": 1}, headers=h)
    assert patched.status_code == 200 and patched.json()["version"] == 2 and patched.json()["name"] == "Code reviewer"
    listed = (await client.get("/v1/custom-modes", headers=h)).json()
    assert listed["total"] == 1 and listed["max_per_user"] == settings.CUSTOM_MODE_MAX_PER_USER
    deleted = await client.delete(f"/v1/custom-modes/{mode['mode_id']}", headers=h)
    assert deleted.status_code == 200 and deleted.json()["status"] == "archived"
    assert (await client.get("/v1/custom-modes", headers=h)).json()["items"] == []
    assert len((await client.get("/v1/custom-modes?include_archived=true", headers=h)).json()["items"]) == 1
    assert (await client.patch(f"/v1/custom-modes/{mode['mode_id']}", json={"name": "x"}, headers=h)).status_code == 404


@pytest.mark.parametrize("over", [
    {"name": ""}, {"name": "   "}, {"name": "x" * 61}, {"instructions": ""}, {"instructions": "x" * 4001},
    {"description": "d" * 201}, {"execution_style": "python"}, {"execution_style": "graph"},
])
async def test_invalid_input_is_rejected(client, over):
    r = await client.post("/v1/custom-modes", json={**BODY, **over}, headers=token(ALICE))
    assert r.status_code == 422


async def test_text_is_normalized_and_control_characters_removed(client):
    mode = await create(client, name="  Spaced \x00 Name  ", instructions="line1\nline2\x07")
    assert mode["name"] == "Spaced Name" and mode["instructions"] == "line1\nline2"


async def test_conflicts_name_taken_stale_version_and_limit(client, monkeypatch):
    mode = await create(client)
    h = token(ALICE)
    dup = await client.post("/v1/custom-modes", json=BODY, headers=h)
    assert dup.status_code == 409 and dup.json()["detail"]["code"] == "name_taken"
    await client.patch(f"/v1/custom-modes/{mode['mode_id']}", json={"description": "new", "version": 1}, headers=h)
    stale = await client.patch(f"/v1/custom-modes/{mode['mode_id']}", json={"description": "old", "version": 1}, headers=h)
    assert stale.status_code == 409 and stale.json()["detail"]["current_version"] == 2
    monkeypatch.setattr(settings, "CUSTOM_MODE_MAX_PER_USER", 2)
    await create(client, name="Second")
    limit = await client.post("/v1/custom-modes", json={**BODY, "name": "Third"}, headers=h)
    assert limit.status_code == 409 and limit.json()["detail"]["code"] == "limit_reached"


async def test_pagination(client):
    for i in range(5):
        await create(client, name=f"M{i}")
    page = (await client.get("/v1/custom-modes?limit=2&offset=2", headers=token(ALICE))).json()
    assert len(page["items"]) == 2 and page["total"] == 5 and page["offset"] == 2
    assert (await client.get("/v1/custom-modes?limit=0", headers=token(ALICE))).status_code == 422
    assert (await client.get("/v1/custom-modes?limit=101", headers=token(ALICE))).status_code == 422


async def test_postgres_outage_is_a_controlled_503_never_a_false_success(session_factory):
    dead = async_sessionmaker(create_async_engine("postgresql+asyncpg://x:y@127.0.0.1:1/none"), expire_on_commit=False)
    transport = httpx.ASGITransport(app=build_app(dead))
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        w = await c.post("/v1/custom-modes", json=BODY, headers=token(ALICE))
        r = await c.get("/v1/preferences", headers=token(ALICE))
        p = await c.patch("/v1/preferences", json={"tone": "friendly"}, headers=token(ALICE))
    assert w.status_code == r.status_code == p.status_code == 503
    assert "postgres" not in w.text.lower() and "asyncpg" not in w.text.lower()


# ------------------------------------------------------------------ preferences API


async def test_preferences_defaults_partial_update_and_reset(client):
    h = token(ALICE)
    p = (await client.get("/v1/preferences", headers=h)).json()
    assert p["version"] == 0 and p["tone"] == "professional" and p["default_mode"] is None
    saved = await client.patch("/v1/preferences", json={"tone": "friendly", "version": 0}, headers=h)
    assert saved.status_code == 200 and saved.json()["version"] == 1
    again = (await client.patch("/v1/preferences", json={"answer_length": "concise", "version": 1}, headers=h)).json()
    assert again["tone"] == "friendly" and again["answer_length"] == "concise" and again["version"] == 2      # partial
    stale = await client.patch("/v1/preferences", json={"tone": "simple", "version": 1}, headers=h)
    assert stale.status_code == 409 and stale.json()["detail"]["current_version"] == 2
    reset = (await client.delete("/v1/preferences", headers=h)).json()
    assert reset["version"] == 0 and reset["tone"] == "professional"
    assert (await client.get("/v1/preferences", headers=token(BOB))).json()["tone"] == "professional"        # per-user


async def test_default_custom_mode_roundtrip_ownership_and_archive(client):
    mode = await create(client, ALICE)
    h = token(ALICE)
    ref = mode["mode_ref"]
    ok = await client.patch("/v1/preferences", json={"default_mode": ref}, headers=h)
    assert ok.status_code == 200 and ok.json()["default_mode"] == ref and ok.json()["default_mode_available"] is True
    foreign = await client.patch("/v1/preferences", json={"default_mode": ref}, headers=token(BOB))
    assert foreign.status_code == 422                                      # someone else's mode cannot be a default
    assert (await client.patch("/v1/preferences", json={"default_mode": f"custom:{uuid.uuid4()}"}, headers=h)).status_code == 422
    assert (await client.patch("/v1/preferences", json={"default_mode": "custom:not-a-uuid"}, headers=h)).status_code == 422
    await client.delete(f"/v1/custom-modes/{mode['mode_id']}", headers=h)
    after = (await client.get("/v1/preferences", headers=h)).json()
    assert after["default_mode"] == ref and after["default_mode_available"] is False        # visible, not silently dropped
    cleared = await client.patch("/v1/preferences", json={"default_mode": "auto"}, headers=h)
    assert cleared.json()["default_mode"] is None and cleared.json()["default_mode_available"] is True


async def test_builtin_mode_and_tool_ids_are_validated_against_the_live_catalog(session_factory):
    async def fetch():
        return Catalog(modes={"tutor", "code"}, tools={"calculator"})

    async with session_factory() as s:
        svc = PreferencesService(PreferencesRepository(s), CustomModeRepository(s), ModeCatalog(fetch))
        from app.models.preferences import PreferencesUpdate
        from app.services.preferences_service import UnknownCatalogItemError

        assert (await svc.update(ALICE, PreferencesUpdate(default_mode="tutor"))).default_mode == "tutor"
        with pytest.raises(UnknownCatalogItemError):
            await svc.update(ALICE, PreferencesUpdate(default_mode="made_up"))
        with pytest.raises(UnknownCatalogItemError):
            await svc.update(ALICE, PreferencesUpdate(default_tools=["nope"]))
        await s.commit()

    async def down():
        raise RuntimeError("llm-service unreachable")

    async with session_factory() as s:                                    # catalog unknown: accepted (documented)
        svc = PreferencesService(PreferencesRepository(s), CustomModeRepository(s), ModeCatalog(down))
        assert (await svc.update(BOB, PreferencesUpdate(default_mode="anything"))).default_mode == "anything"
        await s.commit()


# ------------------------------------------------------------------ message-time resolution


async def resolver(factory):
    s = factory()
    session = await s.__aenter__()
    return session, PreferencesService(PreferencesRepository(session), CustomModeRepository(session)), CustomModeService(CustomModeRepository(session))


async def test_precedence_explicit_then_saved_default_then_builtin(session_factory):
    session, prefs, modes = await resolver(session_factory)
    try:
        from app.models.preferences import PreferencesUpdate
        await prefs.update(ALICE, PreferencesUpdate(default_mode="tutor", default_tools=["calculator"]))
        d = await prefs.resolve_for_message(ALICE, None, None)
        assert (d.mode, d.selected_tools, d.defaults_applied) == ("tutor", ["calculator"], {"mode": True, "tools": True})
        e = await prefs.resolve_for_message(ALICE, "code", [])
        assert (e.mode, e.selected_tools, e.defaults_applied) == ("code", [], {"mode": False, "tools": False})
        other = await prefs.resolve_for_message(BOB, None, None)
        assert (other.mode, other.selected_tools) == (None, None)
    finally:
        await session.close()


async def test_custom_mode_resolves_to_an_immutable_snapshot_on_its_builtin_path(session_factory):
    session, prefs, modes = await resolver(session_factory)
    try:
        from app.models.custom_modes import CustomModeCreate
        direct = await modes.create(ALICE, CustomModeCreate(name="D", instructions="Do D.", execution_style="direct"))
        smart = await modes.create(ALICE, CustomModeCreate(name="S", instructions="Do S.", execution_style="smart"))
        r1 = await prefs.resolve_for_message(ALICE, direct.mode_ref, None)
        r2 = await prefs.resolve_for_message(ALICE, smart.mode_ref, None)
        assert (r1.mode, r1.custom_mode.mode_id, r1.custom_mode.version, r1.custom_mode.instructions) == ("default", str(direct.mode_id), 1, "Do D.")
        assert (r2.mode, r2.custom_mode.execution_style.value) == ("smart", "smart")
        assert r1.defaults_applied["mode"] is False
    finally:
        await session.close()


async def test_explicit_unavailable_custom_mode_is_an_error_never_a_substitute(session_factory):
    session, prefs, modes = await resolver(session_factory)
    try:
        from app.models.custom_modes import CustomModeCreate
        mode = await modes.create(ALICE, CustomModeCreate(name="Gone", instructions="x"))
        await modes.archive(ALICE, mode.mode_id)
        with pytest.raises(CustomModeUnavailableError):
            await prefs.resolve_for_message(ALICE, mode.mode_ref, None)                 # archived
        with pytest.raises(CustomModeUnavailableError):
            await prefs.resolve_for_message(BOB, mode.mode_ref, None)                   # someone else's
        with pytest.raises(CustomModeUnavailableError):
            await prefs.resolve_for_message(ALICE, f"custom:{uuid.uuid4()}", None)      # unknown
        with pytest.raises(CustomModeUnavailableError):
            await prefs.resolve_for_message(ALICE, "custom:garbage", None)              # malformed
    finally:
        await session.close()


async def test_saved_default_pointing_at_an_archived_mode_falls_back_to_auto(session_factory):
    session, prefs, modes = await resolver(session_factory)
    try:
        from app.models.custom_modes import CustomModeCreate
        from app.models.preferences import PreferencesUpdate
        mode = await modes.create(ALICE, CustomModeCreate(name="Default one", instructions="x"))
        await prefs.update(ALICE, PreferencesUpdate(default_mode=mode.mode_ref))
        assert (await prefs.resolve_for_message(ALICE, None, None)).custom_mode is not None
        await modes.archive(ALICE, mode.mode_id)
        r = await prefs.resolve_for_message(ALICE, None, None)
        assert (r.mode, r.custom_mode, r.defaults_applied["mode"]) == (None, None, False)
    finally:
        await session.close()


async def test_resolution_survives_a_postgres_outage_with_builtin_defaults_for_default_lookups(session_factory):
    dead = async_sessionmaker(create_async_engine("postgresql+asyncpg://x:y@127.0.0.1:1/none"), expire_on_commit=False)
    async with dead() as s:
        prefs = PreferencesService(PreferencesRepository(s), CustomModeRepository(s))
        r = await prefs.resolve_for_message(ALICE, "tutor", None)
        assert (r.mode, r.custom_mode, r.generation["tone"]) == ("tutor", None, "professional")
