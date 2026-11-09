"""P6b: model selection. Resolution paths, request/preference validation, preference save + version conflict, and the
Alembic migration (against a throwaway database in the shared dev Postgres)."""

import uuid
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from app.clients.billing_client import ReserveResult
from app.models.preferences import PreferencesUpdate, UserPreferences
from app.repositories.pg_repositories import PreferencesConflictError
from app.schemas.message import CreateMessageRequest, RegenerateRequest
from app.services.model_selection import ModelChoice, choose_model, reserve_with_default_fallback
from app.services.preferences_service import PreferencesService
from tests.support.shared_pg import migrated_pg_url  # noqa: F401  (fixture)

pytestmark = pytest.mark.anyio


# ---- request validation ----------------------------------------------------------------------------------------

@pytest.mark.parametrize("raw,expected", [("auto", "auto"), ("AUTO", "auto"), ("  claude-sonnet-5-5 ", "claude-sonnet-5-5"),
                                          ("openai/gpt-oss-20b", "openai/gpt-oss-20b"), ("gpt-6.1-sol", "gpt-6.1-sol"),
                                          ("", "auto")])
def test_request_model_accepted_forms(raw, expected):
    assert CreateMessageRequest(content="hi", model=raw).model == expected
    assert RegenerateRequest(model=raw).model == expected


@pytest.mark.parametrize("bad", ["Claude Sonnet", "a" * 65, "model;drop", "x y", "é", "a_b"])
def test_request_model_rejected_forms(bad):
    with pytest.raises(ValidationError):
        CreateMessageRequest(content="hi", model=bad)


def test_request_model_defaults_to_none():
    assert CreateMessageRequest(content="hi").model is None
    assert RegenerateRequest().model is None


# ---- resolution ------------------------------------------------------------------------------------------------

def test_explicit_model_wins_over_default():
    assert choose_model("claude-sonnet-5-5", "gpt-6-luna") == ModelChoice("claude-sonnet-5-5", False)


def test_explicit_auto_wins_over_a_saved_model_default():
    assert choose_model("auto", "gpt-6-luna") == ModelChoice(None, False)


def test_omitted_uses_saved_default():
    assert choose_model(None, "gpt-6-luna") == ModelChoice("gpt-6-luna", True)


def test_omitted_with_auto_default_is_auto():
    assert choose_model(None, "auto") == ModelChoice(None, True)
    assert choose_model(None, None) == ModelChoice(None, True)


def _result(allowed, reason=""):
    return ReserveResult(allowed=allowed, deny_reason=reason, hold_id="h" if allowed else None,
                         granted_group="fast" if allowed else None)


async def test_denied_saved_default_retries_once_as_auto():
    calls = []

    async def reserve(model):
        calls.append(model)
        return _result(False, "model_not_in_plan") if model else _result(True)

    result, final, fell_back = await reserve_with_default_fallback(ModelChoice("claude-sonnet-5-5", True), reserve)
    assert result.allowed and fell_back and final.requested_model is None
    assert calls == ["claude-sonnet-5-5", None]


async def test_explicit_model_denied_is_strict():
    calls = []

    async def reserve(model):
        calls.append(model)
        return _result(False, "model_not_in_plan")

    result, final, fell_back = await reserve_with_default_fallback(ModelChoice("claude-sonnet-5-5", False), reserve)
    assert not result.allowed and result.deny_reason == "model_not_in_plan" and not fell_back
    assert calls == ["claude-sonnet-5-5"]


async def test_saved_default_denied_for_another_reason_is_not_retried():
    calls = []

    async def reserve(model):
        calls.append(model)
        return _result(False, "daily_limit")

    result, _, fell_back = await reserve_with_default_fallback(ModelChoice("gpt-6-luna", True), reserve)
    assert result.deny_reason == "daily_limit" and not fell_back and calls == ["gpt-6-luna"]


async def test_fallback_retries_only_once():
    calls = []

    async def reserve(model):
        calls.append(model)
        return _result(False, "model_not_in_plan")

    result, _, fell_back = await reserve_with_default_fallback(ModelChoice("m", True), reserve)
    assert not result.allowed and fell_back and calls == ["m", None]


# ---- preferences: default_model --------------------------------------------------------------------------------

class FakePrefsRepo:
    def __init__(self):
        self.rows = {}

    async def get(self, user_id):
        row = self.rows.get(user_id)
        return dict(row) if row else None

    async def save(self, user_id, values, expected_version):
        current = self.rows[user_id]["version"] if user_id in self.rows else 0
        if current != expected_version:
            raise PreferencesConflictError(current)
        self.rows[user_id] = {**values, "version": expected_version + 1, "updated_at": datetime.now(timezone.utc)}
        return expected_version + 1

    async def delete(self, user_id):
        self.rows.pop(user_id, None)


class FakeModes:
    async def get(self, *_a, **_k):
        return None


@pytest.fixture
def svc():
    return PreferencesService(FakePrefsRepo(), FakeModes())


async def test_default_model_defaults_to_auto(svc):
    assert (await svc.get(uuid.uuid4())).default_model == "auto"


async def test_default_model_save_and_read_back(svc):
    u = uuid.uuid4()
    saved = await svc.update(u, PreferencesUpdate(default_model="Claude-Haiku-5-5"))
    assert saved.default_model == "claude-haiku-5-5" and saved.version == 1
    assert (await svc.get(u)).default_model == "claude-haiku-5-5"
    again = await svc.update(u, PreferencesUpdate(tone="friendly"))          # other fields leave it alone
    assert again.default_model == "claude-haiku-5-5"
    back = await svc.update(u, PreferencesUpdate(default_model="auto"))
    assert back.default_model == "auto"


def test_default_model_rejects_bad_format():
    with pytest.raises(ValidationError):
        PreferencesUpdate(default_model="Not A Model!")


async def test_default_model_version_conflict(svc):
    u = uuid.uuid4()
    await svc.update(u, PreferencesUpdate(default_model="gpt-6-luna"))               # version 1
    await svc.update(u, PreferencesUpdate(default_model="gpt-6.1-sol", version=1))   # version 2
    with pytest.raises(PreferencesConflictError) as e:
        await svc.update(u, PreferencesUpdate(default_model="auto", version=1))      # stale
    assert e.value.current_version == 2
    assert (await svc.get(u)).default_model == "gpt-6.1-sol"


def test_stored_invalid_model_falls_back_to_auto():
    assert UserPreferences.from_row({"default_model": "BAD VALUE!"}).default_model == "auto"


async def test_resolution_carries_saved_default(svc):
    u = uuid.uuid4()
    await svc.update(u, PreferencesUpdate(default_model="gpt-6-luna"))
    resolved = await svc.resolve_for_message(u, None, None)
    assert resolved.default_model == "gpt-6-luna"
    assert (await svc.resolve_for_message(uuid.uuid4(), None, None)).default_model == "auto"


# ---- migration / real Postgres ---------------------------------------------------------------------------------

async def test_migration_adds_default_model_with_auto_default(migrated_pg_url):
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine
    eng = create_async_engine(migrated_pg_url)
    uid = uuid.uuid4()
    try:
        async with eng.begin() as conn:
            await conn.execute(text("INSERT INTO user_preferences (user_id) VALUES (:u)"), {"u": uid})
            got = (await conn.execute(text("SELECT default_model FROM user_preferences WHERE user_id=:u"), {"u": uid})).scalar_one()
        assert got == "auto"
    finally:
        await eng.dispose()


async def test_repository_round_trip_on_real_postgres(migrated_pg_url):
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
    from app.repositories.pg_repositories import CustomModeRepository, PreferencesRepository
    eng = create_async_engine(migrated_pg_url)
    sf = async_sessionmaker(bind=eng, expire_on_commit=False, autoflush=False)
    uid = uuid.uuid4()
    try:
        async with sf() as s:
            svc = PreferencesService(PreferencesRepository(s), CustomModeRepository(s))
            await svc.update(uid, PreferencesUpdate(default_model="gpt-6-luna"))
            await s.commit()
        async with sf() as s:
            svc = PreferencesService(PreferencesRepository(s), CustomModeRepository(s))
            assert (await svc.get(uid)).default_model == "gpt-6-luna"
            with pytest.raises(PreferencesConflictError):
                await svc.update(uid, PreferencesUpdate(default_model="auto", version=99))
    finally:
        await eng.dispose()
