"""User preferences: validation, persistence semantics, precedence, propagation onto the
chat.message.created event, and API authorization / isolation.

Storage is an in-memory fake with the same conditional-write semantics as the Cassandra
repository (insert-if-absent / update-if-version)."""

import json
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import jwt
import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from app.api.deps import get_preferences_service
from app.core.config import settings
from app.main import app
from app.models.preferences import (
    PERSONAL_INSTRUCTIONS_MAX,
    PreferencesUpdate,
    UserPreferences,
)
from app.repositories.preferences_repository import PreferencesConflictError
from app.services.message_service import MessageService
from app.services.preferences_service import PreferencesService

pytestmark = pytest.mark.anyio


class FakePrefsRepo:
    def __init__(self):
        self.rows: dict = {}
        self.fail_reads = False

    def get(self, user_id):
        if self.fail_reads:
            raise RuntimeError("cassandra down")
        row = self.rows.get(user_id)
        return dict(row) if row else None

    def save(self, user_id, values, expected_version):
        current = self.rows.get(user_id, {}).get("version", 0) if user_id in self.rows else 0
        if current != expected_version:
            raise PreferencesConflictError(current)
        self.rows[user_id] = {**values, "version": expected_version + 1, "updated_at": datetime.now(timezone.utc)}
        return expected_version + 1

    def delete(self, user_id):
        self.rows.pop(user_id, None)


@pytest.fixture
def svc():
    return PreferencesService(FakePrefsRepo())


U1, U2 = uuid.uuid4(), uuid.uuid4()


# ---------------------------------------------------------------- model / validation


def test_defaults_when_nothing_saved(svc):
    p = svc.get(U1)
    assert (p.default_mode, p.default_tools, p.answer_length, p.tone, p.language) == (None, [], "balanced", "professional", "auto")
    assert (p.personal_instructions, p.citations, p.document_context, p.version) == ("", "show", "conversation", 0)
    assert {"answer_length", "tone", "languages", "personal_instructions_max"} <= set(p.options)


@pytest.mark.parametrize("body", [
    {"answer_length": "huge"},
    {"tone": "angry"},
    {"language": "xx"},
    {"citations": "always"},
    {"document_context": "everything"},
    {"default_mode": "bad mode!"},
    {"default_tools": ["../etc/passwd"]},
    {"default_tools": [f"t{i}" for i in range(11)]},
    {"personal_instructions": "x" * (PERSONAL_INSTRUCTIONS_MAX + 1)},
    {"unknown_field": 1},
])
def test_invalid_values_are_rejected(body):
    with pytest.raises(ValidationError):
        PreferencesUpdate(**body)


def test_instructions_are_trimmed_and_stripped_of_control_characters():
    assert PreferencesUpdate(personal_instructions="  use\x00 examples\x07  ").personal_instructions == "use examples"


def test_auto_mode_normalises_and_is_distinct_from_not_sent():
    assert PreferencesUpdate(default_mode="auto").changes() == {"default_mode": None}
    assert PreferencesUpdate(default_mode=None).changes() == {"default_mode": None}
    assert PreferencesUpdate(tone="friendly").changes() == {"tone": "friendly"}


def test_stored_invalid_value_falls_back_to_its_default_only():
    p = UserPreferences.from_row({"tone": "removed-tone", "answer_length": "concise", "language": "hi"})
    assert (p.tone, p.answer_length, p.language) == ("professional", "concise", "hi")


# ---------------------------------------------------------------- persistence semantics


def test_partial_update_changes_only_sent_fields(svc):
    svc.update(U1, PreferencesUpdate(tone="friendly", default_tools=["web_search"]))
    p = svc.update(U1, PreferencesUpdate(answer_length="concise"))
    assert (p.tone, p.answer_length, p.default_tools, p.version) == ("friendly", "concise", ["web_search"], 2)


def test_stale_version_is_rejected_and_newer_data_survives(svc):
    svc.update(U1, PreferencesUpdate(tone="friendly"))           # version 1
    svc.update(U1, PreferencesUpdate(tone="simple", version=1))  # version 2 (other device)
    with pytest.raises(PreferencesConflictError):
        svc.update(U1, PreferencesUpdate(tone="professional", version=1))  # stale tab
    assert svc.get(U1).tone == "simple"


def test_reset_restores_defaults(svc):
    svc.update(U1, PreferencesUpdate(tone="friendly", personal_instructions="Prefer Python"))
    p = svc.reset(U1)
    assert (p.tone, p.personal_instructions, p.version) == ("professional", "", 0)


def test_users_are_isolated(svc):
    svc.update(U1, PreferencesUpdate(personal_instructions="secret style"))
    assert svc.get(U2).personal_instructions == ""


# ---------------------------------------------------------------- precedence


def test_saved_defaults_apply_when_the_message_has_no_choice(svc):
    svc.update(U1, PreferencesUpdate(default_mode="code", default_tools=["web_search"], tone="simple"))
    r = svc.resolve_for_message(U1, None, None)
    assert (r.mode, r.selected_tools) == ("code", ["web_search"])
    assert r.defaults_applied == {"mode": True, "tools": True}
    assert r.generation["tone"] == "simple"


def test_explicit_choices_win_including_auto_and_no_tools(svc):
    svc.update(U1, PreferencesUpdate(default_mode="code", default_tools=["web_search"]))
    r = svc.resolve_for_message(U1, "auto", [])
    assert (r.mode, r.selected_tools) == ("auto", [])
    assert r.defaults_applied == {"mode": False, "tools": False}
    # ...and the saved defaults are untouched by the override
    assert (svc.get(U1).default_mode, svc.get(U1).default_tools) == ("code", ["web_search"])


def test_unreadable_preferences_never_block_a_message():
    repo = FakePrefsRepo()
    repo.fail_reads = True
    r = PreferencesService(repo).resolve_for_message(U1, "tutor", None)
    assert (r.mode, r.selected_tools, r.generation["tone"]) == ("tutor", None, "professional")


# ---------------------------------------------------------------- propagation onto the event


def _message_service(prefs_service):
    repo = MagicMock()
    repo.create_with_outbox.side_effect = lambda **kw: MagicMock(response_message_id=None, **{"message_id": kw["message_id"]})
    repo.history.return_value = []
    redis = MagicMock()
    redis.set, redis.zadd, redis.delete, redis.zrem = AsyncMock(), AsyncMock(), AsyncMock(), AsyncMock()
    cache = MagicMock()
    cache.redis = redis
    cache.delete_last_50_messages = AsyncMock()
    return MessageService(repo=repo, cache_service=cache, preferences_service=prefs_service), repo


async def test_send_puts_resolved_defaults_and_generation_preferences_on_the_event(svc):
    svc.update(U1, PreferencesUpdate(default_mode="code", answer_length="concise", language="hi",
                                     personal_instructions="Prefer Python examples", citations="hide"))
    ms, repo = _message_service(svc)
    await ms.send(uuid.uuid4(), uuid.uuid4(), "user", "hello", user_id=U1)
    payload = json.loads(repo.create_with_outbox.call_args.kwargs["outbox_payload"])
    assert payload["schema_version"] == "2.2"
    assert payload["mode_hint"] == "code" and payload["defaults_applied"] == {"mode": True, "tools": False}
    p = payload["preferences"]
    assert (p["answer_length"], p["language"], p["language_name"], p["citations"]) == ("concise", "hi", "Hindi", "hide")
    assert p["personal_instructions"] == "Prefer Python examples"


async def test_send_with_explicit_override_marks_nothing_as_default(svc):
    svc.update(U1, PreferencesUpdate(default_mode="code", default_tools=["web_search"]))
    ms, repo = _message_service(svc)
    await ms.send(uuid.uuid4(), uuid.uuid4(), "user", "hello", user_id=U1, mode="tutor", selected_tools=[])
    payload = json.loads(repo.create_with_outbox.call_args.kwargs["outbox_payload"])
    assert (payload["mode_hint"], payload["selected_tools"]) == ("tutor", [])
    assert payload["defaults_applied"] == {"mode": False, "tools": False}


async def test_service_without_preferences_keeps_the_old_behaviour():
    ms, repo = _message_service(None)
    await ms.send(uuid.uuid4(), uuid.uuid4(), "user", "hello", user_id=U1, mode="tutor")
    payload = json.loads(repo.create_with_outbox.call_args.kwargs["outbox_payload"])
    assert payload["mode_hint"] == "tutor" and payload["preferences"] == {}


# ---------------------------------------------------------------- API: authn / isolation


def _token(user_id):
    return jwt.encode({"sub": str(user_id), "email": "p@example.com",
                       "exp": datetime.now(timezone.utc) + timedelta(minutes=5)},
                      settings.JWT_SECRET_KEY, algorithm=settings.JWT_ALGORITHM)


@pytest.fixture
def client(svc):
    app.dependency_overrides[get_preferences_service] = lambda: svc
    yield TestClient(app)
    app.dependency_overrides.pop(get_preferences_service, None)


def test_api_requires_authentication(client):
    assert client.get("/v1/preferences").status_code in (401, 403)
    assert client.patch("/v1/preferences", json={"tone": "simple"}).status_code in (401, 403)


def test_api_round_trip_and_isolation(client):
    h1, h2 = {"Authorization": f"Bearer {_token(U1)}"}, {"Authorization": f"Bearer {_token(U2)}"}
    r = client.patch("/v1/preferences", json={"tone": "friendly", "version": 0}, headers=h1)
    assert r.status_code == 200 and r.json()["tone"] == "friendly" and r.json()["version"] == 1
    assert client.get("/v1/preferences", headers=h1).json()["tone"] == "friendly"
    assert client.get("/v1/preferences", headers=h2).json()["tone"] == "professional"


def test_api_validation_and_conflict(client):
    h = {"Authorization": f"Bearer {_token(U1)}"}
    assert client.patch("/v1/preferences", json={"tone": "angry"}, headers=h).status_code == 422
    assert client.patch("/v1/preferences", json={"user_id": str(U2)}, headers=h).status_code == 422
    client.patch("/v1/preferences", json={"tone": "simple"}, headers=h)
    r = client.patch("/v1/preferences", json={"tone": "friendly", "version": 0}, headers=h)
    assert r.status_code == 409 and r.json()["detail"]["current_version"] == 1


def test_api_reset(client):
    h = {"Authorization": f"Bearer {_token(U1)}"}
    client.patch("/v1/preferences", json={"tone": "simple"}, headers=h)
    r = client.delete("/v1/preferences", headers=h)
    assert r.status_code == 200 and r.json()["tone"] == "professional" and r.json()["version"] == 0
