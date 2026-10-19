"""User preferences: validation, precedence, propagation onto the chat.message.created event.

Persistence semantics (compare-and-set, ownership, constraints) and the HTTP API are tested against real PostgreSQL in
tests/postgres/. Here the repositories are in-memory fakes with the same async interface, so the rules run fast."""

import json
import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import ValidationError

from app.models.preferences import PERSONAL_INSTRUCTIONS_MAX, PreferencesUpdate, UserPreferences
from app.repositories.pg_repositories import PreferencesConflictError
from app.services.message_service import MessageService
from app.services.preferences_service import PreferencesService

pytestmark = pytest.mark.asyncio


class FakePrefsRepo:
    def __init__(self):
        self.rows: dict = {}
        self.fail_reads = False

    async def get(self, user_id):
        if self.fail_reads:
            raise RuntimeError("postgres down")
        row = self.rows.get(user_id)
        return dict(row) if row else None

    async def save(self, user_id, values, expected_version):
        current = self.rows.get(user_id, {}).get("version", 0) if user_id in self.rows else 0
        if current != expected_version:
            raise PreferencesConflictError(current)
        self.rows[user_id] = {**values, "version": expected_version + 1, "updated_at": datetime.now(timezone.utc)}
        return expected_version + 1

    async def delete(self, user_id):
        self.rows.pop(user_id, None)


class FakeModesRepo:
    async def get(self, *_a, **_k):
        return None


@pytest.fixture
def svc():
    return PreferencesService(FakePrefsRepo(), FakeModesRepo())


U1, U2 = uuid.uuid4(), uuid.uuid4()


# ---------------------------------------------------------------- model / validation


async def test_defaults_when_nothing_saved(svc):
    p = await svc.get(U1)
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
    {"default_mode": "custom:not-a-uuid"},
    {"default_tools": ["../etc/passwd"]},
    {"default_tools": [f"t{i}" for i in range(11)]},
    {"personal_instructions": "x" * (PERSONAL_INSTRUCTIONS_MAX + 1)},
    {"unknown_field": 1},
])
async def test_invalid_values_are_rejected(body):
    with pytest.raises(ValidationError):
        PreferencesUpdate(**body)


async def test_instructions_are_trimmed_and_stripped_of_control_characters():
    assert PreferencesUpdate(personal_instructions="  use\x00 examples\x07  ").personal_instructions == "use examples"


async def test_auto_mode_normalises_and_is_distinct_from_not_sent():
    assert PreferencesUpdate(default_mode="auto").changes() == {"default_mode": None}
    assert PreferencesUpdate(default_mode=None).changes() == {"default_mode": None}
    assert PreferencesUpdate(tone="friendly").changes() == {"tone": "friendly"}


async def test_a_custom_mode_reference_is_a_valid_default_mode():
    ref = f"custom:{uuid.uuid4()}"
    assert PreferencesUpdate(default_mode=ref).changes() == {"default_mode": ref}


async def test_stored_invalid_value_falls_back_to_its_default_only():
    p = UserPreferences.from_row({"tone": "removed-tone", "answer_length": "concise", "language": "hi"})
    assert (p.tone, p.answer_length, p.language) == ("professional", "concise", "hi")


# ---------------------------------------------------------------- update semantics (fake store)


async def test_partial_update_changes_only_sent_fields(svc):
    await svc.update(U1, PreferencesUpdate(tone="friendly", default_tools=["web_search"]))
    p = await svc.update(U1, PreferencesUpdate(answer_length="concise"))
    assert (p.tone, p.answer_length, p.default_tools, p.version) == ("friendly", "concise", ["web_search"], 2)


async def test_stale_version_is_rejected_and_newer_data_survives(svc):
    await svc.update(U1, PreferencesUpdate(tone="friendly"))           # version 1
    await svc.update(U1, PreferencesUpdate(tone="simple", version=1))  # version 2 (other device)
    with pytest.raises(PreferencesConflictError):
        await svc.update(U1, PreferencesUpdate(tone="professional", version=1))  # stale tab
    assert (await svc.get(U1)).tone == "simple"


async def test_reset_and_user_isolation(svc):
    await svc.update(U1, PreferencesUpdate(tone="friendly", personal_instructions="secret style"))
    assert (await svc.get(U2)).personal_instructions == ""
    p = await svc.reset(U1)
    assert (p.tone, p.personal_instructions, p.version) == ("professional", "", 0)


# ---------------------------------------------------------------- precedence


async def test_saved_defaults_apply_when_the_message_has_no_choice(svc):
    await svc.update(U1, PreferencesUpdate(default_mode="code", default_tools=["web_search"], tone="simple"))
    r = await svc.resolve_for_message(U1, None, None)
    assert (r.mode, r.selected_tools) == ("code", ["web_search"])
    assert r.defaults_applied == {"mode": True, "tools": True}
    assert r.generation["tone"] == "simple" and r.custom_mode is None


async def test_explicit_choices_win_including_auto_and_no_tools(svc):
    await svc.update(U1, PreferencesUpdate(default_mode="code", default_tools=["web_search"]))
    r = await svc.resolve_for_message(U1, "auto", [])
    assert (r.mode, r.selected_tools) == ("auto", [])
    assert r.defaults_applied == {"mode": False, "tools": False}
    # ...and the saved defaults are untouched by the override
    p = await svc.get(U1)
    assert (p.default_mode, p.default_tools) == ("code", ["web_search"])


async def test_unreadable_preferences_never_block_a_message():
    repo = FakePrefsRepo()
    repo.fail_reads = True
    r = await PreferencesService(repo, FakeModesRepo()).resolve_for_message(U1, "tutor", None)
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
    await svc.update(U1, PreferencesUpdate(default_mode="code", answer_length="concise", language="hi",
                                           personal_instructions="Prefer Python examples", citations="hide"))
    ms, repo = _message_service(svc)
    await ms.send(uuid.uuid4(), uuid.uuid4(), "user", "hello", user_id=U1)
    payload = json.loads(repo.create_with_outbox.call_args.kwargs["outbox_payload"])
    assert payload["schema_version"] == "2.3" and payload["custom_mode"] is None
    assert payload["mode_hint"] == "code" and payload["defaults_applied"] == {"mode": True, "tools": False}
    p = payload["preferences"]
    assert (p["answer_length"], p["language"], p["language_name"], p["citations"]) == ("concise", "hi", "Hindi", "hide")
    assert p["personal_instructions"] == "Prefer Python examples"


async def test_send_with_explicit_override_marks_nothing_as_default(svc):
    await svc.update(U1, PreferencesUpdate(default_mode="code", default_tools=["web_search"]))
    ms, repo = _message_service(svc)
    await ms.send(uuid.uuid4(), uuid.uuid4(), "user", "hello", user_id=U1, mode="tutor", selected_tools=[])
    payload = json.loads(repo.create_with_outbox.call_args.kwargs["outbox_payload"])
    assert (payload["mode_hint"], payload["selected_tools"]) == ("tutor", [])
    assert payload["defaults_applied"] == {"mode": False, "tools": False}


async def test_service_without_preferences_keeps_the_old_behaviour():
    ms, repo = _message_service(None)
    await ms.send(uuid.uuid4(), uuid.uuid4(), "user", "hello", user_id=U1, mode="tutor")
    payload = json.loads(repo.create_with_outbox.call_args.kwargs["outbox_payload"])
    assert payload["mode_hint"] == "tutor" and payload["preferences"] == {} and payload["custom_mode"] is None


async def test_hinglish_is_a_valid_persisted_language_and_reaches_the_generation_preferences():
    from app.models.preferences import SUPPORTED_LANGUAGES, generation_preferences, preference_options

    assert "hinglish" in SUPPORTED_LANGUAGES
    assert any(l["code"] == "hinglish" for l in preference_options()["languages"])
    assert PreferencesUpdate(language="hinglish").changes()["language"] == "hinglish"
    p = UserPreferences.from_row({"language": "hinglish"})
    assert p.language == "hinglish"
    g = generation_preferences(p)
    assert g["language"] == "hinglish" and g["language_name"] == "Hinglish"
    assert UserPreferences.from_row({"language": "klingon"}).language == "auto"   # unknown still falls back
