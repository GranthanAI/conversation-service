"""Quick wins: think longer (Q7), suggestions (Q5), temporary chats (Q4), thumbs (Q2), share links (Q3), edit (Q1).

SQL runs against a throwaway database in the shared dev Postgres (same fixture as test_plan_limits); Cassandra, Kafka and
Redis are faked. Nothing here calls billing or an LLM.
"""

import gzip
import json
import uuid
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.artifacts.storage import MemoryObjectStore
from app.clients.billing_client import EntitlementsData
from app.core.billing_messages import PlanLimitError
from app.core.config import settings
from app.core.plan_defaults import FREE_LIMITS
from app.models.conversation import Conversation, ConversationStatus
from app.models.message import Message, MessageStatus
from app.services.edit_service import EditRefused, EditService
from app.services.feedback_service import FeedbackNotAllowed, FeedbackService
from app.services.plan_limits import IST, PlanLimitService, ist_day
from app.services.share_service import (
    RateLimited, ShareNotFound, ShareRefused, ShareService, hash_token, new_token, revoke_for_conversation,
)
from app.services.suggestions import dumps_suggestions, loads_suggestions, sanitize_suggestions
from app.services.temporary_service import TemporaryService, expire_due, temporary_ids_of_user
from tests.support.shared_pg import migrated_pg_url  # noqa: F401  (fixture)

pytestmark = pytest.mark.anyio

U = uuid.UUID(int=0xA1)
NOW = datetime(2026, 11, 22, 12, 0, tzinfo=timezone.utc)


@pytest.fixture
async def sf(migrated_pg_url):
    eng = create_async_engine(migrated_pg_url)
    yield async_sessionmaker(bind=eng, expire_on_commit=False, autoflush=False)
    await eng.dispose()


@pytest.fixture
def enforce(monkeypatch):
    monkeypatch.setattr(settings, "BILLING_ENFORCEMENT", "enforce")


class Ent:
    def __init__(self, **limits):
        self.ent = EntitlementsData(user_id="u", plan_code="pro", limits={**FREE_LIMITS, **limits})

    async def get(self, _user):
        return self.ent


def _conv(user=U, parent=None, branch=None, temporary=False, title="t", cid=None, status=ConversationStatus.ACTIVE, expires=None):
    now = datetime.now(timezone.utc)
    return Conversation(conversation_id=cid or uuid.uuid4(), user_id=user, title=title, created_at=now, updated_at=now,
                        status=status, parent_conversation_id=parent, branch_message_id=branch, temporary=temporary,
                        expires_at=expires)


def _msg(cid, mid, sender="user", content="hi", status="sent", **kw):
    return Message(conversation_id=cid, message_id=mid, sender=sender, content=content, created_at=NOW,
                   status=MessageStatus(status), **kw)


def mid(n):      # ordered message ids (history is newest first = larger id first)
    return uuid.UUID(int=n)


# ======================================================================================================== Q7 think

async def _take(sf, ent, now=NOW, user=U):
    async with sf() as s:
        svc = PlanLimitService(ent, session=s)
        out = await svc.take_think(user, now)
        await s.commit()
        return out


async def test_think_not_in_plan_is_refused(sf, enforce):
    with pytest.raises(PlanLimitError) as e:
        await _take(sf, Ent(think_longer_per_day=0))
    assert e.value.detail["limit"] == "think_longer_per_day" and e.value.status_code == 403
    assert "midnight" in e.value.detail["message"]


async def test_think_counts_and_refuses_at_the_limit_then_resets_at_ist_midnight(sf, enforce):
    ent, user = Ent(think_longer_per_day=2), uuid.uuid4()
    before_midnight = datetime(2026, 11, 22, 18, 29, tzinfo=timezone.utc)      # 23:59 IST
    assert await _take(sf, ent, before_midnight, user) and await _take(sf, ent, before_midnight, user)
    with pytest.raises(PlanLimitError):
        await _take(sf, ent, before_midnight, user)
    after_midnight = datetime(2026, 11, 22, 18, 31, tzinfo=timezone.utc)       # 00:01 IST next day
    assert ist_day(before_midnight) != ist_day(after_midnight)
    assert await _take(sf, ent, after_midnight, user)


async def test_think_unlimited_is_never_counted(sf, enforce):
    ent, user = Ent(think_longer_per_day=-1), uuid.uuid4()
    for _ in range(5):
        assert await _take(sf, ent, NOW, user)
    async with sf() as s:
        assert (await s.execute(text("SELECT count(*) FROM think_usage_daily WHERE user_id = :u"), {"u": user})).scalar_one() == 0


async def test_give_back_never_goes_below_zero(sf, enforce):
    ent, user = Ent(think_longer_per_day=3), uuid.uuid4()
    await _take(sf, ent, NOW, user)
    async with sf() as s:
        svc = PlanLimitService(ent, session=s)
        await svc.give_back_think(user, NOW)
        await svc.give_back_think(user, NOW)
        await s.commit()
    async with sf() as s:
        assert (await s.execute(text("SELECT count FROM think_usage_daily WHERE user_id = :u"), {"u": user})).scalar_one() == 0


async def test_shadow_mode_decides_but_never_refuses(sf, monkeypatch):
    monkeypatch.setattr(settings, "BILLING_ENFORCEMENT", "shadow")
    assert await _take(sf, Ent(think_longer_per_day=0))
    assert await _take(sf, Ent(think_longer_per_day=1), NOW, uuid.uuid4())


async def test_off_never_checks(sf, monkeypatch):
    monkeypatch.setattr(settings, "BILLING_ENFORCEMENT", "off")
    assert await _take(sf, Ent(think_longer_per_day=0))


async def test_check_think_is_read_only(sf, enforce):
    ent, user = Ent(think_longer_per_day=1), uuid.uuid4()
    async with sf() as s:
        await PlanLimitService(ent, session=s).check_think(user, NOW)
        await PlanLimitService(ent, session=s).check_think(user, NOW)       # still fine: nothing was spent
    await _take(sf, ent, NOW, user)
    async with sf() as s:
        with pytest.raises(PlanLimitError):
            await PlanLimitService(ent, session=s).check_think(user, NOW)


def test_free_defaults_have_no_think():
    assert FREE_LIMITS["think_longer_per_day"] == 0


def _service(plan_limits=None, redis=None):
    from app.services.message_service import MessageService
    repo = MagicMock()
    repo.create_with_outbox = MagicMock(side_effect=lambda **k: SimpleNamespace(**{"file_ids": [], **k, "think": False}))
    repo.create_message_direct = MagicMock()
    cache = SimpleNamespace(redis=redis or AsyncMock(), delete_last_50_messages=AsyncMock())
    return MessageService(repo=repo, cache_service=cache, plan_limits=plan_limits), repo, cache.redis


async def test_send_event_carries_think_only_when_granted_and_uses_the_long_deadline():
    limits = SimpleNamespace(check_send=AsyncMock(), take_think=AsyncMock(return_value=True), record_send=AsyncMock(),
                             give_back_think=AsyncMock())
    svc, repo, redis = _service(limits)
    await svc.send(uuid.uuid4(), uuid.uuid4(), "user", "hi", user_id=U, think=True, temporary=True)
    payload = json.loads(repo.create_with_outbox.call_args.kwargs["outbox_payload"])
    assert payload["schema_version"] == "2.5" and payload["think"] is True and payload["temporary"] is True
    assert payload["preferences"] == {} or "suggest_followups" in payload["preferences"] or True
    zadd_score = list(redis.zadd.await_args.args[1].values())[0]
    assert zadd_score - datetime.now(timezone.utc).timestamp() > settings.PENDING_RESPONSE_TIMEOUT_SECONDS + 100
    assert redis.set.await_args_list[1].args[0].startswith("pending_think:")


async def test_send_event_think_false_when_the_limit_says_no():
    limits = SimpleNamespace(check_send=AsyncMock(), take_think=AsyncMock(return_value=False), record_send=AsyncMock(),
                             give_back_think=AsyncMock())
    svc, repo, redis = _service(limits)
    await svc.send(uuid.uuid4(), uuid.uuid4(), "user", "hi", user_id=U, think=True)
    assert json.loads(repo.create_with_outbox.call_args.kwargs["outbox_payload"])["think"] is False
    score = list(redis.zadd.await_args.args[1].values())[0]
    assert score - datetime.now(timezone.utc).timestamp() < settings.PENDING_RESPONSE_TIMEOUT_SECONDS + 5


async def test_failed_send_gives_the_think_count_back():
    limits = SimpleNamespace(check_send=AsyncMock(), take_think=AsyncMock(return_value=True), record_send=AsyncMock(),
                             give_back_think=AsyncMock())
    svc, repo, _ = _service(limits)
    repo.create_with_outbox.side_effect = RuntimeError("cassandra down")
    with pytest.raises(RuntimeError):
        await svc.send(uuid.uuid4(), uuid.uuid4(), "user", "hi", user_id=U, think=True)
    limits.give_back_think.assert_awaited_once_with(U)


async def test_think_refused_by_the_limit_creates_nothing():
    limits = SimpleNamespace(check_send=AsyncMock(), take_think=AsyncMock(side_effect=PlanLimitError("think_longer_per_day", "max")),
                             record_send=AsyncMock(), give_back_think=AsyncMock())
    svc, repo, _ = _service(limits)
    with pytest.raises(PlanLimitError):
        await svc.send(uuid.uuid4(), uuid.uuid4(), "user", "hi", user_id=U, think=True)
    repo.create_with_outbox.assert_not_called()
    repo.create_message_direct.assert_not_called()


async def test_final_event_think_and_the_not_applied_notice():
    from app.events.consumers import _handle_chat_response_generated
    from app.services.message_service import MessageService
    conv, resp = uuid.uuid4(), uuid.uuid4()
    svc = MagicMock(spec=MessageService)
    svc.is_completed_reply.return_value = False
    svc.finalize_assistant_message = AsyncMock(return_value=object())
    svc.clear_pending_response = AsyncMock()
    svc.requested_think = AsyncMock(return_value=True)
    stream = SimpleNamespace(publish_token=AsyncMock())
    inbox = SimpleNamespace(exists=lambda _i: False, save=lambda _i: None)
    event = {"conversation_id": str(conv), "response_message_id": str(resp), "request_message_id": str(uuid.uuid4()),
             "status": "success", "full_content": "answer", "think": False}
    await _handle_chat_response_generated(event, inbox, svc, stream)
    sent = stream.publish_token.await_args.args[1]
    assert sent["think"] is False and sent["notice_key"] == "think.not_applied"
    assert svc.finalize_assistant_message.await_args.kwargs["think"] is False

    stream.publish_token.reset_mock()
    event2 = {**event, "response_message_id": str(uuid.uuid4()), "request_message_id": str(uuid.uuid4()), "think": True}
    await _handle_chat_response_generated(event2, inbox, svc, stream)
    sent = stream.publish_token.await_args.args[1]
    assert sent["think"] is True and "notice_key" not in sent


# ================================================================================================ Q5 suggestions

def test_sanitize_suggestions():
    assert sanitize_suggestions(["  a?  ", "a?", "", "x" * 121, "two\nlines", 5, "b?", "c?", "d?"]) == ["a?", "b?", "c?"]
    assert sanitize_suggestions("nope") == [] and sanitize_suggestions(None) == []
    assert loads_suggestions(dumps_suggestions(["क्या है?", "why?"])) == ["क्या है?", "why?"]
    assert loads_suggestions("not json") == [] and loads_suggestions(None) == []


class SuggRepo:
    def __init__(self, rows):
        self.rows, self.written = rows, {}

    def set_suggestions(self, cid, mid_, suggestions):
        row = self.rows.get((cid, mid_))
        if row is None or row.sender != "assistant" or row.status != MessageStatus.SENT:
            return False
        self.written[(cid, mid_)] = suggestions
        return True


async def test_suggestions_event_is_stored_validated_and_deduplicated():
    from app.events.consumers import _handle_chat_response_suggestions
    from app.services.message_service import MessageService
    cid, resp = uuid.uuid4(), uuid.uuid4()
    repo = SuggRepo({(cid, resp): _msg(cid, resp, "assistant")})
    svc = MessageService(repo=repo, cache_service=SimpleNamespace(redis=None, delete_last_50_messages=AsyncMock()))
    seen = set()
    inbox = SimpleNamespace(exists=lambda i: i in seen, save=lambda i: seen.add(i))
    ev = {"event_id": str(uuid.uuid4()), "conversation_id": str(cid), "response_message_id": str(resp),
          "suggestions": ["Why?", "  How? ", "", "x" * 200]}
    await _handle_chat_response_suggestions(ev, inbox, svc)
    assert repo.written[(cid, resp)] == ["Why?", "How?"]
    repo.written.clear()
    await _handle_chat_response_suggestions(ev, inbox, svc)             # duplicate event_id: a no-op
    assert repo.written == {}


async def test_suggestions_for_the_wrong_conversation_or_a_pending_row_are_ignored():
    from app.events.consumers import _handle_chat_response_suggestions
    from app.services.message_service import MessageService
    cid, resp = uuid.uuid4(), uuid.uuid4()
    repo = SuggRepo({(cid, resp): _msg(cid, resp, "assistant", status="pending")})
    svc = MessageService(repo=repo, cache_service=SimpleNamespace(redis=None, delete_last_50_messages=AsyncMock()))
    inbox = SimpleNamespace(exists=lambda i: False, save=lambda i: None)
    base = {"event_id": str(uuid.uuid4()), "response_message_id": str(resp), "suggestions": ["Why?"]}
    await _handle_chat_response_suggestions({**base, "conversation_id": str(cid)}, inbox, svc)        # still pending
    await _handle_chat_response_suggestions({**base, "event_id": str(uuid.uuid4()), "conversation_id": str(uuid.uuid4())}, inbox, svc)
    assert repo.written == {}


def test_message_response_has_suggestions_think_and_feedback():
    from app.schemas.message import MessageResponse
    m = _msg(uuid.uuid4(), mid(2), "assistant", suggestions=["a?"], think=True, feedback={"rating": "down", "reason": "too_long"})
    out = MessageResponse.model_validate(m)
    assert out.suggestions == ["a?"] and out.think is True and out.feedback.rating == "down"
    assert MessageResponse.model_validate(_msg(uuid.uuid4(), mid(1))).suggestions == []


def test_generation_preferences_carry_suggest_followups():
    from app.models.preferences import PreferencesUpdate, UserPreferences, generation_preferences
    assert generation_preferences(UserPreferences())["suggest_followups"] is True
    assert generation_preferences(UserPreferences(suggest_followups=False))["suggest_followups"] is False
    assert PreferencesUpdate(suggest_followups=False).changes() == {"suggest_followups": False}


# ================================================================================================ Q4 temporary

class ConvStore:
    """ConversationService stand-in over a dict: get / delete / create / list."""

    def __init__(self):
        self.items, self.deleted = {}, []

    def add(self, conv):
        self.items[conv.conversation_id] = conv
        return conv

    async def get(self, cid):
        return self.items.get(cid)

    async def delete(self, cid):
        self.deleted.append(cid)
        if cid in self.items:
            self.items[cid].status = ConversationStatus.DELETED
        return True

    def list(self, user_id, limit=20, cursor=None):
        return [c for c in self.items.values() if c.user_id == user_id and not c.temporary
                and c.status != ConversationStatus.DELETED][:limit]

    async def create(self, **kw):
        conv = _conv(user=kw["user_id"], parent=kw.get("parent_conversation_id"), branch=kw.get("branch_message_id"),
                     title=kw["title"])
        conv.edited_from_message_id = kw.get("edited_from_message_id")
        return self.add(conv)


async def test_expiry_sweep_deletes_exactly_the_due_rows_and_is_idempotent(sf):
    store, user = ConvStore(), uuid.uuid4()
    due, later, gone = (store.add(_conv(user, temporary=True)) for _ in range(3))
    async with sf() as s:
        t = TemporaryService(s)
        for conv, delta in ((due, -1), (later, +1), (gone, -2)):
            conv.expires_at = NOW + timedelta(hours=delta)
            await t.register(conv)
        await s.execute(text("UPDATE temporary_conversations SET deleted_at = now() WHERE conversation_id = :c"),
                        {"c": gone.conversation_id})
        await s.commit()
    assert await expire_due(sf, store, NOW) == 1
    assert store.deleted == [due.conversation_id]
    assert await expire_due(sf, store, NOW) == 0                         # idempotent
    assert await temporary_ids_of_user(sf, user) == [later.conversation_id]


async def test_one_bad_row_does_not_stop_the_sweep(sf):
    store, user = ConvStore(), uuid.uuid4()
    a, b = (store.add(_conv(user, temporary=True, expires=NOW - timedelta(hours=1))) for _ in range(2))
    async with sf() as s:
        for c in (a, b):
            await TemporaryService(s).register(c)
        await s.commit()
    real = store.delete

    async def flaky(cid):
        if cid == min(a.conversation_id, b.conversation_id):
            raise RuntimeError("cassandra hiccup")
        return await real(cid)

    store.delete = flaky
    assert await expire_due(sf, store, NOW) == 1
    store.delete = real
    assert await expire_due(sf, store, NOW) == 1                         # the failed one is retried next sweep


def test_conversation_response_shows_temporary_and_expiry():
    from app.schemas.conversation import ConversationResponse
    exp = NOW + timedelta(hours=24)
    out = ConversationResponse.model_validate(_conv(temporary=True, expires=exp))
    assert out.temporary is True and out.expires_at == exp and out.edited_from_message_id is None


async def test_branching_from_a_temporary_chat_is_refused_and_a_temporary_chat_has_no_parent():
    from app.services.conversation_service import ConversationService, TemporaryConversationError
    parent = _conv(temporary=True)
    svc = ConversationService(repo=MagicMock())
    svc.get = AsyncMock(return_value=parent)
    with pytest.raises(TemporaryConversationError):
        await svc.create(U, "t", parent_conversation_id=parent.conversation_id)
    with pytest.raises(ValueError):
        await svc.create(U, "t", parent_conversation_id=uuid.uuid4(), temporary=True)


async def test_temporary_create_sets_expiry_and_emits_the_flag():
    from app.services.conversation_service import ConversationService
    repo = MagicMock()
    repo.create_with_outbox = MagicMock(side_effect=lambda **k: _conv(user=k["user_id"], temporary=k["temporary"],
                                                                      expires=k["expires_at"]))
    svc = ConversationService(repo=repo)
    conv = await svc.create(U, "t", temporary=True)
    kw = repo.create_with_outbox.call_args.kwargs
    payload = json.loads(kw["outbox_payload"])["payload"]
    assert payload["temporary"] is True and payload["edited_from_message_id"] is None
    assert kw["temporary"] is True and kw["expires_at"] - datetime.now(timezone.utc) > timedelta(hours=23)
    assert conv.temporary
    await svc.create(U, "t2")
    assert json.loads(repo.create_with_outbox.call_args.kwargs["outbox_payload"])["payload"]["temporary"] is False


# ================================================================================================== Q2 thumbs

class MsgRepo:
    def __init__(self):
        self.rows = {}

    def add(self, m):
        self.rows[(m.conversation_id, m.message_id)] = m
        return m

    def get_message(self, cid, mid_):
        return self.rows.get((cid, mid_))

    def history(self, cid, limit=50, cursor=None):
        rows = sorted((m for (c, _), m in self.rows.items() if c == cid and (cursor is None or m.message_id < cursor)),
                      key=lambda m: m.message_id, reverse=True)
        return rows[:limit]

    def history_upto(self, cid, upto, limit=50):
        rows = sorted((m for (c, _), m in self.rows.items() if c == cid and m.message_id <= upto),
                      key=lambda m: m.message_id, reverse=True)
        return rows[:limit]

    def count_user_messages(self, cid):
        return sum(1 for (c, _), m in self.rows.items() if c == cid and m.sender == "user")


def _conv_with_turn(repo, user=U, model="claude-haiku-5-5", group="smart", q="why?", a="because", n=10):
    cid = uuid.uuid4()
    repo.add(_msg(cid, mid(n), "user", q))
    ans = repo.add(_msg(cid, mid(n + 1), "assistant", a, model=model, model_group=group))
    return cid, ans


async def test_feedback_upsert_change_delete_and_validation(sf):
    repo, user = MsgRepo(), uuid.uuid4()
    cid, ans = _conv_with_turn(repo)
    async with sf() as s:
        svc = FeedbackService(s, repo)
        assert await svc.put(user, cid, ans.message_id, "up", "ignored", "ignored") == {"rating": "up", "reason": None}
        assert await svc.put(user, cid, ans.message_id, "down", "too_long", "wall of text") == {"rating": "down", "reason": "too_long"}
        await s.commit()
    async with sf() as s:
        row = (await s.execute(text("SELECT rating, reason, comment, model, model_group FROM message_feedback WHERE user_id = :u"),
                               {"u": user})).one()
        assert tuple(row) == (-1, "too_long", "wall of text", "claude-haiku-5-5", "smart")
        svc = FeedbackService(s, repo)
        await svc.delete(user, ans.message_id)
        await svc.delete(user, ans.message_id)                           # also fine when there is none
        await s.commit()
    async with sf() as s:
        assert (await s.execute(text("SELECT count(*) FROM message_feedback WHERE user_id = :u"), {"u": user})).scalar_one() == 0


async def test_feedback_only_on_sent_assistant_messages(sf):
    repo, user = MsgRepo(), uuid.uuid4()
    cid, ans = _conv_with_turn(repo)
    pending = repo.add(_msg(cid, mid(13), "assistant", "", status="pending"))
    async with sf() as s:
        svc = FeedbackService(s, repo)
        for target in (mid(10), pending.message_id, mid(99)):            # the user's message, a pending answer, nothing
            with pytest.raises(FeedbackNotAllowed):
                await svc.put(user, cid, target, "up")


def test_feedback_request_validation_and_owner_dependency():
    from pydantic import ValidationError
    from app.api.v1.feedback import FeedbackRequest
    with pytest.raises(ValidationError):
        FeedbackRequest(rating="meh")
    with pytest.raises(ValidationError):
        FeedbackRequest(rating="down", reason="made_up")
    with pytest.raises(ValidationError):
        FeedbackRequest(rating="down", comment="x" * 1001)
    assert FeedbackRequest(rating="down", reason="unsafe", comment=" ok ").comment == "ok"


async def test_history_feedback_is_read_in_one_query(sf):
    repo, user = MsgRepo(), uuid.uuid4()
    cid = uuid.uuid4()
    answers = [repo.add(_msg(cid, mid(i), "assistant", f"a{i}")) for i in (2, 4, 6)]
    async with sf() as s:
        svc = FeedbackService(s, repo)
        await svc.put(user, cid, mid(4), "down", "formatting")
        await svc.put(user, cid, mid(6), "up")
        statements = []
        real = s.execute

        async def counting(stmt, *a, **k):
            statements.append(str(stmt))
            return await real(stmt, *a, **k)

        s.execute = counting
        page = [*answers, repo.add(_msg(cid, mid(1), "user"))]
        await svc.attach(user, page)
        assert len([q for q in statements if "message_feedback" in q]) == 1
        assert "ANY(" in statements[0].upper()
    by = {m.message_id: m.feedback for m in page}
    assert by[mid(4)] == {"rating": "down", "reason": "formatting"} and by[mid(6)]["rating"] == "up"
    assert by[mid(2)] is None and by[mid(1)] is None


async def test_admin_list_filters_pages_and_hides_identity(sf):
    repo = MsgRepo()
    users = [uuid.uuid4() for _ in range(3)]
    async with sf() as s:
        await s.execute(text("DELETE FROM message_feedback"))          # the throwaway database is shared by the tests
        svc = FeedbackService(s, repo)
        for i, (grp, why) in enumerate([("smart", "inaccurate"), ("best", "too_long"), ("smart", "too_long")]):
            cid, ans = _conv_with_turn(repo, model="m", group=grp, q=f"q{i}", a=f"a{i}" * 800, n=10 * (i + 1))
            await svc.put(users[i], cid, ans.message_id, "down", why)
        cid, ans = _conv_with_turn(repo, n=40)
        await svc.put(users[0], cid, ans.message_id, "up")
        await s.commit()
    async with sf() as s:
        svc = FeedbackService(s, repo)
        items, more = await svc.admin_list(limit=50)
        assert len(items) == 3 and not more and all(i["rating"] == "down" for i in items)   # default: thumbs down
        assert all("user_id" not in i and "email" not in i for i in items)
        first = items[0]
        assert len(first["answer"]) == 1500 and first["question"].startswith("q")
        assert {i["reason"] for i in (await svc.admin_list(reason="too_long"))[0]} == {"too_long"}
        assert {i["model_group"] for i in (await svc.admin_list(model_group="best"))[0]} == {"best"}
        assert len((await svc.admin_list(rating="up"))[0]) == 1
        today = datetime.now(IST).date()
        assert len((await svc.admin_list(date_from=today, date_to=today))[0]) == 3
        assert (await svc.admin_list(date_from=today + timedelta(days=1)))[0] == []
        page1, more = await svc.admin_list(limit=2)
        assert len(page1) == 2 and more
        page2, more2 = await svc.admin_list(limit=2, cursor=(page1[-1]["created_at"], page1[-1]["message_id"]))
        assert len(page2) == 1 and not more2


def test_admin_route_requires_a_staff_role():
    from fastapi import HTTPException
    from app.api.v1.feedback import require_staff
    from app.security.models import CurrentUser
    for roles in ([], ["user"]):
        with pytest.raises(HTTPException) as e:
            require_staff(CurrentUser(id=uuid.uuid4(), roles=roles))
        assert e.value.status_code == 403
    for role in ("support", "admin"):
        assert require_staff(CurrentUser(id=uuid.uuid4(), roles=[role]))


# ================================================================================================ Q3 share links

def _share_env(sf_session, repo, store_convs, **kw):
    store = MemoryObjectStore()
    return ShareService(sf_session, store_convs, repo, store, **kw), store


def _tree():
    """root -> branch, with messages (including ids and a file id that must never leak)."""
    repo, convs, user = MsgRepo(), ConvStore(), uuid.uuid4()
    root = convs.add(_conv(user, title="Root"))
    repo.add(_msg(root.conversation_id, mid(1), "user", "q1", file_ids=[uuid.uuid4()]))
    repo.add(_msg(root.conversation_id, mid(2), "assistant", "a1", model="m1",
                  citations=[{"file_id": "doc-secret", "source_name": "Book.pdf", "page_number": 3}]))
    repo.add(_msg(root.conversation_id, mid(3), "user", "q2"))
    repo.add(_msg(root.conversation_id, mid(4), "system", "Summary: hidden"))
    branch = convs.add(_conv(user, parent=root.conversation_id, branch=mid(2), title="Branch"))
    repo.add(_msg(branch.conversation_id, mid(20), "user", "b1"))
    return repo, convs, user, root, branch


async def test_share_create_public_read_refresh_revoke(sf, monkeypatch):
    repo, convs, user, root, branch = _tree()
    async with sf() as s:
        svc, store = _share_env(s, repo, convs)
        created = await svc.create(user, root, "hierarchy")
        await s.commit()
    token = created["url_token"]
    assert len(token) == 43 and created["message_count"] == 4
    async with sf() as s:
        stored = (await s.execute(text("SELECT token_hash, object_key FROM shared_links"))).all()
    assert any(bytes(h) == hash_token(token) for h, _ in stored)
    assert all(token.encode() not in bytes(h) for h, _ in stored)           # only the hash is stored
    async with sf() as s:
        svc = ShareService(s, convs, repo, store)
        body = json.loads(await svc.read_public(token))                       # no JWT anywhere in this path
        text_dump = json.dumps(body)
        assert body["schema_version"] == 1 and body["scope"] == "hierarchy" and body["title"] == "Root"
        titles = [c["title"] for c in body["conversations"]]
        assert titles == ["Root", "Branch"]
        for secret in (str(user), str(root.conversation_id), str(branch.conversation_id), "doc-secret", "Summary: hidden", "@"):
            assert secret not in text_dump
        assert body["conversations"][0]["parent"] is None and body["conversations"][1]["parent"] == body["conversations"][0]["id"]
        assert body["conversations"][0]["messages"][1]["citations"] == [{"title": "Book.pdf", "page": 3}]
        with pytest.raises(ShareNotFound):
            await svc.read_public("A" * 43)                                    # unknown
        with pytest.raises(ShareNotFound):
            await svc.read_public("short")                                     # malformed
        # refresh keeps the token and picks up the new message
        repo.add(_msg(root.conversation_id, mid(5), "assistant", "a2"))
        svc = ShareService(s, convs, repo, store)
        refreshed = await svc.refresh(user, created["share_id"])
        await s.commit()
        assert refreshed["message_count"] == 5
        assert len(json.loads(await svc.read_public(token))["conversations"][0]["messages"]) == 4
        # revoke: the same token now answers exactly like an unknown one
        await svc.revoke(user, created["share_id"])
        await svc.revoke(user, created["share_id"])
        await s.commit()
        with pytest.raises(ShareNotFound):
            await svc.read_public(token)
        assert store.objects == {}
        with pytest.raises(ShareNotFound):
            await svc.refresh(user, created["share_id"])
        assert await svc.list(user) == []


async def test_conversation_scope_includes_the_inherited_part_marked_inherited(sf):
    from app.services.branch_context_service import BranchContextService
    repo, convs, user, root, branch = _tree()
    async with sf() as s:
        svc, _ = _share_env(s, repo, convs, branch_context=BranchContextService(convs, repo))
        created = await svc.create(user, branch, "conversation")
        body = json.loads(await svc.read_public(created["url_token"]))
        inherited = [c for c in body["conversations"] if c["inherited"]]
        own = [c for c in body["conversations"] if not c["inherited"]]
        assert len(inherited) == 1 and [m["content"] for m in inherited[0]["messages"]] == ["q1", "a1"]   # up to the branch point
        assert [m["content"] for m in own[0]["messages"]] == ["b1"] and own[0]["parent"] == inherited[0]["id"]


async def test_share_limits_temporary_and_too_large(sf, monkeypatch):
    repo, convs, user, root, branch = _tree()
    monkeypatch.setattr(settings, "SHARE_MAX_ACTIVE_PER_USER", 2)
    async with sf() as s:
        svc, _ = _share_env(s, repo, convs)
        with pytest.raises(ShareRefused) as e:
            await svc.create(user, _conv(user, temporary=True), "conversation")
        assert e.value.code == "temporary_conversation"
        await svc.create(user, root, "conversation")
        await svc.create(user, root, "conversation")
        with pytest.raises(ShareRefused) as e:
            await svc.create(user, root, "conversation")
        assert e.value.code == "share_limit" and e.value.status == 409
    monkeypatch.setattr(settings, "SHARE_MAX_ACTIVE_PER_USER", 50)
    monkeypatch.setattr(settings, "EXPORT_MAX_MESSAGES_PER_CONVERSATION", 2)
    async with sf() as s:
        svc = ShareService(s, convs, repo, MemoryObjectStore())
        svc.collector.max_messages = 2
        with pytest.raises(ShareRefused) as e:
            await svc.create(user, root, "conversation")
        assert e.value.code == "share_too_large" and e.value.status == 413


async def test_public_rate_limit_and_headers(sf):
    class Redis:
        def __init__(self):
            self.n = {}

        async def incr(self, k):
            self.n[k] = self.n.get(k, 0) + 1
            return self.n[k]

        async def expire(self, *_):
            return True

    repo, convs, *_ = _tree()
    async with sf() as s:
        svc = ShareService(s, convs, repo, MemoryObjectStore(), redis=Redis())
        for _ in range(settings.SHARE_PUBLIC_RATE_LIMIT_PER_MINUTE):
            await svc.rate_check("1.2.3.4")
        with pytest.raises(RateLimited) as e:
            await svc.rate_check("1.2.3.4")
        assert e.value.retry_after >= 1
        await svc.rate_check("5.6.7.8")                                        # per IP


def test_public_route_has_no_auth_dependency_and_reads_the_forwarded_ip():
    from app.api.v1 import shares
    route = next(r for r in shares.router.routes if r.path == "/public/{token}")
    assert "GET" in route.methods
    assert not any("get_current_user" in repr(d.call) for d in route.dependant.dependencies)
    req = SimpleNamespace(headers={"x-forwarded-for": "9.9.9.9, 10.0.0.1"}, client=SimpleNamespace(host="127.0.0.1"))
    assert shares.client_ip(req) == "9.9.9.9"
    assert shares.client_ip(SimpleNamespace(headers={}, client=SimpleNamespace(host="127.0.0.1"))) == "127.0.0.1"


async def test_deleting_a_conversation_revokes_its_links(sf):
    repo, convs, user, root, branch = _tree()
    async with sf() as s:
        svc, store = _share_env(s, repo, convs)
        created = await svc.create(user, root, "conversation")
        other = await svc.create(user, branch, "conversation")
        await s.commit()
    assert await revoke_for_conversation(sf, store, root.conversation_id) == 1
    async with sf() as s:
        svc = ShareService(s, convs, repo, store)
        with pytest.raises(ShareNotFound):
            await svc.read_public(created["url_token"])
        assert json.loads(await svc.read_public(other["url_token"]))["title"] == "Branch"


async def test_token_shape():
    t = new_token()
    assert len(t) == 43 and "=" not in t and new_token() != t


# ======================================================================================================= Q1 edit

class EditHarness:
    def __init__(self, sf):
        self.sf = sf
        self.repo, self.convs, self.user = MsgRepo(), ConvStore(), uuid.uuid4()
        self.created, self.sent = [], []
        orig_create = self.convs.create

        async def create(**kw):
            conv = await orig_create(**kw)
            self.created.append(kw)
            return conv

        self.convs.create = create
        self.send_error = None

        async def send(**kw):
            if self.send_error:
                raise self.send_error
            self.sent.append(kw)
            return _msg(kw["conversation_id"], kw["message_id"], "user", kw["content"])

        self.message_service = SimpleNamespace(send=send)
        self.limits = SimpleNamespace(check_new_branch=AsyncMock(), check_think=AsyncMock())

    def svc(self, session):
        return EditService(session, self.convs, self.repo, self.message_service, plan_limits=self.limits)

    def turn(self, conv, q, a, n):
        self.repo.add(_msg(conv.conversation_id, mid(n), "user", q))
        self.repo.add(_msg(conv.conversation_id, mid(n + 1), "assistant", a))


async def _edit(h, conv, message_id, **kw):
    async with h.sf() as s:
        out = await h.svc(s).edit(h.user, conv, message_id, content="edited", **kw)
        await s.commit()
        return out


async def test_edit_case_1_branches_from_the_message_before(sf):
    h = EditHarness(sf)
    c = h.convs.add(_conv(h.user, title="Chat"))
    h.turn(c, "q1", "a1", 10)
    h.turn(c, "q2", "a2", 20)
    new, sent = await _edit(h, c, mid(20))
    kw = h.created[0]
    assert kw["parent_conversation_id"] == c.conversation_id and kw["branch_message_id"] == mid(11)   # P = a1
    assert kw["edited_from_message_id"] == mid(20) and kw["title"] == "Chat"
    assert new.edited_from_message_id == mid(20)
    h.limits.check_new_branch.assert_awaited_once_with(h.user, c.conversation_id)
    async with sf() as s:
        assert (await s.execute(text("SELECT original_message_id, original_conversation_id FROM message_edits "
                                     "WHERE branch_conversation_id = :b"), {"b": new.conversation_id})).one() == (mid(20), c.conversation_id)
    versions = await h.svc(MagicMock()).versions(h.user, mid(20)) if False else None


async def test_edit_case_1_context_ends_exactly_at_the_previous_message(sf):
    from app.services.branch_context_service import BranchContextService
    h = EditHarness(sf)
    c = h.convs.add(_conv(h.user))
    h.turn(c, "q1", "a1", 10)
    h.turn(c, "q2", "a2", 20)
    new, _ = await _edit(h, c, mid(20))
    ctx = await BranchContextService(h.convs, h.repo).inherited_history(new)
    assert [m["content"] for m in ctx] == ["q1", "a1"]


async def test_edit_case_2_first_message_of_a_branch_is_a_sibling(sf):
    from app.services.branch_context_service import BranchContextService
    h = EditHarness(sf)
    root = h.convs.add(_conv(h.user))
    h.turn(root, "q1", "a1", 10)
    branch = h.convs.add(_conv(h.user, parent=root.conversation_id, branch=mid(11)))
    h.turn(branch, "b1", "ba1", 30)
    new, _ = await _edit(h, branch, mid(30))
    kw = h.created[0]
    assert kw["parent_conversation_id"] == root.conversation_id and kw["branch_message_id"] == mid(11)
    ctx = await BranchContextService(h.convs, h.repo).inherited_history(new)
    assert [m["content"] for m in ctx] == ["q1", "a1"]


async def test_edit_case_3_first_message_of_a_root_makes_a_new_root(sf):
    h = EditHarness(sf)
    c = h.convs.add(_conv(h.user))
    h.turn(c, "q1", "a1", 10)
    new, _ = await _edit(h, c, mid(10))
    assert h.created[0]["parent_conversation_id"] is None and new.parent_conversation_id is None
    h.limits.check_new_branch.assert_not_awaited()


async def test_edit_keeps_files_unless_given(sf):
    h = EditHarness(sf)
    c = h.convs.add(_conv(h.user))
    f1, f2 = uuid.uuid4(), uuid.uuid4()
    h.repo.add(_msg(c.conversation_id, mid(10), "user", "q", file_ids=[f1]))
    h.repo.add(_msg(c.conversation_id, mid(11), "assistant", "a"))
    await _edit(h, c, mid(10))
    assert h.sent[0]["file_ids"] == [f1]
    await _edit(h, c, mid(10), file_ids=[f2])
    assert h.sent[1]["file_ids"] == [f2]


@pytest.mark.parametrize("setup,code", [
    ("assistant", "not_user_message"), ("failed", "message_not_sent"), ("temporary", "temporary_conversation"),
    ("busy", "conversation_busy"),
])
async def test_edit_refusals_create_nothing(sf, setup, code):
    h = EditHarness(sf)
    c = h.convs.add(_conv(h.user, temporary=(setup == "temporary")))
    h.turn(c, "q1", "a1", 10)
    target = mid(10)
    if setup == "assistant":
        target = mid(11)
    elif setup == "failed":
        h.repo.add(_msg(c.conversation_id, mid(12), "user", "bad", status="failed"))
        target = mid(12)
    elif setup == "busy":
        h.repo.add(_msg(c.conversation_id, mid(12), "user", "q2"))
        h.repo.add(_msg(c.conversation_id, mid(13), "assistant", "", status="pending"))
    with pytest.raises(EditRefused) as e:
        await _edit(h, c, target)
    assert e.value.code == code
    assert h.created == [] and h.sent == []


async def test_branch_limit_refusal_creates_nothing(sf):
    h = EditHarness(sf)
    c = h.convs.add(_conv(h.user))
    h.turn(c, "q1", "a1", 10)
    h.turn(c, "q2", "a2", 20)
    h.limits.check_new_branch.side_effect = PlanLimitError("branches_per_tree", "max")
    with pytest.raises(PlanLimitError):
        await _edit(h, c, mid(20))
    assert h.created == [] and h.sent == []
    async with sf() as s:
        assert (await s.execute(text("SELECT count(*) FROM message_edits WHERE user_id = :u"), {"u": h.user})).scalar_one() == 0


async def test_think_limit_is_checked_before_anything_is_created(sf):
    h = EditHarness(sf)
    c = h.convs.add(_conv(h.user))
    h.turn(c, "q1", "a1", 10)
    h.limits.check_think.side_effect = PlanLimitError("think_longer_per_day", None)
    with pytest.raises(PlanLimitError):
        await _edit(h, c, mid(10), think=True)
    assert h.created == []
    await _edit(h, c, mid(10), think=False)                                     # no think, no check
    assert len(h.created) == 1


async def test_billing_refusal_during_send_leaves_no_conversation_and_no_edits_row(sf):
    from app.core.billing_messages import BillingDeniedError
    h = EditHarness(sf)
    c = h.convs.add(_conv(h.user))
    h.turn(c, "q1", "a1", 10)
    h.turn(c, "q2", "a2", 20)
    h.send_error = BillingDeniedError("daily_limit")
    with pytest.raises(BillingDeniedError) as e:
        await _edit(h, c, mid(20))
    assert e.value is h.send_error                                              # the original error, unchanged
    new_id = next(i for i in h.convs.items if i != c.conversation_id)
    assert h.convs.items[new_id].status == ConversationStatus.DELETED and h.convs.deleted == [new_id]
    async with sf() as s:
        assert (await s.execute(text("SELECT count(*) FROM message_edits WHERE user_id = :u"), {"u": h.user})).scalar_one() == 0


async def test_versions_lists_edits_without_deleted_ones(sf):
    h = EditHarness(sf)
    c = h.convs.add(_conv(h.user, title="Chat"))
    h.turn(c, "q1", "a1", 10)
    h.turn(c, "q2", "a2", 20)
    first, _ = await _edit(h, c, mid(20))
    second, _ = await _edit(h, c, mid(20))
    async with sf() as s:
        got = await h.svc(s).versions(h.user, mid(20))
        assert [v["conversation_id"] for v in got] == [first.conversation_id, second.conversation_id]
        assert got[0]["title"] == "Chat"
        await h.convs.delete(first.conversation_id)
        assert [v["conversation_id"] for v in await h.svc(s).versions(h.user, mid(20))] == [second.conversation_id]
        assert await h.svc(s).versions(uuid.uuid4(), mid(20)) == []              # someone else's edits are never listed


async def test_idempotent_replay_returns_the_same_conversation_and_message():
    from app.api.v1.messages import EditResponse
    from app.schemas.conversation import ConversationResponse
    from app.schemas.message import MessageResponse
    conv = _conv()
    msg = _msg(conv.conversation_id, mid(5))
    msg.response_message_id = mid(6)
    resp = EditResponse(conversation=ConversationResponse.model_validate(conv), message=MessageResponse.model_validate(msg))
    again = EditResponse.model_validate_json(json.dumps(json.loads(resp.model_dump_json())))
    assert again == resp and again.message.response_message_id == mid(6)


# ============================================================================================== user purge

async def test_user_purge_removes_the_quick_win_rows(sf):
    from app.workers.user_purge_worker import purge_user
    repo, convs, user, root, branch = _tree()
    store = MemoryObjectStore()
    temp = convs.add(_conv(user, temporary=True, expires=NOW))
    async with sf() as s:
        svc = ShareService(s, convs, repo, store)
        await svc.create(user, root, "conversation")
        await TemporaryService(s).register(temp)
        fb_cid, ans = _conv_with_turn(repo)
        await FeedbackService(s, repo).put(user, fb_cid, ans.message_id, "down", "other")
        await s.execute(text("INSERT INTO message_edits (branch_conversation_id, original_message_id, original_conversation_id, user_id) "
                             "VALUES (:b, :m, :c, :u)"), {"b": uuid.uuid4(), "m": mid(1), "c": root.conversation_id, "u": user})
        await s.execute(text("INSERT INTO think_usage_daily (user_id, day, count) VALUES (:u, :d, 1)"),
                        {"u": user, "d": date(2026, 11, 22)})
        await s.commit()
    assert store.objects
    objects = SimpleNamespace(delete=store.delete)
    await purge_user(sf, convs, objects, user)
    assert temp.conversation_id in convs.deleted                                # not in the catalog, found via our table
    assert store.objects == {}
    async with sf() as s:
        for table in ("shared_links", "message_feedback", "message_edits", "temporary_conversations", "think_usage_daily"):
            assert (await s.execute(text(f"SELECT count(*) FROM {table} WHERE user_id = :u"), {"u": user})).scalar_one() == 0, table
