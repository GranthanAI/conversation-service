"""P6d: reserve, grant and compensation on send and regenerate.

  off snapshot; shadow allow/deny; enforce (each status and body, nothing persisted); compensation releases exactly once;
  degraded mode (10 per IST day, Fast/Auto only); regenerate reserves under a new id; an idempotent replay does not
  reserve twice and a deny is not cached; plan limits run before the reserve.
"""

import json
import pathlib
import uuid
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

import pytest
import structlog

from app.api.v1.messages import create_message, regenerate_message
from app.clients.billing_client import BillingClient, BillingUnavailable, ReserveResult
from app.core.billing_messages import BillingDeniedError, IST, PlanLimitError
from app.core.config import settings
from app.models.message import Message, MessageStatus
from app.schemas.message import CreateMessageRequest, RegenerateRequest
from app.services.billing_gate import BillingGate
from app.services.idempotency_service import IdempotencyService
from app.services.message_service import MessageService, PendingResponseRegistrationError
from app.services.preferences_service import ResolvedMessagePreferences
from app.telemetry import metrics
from tests.support.fake_billing import FakeBilling, start_fake_billing

pytestmark = pytest.mark.anyio

SNAPSHOT = json.loads((pathlib.Path(__file__).parent.parent / "fixtures" / "outbox_payload_off_2_3.json").read_text())
CONV, MSG, USER = uuid.UUID(int=1), uuid.UUID("0194a1b2-0000-7000-8000-000000000001"), uuid.UUID(int=3)


class FakeRedis:
    def __init__(self):
        self.store, self.ttl, self.sets = {}, {}, {}

    async def set(self, key, value, ex=None, nx=False):
        if nx and key in self.store:
            return None
        self.store[key] = value
        self.ttl[key] = ex
        return True

    async def get(self, key):
        return self.store.get(key)

    async def delete(self, *keys):
        return sum(1 for k in keys if self.store.pop(k, None) is not None)

    async def incr(self, key):
        self.store[key] = int(self.store.get(key, 0)) + 1
        return self.store[key]

    async def decr(self, key):
        self.store[key] = int(self.store.get(key, 0)) - 1
        return self.store[key]

    async def expire(self, key, ttl):
        self.ttl[key] = ttl
        return 1

    async def zadd(self, name, mapping):
        return len(mapping)

    async def zrem(self, name, member):
        return 1


def allow(group="fast", **kw):
    return ReserveResult(allowed=True, hold_id=kw.pop("hold_id", "hold-1"), granted_group=group,
                         max_context_tokens=kw.pop("max_context_tokens", 16000), remaining_today=kw.pop("remaining_today", 24),
                         funded_by="plan", **kw)


def deny(reason):
    return ReserveResult(allowed=False, deny_reason=reason, message_key="x")


class FakeClient:
    """Programmable stand-in for BillingClient."""

    def __init__(self, result=None):
        self.result = result or allow()
        self.reserves, self.releases = [], []
        self.fail_with = None
        self.by_model = {}

    async def check_and_reserve(self, user_id, key, group=None, model=""):
        self.reserves.append((user_id, key, group, model))
        if self.fail_with:
            raise BillingUnavailable(self.fail_with)
        return self.by_model.get(model or None, self.result)

    async def release(self, user_id, hold_id, reason):
        self.releases.append((user_id, hold_id, reason))
        return True


class Harness:
    def __init__(self, mode="enforce", client=None, prefs=None, plan_limits=None, monkeypatch=None):
        monkeypatch.setattr(settings, "BILLING_ENFORCEMENT", mode)
        self.client = client or FakeClient()
        self.redis = FakeRedis()
        self.gate = BillingGate(self.client, redis_client=self.redis, entitlements=None)
        self.payloads = []
        self.repo = MagicMock()
        self.repo.history.return_value = []
        self.repo.create_with_outbox.side_effect = self._create
        self.repo.fail = None
        cache = MagicMock()
        cache.redis = self.redis
        cache.delete_last_50_messages = AsyncMock()
        cache.get_last_50_messages = AsyncMock(return_value=None)
        cache.set_last_50_messages = AsyncMock()
        self.svc = MessageService(repo=self.repo, cache_service=cache, preferences_service=prefs,
                                  billing_gate=self.gate, plan_limits=plan_limits)

    def _create(self, **kw):
        if self.repo.fail:
            raise self.repo.fail
        self.payloads.append(json.loads(kw["outbox_payload"]))
        return Message(conversation_id=kw["conversation_id"], message_id=kw["message_id"], sender=kw["sender"],
                       content=kw["content"], created_at=datetime.now(IST), status=MessageStatus(kw["status"]))

    async def send(self, **kw):
        args = dict(conversation_id=CONV, message_id=uuid.uuid4(), sender="user", content="hello world", user_id=USER)
        args.update(kw)
        return await self.svc.send(**args)

    @property
    def persisted(self):
        return bool(self.payloads) or self.repo.create_message_direct.called


class Prefs:
    def __init__(self, default_model="auto"):
        self.default_model = default_model

    async def resolve_for_message(self, user_id, mode, tools):
        return ResolvedMessagePreferences(mode=mode, selected_tools=tools, defaults_applied={"mode": False, "tools": False},
                                          default_model=self.default_model)


# ---- off ---------------------------------------------------------------------------------------------------------

def _normalise(p):
    p = dict(p)
    p["timestamp"], p["response_message_id"] = "<ts>", "<resp>"
    p["trace_context"] = {"traceparent": "<corr>", "tracestate": ""}
    return p


async def test_off_payload_is_the_old_payload_apart_from_schema_version(monkeypatch):
    h = Harness("off", monkeypatch=monkeypatch)
    msg = await h.send(message_id=MSG, mode="smart", selected_tools=["web_search"],
                       file_ids=[uuid.UUID(int=9)], include_diagrams=True)
    payload = _normalise(h.payloads[0])
    assert payload.pop("schema_version") == "2.4"
    expected = dict(SNAPSHOT)                       # produced by the pre-P6 code (commit 988df1b), schema_version 2.3
    assert expected.pop("schema_version") == "2.3"
    assert payload == expected
    assert "billing" not in h.payloads[0] and msg.billing is None
    assert h.client.reserves == []                  # no call at all


# ---- shadow ------------------------------------------------------------------------------------------------------

async def test_shadow_allow_includes_the_block_and_logs(monkeypatch):
    h = Harness("shadow", FakeClient(allow("smart", granted_model="gpt-6-luna")), monkeypatch=monkeypatch)
    with structlog.testing.capture_logs() as logs:
        msg = await h.send()
    block = h.payloads[0]["billing"]
    assert block == {"hold_id": "hold-1", "requested_model": None, "granted_group": "smart", "granted_model": "gpt-6-luna",
                     "max_context_tokens": 16000, "funded_by": "plan", "downgraded": False, "enforcement": "shadow"}
    assert any(l["event"] == "billing_shadow_decision" and l["decision"] == "allow" for l in logs)
    assert msg.billing["enforcement"] == "shadow"


@pytest.mark.parametrize("reason", ["daily_limit", "model_not_in_plan", "account_blocked"])
async def test_shadow_deny_proceeds_with_a_null_grant(monkeypatch, reason):
    h = Harness("shadow", FakeClient(deny(reason)), monkeypatch=monkeypatch)
    with structlog.testing.capture_logs() as logs:
        await h.send()
    assert h.payloads, "shadow must always proceed"
    block = h.payloads[0]["billing"]
    assert block["hold_id"] is None and block["granted_group"] is None and block["enforcement"] == "shadow"
    assert any(l["event"] == "billing_shadow_decision" and l["decision"] == "deny" and l["reason"] == reason for l in logs)


async def test_shadow_billing_down_still_proceeds(monkeypatch):
    client = FakeClient()
    client.fail_with = "deadline_exceeded"
    h = Harness("shadow", client, monkeypatch=monkeypatch)
    await h.send()
    assert h.payloads[0]["billing"]["granted_group"] is None


# ---- enforce -----------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("reason,http,key", [
    ("daily_limit", 429, "limit.daily"), ("monthly_limit", 429, "limit.monthly"),
    ("model_not_in_plan", 403, "plan.model_not_included"), ("account_blocked", 403, "account.blocked"),
    ("no_credit", 402, "credits.empty"), ("spend_cap", 402, "credits.spend_cap"),
    ("member_spend_cap", 402, "credits.member_cap")])
async def test_enforce_deny_status_body_and_nothing_persisted(monkeypatch, reason, http, key):
    h = Harness("enforce", FakeClient(deny(reason)), monkeypatch=monkeypatch)
    with pytest.raises(BillingDeniedError) as e:
        await h.send()
    assert e.value.status_code == http
    d = e.value.detail
    assert d["code"] == "billing_denied" and d["reason"] == reason and d["message_key"] == key and d["message"]
    assert ("resets_at" in d) == (reason in ("daily_limit", "monthly_limit"))
    assert not h.persisted and h.client.releases == []
    h.repo.create_message_direct.assert_not_called()       # not even the pending assistant placeholder
    assert not any(k.startswith("pending_response") for k in h.redis.store)


async def test_enforce_daily_limit_body_matches_plan_4_6(monkeypatch):
    h = Harness("enforce", FakeClient(deny("daily_limit")), monkeypatch=monkeypatch)
    h.gate.entitlements = MagicMock()
    from app.clients.billing_client import EntitlementsData
    h.gate.entitlements.get = AsyncMock(return_value=EntitlementsData(user_id="u", plan_code="pro"))
    with pytest.raises(BillingDeniedError) as e:
        await h.send()
    assert e.value.detail["upgrade_to"] == "max"
    assert e.value.detail["message"] == "Daily limit reached. Resets at midnight, or upgrade / top up."
    assert e.value.detail["resets_at"].endswith("+05:30")


async def test_enforce_allow_block_response_and_metrics(monkeypatch):
    before = metrics.billing_reserve_total.get(mode="enforce", decision="allow", reason="")
    h = Harness("enforce", FakeClient(allow("smart", granted_model="gpt-6-luna", hold_id="h-9", max_context_tokens=64000)), monkeypatch=monkeypatch)
    msg = await h.send(model="gpt-6-luna")
    assert h.payloads[0]["schema_version"] == "2.4"
    assert h.payloads[0]["billing"] == {
        "hold_id": "h-9", "requested_model": "gpt-6-luna", "granted_group": "smart", "granted_model": "gpt-6-luna",
        "max_context_tokens": 64000, "funded_by": "plan", "downgraded": False, "enforcement": "enforce"}
    user_id, key, group, model = h.client.reserves[0]
    assert (user_id, key, model) == (str(USER), str(msg.message_id), "gpt-6-luna")   # idempotency key = request message id
    assert msg.billing == {"granted_group": "smart", "granted_model": "gpt-6-luna", "downgraded": False,
                           "remaining_today": 24, "funded_by": "plan", "enforcement": "enforce", "notice_key": None}
    assert metrics.billing_reserve_total.get(mode="enforce", decision="allow", reason="") == before + 1


async def test_downgraded_response_carries_the_notice_key(monkeypatch):
    r = allow("smart", hold_id="h")
    r.downgraded = True
    h = Harness("enforce", FakeClient(r), monkeypatch=monkeypatch)
    msg = await h.send()
    assert msg.billing["downgraded"] is True and msg.billing["notice_key"] == "limit.best_switched"
    assert h.payloads[0]["billing"]["downgraded"] is True


async def test_plan_limit_refusal_happens_before_the_reserve(monkeypatch):
    limits = MagicMock()
    limits.check_send = AsyncMock(side_effect=PlanLimitError("messages_per_conversation", "pro"))
    h = Harness("enforce", plan_limits=limits, monkeypatch=monkeypatch)
    with pytest.raises(PlanLimitError):
        await h.send()
    assert h.client.reserves == [] and not h.persisted


# ---- model selection through the send -------------------------------------------------------------------------

async def test_denied_saved_default_model_retries_as_auto(monkeypatch):
    client = FakeClient(allow("fast"))
    client.by_model["claude-sonnet-5-5"] = deny("model_not_in_plan")
    h = Harness("enforce", client, prefs=Prefs("claude-sonnet-5-5"), monkeypatch=monkeypatch)
    msg = await h.send()
    assert [r[3] for r in client.reserves] == ["claude-sonnet-5-5", ""]
    assert client.reserves[0][1] == client.reserves[1][1]                 # same idempotency key
    assert h.payloads[0]["defaults_applied"] == {"mode": False, "tools": False, "model": True}
    assert h.payloads[0]["billing"]["requested_model"] is None
    assert msg.defaults_applied["model"] is True


async def test_explicit_model_denied_is_strict(monkeypatch):
    client = FakeClient(allow("fast"))
    client.by_model["claude-sonnet-5-5"] = deny("model_not_in_plan")
    h = Harness("enforce", client, prefs=Prefs("auto"), monkeypatch=monkeypatch)
    with pytest.raises(BillingDeniedError) as e:
        await h.send(model="claude-sonnet-5-5")
    assert e.value.status_code == 403 and e.value.detail["message_key"] == "plan.model_not_included"
    assert len(client.reserves) == 1 and not h.persisted


async def test_no_model_defaults_key_when_nothing_fell_back(monkeypatch):
    h = Harness("enforce", prefs=Prefs("auto"), monkeypatch=monkeypatch)
    await h.send()
    assert "model" not in h.payloads[0]["defaults_applied"]


# ---- compensation ------------------------------------------------------------------------------------------------

async def test_persist_failure_releases_exactly_once_and_reraises(monkeypatch):
    h = Harness("enforce", FakeClient(allow(hold_id="h-1")), monkeypatch=monkeypatch)
    h.repo.fail = RuntimeError("cassandra down")
    with pytest.raises(RuntimeError, match="cassandra down"):
        await h.send()
    assert h.client.releases == [(str(USER), "h-1", "send_failed")]


async def test_pending_marker_failure_releases_once(monkeypatch):
    h = Harness("enforce", FakeClient(allow(hold_id="h-2")), monkeypatch=monkeypatch)
    h.redis.set = AsyncMock(side_effect=RuntimeError("redis down"))
    with pytest.raises(PendingResponseRegistrationError):
        await h.send()
    assert h.client.releases == [(str(USER), "h-2", "send_failed")]


async def test_limit_race_loss_after_reserve_releases(monkeypatch):
    limits = MagicMock()
    limits.check_send = AsyncMock()
    limits.record_send = AsyncMock(side_effect=PlanLimitError("documents_per_tree", "pro"))
    h = Harness("enforce", FakeClient(allow(hold_id="h-3")), plan_limits=limits, monkeypatch=monkeypatch)
    with pytest.raises(PlanLimitError):
        await h.send()
    assert h.client.releases == [(str(USER), "h-3", "send_failed")] and not h.persisted


async def test_success_does_not_release(monkeypatch):
    h = Harness("enforce", monkeypatch=monkeypatch)
    await h.send()
    assert h.client.releases == []


async def test_release_failure_never_masks_the_original_error(monkeypatch):
    h = Harness("enforce", monkeypatch=monkeypatch)
    h.client.release = AsyncMock(side_effect=RuntimeError("billing exploded"))
    h.repo.fail = ValueError("original")
    with pytest.raises(ValueError, match="original"):
        await h.send()


async def test_shadow_deny_has_no_hold_to_release(monkeypatch):
    h = Harness("shadow", FakeClient(deny("daily_limit")), monkeypatch=monkeypatch)
    h.repo.fail = RuntimeError("down")
    with pytest.raises(RuntimeError):
        await h.send()
    assert h.client.releases == []


# ---- degraded ----------------------------------------------------------------------------------------------------

def degraded_client(why="breaker_open"):
    c = FakeClient()
    c.fail_with = why
    return c


async def test_degraded_allows_ten_then_refuses_the_eleventh(monkeypatch):
    h = Harness("enforce", degraded_client(), monkeypatch=monkeypatch)
    before = metrics.billing_degraded_messages_total.get()
    for i in range(10):
        msg = await h.send()
        assert msg.billing["enforcement"] == "degraded" and msg.billing["granted_group"] == "fast"
    block = h.payloads[0]["billing"]
    assert block["hold_id"] is None and block["granted_group"] == "fast" and block["enforcement"] == "degraded"
    assert metrics.billing_degraded_messages_total.get() == before + 10
    with pytest.raises(BillingDeniedError) as e:
        await h.send()
    assert e.value.status_code == 503 and e.value.detail["reason"] == "billing_unavailable"
    assert e.value.detail["message_key"] == "billing.degraded"
    assert len(h.payloads) == 10
    key = f"billing:degraded:{USER}:{datetime.now(IST).strftime('%Y%m%d')}"
    assert h.redis.store[key] == 10 and h.redis.ttl[key] == 48 * 3600       # refused attempt did not count
    assert h.client.releases == []                                         # degraded messages hold nothing


async def test_degraded_refuses_best_and_other_non_fast_models(monkeypatch):
    h = Harness("enforce", degraded_client("deadline_exceeded"), monkeypatch=monkeypatch)
    for model in ("claude-sonnet-5-5", "openai/gpt-oss-120b", "gpt-6-luna"):
        with pytest.raises(BillingDeniedError) as e:
            await h.send(model=model)
        assert e.value.status_code == 503
    assert not h.persisted and h.redis.store == {}                           # refusals do not burn the daily count


async def test_degraded_allows_an_explicit_fast_model(monkeypatch):
    h = Harness("enforce", degraded_client(), monkeypatch=monkeypatch)
    await h.send(model="openai/gpt-oss-20b")
    b = h.payloads[0]["billing"]
    assert b["granted_model"] == "openai/gpt-oss-20b" and b["requested_model"] == "openai/gpt-oss-20b"


async def test_degraded_saved_default_non_fast_falls_back_to_auto(monkeypatch):
    h = Harness("enforce", degraded_client(), prefs=Prefs("claude-sonnet-5-5"), monkeypatch=monkeypatch)
    msg = await h.send()
    assert h.payloads[0]["billing"]["requested_model"] is None and msg.defaults_applied["model"] is True


async def test_degraded_persist_failure_gives_the_daily_slot_back(monkeypatch):
    h = Harness("enforce", degraded_client(), monkeypatch=monkeypatch)
    h.repo.fail = RuntimeError("down")
    with pytest.raises(RuntimeError):
        await h.send()
    key = f"billing:degraded:{USER}:{datetime.now(IST).strftime('%Y%m%d')}"
    assert h.redis.store[key] == 0


async def test_degraded_fails_closed_without_redis(monkeypatch):
    h = Harness("enforce", degraded_client(), monkeypatch=monkeypatch)
    h.gate.redis = None
    with pytest.raises(BillingDeniedError) as e:
        await h.send()
    assert e.value.status_code == 503


async def test_real_client_timeout_leads_to_degraded(monkeypatch):
    """The real BillingClient against a slow in-process gRPC server: the 150 ms deadline becomes a degraded send."""
    fake = FakeBilling()
    fake.delay = 1.0
    server, target = await start_fake_billing(fake)
    client = BillingClient(target=target, token="t")
    try:
        h = Harness("enforce", client, monkeypatch=monkeypatch)
        msg = await h.send()
        assert msg.billing["enforcement"] == "degraded"
    finally:
        await client.close()
        await server.stop(None)


async def test_real_client_deny_over_grpc(monkeypatch):
    from app.clients.billing_grpc import billing_pb2 as pb
    fake = FakeBilling()
    fake.reserve_handler = lambda req: pb.CheckAndReserveResponse(allowed=False, deny_reason=pb.DENY_REASON_NO_CREDIT)
    server, target = await start_fake_billing(fake)
    client = BillingClient(target=target, token="t")
    try:
        h = Harness("enforce", client, monkeypatch=monkeypatch)
        with pytest.raises(BillingDeniedError) as e:
            await h.send()
        assert e.value.status_code == 402 and not h.persisted
    finally:
        await client.close()
        await server.stop(None)


# ---- regenerate --------------------------------------------------------------------------------------------------

def history_for_regenerate(h):
    prompt = Message(conversation_id=CONV, message_id=uuid.UUID("0194a1b2-0000-7000-8000-0000000000a1"), sender="user",
                     content="question", created_at=datetime.now(IST), status=MessageStatus.SENT)
    answer = Message(conversation_id=CONV, message_id=uuid.UUID("0194a1b2-0000-7000-8000-0000000000a2"), sender="assistant",
                     content="answer", created_at=datetime.now(IST), status=MessageStatus.SENT)
    h.repo.history.return_value = [answer, prompt]
    return prompt, answer


async def test_regenerate_reserves_with_a_new_id_each_time(monkeypatch):
    h = Harness("enforce", FakeClient(allow("smart")), monkeypatch=monkeypatch)
    prompt, answer = history_for_regenerate(h)
    r1 = await h.svc.regenerate(CONV, answer.message_id, user_id=USER)
    r2 = await h.svc.regenerate(CONV, answer.message_id, user_id=USER)
    keys = [r[1] for r in h.client.reserves]
    assert len(keys) == 2 and keys[0] != keys[1]
    assert str(prompt.message_id) not in keys and str(answer.message_id) not in keys
    assert keys == [p["message_id"] for p in h.payloads]                      # the event's request id IS the reserve key
    assert h.payloads[0]["billing"]["granted_group"] == "smart" and h.payloads[0]["metadata"]["regeneration"] is True
    assert r1.billing["enforcement"] == "enforce"


async def test_regenerate_accepts_a_model_and_is_strict(monkeypatch):
    client = FakeClient(allow("smart"))
    client.by_model["claude-sonnet-5-5"] = deny("model_not_in_plan")
    h = Harness("enforce", client, monkeypatch=monkeypatch)
    _, answer = history_for_regenerate(h)
    with pytest.raises(BillingDeniedError) as e:
        await h.svc.regenerate(CONV, answer.message_id, user_id=USER, model="claude-sonnet-5-5")
    assert e.value.status_code == 403 and not h.persisted
    h.repo.delete.assert_not_called()                                         # the existing answer is untouched


async def test_regenerate_persist_failure_releases(monkeypatch):
    h = Harness("enforce", FakeClient(allow(hold_id="h-r")), monkeypatch=monkeypatch)
    _, answer = history_for_regenerate(h)
    h.repo.fail = RuntimeError("down")
    with pytest.raises(RuntimeError):
        await h.svc.regenerate(CONV, answer.message_id, user_id=USER)
    assert h.client.releases == [(str(USER), "h-r", "send_failed")]


async def test_regenerate_off_has_no_billing_block_and_no_call(monkeypatch):
    h = Harness("off", monkeypatch=monkeypatch)
    _, answer = history_for_regenerate(h)
    await h.svc.regenerate(CONV, answer.message_id, user_id=USER)
    assert "billing" not in h.payloads[0] and h.client.reserves == []


async def test_regenerate_endpoint_passes_the_optional_body_model(monkeypatch):
    svc = MagicMock()
    svc.regenerate = AsyncMock(return_value=Message(conversation_id=CONV, message_id=MSG, sender="assistant", content="x",
                                                    created_at=datetime.now(IST), status=MessageStatus.SENT))
    conv = MagicMock(user_id=USER)
    await regenerate_message(CONV, MSG, payload=RegenerateRequest(model="gpt-6-luna"), conv=conv, service=svc)
    assert svc.regenerate.call_args.kwargs["model"] == "gpt-6-luna"
    await regenerate_message(CONV, MSG, payload=None, conv=conv, service=svc)
    assert svc.regenerate.call_args.kwargs["model"] is None


# ---- idempotency (the HTTP layer) ----------------------------------------------------------------------------------

async def test_idempotent_replay_does_not_reserve_twice(monkeypatch):
    h = Harness("enforce", FakeClient(allow("smart", hold_id="h-1")), monkeypatch=monkeypatch)
    idem = IdempotencyService(h.redis)
    conv = MagicMock(user_id=USER)
    key = str(uuid.uuid4())
    first = await create_message(CONV, CreateMessageRequest(content="hi"), x_idempotency_key=key, conv=conv,
                                 service=h.svc, idempotency_service=idem)
    second = await create_message(CONV, CreateMessageRequest(content="hi"), x_idempotency_key=key, conv=conv,
                                  service=h.svc, idempotency_service=idem)
    assert len(h.client.reserves) == 1 and len(h.payloads) == 1
    assert second.message_id == first.message_id and second.billing.granted_group == "smart"


async def test_denied_response_is_not_cached_so_a_retry_after_upgrading_works(monkeypatch):
    client = FakeClient(deny("daily_limit"))
    h = Harness("enforce", client, monkeypatch=monkeypatch)
    idem = IdempotencyService(h.redis)
    conv = MagicMock(user_id=USER)
    key = str(uuid.uuid4())
    with pytest.raises(BillingDeniedError):
        await create_message(CONV, CreateMessageRequest(content="hi"), x_idempotency_key=key, conv=conv,
                             service=h.svc, idempotency_service=idem)
    assert f"idempotency:{key}" not in h.redis.store                          # nothing cached, lock removed
    client.result = allow("smart")                                            # the user upgraded
    ok = await create_message(CONV, CreateMessageRequest(content="hi"), x_idempotency_key=key, conv=conv,
                              service=h.svc, idempotency_service=idem)
    assert ok.billing["granted_group"] == "smart" if isinstance(ok.billing, dict) else ok.billing.granted_group == "smart"
    assert len(h.client.reserves) == 2 and len(h.payloads) == 1


# ---- contract with llm-service (app/models/billing.py, the exact 2.4 grant shape) ----------------------------------

LLM_BILLING = pathlib.Path(__file__).resolve().parents[3] / "llm-service" / "app" / "models" / "billing.py"


@pytest.mark.skipif(not LLM_BILLING.exists(), reason="llm-service repo not next to this one")
@pytest.mark.parametrize("mode,result,fail", [("enforce", allow("best", granted_model="claude-sonnet-5-5"), None),
                                              ("shadow", deny("daily_limit"), None),
                                              ("enforce", None, "breaker_open")])
async def test_event_block_matches_llm_service_grant_model(monkeypatch, mode, result, fail):
    import importlib.util
    spec = importlib.util.spec_from_file_location("llm_billing_model", LLM_BILLING)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    client = FakeClient(result)
    client.fail_with = fail
    h = Harness(mode, client, monkeypatch=monkeypatch)
    await h.send()
    block = h.payloads[0]["billing"]
    assert set(block) == set(mod.BillingGrant.model_fields)           # field for field
    parsed = mod.BillingGrant.model_validate(block)                   # and every value is valid for it
    assert parsed.enforcement == block["enforcement"]
