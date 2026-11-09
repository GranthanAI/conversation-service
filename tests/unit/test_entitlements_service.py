"""EntitlementsService cache behaviour, the Free fallback, -1 handling, the invalidating consumers, and the
bundled-Free-limits-match-plans.yaml check."""

import json
import uuid
from pathlib import Path

import pytest
import yaml

from app.clients.billing_client import BillingUnavailable, EntitlementsData
from app.core.plan_defaults import FREE_LIMITS, next_plan
from app.events.consumers import _handle_entitlements_event
from app.services.entitlements_service import (
    EntitlementsService, cache_key, free_entitlements, limit_of, stale_key, upgrade_to,
)

pytestmark = pytest.mark.anyio


class FakeRedis:
    def __init__(self):
        self.store, self.ttl = {}, {}

    async def get(self, key):
        return self.store.get(key)

    async def set(self, key, value, ex=None, nx=False):
        self.store[key] = value
        self.ttl[key] = ex
        return True

    async def delete(self, *keys):
        return sum(1 for k in keys if self.store.pop(k, None) is not None)


class FakeBillingClient:
    def __init__(self, plan="pro", limits=None):
        self.calls = 0
        self.down = False
        self.ent = EntitlementsData(user_id="u", plan_code=plan, limits=limits or {"messages_per_day": 60},
                                    source="subscription")

    async def get_entitlements(self, user_id):
        self.calls += 1
        if self.down:
            raise BillingUnavailable("unavailable")
        return self.ent


async def test_miss_calls_billing_and_fills_both_keys():
    redis, billing = FakeRedis(), FakeBillingClient()
    svc = EntitlementsService(billing, redis)
    ent = await svc.get("u1")
    assert ent.plan_code == "pro" and billing.calls == 1
    assert redis.ttl[cache_key("u1")] == 60 and redis.ttl[stale_key("u1")] == 3600


async def test_hit_does_not_call_billing():
    redis, billing = FakeRedis(), FakeBillingClient()
    svc = EntitlementsService(billing, redis)
    await svc.get("u1")
    await svc.get("u1")
    assert billing.calls == 1


async def test_cache_is_shared_between_replicas():
    redis, billing = FakeRedis(), FakeBillingClient()
    await EntitlementsService(billing, redis).get("u1")
    other = FakeBillingClient()
    assert (await EntitlementsService(other, redis).get("u1")).plan_code == "pro" and other.calls == 0


async def test_billing_down_uses_stale_value():
    redis, billing = FakeRedis(), FakeBillingClient(plan="max")
    svc = EntitlementsService(billing, redis)
    await svc.get("u1")
    del redis.store[cache_key("u1")]            # the 60 s key expired; the 1 h key is still there
    billing.down = True
    ent = await svc.get("u1")
    assert ent.plan_code == "max" and ent.source == "stale"


async def test_billing_down_and_nothing_cached_falls_back_to_free():
    billing = FakeBillingClient()
    billing.down = True
    ent = await EntitlementsService(billing, FakeRedis()).get("u1")
    assert ent.plan_code == "free" and ent.source == "free_fallback" and ent.limits == FREE_LIMITS


async def test_stale_key_expired_too_falls_back_to_free():
    redis, billing = FakeRedis(), FakeBillingClient()
    svc = EntitlementsService(billing, redis)
    await svc.get("u1")
    redis.store.clear()
    billing.down = True
    assert (await svc.get("u1")).source == "free_fallback"


async def test_works_without_redis():
    billing = FakeBillingClient()
    svc = EntitlementsService(billing, None)
    assert (await svc.get("u1")).plan_code == "pro"
    assert billing.calls == 1


def test_unlimited_and_missing_limits():
    ent = EntitlementsData(user_id="u", plan_code="max", limits={"branches_per_tree": -1})
    assert limit_of(ent, "branches_per_tree") == -1
    assert limit_of(ent, "custom_modes") == FREE_LIMITS["custom_modes"]    # missing -> Free value


def test_upgrade_target_follows_plan_order():
    assert next_plan("free") == "pro" and next_plan("pro") == "max" and next_plan("max") is None
    assert next_plan("org_pro") is None and next_plan("payg") is None
    assert upgrade_to(free_entitlements("u")) == "pro"


# ---- consumers -------------------------------------------------------------------------------------------------

class FakeInbox:
    def __init__(self):
        self.seen = set()

    def exists(self, eid):
        return eid in self.seen

    def save(self, eid):
        self.seen.add(eid)


def _event(user_id, topic_type):
    return {"event_type": topic_type, "schema_version": "1.0", "event_id": str(uuid.uuid4()),
            "occurred_at": "2026-11-10T00:00:00Z", "user_id": str(user_id)}


@pytest.mark.parametrize("topic", ["entitlements.changed", "user.deleted"])
async def test_consumers_delete_both_keys_and_dedupe(topic):
    redis, billing = FakeRedis(), FakeBillingClient()
    svc = EntitlementsService(billing, redis)
    uid = uuid.uuid4()
    await svc.get(uid)
    assert cache_key(uid) in redis.store and stale_key(uid) in redis.store
    inbox, event = FakeInbox(), _event(uid, topic)
    await _handle_entitlements_event(topic, event, inbox, svc)
    assert cache_key(uid) not in redis.store and stale_key(uid) not in redis.store
    # redelivery: nothing happens a second time (the refilled cache survives)
    await svc.get(uid)
    await _handle_entitlements_event(topic, event, inbox, svc)
    assert cache_key(uid) in redis.store


async def test_consumer_ignores_malformed_event():
    redis = FakeRedis()
    svc = EntitlementsService(FakeBillingClient(), redis)
    await _handle_entitlements_event("user.deleted", {"event_id": "nope"}, FakeInbox(), svc)


# ---- bundled Free copy vs plans.yaml -----------------------------------------------------------------------------

PLANS_YAML = Path(__file__).resolve().parents[3] / "billing-service" / "config" / "plans.yaml"


@pytest.mark.skipif(not PLANS_YAML.exists(), reason="billing-service repo not next to this one")
def test_bundled_free_limits_match_plans_yaml():
    plans = yaml.safe_load(PLANS_YAML.read_text(encoding="utf-8"))
    free = next(p for p in plans["plans"] if p["code"] == "free")
    assert dict(free["limits"]) == FREE_LIMITS
    assert [p["code"] for p in plans["plans"] if not p.get("org_only") and p["code"] in ("free", "pro", "max")] == \
        ["free", "pro", "max"]
    fast_ids = {m["id"] for m in plans["model_groups"]["fast"]}
    from app.core.plan_defaults import FAST_MODELS
    assert FAST_MODELS == fast_ids
