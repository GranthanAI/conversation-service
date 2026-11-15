"""M3: model groups from billing, cached in Redis, bundled fallback, and model.unavailable -> 409."""

import json
import pathlib

import pytest

from app.core.billing_messages import BillingDeniedError
from app.core.plan_defaults import MODEL_GROUPS
from app.services.model_groups import CACHE_KEY, CACHE_TTL_SECONDS, ModelGroups

pytestmark = pytest.mark.anyio
SNAPSHOT = json.loads((pathlib.Path(__file__).parent.parent / "fixtures" / "billing_models_catalog.json").read_text())["models"]


class Redis:
    def __init__(self):
        self.d, self.ttl = {}, {}

    async def get(self, k):
        return self.d.get(k)

    async def set(self, k, v, ex=None):
        self.d[k], self.ttl[k] = v, ex


def service(fetch, redis=None, clock=None):
    t = [0.0]
    return ModelGroups(redis, fetch, "http://billing", clock or (lambda: t[0])), t


async def test_groups_and_labels_come_from_billing_and_are_cached_in_redis_for_10_minutes():
    calls = []

    async def fetch(url):
        calls.append(url)
        return SNAPSHOT

    r = Redis()
    mg, _ = service(fetch, r)
    assert await mg.group_of("claude-haiku-5-5") == "smart" and await mg.label_of("claude-haiku-5-5") == "Claude Haiku 5.5"
    assert calls == ["http://billing"] and r.ttl[CACHE_KEY] == CACHE_TTL_SECONDS
    other, _ = service(fetch, r)                      # another process: served from Redis, no second fetch
    assert await other.group_of("claude-sonnet-5-5") == "best" and len(calls) == 1


async def test_a_new_model_in_billing_needs_no_code_change():
    async def fetch(url):
        return [*SNAPSHOT, {"id": "brand-new", "label": "Brand New", "group": "smart"}]

    mg, _ = service(fetch)
    assert await mg.group_of("brand-new") == "smart"
    assert "brand-new" not in MODEL_GROUPS


async def test_billing_down_at_startup_uses_the_bundled_map():
    async def fetch(url):
        raise ConnectionError("billing down")

    mg, _ = service(fetch)
    assert await mg.group_of("gpt-6-luna") == "smart"
    assert await mg.label_of("gpt-6-luna") == "gpt-6-luna"        # no label known: the id
    await mg.start()                                              # never raises
    await mg.stop()


async def test_a_stale_copy_beats_the_bundled_map_when_billing_goes_down_later():
    state = {"down": False}

    async def fetch(url):
        if state["down"]:
            raise ConnectionError("down")
        return [{"id": "only-here", "label": "Only Here", "group": "best"}]

    mg, t = service(fetch)
    assert await mg.group_of("only-here") == "best"
    state["down"] = True
    t[0] = 1000.0                                                 # local copy expired
    assert await mg.group_of("only-here") == "best"


def test_model_unavailable_is_a_409_with_the_model_name_and_no_plan_hint():
    e = BillingDeniedError("model_not_in_plan", upgrade_to="pro", message_key="model.unavailable",
                           model="claude-haiku-5-5", model_label="Claude Haiku 5.5")
    assert e.status_code == 409
    assert e.detail == {"code": "billing_denied", "reason": "model_not_in_plan", "message_key": "model.unavailable",
                        "message": "Claude Haiku 5.5 isn't available right now. Please pick another model.",
                        "upgrade_to": None, "model": "claude-haiku-5-5"}


def test_an_ordinary_model_not_in_plan_is_still_a_403():
    e = BillingDeniedError("model_not_in_plan", upgrade_to="pro", message_key="plan.model_not_included")
    assert e.status_code == 403 and e.detail["message_key"] == "plan.model_not_included"
