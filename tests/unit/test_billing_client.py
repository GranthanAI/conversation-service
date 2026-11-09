"""BillingClient against an in-process fake gRPC server: deadlines, metadata, breaker transitions, release retries."""

import time

import grpc
import pytest

from app.clients.billing_client import BillingClient, BillingUnavailable
from app.middleware.correlation import _correlation_id_ctx
from tests.support.fake_billing import FakeBilling, start_fake_billing

pytestmark = pytest.mark.anyio


class Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


@pytest.fixture
async def env():
    fake = FakeBilling()
    server, target = await start_fake_billing(fake)
    clock = Clock()
    sleeps = []

    async def fake_sleep(s):
        sleeps.append(s)

    client = BillingClient(target=target, token="tok-123", clock=clock, sleep=fake_sleep)
    yield fake, client, clock, sleeps
    await client.close()
    await server.stop(None)


async def test_reserve_sends_metadata_and_maps_response(env):
    fake, client, *_ = env
    token = _correlation_id_ctx.set("corr-42")
    try:
        r = await client.check_and_reserve("user-1", "req-1", requested_group="smart", requested_model="m")
    finally:
        _correlation_id_ctx.reset(token)
    assert r.allowed and r.hold_id == "hold-1" and r.granted_group == "fast" and r.remaining_today == 24
    assert fake.metadata[0]["authorization"] == "Bearer tok-123"
    assert fake.metadata[0]["x-correlation-id"] == "corr-42"
    req = fake.reserve_calls[0]
    assert (req.user_id, req.idempotency_key, req.requested_model) == ("user-1", "req-1", "m")
    assert req.kind == 1 and req.requested_group == 2   # MESSAGE, SMART


async def test_reserve_deadline_is_150ms(env):
    fake, client, *_ = env
    fake.delay = 1.0
    started = time.perf_counter()
    with pytest.raises(BillingUnavailable) as e:
        await client.check_and_reserve("u", "k")
    elapsed = time.perf_counter() - started
    assert e.value.reason == "deadline_exceeded"
    assert 0.1 < elapsed < 0.6


async def test_entitlements_deadline_is_300ms(env):
    fake, client, *_ = env
    fake.delay = 1.0
    started = time.perf_counter()
    with pytest.raises(BillingUnavailable):
        await client.get_entitlements("u")
    assert 0.25 < time.perf_counter() - started < 0.8


async def test_deny_is_mapped(env):
    from app.clients.billing_grpc import billing_pb2 as pb
    fake, client, *_ = env
    fake.reserve_handler = lambda req: pb.CheckAndReserveResponse(
        allowed=False, deny_reason=pb.DENY_REASON_DAILY_LIMIT, message_key="limit.daily")
    r = await client.check_and_reserve("u", "k")
    assert not r.allowed and r.deny_reason == "daily_limit" and r.message_key == "limit.daily" and r.hold_id is None


async def test_breaker_opens_after_5_failures_then_half_open_probe(env):
    fake, client, clock, _ = env
    fake.fail_code = grpc.StatusCode.UNAVAILABLE
    for _ in range(5):
        with pytest.raises(BillingUnavailable):
            await client.check_and_reserve("u", "k")
    assert len(fake.reserve_calls) == 5 and client.breaker.state == "open"
    # open: refused locally, the server sees nothing
    with pytest.raises(BillingUnavailable) as e:
        await client.check_and_reserve("u", "k")
    assert e.value.reason == "breaker_open" and len(fake.reserve_calls) == 5
    # after 30 s: half-open, a failed probe re-opens
    clock.t += 30
    assert client.breaker.state == "half_open"
    with pytest.raises(BillingUnavailable):
        await client.check_and_reserve("u", "k")
    assert len(fake.reserve_calls) == 6 and client.breaker.state == "open"
    # next half-open probe succeeds -> closed
    clock.t += 30
    fake.fail_code = None
    assert (await client.check_and_reserve("u", "k")).allowed
    assert client.breaker.state == "closed" and client.breaker.consecutive == 0


async def test_four_failures_do_not_open_and_success_resets(env):
    fake, client, *_ = env
    fake.fail_code = grpc.StatusCode.UNAVAILABLE
    for _ in range(4):
        with pytest.raises(BillingUnavailable):
            await client.check_and_reserve("u", "k")
    fake.fail_code = None
    assert (await client.check_and_reserve("u", "k")).allowed
    assert client.breaker.consecutive == 0 and client.breaker.state == "closed"


async def test_deadline_timeouts_count_towards_the_breaker(env):
    fake, client, *_ = env
    fake.delay = 1.0
    for _ in range(5):
        with pytest.raises(BillingUnavailable):
            await client.check_and_reserve("u", "k")
    assert client.breaker.state == "open"


async def test_unauthenticated_does_not_trip_the_breaker(env):
    fake, client, *_ = env
    fake.fail_code = grpc.StatusCode.UNAUTHENTICATED
    for _ in range(6):
        with pytest.raises(BillingUnavailable):
            await client.check_and_reserve("u", "k")
    assert client.breaker.state == "closed" and len(fake.reserve_calls) == 6


async def test_release_retries_three_times_with_backoff(env):
    fake, client, _, sleeps = env
    fake.release_fail_times = 99
    assert await client.release("u", "h", "send_failed") is None
    assert len(fake.release_calls) == 3 and sleeps == [0.1, 0.2]


async def test_release_succeeds_on_retry(env):
    fake, client, *_ = env
    fake.release_fail_times = 1
    assert await client.release("u", "h", "send_failed") is True
    assert len(fake.release_calls) == 2 and fake.release_calls[1].reason == "send_failed"
