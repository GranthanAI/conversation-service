r"""Contract smoke test of BillingClient against the SHARED dev billing (no HTTP user needed).

Uses the conversation-service token from the environment (BILLING_SERVICE_TOKEN) and a throwaway random user id:
  GetEntitlements -> 25 x CheckAndReserve (Free daily limit) -> the 26th must be denied daily_limit -> Release every hold.
Run:  .venv\Scripts\python scripts\billing_smoke.py     (needs BILLING_GRPC_TARGET and BILLING_SERVICE_TOKEN)
"""
import asyncio
import sys
import types
import uuid

asyncore = types.ModuleType("asyncore")
asyncore.dispatcher = type("D", (), {})
sys.modules.setdefault("asyncore", asyncore)

from app.clients.billing_client import BillingClient  # noqa: E402
from app.telemetry import metrics  # noqa: E402


async def main() -> int:
    client = BillingClient()
    user = str(uuid.uuid4())
    ent = await client.get_entitlements(user)
    print("entitlements:", ent.plan_code, ent.source, "messages_per_day =", ent.limits.get("messages_per_day"))
    holds, last = [], None
    for i in range(1, 27):
        r = await client.check_and_reserve(user, str(uuid.uuid4()))
        if r.allowed:
            holds.append(r.hold_id)
            last = r
        else:
            print(f"reserve #{i}: DENIED reason={r.deny_reason} message_key={r.message_key}")
            break
    print(f"allowed before the deny: {len(holds)}; last allow granted_group={last.granted_group} remaining_today={last.remaining_today}")
    model = await client.check_and_reserve(user, str(uuid.uuid4()), requested_model="claude-sonnet-5-5")
    print("claude-sonnet-5-5 on Free:", "allowed" if model.allowed else f"DENIED {model.deny_reason} ({model.message_key})")
    if model.allowed and model.hold_id:
        holds.append(model.hold_id)
    released = [await client.release(user, h, "cancelled") for h in holds]
    print("released:", sum(1 for x in released if x), "of", len(holds))
    print("p95 reserve latency (histogram bucket upper bound, s):", metrics.billing_reserve_seconds.quantile(0.95))
    await client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
