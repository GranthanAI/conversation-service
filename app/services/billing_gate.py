"""The reserve step of a send / regenerate (integration plan 4.4.1, 4.6, 4.8 and P6 task 4).

  off      no call, no billing block (today's behaviour).
  shadow   call billing, log `billing_shadow_decision{decision,reason}`, ALWAYS proceed. An allow carries the grant
           (enforcement "shadow"); a deny / failure carries a block with hold_id and granted_group null (llm-service
           then uses its legacy routing).
  enforce  a deny becomes the plan 4.6 error (429/403/402) and nothing is persisted. If billing cannot answer (breaker
           open, timeout, down): DEGRADED mode - Auto or a Fast model only, DEGRADED_DAILY_LIMIT (10) messages per user
           per IST day counted in Redis (billing:degraded:{uid}:{yyyymmdd}, TTL 48 h); anything else is 503.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from app.clients.billing_client import BillingClient, BillingUnavailable, ReserveResult
from app.core.billing_messages import BEST_SWITCHED_KEY, IST, BillingDeniedError
from app.core.config import settings
from app.core.logging import logger
from app.core.plan_defaults import FAST_MODELS, FREE_LIMITS, FREE_MODELS
from app.services.model_groups import model_groups
from app.services.model_selection import ModelChoice, reserve_with_default_fallback
from app.telemetry import metrics

DEGRADED_KEY_TTL_SECONDS = 48 * 3600
_UPGRADE_HINT_REASONS = {"daily_limit", "monthly_limit", "model_not_in_plan"}


@dataclass
class BillingGrant:
    enforcement: str                              # enforce | shadow | degraded
    requested_model: Optional[str] = None         # None = Auto
    hold_id: Optional[str] = None
    granted_group: Optional[str] = None
    granted_model: Optional[str] = None
    max_context_tokens: Optional[int] = None
    funded_by: Optional[str] = None
    downgraded: bool = False
    remaining_today: Optional[int] = None
    degraded_key: Optional[str] = None            # set when this message was counted in the degraded daily counter
    allowed_models: Optional[list] = None         # the plan's models inside granted_group (None = not sent)

    def event_block(self) -> dict:
        """The `billing` block of chat.message.created 2.4 (llm-service app/models/billing.py, field for field)."""
        block = {"hold_id": self.hold_id, "requested_model": self.requested_model, "granted_group": self.granted_group,
                 "granted_model": self.granted_model, "max_context_tokens": self.max_context_tokens,
                 "funded_by": self.funded_by, "downgraded": self.downgraded, "enforcement": self.enforcement}
        if self.allowed_models is not None:           # additive and optional (2.4): omitted when unknown
            block["allowed_models"] = list(self.allowed_models)
        return block

    def response_block(self) -> dict:
        """MessageResponse.billing."""
        return {"granted_group": self.granted_group, "granted_model": self.granted_model,
                "downgraded": self.downgraded, "remaining_today": self.remaining_today, "funded_by": self.funded_by,
                "enforcement": self.enforcement, "notice_key": BEST_SWITCHED_KEY if self.downgraded else None}


def _grant(r: ReserveResult, choice: ModelChoice, enforcement: str) -> BillingGrant:
    return BillingGrant(enforcement=enforcement, requested_model=choice.requested_model, hold_id=r.hold_id,
                        granted_group=r.granted_group, granted_model=r.granted_model,
                        max_context_tokens=r.max_context_tokens, funded_by=r.funded_by, downgraded=r.downgraded,
                        remaining_today=r.remaining_today)


class BillingGate:
    def __init__(self, client: BillingClient, redis_client=None, entitlements=None):
        self.client = client
        self.redis = redis_client
        self.entitlements = entitlements

    async def reserve(self, user_id, request_message_id, choice: ModelChoice):
        """Returns (grant or None, fell_back). `fell_back` = the saved default model was refused with
        model_not_in_plan and the send went ahead as Auto (-> defaults_applied.model = true).
        Raises BillingDeniedError in enforce mode when billing refuses or degraded mode cannot serve the message."""
        mode = settings.BILLING_ENFORCEMENT
        if mode == "off":
            return None, False
        uid, key = str(user_id), str(request_message_id)

        async def call(model: Optional[str]) -> ReserveResult:
            return await self.client.check_and_reserve(uid, key, None, model or "")

        if mode == "shadow":
            return await self._shadow(call, choice), False

        try:
            result, final, fell_back = await reserve_with_default_fallback(choice, call)
        except BillingUnavailable as e:
            return await self._degraded(uid, choice, e.reason)
        if not result.allowed:
            metrics.billing_reserve_total.inc(mode="enforce", decision="deny", reason=result.deny_reason)
            model = final.requested_model
            raise BillingDeniedError(result.deny_reason, upgrade_to=await self._upgrade_to(uid, result.deny_reason),
                                     message_key=result.message_key, model=model,
                                     model_label=await model_groups.label_of(model) if model else None)
        metrics.billing_reserve_total.inc(mode="enforce", decision="allow", reason="")
        grant = _grant(result, final, "enforce")
        grant.allowed_models = await self._allowed_in_group(uid, grant)
        return grant, fell_back

    async def _shadow(self, call, choice: ModelChoice) -> BillingGrant:
        try:
            result = await call(choice.requested_model)
        except BillingUnavailable as e:
            decision, reason, result = "unavailable", e.reason, None
        else:
            decision, reason = ("allow", "") if result.allowed else ("deny", result.deny_reason)
        logger.info("billing_shadow_decision", decision=decision, reason=reason)
        metrics.billing_reserve_total.inc(mode="shadow", decision=decision, reason=reason)
        if result is not None and result.allowed:
            return _grant(result, choice, "shadow")
        return BillingGrant(enforcement="shadow", requested_model=choice.requested_model)   # hold_id/granted_group null

    async def _allowed_in_group(self, uid: str, grant: BillingGrant) -> Optional[list]:
        """The plan's models inside the granted group, from the cached GetEntitlements. None (field omitted) when
        unknown: no entitlements service or an empty list (the Free fallback), no granted group, or credits paid the
        message (the plan does not govern what credits can buy)."""
        if self.entitlements is None or not grant.granted_group or grant.funded_by == "credits":
            return None
        try:
            ent = await self.entitlements.get(uid)
        except Exception:
            return None
        if not ent.allowed_models:
            return None
        groups = await model_groups.index()
        return [m for m in ent.allowed_models if groups.get(m, {}).get("group") == grant.granted_group]

    async def _degraded(self, uid: str, choice: ModelChoice, why: str):
        """Billing could not answer: Auto, or a model of the bundled Free plan (Fast + its extras), at most
        DEGRADED_DAILY_LIMIT per user per IST day. Advanced, Ultra and anything else is refused (503)."""
        model = choice.requested_model
        fell_back = False
        if model is not None and model not in FREE_MODELS:
            if not choice.from_default:                     # an explicit non-Free choice is never served here
                return self._degraded_refusal(uid, why, "model")
            model, fell_back = None, True                   # a saved default is not an explicit choice: use Auto
        counter = await self._degraded_count(uid)
        if counter is None or counter[0] > settings.BILLING_DEGRADED_DAILY_LIMIT:
            if counter is not None:
                await self._degraded_undo(counter[1])
            return self._degraded_refusal(uid, why, "daily_cap" if counter is not None else "counter_unavailable")
        metrics.billing_degraded_messages_total.inc()
        metrics.billing_reserve_total.inc(mode="enforce", decision="degraded", reason=why)
        logger.warning("billing_degraded_allow", reason=why, used_today=counter[0])
        groups = await model_groups.index()
        group = groups.get(model, {}).get("group", "fast") if model else "fast"   # a Free extra keeps its catalog group
        in_group = sorted(m for m in FREE_MODELS if groups.get(m, {}).get("group") == group)
        grant = BillingGrant(enforcement="degraded", requested_model=model if not fell_back else None,
                             hold_id=None, granted_group=group, granted_model=model, allowed_models=in_group,
                             max_context_tokens=FREE_LIMITS["context_tokens"], funded_by="plan",
                             remaining_today=settings.BILLING_DEGRADED_DAILY_LIMIT - counter[0], degraded_key=counter[1])
        return grant, fell_back

    def _degraded_refusal(self, uid: str, why: str, cause: str):
        metrics.billing_reserve_total.inc(mode="enforce", decision="degraded_refused", reason=cause)
        logger.warning("billing_degraded_refused", reason=why, cause=cause)
        raise BillingDeniedError("billing_unavailable")

    async def _degraded_count(self, uid: str):
        """(count including this message, key), or None if Redis is not usable (fail closed)."""
        if not self.redis:
            return None
        key = f"billing:degraded:{uid}:{datetime.now(IST).strftime('%Y%m%d')}"
        try:
            n = await self.redis.incr(key)
            await self.redis.expire(key, DEGRADED_KEY_TTL_SECONDS)
            return int(n), key
        except Exception as e:
            logger.warning("billing_degraded_counter_failed", error=str(e))
            return None

    async def _degraded_undo(self, key: Optional[str]) -> None:
        if not key or not self.redis:
            return
        try:
            await self.redis.decr(key)
        except Exception as e:
            logger.warning("billing_degraded_undo_failed", error=str(e))

    async def _upgrade_to(self, uid: str, reason: str) -> Optional[str]:
        if reason not in _UPGRADE_HINT_REASONS or self.entitlements is None:
            return None
        try:
            from app.services.entitlements_service import upgrade_to
            return upgrade_to(await self.entitlements.get(uid))
        except Exception:
            return None

    async def release(self, user_id, grant: Optional[BillingGrant], reason: str = "send_failed") -> None:
        """Compensation for a send that failed after a successful reserve. Never raises."""
        if grant is None:
            return
        try:
            if grant.hold_id:
                await self.client.release(str(user_id), grant.hold_id, reason)
            elif grant.degraded_key:
                await self._degraded_undo(grant.degraded_key)
        except Exception as e:
            logger.error("billing_release_failed", error=str(e))
