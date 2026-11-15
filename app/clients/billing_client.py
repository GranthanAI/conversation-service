"""Async gRPC client for billing-service `BillingInternal` (integration plan 4.5).

Caller rules:
  CheckAndReserve  deadline 150 ms, no retry (the idempotency key makes a later retry safe)
  Release          deadline 500 ms, 3 attempts with backoff
  GetEntitlements  deadline 300 ms
  Circuit breaker  opens after 5 consecutive failures/timeouts; half-open (one probe) after 30 s
  Metadata         authorization: Bearer <service token>, x-correlation-id from the current request
"""

import asyncio
import time
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Dict, List, Optional

import grpc

from app.clients.billing_grpc import billing_pb2 as pb
from app.clients.billing_grpc import billing_pb2_grpc as pb_grpc
from app.core.config import settings
from app.core.logging import logger
from app.middleware.correlation import get_correlation_id
from app.telemetry import metrics

# gRPC statuses that mean "billing could not answer" and count towards the breaker.
_TRANSPORT_FAILURES = {
    grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.INTERNAL,
    grpc.StatusCode.UNKNOWN, grpc.StatusCode.RESOURCE_EXHAUSTED, grpc.StatusCode.ABORTED,
}

DENY_REASONS = {
    pb.DENY_REASON_DAILY_LIMIT: "daily_limit",
    pb.DENY_REASON_MONTHLY_LIMIT: "monthly_limit",
    pb.DENY_REASON_MODEL_NOT_IN_PLAN: "model_not_in_plan",
    pb.DENY_REASON_NO_CREDIT: "no_credit",
    pb.DENY_REASON_SPEND_CAP: "spend_cap",
    pb.DENY_REASON_ACCOUNT_BLOCKED: "account_blocked",
    pb.DENY_REASON_MEMBER_SPEND_CAP: "member_spend_cap",
    pb.DENY_REASON_RATE_LIMIT: "rate_limit",
}
GROUP_NAMES = {pb.MODEL_GROUP_FAST: "fast", pb.MODEL_GROUP_SMART: "smart", pb.MODEL_GROUP_BEST: "best",
               pb.MODEL_GROUP_ULTRA: "ultra"}
GROUP_ENUMS = {v: k for k, v in GROUP_NAMES.items()}


class BillingUnavailable(Exception):
    """Billing could not give an answer (down, too slow, breaker open, or it refused the call itself)."""

    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(reason)


@dataclass
class ReserveResult:
    allowed: bool
    deny_reason: str = ""            # daily_limit | monthly_limit | model_not_in_plan | ... ("" when allowed)
    message_key: str = ""
    hold_id: Optional[str] = None
    granted_group: Optional[str] = None   # fast | smart | best | ultra
    granted_model: Optional[str] = None
    downgraded: bool = False
    max_context_tokens: Optional[int] = None
    remaining_today: Optional[int] = None  # -1 = unlimited
    funded_by: Optional[str] = None
    hold_expires_at_unix: int = 0


@dataclass
class EntitlementsData:
    user_id: str
    plan_code: str
    limits: Dict[str, int] = field(default_factory=dict)
    allowed_models: List[str] = field(default_factory=list)
    source: str = ""
    version_hash: str = ""

    def to_json(self) -> dict:
        return {"user_id": self.user_id, "plan_code": self.plan_code, "limits": self.limits,
                "allowed_models": self.allowed_models, "source": self.source, "version_hash": self.version_hash}

    @classmethod
    def from_json(cls, d: dict) -> "EntitlementsData":
        return cls(user_id=d.get("user_id", ""), plan_code=d.get("plan_code", "free"),
                   limits={k: int(v) for k, v in (d.get("limits") or {}).items()},
                   allowed_models=list(d.get("allowed_models") or []), source=d.get("source", ""),
                   version_hash=d.get("version_hash", ""))


class CircuitBreaker:
    """closed -> (N consecutive failures) -> open -> (reset_seconds) -> half-open (one probe) -> closed | open."""

    def __init__(self, failures: int, reset_seconds: float, clock: Callable[[], float] = time.monotonic):
        self.threshold, self.reset_seconds, self.clock = failures, reset_seconds, clock
        self.consecutive = 0
        self.opened_at: Optional[float] = None
        self.probing = False

    @property
    def state(self) -> str:
        if self.opened_at is None:
            return "closed"
        if self.clock() - self.opened_at >= self.reset_seconds:
            return "half_open"
        return "open"

    def allow(self) -> bool:
        """True if a call may go out. In half-open state exactly one probe is allowed at a time."""
        state = self.state
        if state == "closed":
            return True
        if state == "half_open" and not self.probing:
            self.probing = True
            return True
        return False

    def success(self) -> None:
        self.consecutive, self.opened_at, self.probing = 0, None, False
        metrics.billing_breaker_open.set(0)

    def failure(self) -> None:
        self.consecutive += 1
        if self.probing or self.consecutive >= self.threshold:
            self.opened_at = self.clock()
            self.probing = False
            metrics.billing_breaker_open.set(1)
            logger.warning("billing_breaker_open", consecutive_failures=self.consecutive)


class BillingClient:
    def __init__(self, target: Optional[str] = None, token: Optional[str] = None,
                 clock: Callable[[], float] = time.monotonic,
                 sleep: Callable[[float], Awaitable[None]] = asyncio.sleep):
        self.target = target or settings.BILLING_GRPC_TARGET
        self.token = settings.BILLING_SERVICE_TOKEN if token is None else token
        self.breaker = CircuitBreaker(settings.BILLING_BREAKER_FAILURES, settings.BILLING_BREAKER_RESET_SECONDS, clock)
        self._sleep = sleep
        self._channel = None
        self._stub = None

    def _get_stub(self):
        if self._stub is None:
            self._channel = grpc.aio.insecure_channel(self.target)
            self._stub = pb_grpc.BillingInternalStub(self._channel)
        return self._stub

    async def close(self) -> None:
        if self._channel is not None:
            await self._channel.close()
            self._channel, self._stub = None, None

    def _metadata(self):
        md = [("authorization", f"Bearer {self.token}")]
        cid = get_correlation_id()
        if cid:
            md.append(("x-correlation-id", cid))
        return md

    async def _call(self, rpc: str, request, deadline: float):
        if not self.breaker.allow():
            raise BillingUnavailable("breaker_open")
        try:
            response = await getattr(self._get_stub(), rpc)(request, timeout=deadline, metadata=self._metadata())
        except grpc.aio.AioRpcError as e:
            code = e.code()
            if code in _TRANSPORT_FAILURES:
                self.breaker.failure()
            else:
                # Billing answered but refused the call (bad token/arguments): a configuration problem, not an
                # outage, so it does not trip the breaker; the caller still cannot get a decision.
                self.breaker.success()
                logger.error("billing_call_refused", rpc=rpc, code=code.name, detail=e.details())
            raise BillingUnavailable(code.name.lower()) from e
        except Exception as e:  # channel errors outside gRPC status codes
            self.breaker.failure()
            raise BillingUnavailable(type(e).__name__) from e
        self.breaker.success()
        return response

    async def check_and_reserve(self, user_id: str, idempotency_key: str, requested_group: Optional[str] = None,
                                requested_model: str = "") -> ReserveResult:
        """Kind is always MESSAGE (chat send / regenerate). Raises BillingUnavailable when no decision was possible."""
        request = pb.CheckAndReserveRequest(
            user_id=user_id, idempotency_key=idempotency_key, kind=pb.RESERVE_KIND_MESSAGE,
            requested_group=GROUP_ENUMS.get(requested_group or "", pb.MODEL_GROUP_UNSPECIFIED),
            requested_model=requested_model or "")
        started = time.perf_counter()
        try:
            r = await self._call("CheckAndReserve", request, settings.BILLING_RESERVE_DEADLINE_SECONDS)
        finally:
            metrics.billing_reserve_seconds.observe(time.perf_counter() - started)
        return ReserveResult(
            allowed=r.allowed,
            deny_reason=DENY_REASONS.get(r.deny_reason, "" if r.allowed else "unknown"),
            message_key=r.message_key, hold_id=r.hold_id or None,
            granted_group=GROUP_NAMES.get(r.granted_group), granted_model=r.granted_model or None,
            downgraded=r.downgraded, max_context_tokens=r.max_context_tokens,  # 0 = no cap from billing: passed on unchanged
            remaining_today=r.remaining_today, funded_by=r.funded_by or None,
            hold_expires_at_unix=r.hold_expires_at_unix)

    async def release(self, user_id: str, hold_id: str, reason: str) -> Optional[bool]:
        """Gives a reservation back. Up to 3 attempts with backoff. Never raises (compensation must not mask the
        original error): True/False = billing's answer, None = could not be delivered (the hold expires by itself)."""
        request = pb.ReleaseRequest(user_id=user_id, hold_id=hold_id, reason=reason)
        for attempt in range(settings.BILLING_RELEASE_ATTEMPTS):
            try:
                r = await self._call("Release", request, settings.BILLING_RELEASE_DEADLINE_SECONDS)
                metrics.billing_release_total.inc(reason=reason)
                return r.released
            except BillingUnavailable as e:
                if e.reason == "breaker_open" or attempt == settings.BILLING_RELEASE_ATTEMPTS - 1:
                    break
                await self._sleep(0.1 * (2 ** attempt))
        logger.warning("billing_release_undelivered", hold_id=hold_id, reason=reason)
        return None

    async def get_entitlements(self, user_id: str) -> EntitlementsData:
        r = await self._call("GetEntitlements", pb.GetEntitlementsRequest(user_id=user_id),
                             settings.BILLING_ENTITLEMENTS_DEADLINE_SECONDS)
        return EntitlementsData(user_id=r.user_id or user_id, plan_code=r.plan_code or "free",
                                limits={k: int(v) for k, v in r.limits.items()},
                                allowed_models=list(r.allowed_models), source=r.source, version_hash=r.version_hash)


billing_client = BillingClient()
