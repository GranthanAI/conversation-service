"""English copy and error shapes for billing / plan-limit refusals (integration plan 4.6).

Every refusal is an HTTPException whose `detail` is
  {"code": "billing_denied", "reason": ..., "message_key": ..., "message": ..., "upgrade_to": ..., [resets_at, limit]}
so the frontend can pick its own copy by `message_key` and fall back to `message`.
"""

from datetime import datetime, time, timedelta, timezone
from typing import Optional

from fastapi import HTTPException, status

# Asia/Kolkata is a fixed +05:30 offset (no DST); a fixed zone avoids a tzdata dependency on Windows.
IST = timezone(timedelta(hours=5, minutes=30), name="IST")

# billing deny reason -> (HTTP status, message_key, English message)   [plan 4.6]
BILLING_DENIALS: dict[str, tuple[int, str, str]] = {
    "daily_limit": (429, "limit.daily", "Daily limit reached. Resets at midnight, or upgrade / top up."),
    "monthly_limit": (429, "limit.monthly", "You've used this month's allowance. It resets on the 1st, or upgrade."),
    "model_not_in_plan": (403, "plan.model_not_included", "This model isn't included in your plan."),
    "account_blocked": (403, "account.blocked", "Your account can't send messages right now. Please contact support."),
    "no_credit": (402, "credits.empty", "Top up to continue."),
    "spend_cap": (402, "credits.spend_cap",
                  "Monthly spend cap reached. Sending is paused until the cap is raised or next month."),
    "member_spend_cap": (402, "credits.member_cap",
                         "Your organisation's monthly limit for you has been reached. "
                         "Ask your organisation admin to raise it."),
    "billing_unavailable": (503, "billing.degraded",
                            "Only Fast models are available for a few minutes. Please try again shortly."),
}

BEST_SWITCHED_KEY = "limit.best_switched"
BEST_SWITCHED_MESSAGE = "You've used today's Best-model messages, so this answer uses a Smart model."

# Service-owned plan limits (plan 4.8): name -> what the message calls it.
PLAN_LIMIT_WHAT: dict[str, str] = {
    "messages_per_conversation": "Conversation message",
    "branches_per_tree": "Branch",
    "documents_per_tree": "Document",
    "custom_modes": "Custom mode",
    "mode_instruction_chars": "Instruction length",
}


def plan_limit_message(limit: str) -> str:
    return f"{PLAN_LIMIT_WHAT.get(limit, limit)} limit for your plan reached. Upgrade to add more."


def next_ist_midnight(now: Optional[datetime] = None) -> str:
    now = (now or datetime.now(IST)).astimezone(IST)
    return datetime.combine(now.date() + timedelta(days=1), time(0, 0), tzinfo=IST).isoformat()


def next_ist_month_start(now: Optional[datetime] = None) -> str:
    now = (now or datetime.now(IST)).astimezone(IST)
    first = now.date().replace(day=1)
    nxt = (first + timedelta(days=32)).replace(day=1)
    return datetime.combine(nxt, time(0, 0), tzinfo=IST).isoformat()


class PlanLimitError(HTTPException):
    """403 plan_limit: a service-owned limit (branches, documents, ...) is reached. Nothing was created."""

    def __init__(self, limit: str, upgrade_to: Optional[str]):
        self.limit = limit
        super().__init__(status_code=status.HTTP_403_FORBIDDEN, detail={
            "code": "billing_denied", "reason": "plan_limit", "limit": limit, "message_key": f"limit.{limit}",
            "message": plan_limit_message(limit), "upgrade_to": upgrade_to})


class BillingDeniedError(HTTPException):
    """Billing refused the send (429 / 403 / 402) or is unavailable (503). Nothing was persisted."""

    def __init__(self, reason: str, upgrade_to: Optional[str] = None, now: Optional[datetime] = None):
        http_status, key, message = BILLING_DENIALS.get(
            reason, (status.HTTP_403_FORBIDDEN, "account.blocked", BILLING_DENIALS["account_blocked"][2]))
        detail = {"code": "billing_denied", "reason": reason, "message_key": key, "message": message,
                  "upgrade_to": upgrade_to}
        if reason == "daily_limit":
            detail["resets_at"] = next_ist_midnight(now)
        elif reason == "monthly_limit":
            detail["resets_at"] = next_ist_month_start(now)
        self.reason = reason
        super().__init__(status_code=http_status, detail=detail)
