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
# Copy says what is included and what is left, never "limit / denied / quota"; counts and times come from the
# response fields (resets_at, ...) and the frontend, never from this text.
BILLING_DENIALS: dict[str, tuple[int, str, str]] = {
    "daily_limit": (429, "limit.daily", "Today's messages are all used. They refresh at 12:00 am."),
    "monthly_limit": (429, "limit.monthly", "This month's allowance is all used. It refreshes on the 1st."),
    "model_not_in_plan": (403, "plan.model_not_included", "This model is included with a bigger plan."),
    "account_blocked": (403, "account.blocked", "Your account can't send messages right now. Please contact support."),
    "no_credit": (402, "credits.empty",
                  "Your credits are used up. Top up to keep going; Fast models stay available every day."),
    "spend_cap": (402, "credits.spend_cap",
                  "Your monthly spend cap is reached, so credits are paused. Raise the cap, or they continue next month."),
    "member_spend_cap": (402, "credits.member_cap",
                         "Your share of your organisation's monthly credits is used. "
                         "Ask your organisation admin to add more."),
    "rate_limit": (429, "limit.per_minute", "You're sending quickly. Give it a moment and try again."),
    "billing_unavailable": (503, "billing.degraded",
                            "Fast models are available for a few minutes while we reconnect. Please try again shortly."),
}

# A deny reason this service does not know (billing added one): a 429 with generic copy, never a 500 or a wrong 403.
UNKNOWN_DENIAL = (429, "limit.unknown", "We can't send this message right now. Please try again in a moment.")

MODEL_UNAVAILABLE_KEY = "model.unavailable"


def model_unavailable_message(model: str) -> str:
    return f"{model} isn't available right now. Please pick another model."


BEST_SWITCHED_KEY = "limit.best_switched"
BEST_SWITCHED_MESSAGE = "Today's Advanced answers are used, so this one comes from a Smart model. Advanced refreshes at 12:00 am."

# Service-owned plan limits (plan 4.8): name -> (what is full, the one-line fix). The tail " or see plans for more." is
# added only when a bigger plan exists (upgrade_to is set).
PLAN_LIMIT_COPY: dict[str, tuple[str, str]] = {
    "branches_per_tree": ("This tree has all the branches it includes.", "Start a new tree"),
    "documents_per_tree": ("This tree has all the documents it includes.", "Remove one"),
    "custom_modes": ("You have all the custom modes your plan includes.", "Remove one"),
    "mode_instruction_chars": ("That instruction is longer than your plan includes.", "Shorten it"),
}

CONVERSATION_FULL_MESSAGE = (
    "This conversation has reached its full length. Branch from any message to continue; your context comes with you."
)


def plan_limit_message(limit: str, can_upgrade: bool = True) -> str:
    if limit == "messages_per_conversation":
        return CONVERSATION_FULL_MESSAGE
    what, fix = PLAN_LIMIT_COPY.get(limit, ("Your plan's allowance for this is all used.", ""))
    if can_upgrade:
        return f"{what} {fix + ', or see' if fix else 'See'} plans for more."
    return f"{what} {fix}." if fix else what


def next_ist_minute(now: Optional[datetime] = None) -> str:
    now = (now or datetime.now(IST)).astimezone(IST)
    return (now.replace(second=0, microsecond=0) + timedelta(minutes=1)).isoformat()


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
            "message": plan_limit_message(limit, upgrade_to is not None),
            "upgrade_to": upgrade_to})


class BillingDeniedError(HTTPException):
    """Billing refused the send (429 / 403 / 402) or is unavailable (503). Nothing was persisted."""

    def __init__(self, reason: str, upgrade_to: Optional[str] = None, now: Optional[datetime] = None,
                 message_key: str = "", model: Optional[str] = None, model_label: Optional[str] = None):
        http_status, key, message = BILLING_DENIALS.get(reason, UNKNOWN_DENIAL)
        extra = {}
        if message_key == MODEL_UNAVAILABLE_KEY:
            # billing refused an explicit model that cannot answer: a conflict with the request (409), not a rate
            # limit, and nothing was reserved. upgrade_to stays null: a bigger plan does not help.
            http_status, key = 409, MODEL_UNAVAILABLE_KEY
            message = model_unavailable_message(model_label or model or "This model")
            upgrade_to = None
            if model:
                extra["model"] = model
        detail = {"code": "billing_denied", "reason": reason, "message_key": key, "message": message,
                  "upgrade_to": upgrade_to}
        detail.update(extra)
        if reason == "daily_limit":
            detail["resets_at"] = next_ist_midnight(now)
        elif reason == "monthly_limit":
            detail["resets_at"] = next_ist_month_start(now)
        elif reason == "rate_limit":
            detail["resets_at"] = next_ist_minute(now)
        self.reason = reason
        super().__init__(status_code=http_status, detail=detail)
