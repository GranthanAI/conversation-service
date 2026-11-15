"""B4f: the new rate_limit deny reason, unknown reasons never a 500, the conversation-full copy, and max_context_tokens 0."""

from datetime import datetime

import pytest

from app.core.billing_messages import IST, BillingDeniedError, PlanLimitError


def test_rate_limit_is_a_429_with_the_per_minute_key_and_the_next_minute():
    now = datetime(2026, 11, 15, 10, 30, 41, tzinfo=IST)
    e = BillingDeniedError("rate_limit", now=now)
    assert e.status_code == 429
    assert e.detail == {
        "code": "billing_denied", "reason": "rate_limit", "message_key": "limit.per_minute",
        "message": "You're sending quickly. Give it a moment and try again.", "upgrade_to": None,
        "resets_at": "2026-11-15T10:31:00+05:30",
    }


@pytest.mark.parametrize("reason", ["something_new", "", "unknown"])
def test_an_unknown_reason_is_a_429_with_generic_copy(reason):
    e = BillingDeniedError(reason)
    assert e.status_code == 429
    assert e.detail["reason"] == reason
    assert e.detail["message_key"] == "limit.unknown"
    assert "try again" in e.detail["message"].lower()


def test_the_conversation_full_copy_keeps_the_plan_limit_shape():
    e = PlanLimitError("messages_per_conversation", "pro")
    assert e.status_code == 403
    assert e.detail == {
        "code": "billing_denied", "reason": "plan_limit", "limit": "messages_per_conversation",
        "message_key": "limit.messages_per_conversation",
        "message": "This conversation has reached its full length. Branch from any message to continue; your context comes with you.",
        "upgrade_to": "pro",
    }
    other = PlanLimitError("branches_per_tree", "pro")
    assert other.detail["message"] == "This tree has all the branches it includes. Start a new tree, or see plans for more."
    assert PlanLimitError("branches_per_tree", None).detail["message"] == "This tree has all the branches it includes. Start a new tree."


def test_the_other_reasons_are_unchanged():
    assert BillingDeniedError("daily_limit").status_code == 429
    assert BillingDeniedError("no_credit").detail["message"] == (
        "Your credits are used up. Top up to keep going; Fast models stay available every day.")
    assert BillingDeniedError("billing_unavailable").status_code == 503


BANNED = ("limit", "limited", "restricted", "locked", "blocked", "denied", "quota", "not allowed", "only", "exceeded",
          "insufficient", "upgrade required")


def test_user_copy_follows_the_wording_rules():
    from app.core.billing_messages import BEST_SWITCHED_MESSAGE, BILLING_DENIALS, PLAN_LIMIT_COPY, UNKNOWN_DENIAL, plan_limit_message
    texts = [m for _, _, m in BILLING_DENIALS.values()] + [UNKNOWN_DENIAL[2], BEST_SWITCHED_MESSAGE]
    texts += [plan_limit_message(k, u) for k in [*PLAN_LIMIT_COPY, "messages_per_conversation", "other"] for u in (True, False)]
    for key, (_, _, m) in BILLING_DENIALS.items():
        if key != "account_blocked":                      # a genuinely blocked account may say so
            texts.append(m)
    for text in texts:
        low = text.lower()
        for word in BANNED:
            if word == "blocked" and "can't send messages right now" in low:
                continue
            assert word not in low.replace("limit.", ""), (word, text)
