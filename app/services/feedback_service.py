"""Thumbs up / down on assistant answers (Q2).

One row per (user, message). The caller's own rating is attached to history pages with ONE query (message_id = ANY(:ids)),
never per message. Staff (roles `support` / `admin`) read the thumbs-down answers through `admin_list`; a user's feedback is
the consent for staff to see that one question and answer, and nothing else about the user is returned (no email, no user id).
"""

from datetime import date, datetime, timedelta, timezone
from typing import Iterable, Optional
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.telemetry import metrics

REASONS = ("inaccurate", "not_helpful", "wrong_level", "too_long", "too_short", "formatting", "unsafe", "other")
RATINGS = {"up": 1, "down": -1}
RATING_NAMES = {1: "up", -1: "down"}
IST = timezone(timedelta(hours=5, minutes=30))
ANSWER_EXCERPT_CHARS = 1500
QUESTION_EXCERPT_CHARS = 500


class FeedbackNotAllowed(Exception):
    """The message is not a sent assistant message (HTTP 409 not_assistant_message)."""


class FeedbackService:
    def __init__(self, session: AsyncSession, message_repo=None):
        self.session = session
        self.messages = message_repo

    async def put(self, user_id: UUID, conversation_id: UUID, message_id: UUID, rating: str,
                  reason: Optional[str] = None, comment: Optional[str] = None) -> dict:
        msg = self.messages.get_message(conversation_id, message_id)
        if msg is None or msg.sender != "assistant" or getattr(msg.status, "value", msg.status) != "sent":
            raise FeedbackNotAllowed()
        if rating != "down":                      # reason and comment only make sense for a thumbs down
            reason, comment = None, None
        await self.session.execute(
            text("INSERT INTO message_feedback (user_id, message_id, conversation_id, rating, reason, comment, model, "
                 "model_group) VALUES (:u, :m, :c, :r, :why, :cm, :model, :grp) "
                 "ON CONFLICT (user_id, message_id) DO UPDATE SET rating = EXCLUDED.rating, reason = EXCLUDED.reason, "
                 "comment = EXCLUDED.comment, model = EXCLUDED.model, model_group = EXCLUDED.model_group, "
                 "updated_at = now()"),
            {"u": user_id, "m": message_id, "c": conversation_id, "r": RATINGS[rating], "why": reason, "cm": comment,
             "model": msg.model, "grp": msg.model_group})
        metrics.message_feedback_total.inc(rating=rating, reason=reason or "none")
        return {"rating": rating, "reason": reason}

    async def delete(self, user_id: UUID, message_id: UUID) -> None:
        await self.session.execute(text("DELETE FROM message_feedback WHERE user_id = :u AND message_id = :m"),
                                   {"u": user_id, "m": message_id})

    async def for_messages(self, user_id: UUID, message_ids: Iterable[UUID]) -> dict[UUID, dict]:
        """The caller's own ratings for a history page: ONE query for all messages."""
        ids = list(message_ids)
        if not ids:
            return {}
        rows = await self.session.execute(
            text("SELECT message_id, rating, reason FROM message_feedback WHERE user_id = :u AND message_id = ANY(:ids)"),
            {"u": user_id, "ids": ids})
        return {m: {"rating": RATING_NAMES[r], "reason": why} for m, r, why in rows}

    async def attach(self, user_id: UUID, messages: list) -> None:
        """Sets `.feedback` on the assistant messages of a history page (one query)."""
        found = await self.for_messages(user_id, [m.message_id for m in messages if m.sender == "assistant"])
        for m in messages:
            m.feedback = found.get(m.message_id)

    # ---- staff review ------------------------------------------------------------------------------------------

    async def admin_list(self, *, rating: str = "down", date_from: Optional[date] = None, date_to: Optional[date] = None,
                         model_group: Optional[str] = None, reason: Optional[str] = None,
                         cursor: Optional[tuple[datetime, UUID]] = None, limit: int = 50) -> tuple[list[dict], bool]:
        """(items, has_more), newest first. Dates are IST calendar days (inclusive). No user identity is returned."""
        where, params = ["rating = :rating"], {"rating": RATINGS[rating], "lim": limit + 1}
        if date_from:
            where.append("created_at >= :dfrom")
            params["dfrom"] = datetime.combine(date_from, datetime.min.time(), tzinfo=IST)
        if date_to:
            where.append("created_at < :dto")
            params["dto"] = datetime.combine(date_to + timedelta(days=1), datetime.min.time(), tzinfo=IST)
        if model_group:
            where.append("model_group = :grp")
            params["grp"] = model_group
        if reason:
            where.append("reason = :reason")
            params["reason"] = reason
        if cursor:
            where.append("(created_at, message_id) < (:cat, :cmid)")
            params["cat"], params["cmid"] = cursor
        rows = (await self.session.execute(
            text("SELECT created_at, rating, reason, comment, model, model_group, conversation_id, message_id "
                 "FROM message_feedback WHERE " + " AND ".join(where) +
                 " ORDER BY created_at DESC, message_id DESC LIMIT :lim"), params)).all()
        has_more = len(rows) > limit
        items = []
        for created_at, rt, why, comment, model, grp, cid, mid in rows[:limit]:
            answer, question = self._excerpts(cid, mid)
            items.append({
                "created_at": created_at, "rating": RATING_NAMES[rt], "reason": why, "comment": comment,
                "model": model, "model_group": grp, "conversation_id": cid, "message_id": mid,
                "answer": answer, "question": question,
            })
        return items, has_more

    def _excerpts(self, conversation_id: UUID, message_id: UUID) -> tuple[Optional[str], Optional[str]]:
        """(answer excerpt, the user message just before it); None when the conversation was deleted since."""
        try:
            answer = self.messages.get_message(conversation_id, message_id)
            if answer is None:
                return None, None
            question = None
            for m in self.messages.history(conversation_id, 6, message_id):      # newest first, older than the answer
                if m.sender == "user":
                    question = (m.content or "")[:QUESTION_EXCERPT_CHARS]
                    break
            return (answer.content or "")[:ANSWER_EXCERPT_CHARS], question
        except Exception:  # noqa: BLE001 - an unreadable row must not fail the whole page
            return None, None
