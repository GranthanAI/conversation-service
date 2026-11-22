"""Service-owned plan limits (integration plan 4.8), enforced from EntitlementsService.

  messages_per_conversation  USER messages in the conversation's own turns. A Postgres counter row per conversation,
                             initialised lazily from ONE Cassandra count, then incremented in the send transaction.
  branches_per_tree          branches (conversations with a parent) in the tree; counted from the user's conversation
                             catalog when a branch is created.
  documents_per_tree         distinct document ids attached anywhere in the tree (table tree_documents).
  custom_modes / mode_instruction_chars   see CustomModeService / PreferencesService.

Each refusal is HTTP 403 `plan_limit` (see app/core/billing_messages.py) and is decided BEFORE billing reserves anything.
Nothing is ever deleted or hidden when a limit shrinks: over-limit content stays readable, only creating more is refused.
-1 means unlimited. Modes: off = never checked (today's behaviour); shadow = decided and logged but never refused;
enforce = refused.
"""

import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Iterable, List, Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.billing_messages import PlanLimitError
from app.core.config import settings
from app.core.logging import logger
from app.services.entitlements_service import EntitlementsService, limit_of, upgrade_to
from app.services.hierarchy_service import HierarchyService
from app.telemetry import metrics

MAX_PARENT_WALK = 200
IST = timezone(timedelta(hours=5, minutes=30))     # India has no DST, so a fixed offset is the Asia/Kolkata day


def ist_day(now: Optional[datetime] = None) -> date:
    """The calendar day in IST (the day daily limits reset on)."""
    return (now or datetime.now(timezone.utc)).astimezone(IST).date()


def plan_limits_active() -> bool:
    return settings.BILLING_ENFORCEMENT != "off"


class PlanLimitService:
    def __init__(self, entitlements: EntitlementsService, session: Optional[AsyncSession] = None,
                 message_repo=None, conversations=None):
        self.entitlements = entitlements
        self.session = session
        self.message_repo = message_repo
        self.conversations = conversations           # ConversationService (for the tree walk / branch count)

    # ---- decision ------------------------------------------------------------------------------------------

    def _refuse(self, limit: str, ent, **ctx) -> None:
        """enforce: raise 403 plan_limit. shadow: log only."""
        if settings.BILLING_ENFORCEMENT == "enforce":
            metrics.plan_limit_denied_total.inc(limit=limit)
            logger.info("plan_limit_denied", limit=limit, plan=ent.plan_code, **ctx)
            raise PlanLimitError(limit, upgrade_to(ent))
        logger.info("plan_limit_shadow_decision", limit=limit, decision="deny", plan=ent.plan_code, **ctx)

    async def _limit(self, user_id, name: str):
        ent = await self.entitlements.get(user_id)
        return ent, limit_of(ent, name)

    # ---- custom modes / instruction length ----------------------------------------------------------------------

    async def custom_mode_limit(self, user_id) -> tuple[Optional[int], object]:
        """(max active custom modes for the repository's atomic count, entitlements), or (None, None) when plan limits
        are off or unlimited."""
        if not plan_limits_active():
            return None, None
        ent, limit = await self._limit(user_id, "custom_modes")
        return (None if limit < 0 else limit), ent

    def refuse_custom_modes(self, ent) -> None:
        self._refuse("custom_modes", ent)

    async def check_instruction_chars(self, user_id, instructions: Optional[str]) -> None:
        if not plan_limits_active() or instructions is None:
            return
        ent, limit = await self._limit(user_id, "mode_instruction_chars")
        if limit >= 0 and len(instructions) > limit:
            self._refuse("mode_instruction_chars", ent, length=len(instructions), limit_value=limit)

    # ---- branches ------------------------------------------------------------------------------------------------

    async def check_new_branch(self, user_id, parent_conversation_id) -> None:
        """Called when a conversation is created with a parent. Counts the tree's branches (all non-deleted
        conversations in the tree except the root) and refuses when the plan's limit is already reached."""
        if not plan_limits_active():
            return
        ent, limit = await self._limit(user_id, "branches_per_tree")
        if limit < 0:
            return
        h = await HierarchyService(self.conversations).load(user_id)
        if parent_conversation_id not in h.nodes:
            logger.warning("branch_count_skipped_parent_not_in_catalog", parent=str(parent_conversation_id))
            return
        root, _ = h.root_of(parent_conversation_id)
        branches = len(h.subtree(root)) - 1
        if branches >= limit:
            self._refuse("branches_per_tree", ent, branches=branches, limit_value=limit)

    # ---- think longer (daily) ------------------------------------------------------------------------------------

    async def take_think(self, user_id, now: Optional[datetime] = None) -> bool:
        """Spends one "think longer" answer of today's allowance BEFORE billing reserves anything. True = the request
        may think (a count was taken when the plan is limited); False = the request must run without thinking.

        0 means not in the plan. Otherwise one atomic upsert per IST day: the row is only updated while count < limit,
        so two requests racing for the last answer cannot both get it. enforce: a refusal raises 403 plan_limit.
        shadow: decided and logged, never refused (the request thinks, uncounted against the limit).
        off: never checked. Give the count back with give_back_think when the send fails afterwards."""
        if not plan_limits_active():
            return True
        ent, limit = await self._limit(user_id, "think_longer_per_day")
        if limit < 0:
            return True
        if limit == 0:
            self._refuse("think_longer_per_day", ent, limit_value=0)
            return True                                   # shadow: _refuse only logged
        enforce = settings.BILLING_ENFORCEMENT == "enforce"
        got = (await self.session.execute(
            text("INSERT INTO think_usage_daily (user_id, day, count) VALUES (:u, :d, 1) "
                 "ON CONFLICT (user_id, day) DO UPDATE SET count = think_usage_daily.count + 1 "
                 "WHERE (:g < 0 OR think_usage_daily.count < :g) RETURNING count"),
            {"u": user_id, "d": ist_day(now), "g": limit if enforce else -1})).scalar_one_or_none()
        if got is None:
            self._refuse("think_longer_per_day", ent, used=limit, limit_value=limit)
        return True

    async def check_think(self, user_id, now: Optional[datetime] = None) -> None:
        """Read-only pre-check of the daily think allowance (used before anything is created, e.g. by an edit): refuses
        in enforce mode when the plan has none or today's count is used up. take_think() is the atomic spend."""
        if not plan_limits_active():
            return
        ent, limit = await self._limit(user_id, "think_longer_per_day")
        if limit < 0:
            return
        if limit == 0:
            self._refuse("think_longer_per_day", ent, limit_value=0)
            return
        used = (await self.session.execute(
            text("SELECT count FROM think_usage_daily WHERE user_id = :u AND day = :d"),
            {"u": user_id, "d": ist_day(now)})).scalar_one_or_none() or 0
        if used >= limit:
            self._refuse("think_longer_per_day", ent, used=used, limit_value=limit)

    async def give_back_think(self, user_id, now: Optional[datetime] = None) -> None:
        """Compensation: count - 1, never below 0. Never raises."""
        if not plan_limits_active() or self.session is None:
            return
        try:
            await self.session.execute(
                text("UPDATE think_usage_daily SET count = count - 1 WHERE user_id = :u AND day = :d AND count > 0"),
                {"u": user_id, "d": ist_day(now)})
        except Exception as e:
            logger.warning("think_count_not_returned", error=str(e))

    # ---- message counter + documents (the send) ----------------------------------------------------------------

    async def _root_of(self, conversation_id) -> uuid.UUID:
        """Root of the tree, following parents the way HierarchyService does: a missing/deleted parent ends the chain."""
        current, seen = conversation_id, {conversation_id}
        for _ in range(MAX_PARENT_WALK):
            conv = await self.conversations.get(current)
            parent = getattr(conv, "parent_conversation_id", None) if conv else None
            if not parent or parent in seen:
                return current
            parent_conv = await self.conversations.get(parent)
            if parent_conv is None or getattr(getattr(parent_conv, "status", None), "value", None) == "deleted":
                return current
            seen.add(parent)
            current = parent
        return current

    async def _counter(self, conversation_id) -> int:
        row = (await self.session.execute(
            text("SELECT user_message_count FROM conversation_message_counters WHERE conversation_id = :c"),
            {"c": conversation_id})).scalar_one_or_none()
        if row is not None:
            return row
        initial = self.message_repo.count_user_messages(conversation_id)       # the one Cassandra count
        await self.session.execute(
            text("INSERT INTO conversation_message_counters (conversation_id, user_message_count) VALUES (:c, :n) "
                 "ON CONFLICT (conversation_id) DO NOTHING"), {"c": conversation_id, "n": initial})
        return (await self.session.execute(
            text("SELECT user_message_count FROM conversation_message_counters WHERE conversation_id = :c"),
            {"c": conversation_id})).scalar_one()

    async def _tree_documents(self, root_id) -> set:
        rows = await self.session.execute(text("SELECT document_id FROM tree_documents WHERE root_id = :r"),
                                          {"r": root_id})
        return {r[0] for r in rows}

    async def check_send(self, user_id, conversation_id, file_ids: Optional[Iterable[uuid.UUID]]) -> None:
        """Read-only pre-check for a send (no side effects except the lazy counter initialisation)."""
        if not plan_limits_active():
            return
        ent = await self.entitlements.get(user_id)
        per_conv = limit_of(ent, "messages_per_conversation")
        if per_conv >= 0:
            count = await self._counter(conversation_id)
            if count >= per_conv:
                self._refuse("messages_per_conversation", ent, count=count, limit_value=per_conv)
        docs = set(file_ids or [])
        per_tree = limit_of(ent, "documents_per_tree")
        if docs and per_tree >= 0:
            existing = await self._tree_documents(await self._root_of(conversation_id))
            new = docs - existing
            if new and len(existing) + len(new) > per_tree:
                self._refuse("documents_per_tree", ent, existing=len(existing), new=len(new), limit_value=per_tree)

    async def record_send(self, user_id, conversation_id, file_ids: Optional[Iterable[uuid.UUID]]) -> None:
        """In the send transaction (the request's Postgres session, committed only if the whole request succeeds):
        count the message and register new documents. Both writes re-check their limit atomically, so two sends racing
        for the last slot cannot both get it; the loser is refused here and the caller compensates its reservation."""
        if not plan_limits_active():
            return
        ent = await self.entitlements.get(user_id)
        enforce = settings.BILLING_ENFORCEMENT == "enforce"
        per_conv = limit_of(ent, "messages_per_conversation")
        guard = per_conv if (enforce and per_conv >= 0) else -1
        await self._counter(conversation_id)                       # makes sure the row exists
        updated = (await self.session.execute(
            text("UPDATE conversation_message_counters SET user_message_count = user_message_count + 1, "
                 "updated_at = now() WHERE conversation_id = :c AND (:g < 0 OR user_message_count < :g) "
                 "RETURNING user_message_count"), {"c": conversation_id, "g": guard})).scalar_one_or_none()
        if updated is None:
            self._refuse("messages_per_conversation", ent, race=True)

        docs = sorted(set(file_ids or []))
        if not docs:
            return
        root = await self._root_of(conversation_id)
        per_tree = limit_of(ent, "documents_per_tree")
        await self.session.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:k, 0))"),
                                   {"k": f"tree_documents:{root}"})
        existing = await self._tree_documents(root)
        new = [d for d in docs if d not in existing]
        if enforce and new and per_tree >= 0 and len(existing) + len(new) > per_tree:
            self._refuse("documents_per_tree", ent, race=True)
        for d in new:
            await self.session.execute(
                text("INSERT INTO tree_documents (root_id, document_id) VALUES (:r, :d) ON CONFLICT DO NOTHING"),
                {"r": root, "d": d})
