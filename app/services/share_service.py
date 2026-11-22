"""Share links (Q3): a PUBLIC, read-only SNAPSHOT of one conversation or a whole tree.

* The snapshot is built from what is stored (ExportCollector) and written gzipped to the object store under
  shares/<share_id>.json.gz. It holds fresh per-snapshot ids, never the real conversation, user, file or document ids, and
  leaves out notes, source edits, emails and artifact media.
* The token is 32 random bytes (base64url, 43 characters) shown ONCE; only sha256(token) is stored. A link can be refreshed
  (the snapshot is rebuilt under the same token) or revoked (the object is deleted). An unknown and a revoked token answer
  the same 404.
* The public read is rate limited per client IP in Redis.
"""

import asyncio
import base64
import gzip
import hashlib
import json
import re
import secrets
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import settings
from app.core.logging import logger
from app.exports.collector import ExportCollector, SourceMissing
from app.services.hierarchy_service import HierarchyService
from app.telemetry import metrics

TOKEN_RE = re.compile(r"^[A-Za-z0-9_-]{43}$")
SNAPSHOT_VERSION = 1
SCOPES = ("conversation", "hierarchy")
SENDERS = ("user", "assistant")


class ShareRefused(Exception):
    """409 `code` (temporary_conversation | share_limit) or 413 share_too_large."""

    def __init__(self, code: str, status: int = 409):
        self.code, self.status = code, status
        super().__init__(code)


class ShareNotFound(LookupError):
    pass


class RateLimited(Exception):
    def __init__(self, retry_after: int):
        self.retry_after = retry_after


class _NoArtifacts:
    """A share never carries artifact media; the collector only asks for lookups of Manim blocks."""

    async def lookup(self, *_a, **_k):
        return None


def new_token() -> str:
    return base64.urlsafe_b64encode(secrets.token_bytes(32)).rstrip(b"=").decode("ascii")


def hash_token(token: str) -> bytes:
    return hashlib.sha256(token.encode("ascii")).digest()


def object_key(share_id: UUID) -> str:
    return f"shares/{share_id}.json.gz"


def _citation(c: dict) -> dict:
    """Title and page / time only: no document or file ids."""
    out = {"title": c.get("source_name") or ""}
    for src, dst in (("page_number", "page"), ("start_seconds", "start_seconds"), ("end_seconds", "end_seconds")):
        if c.get(src) is not None:
            out[dst] = c[src]
    return out


def _message(m) -> dict:
    return {
        "sender": m.sender, "content": m.content, "created_at": m.created_at.isoformat(),
        "citations": [_citation(c) for c in (m.citations or [])], "model": m.model,
    }


class ShareService:
    def __init__(self, session: AsyncSession, conversations, message_repo, store, branch_context=None, redis=None,
                 session_factory: Optional[async_sessionmaker[AsyncSession]] = None):
        self.session = session
        self.conversations = conversations
        self.messages = message_repo
        self.store = store
        self.branch_context = branch_context
        self.redis = redis
        self.session_factory = session_factory
        self.hierarchy = HierarchyService(conversations, settings.EXPORT_MAX_CONVERSATIONS * 4)
        self.collector = ExportCollector(conversations, message_repo, _NoArtifacts(),
                                         settings.EXPORT_MAX_MESSAGES_PER_CONVERSATION)

    # ---- snapshot ---------------------------------------------------------------------------------------------------

    async def build_snapshot(self, user_id: UUID, conv, scope: str) -> tuple[dict, int]:
        """(snapshot JSON, message count). Raises ShareRefused(share_too_large) over the export message cap."""
        h = await self.hierarchy.load(user_id)
        if conv.conversation_id not in h.nodes:
            raise SourceMissing(str(conv.conversation_id))
        ids = [conv.conversation_id] if scope == "conversation" else h.subtree(h.root_of(conv.conversation_id)[0])
        marker = await self.collector.snapshot(ids)
        bundle = await self.collector.collect(user_id, h, conv.conversation_id, ids, marker, "", "")
        cap = settings.EXPORT_MAX_MESSAGES_PER_CONVERSATION
        fresh = {c.conversation.conversation_id: str(uuid.uuid4()) for c in bundle.conversations}
        conversations: list[dict] = []
        total = 0

        if scope == "conversation" and self.branch_context is not None:       # the inherited part, read-only context
            lineage = await self.branch_context.lineage(conv)
            parent_snap: Optional[str] = None
            for seg in lineage.segments:
                rows = [m for m in reversed(self.branch_context._segment_messages(seg, cap)) if m.sender in SENDERS]
                sid = str(uuid.uuid4())
                conversations.append({"id": sid, "parent": parent_snap, "title": seg.title, "inherited": True,
                                      "messages": [_message(m) for m in rows]})
                parent_snap = sid
            if conversations:
                fresh[conv.conversation_id] = fresh.get(conv.conversation_id) or str(uuid.uuid4())
            inherited_parent = parent_snap
        else:
            inherited_parent = None

        for c in bundle.conversations:
            rows = [m for m in c.messages if m.sender in SENDERS]
            total += len(rows)
            if len(rows) >= cap or total > cap:
                raise ShareRefused("share_too_large", 413)
            conversations.append({
                "id": fresh[c.conversation.conversation_id],
                "parent": fresh.get(c.parent_id) if c.parent_id in fresh else inherited_parent,
                "title": c.conversation.title, "inherited": False,
                "messages": [_message(m) for m in rows],
            })
        snapshot = {"schema_version": SNAPSHOT_VERSION, "title": conv.title, "scope": scope,
                    "created_at": datetime.now(timezone.utc).isoformat(), "conversations": conversations}
        return snapshot, total

    async def _store(self, share_id: UUID, snapshot: dict) -> None:
        data = gzip.compress(json.dumps(snapshot, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "snapshot.json.gz"
            path.write_bytes(data)
            await self.store.put_file(object_key(share_id), path, "application/gzip")

    # ---- owner operations --------------------------------------------------------------------------------------------

    async def create(self, user_id: UUID, conv, scope: str) -> dict:
        if conv.temporary:
            raise ShareRefused("temporary_conversation")
        active = (await self.session.execute(
            text("SELECT count(*) FROM shared_links WHERE user_id = :u AND revoked_at IS NULL"), {"u": user_id})).scalar_one()
        if active >= settings.SHARE_MAX_ACTIVE_PER_USER:
            raise ShareRefused("share_limit")
        snapshot, count = await self.build_snapshot(user_id, conv, scope)
        share_id, token = uuid.uuid4(), new_token()
        await self._store(share_id, snapshot)
        try:
            row = (await self.session.execute(
                text("INSERT INTO shared_links (share_id, user_id, conversation_id, scope, token_hash, title, object_key, "
                     "message_count) VALUES (:s, :u, :c, :scope, :h, :t, :k, :n) RETURNING created_at"),
                {"s": share_id, "u": user_id, "c": conv.conversation_id, "scope": scope, "h": hash_token(token),
                 "t": conv.title, "k": object_key(share_id), "n": count})).one()
        except Exception:
            await self._delete_object(share_id)
            raise
        return {"share_id": share_id, "url_token": token, "scope": scope, "title": conv.title,
                "message_count": count, "created_at": row[0]}

    async def list(self, user_id: UUID) -> list[dict]:
        rows = (await self.session.execute(
            text("SELECT share_id, conversation_id, scope, title, message_count, created_at, refreshed_at, view_count "
                 "FROM shared_links WHERE user_id = :u AND revoked_at IS NULL ORDER BY created_at DESC"),
            {"u": user_id})).all()
        return [dict(zip(("share_id", "conversation_id", "scope", "title", "message_count", "created_at",
                          "refreshed_at", "view_count"), r)) for r in rows]

    async def _owned(self, user_id: UUID, share_id: UUID):
        row = (await self.session.execute(
            text("SELECT conversation_id, scope FROM shared_links "
                 "WHERE share_id = :s AND user_id = :u AND revoked_at IS NULL"), {"s": share_id, "u": user_id})).one_or_none()
        if row is None:
            raise ShareNotFound(str(share_id))
        return row

    async def refresh(self, user_id: UUID, share_id: UUID) -> dict:
        conversation_id, scope = await self._owned(user_id, share_id)
        conv = await self.conversations.get(conversation_id)
        if conv is None or conv.user_id != user_id or getattr(conv.status, "value", conv.status) == "deleted":
            raise ShareNotFound(str(share_id))
        snapshot, count = await self.build_snapshot(user_id, conv, scope)
        await self._store(share_id, snapshot)
        row = (await self.session.execute(
            text("UPDATE shared_links SET refreshed_at = now(), message_count = :n, title = :t "
                 "WHERE share_id = :s RETURNING created_at, refreshed_at"), {"n": count, "t": conv.title, "s": share_id})).one()
        return {"share_id": share_id, "scope": scope, "title": conv.title, "message_count": count,
                "created_at": row[0], "refreshed_at": row[1]}

    async def revoke(self, user_id: UUID, share_id: UUID) -> None:
        """204 whether or not the link was active (a second DELETE changes nothing)."""
        done = (await self.session.execute(
            text("UPDATE shared_links SET revoked_at = now() WHERE share_id = :s AND user_id = :u AND revoked_at IS NULL "
                 "RETURNING share_id"), {"s": share_id, "u": user_id})).scalar_one_or_none()
        if done is not None:
            await self._delete_object(share_id)

    async def _delete_object(self, share_id: UUID) -> None:
        try:
            await self.store.delete(object_key(share_id))
        except Exception as e:  # noqa: BLE001 - the row is already revoked; a leftover object is unreachable
            logger.warning("share_object_delete_failed", share_id=str(share_id), error=str(e))

    # ---- public read -----------------------------------------------------------------------------------------------

    async def rate_check(self, client_ip: str) -> None:
        """60 requests per minute per client IP (Redis). Redis down = allowed (a read-only public page should not fail)."""
        if self.redis is None:
            return
        minute = int(datetime.now(timezone.utc).timestamp() // 60)
        key = f"share_rl:{client_ip}:{minute}"
        try:
            n = await self.redis.incr(key)
            if n == 1:
                await self.redis.expire(key, 70)
        except Exception as e:  # noqa: BLE001
            logger.warning("share_rate_limit_unavailable", error=str(e))
            return
        if n > settings.SHARE_PUBLIC_RATE_LIMIT_PER_MINUTE:
            metrics.share_views_total.inc(outcome="rate_limited")
            raise RateLimited(60 - int(datetime.now(timezone.utc).timestamp() % 60))

    async def read_public(self, token: str) -> bytes:
        """The snapshot JSON (plain, decompressed). Unknown, malformed and revoked tokens are all ShareNotFound."""
        if not TOKEN_RE.match(token or ""):
            metrics.share_views_total.inc(outcome="not_found")
            raise ShareNotFound("token")
        row = (await self.session.execute(
            text("SELECT share_id FROM shared_links WHERE token_hash = :h AND revoked_at IS NULL"),
            {"h": hash_token(token)})).one_or_none()
        if row is None:
            metrics.share_views_total.inc(outcome="not_found")
            raise ShareNotFound("token")
        share_id = row[0]
        chunks = []
        try:
            async for part in self.store.stream(object_key(share_id)):
                chunks.append(part)
        except Exception as e:  # noqa: BLE001 - an object that cannot be read is as good as gone
            logger.warning("share_object_unreadable", share_id=str(share_id), error=str(e))
            metrics.share_views_total.inc(outcome="not_found")
            raise ShareNotFound("object") from e
        if not chunks:
            metrics.share_views_total.inc(outcome="not_found")
            raise ShareNotFound("object")
        metrics.share_views_total.inc(outcome="ok")
        self._count_view(share_id)
        return gzip.decompress(b"".join(chunks))

    def _count_view(self, share_id: UUID) -> None:
        """view_count + 1 in the background: never blocks, never fails, the response."""
        if self.session_factory is None:
            return

        async def bump() -> None:
            try:
                async with self.session_factory() as db:
                    await db.execute(text("UPDATE shared_links SET view_count = view_count + 1 WHERE share_id = :s"),
                                     {"s": share_id})
                    await db.commit()
            except Exception as e:  # noqa: BLE001
                logger.warning("share_view_count_failed", error=str(e))

        asyncio.get_running_loop().create_task(bump())


async def revoke_for_conversation(factory: async_sessionmaker[AsyncSession], store, conversation_id: UUID) -> int:
    """A deleted conversation (or tree root) stops being shared: revoke every active link on it and delete the snapshots.
    Called from the conversation.deleted cleanup. Returns how many links were revoked."""
    async with factory() as db:
        ids = [s for (s,) in (await db.execute(
            text("UPDATE shared_links SET revoked_at = now() WHERE conversation_id = :c AND revoked_at IS NULL "
                 "RETURNING share_id"), {"c": conversation_id}))]
        await db.commit()
    for share_id in ids:
        try:
            await store.delete(object_key(share_id))
        except Exception as e:  # noqa: BLE001
            logger.warning("share_object_delete_failed", share_id=str(share_id), error=str(e))
    return len(ids)
