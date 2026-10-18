"""
Branch context: what a child conversation inherits from its ancestors.

Authoritative rules (every client gets the same ones, enforced here, not in the UI):

* A branch records WHERE it was taken: `branch_message_id`, the last message of the parent that
  was in scope. A child sees its parent only up to and including that message; anything the parent
  says later never leaks into the child. Branches made before this field existed fall back to the
  child's creation time as the boundary.
* Context is transitive. For A -> B -> C, C inherits B up to C's branch point, and A up to B's branch
  point, in chronological order (A's part, then B's part, then C's own messages).
* The chain stops (fail closed, never guessed) at an ancestor that is missing, deleted, owned by
  another user, repeats (cycle), or is deeper than MAX_LINEAGE_DEPTH. `truncated` tells which.
* Siblings are independent: only the direct ancestor chain is ever read.
* The history sent to the model is bounded and deterministic: newest-first from the nearest
  ancestor, then re-ordered oldest-first.
* Documents attached anywhere in the inherited part are inherited too, each with the conversation
  it came from. Authorization and readiness are enforced downstream by File Service / Retrieval.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Tuple
from uuid import UUID

from app.core.logging import logger
from app.models.conversation import Conversation, ConversationStatus
from app.models.message import Message

MAX_LINEAGE_DEPTH = 25
INHERITED_MAX_MESSAGES = 16
INHERITED_MAX_MESSAGE_CHARS = 2000
INHERITED_MAX_CHARS = 12000
SCAN_PER_SEGMENT = 200
MAX_INHERITED_DOCUMENTS = 50


@dataclass
class Segment:
    """The part of one ancestor that a descendant inherits."""
    conversation_id: UUID
    title: str
    upto_message_id: Optional[UUID] = None   # inclusive boundary (None with upto_time None = whole conversation)
    upto_time: Optional[datetime] = None     # legacy boundary when the child has no branch_message_id


@dataclass
class Lineage:
    segments: List[Segment] = field(default_factory=list)   # root first, direct parent last
    truncated: Optional[str] = None                          # why the chain stopped early, if it did


def _usable(m: Message) -> bool:
    status = getattr(m.status, "value", m.status)
    return m.sender in ("user", "assistant") and status == "sent" and bool((m.content or "").strip())


class BranchContextService:
    def __init__(self, conversations, message_repo):
        self.conversations = conversations     # ConversationService (read-through cache)
        self.messages = message_repo           # CassandraMessageRepository

    async def lineage(self, conv: Conversation) -> Lineage:
        """Ancestor chain of `conv` (not including `conv`), root first, with each branch boundary."""
        chain: List[Segment] = []
        seen = {conv.conversation_id}
        cur = conv
        truncated: Optional[str] = None
        while cur.parent_conversation_id:
            if len(chain) >= MAX_LINEAGE_DEPTH:
                truncated = "max_depth"
                break
            pid = cur.parent_conversation_id
            if pid in seen:
                truncated = "cycle"
                break
            parent = await self.conversations.get(pid)
            if parent is None:
                truncated = "missing_ancestor"
                break
            if parent.user_id != conv.user_id:
                truncated = "inaccessible_ancestor"
                break
            if parent.status == ConversationStatus.DELETED:
                truncated = "deleted_ancestor"
                break
            seen.add(pid)
            boundary = getattr(cur, "branch_message_id", None)
            chain.append(Segment(
                conversation_id=pid,
                title=parent.title,
                upto_message_id=boundary,
                upto_time=None if boundary else cur.created_at,
            ))
            cur = parent
        chain.reverse()
        return Lineage(segments=chain, truncated=truncated)

    def _segment_messages(self, seg: Segment, limit: int) -> List[Message]:
        """Newest-first messages of the segment within its boundary, unusable rows skipped."""
        if seg.upto_message_id is not None:
            rows = self.messages.history_upto(seg.conversation_id, seg.upto_message_id, limit)
        else:
            rows = self.messages.history(seg.conversation_id, limit)
            if seg.upto_time is not None:
                rows = [m for m in rows if m.created_at <= seg.upto_time]
        return [m for m in rows if _usable(m)]

    @staticmethod
    def ancestor_refs(lineage: Lineage) -> List[dict]:
        """Branch lineage for downstream services (root first): each ancestor with the last message of it
        that is in scope at the branch point. Ancestors without a recorded branch message (legacy branches)
        are omitted, since downstream consumers could not prove what the branch may see."""
        return [
            {"conversation_id": str(s.conversation_id), "upto_message_id": str(s.upto_message_id)}
            for s in lineage.segments if s.upto_message_id is not None
        ]

    async def event_context(self, conv: Conversation) -> Tuple[List[dict], List[dict]]:
        """(inherited history, ancestor refs) for the request event, resolving the lineage once."""
        try:
            lineage = await self.lineage(conv)
        except Exception as e:
            logger.warning("Could not resolve lineage", conversation_id=str(conv.conversation_id), error=str(e))
            return [], []
        return await self.inherited_history(conv, lineage), self.ancestor_refs(lineage)

    async def inherited_history(self, conv: Conversation, lineage: Optional[Lineage] = None) -> List[dict]:
        """Ancestor messages for the model, oldest-first: [{role, content, conversation_id, message_id}]."""
        try:
            lineage = lineage or await self.lineage(conv)
            picked: List[Message] = []
            seen: set = set()
            chars = INHERITED_MAX_CHARS
            full = False
            for seg in reversed(lineage.segments):                 # nearest ancestor first
                for m in self._segment_messages(seg, INHERITED_MAX_MESSAGES * 2):
                    if m.message_id in seen:
                        continue
                    text = m.content.strip()[:INHERITED_MAX_MESSAGE_CHARS]
                    if len(picked) >= INHERITED_MAX_MESSAGES or (picked and chars - len(text) < 0):
                        full = True
                        break
                    seen.add(m.message_id)
                    chars -= len(text)
                    picked.append(m)
                if full:
                    break
            picked.reverse()                                       # chronological: A's part, then B's part
            return [{
                "role": m.sender,
                "content": m.content.strip()[:INHERITED_MAX_MESSAGE_CHARS],
                "conversation_id": str(m.conversation_id),
                "message_id": str(m.message_id),
            } for m in picked]
        except Exception as e:  # best effort: a lineage read problem must never block sending
            logger.warning("Could not resolve inherited history", conversation_id=str(conv.conversation_id), error=str(e))
            return []

    async def context(self, conv: Conversation) -> dict:
        """Lineage + inherited documents for the client (sidebar / document selector)."""
        lineage = await self.lineage(conv)
        documents: List[dict] = []
        seen: set = set()
        titles = {s.conversation_id: s.title for s in lineage.segments}
        for seg in lineage.segments:                               # root first = stable order
            for m in reversed(self._segment_messages(seg, SCAN_PER_SEGMENT)):
                if m.sender != "user":
                    continue
                for fid in m.file_ids or []:
                    if fid in seen or len(documents) >= MAX_INHERITED_DOCUMENTS:
                        continue
                    seen.add(fid)
                    documents.append({
                        "document_id": str(fid),
                        "source_conversation_id": str(seg.conversation_id),
                        "source_title": titles.get(seg.conversation_id, ""),
                        "source_message_id": str(m.message_id),
                    })
        return {
            "conversation_id": str(conv.conversation_id),
            "branch_message_id": str(conv.branch_message_id) if getattr(conv, "branch_message_id", None) else None,
            "lineage": [{
                "conversation_id": str(s.conversation_id),
                "title": s.title,
                "upto_message_id": str(s.upto_message_id) if s.upto_message_id else None,
            } for s in lineage.segments],
            "truncated": lineage.truncated,
            "documents": documents,
        }
