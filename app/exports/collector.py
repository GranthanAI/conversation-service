"""Collects an export's source data: the authorised conversation tree, its persisted messages and the registry artifacts
its messages refer to. Nothing here calls an LLM or renders anything; it reads exactly what is stored."""

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional
from uuid import UUID

from app.artifacts.fingerprint import ArtifactSpecError, canonical_manim_spec
from app.exports.blocks import find_blocks
from app.models.artifact_models import ArtifactRecord
from app.models.conversation import Conversation
from app.models.message import Message, MessageStatus
from app.services.hierarchy_service import Hierarchy

PAGE = 100


class SourceMissing(LookupError):
    """The selected conversation no longer exists (deleted) or is not the caller's."""


@dataclass
class ExportConversation:
    conversation: Conversation
    depth: int
    messages: list[Message]
    parent_id: Optional[UUID]
    missing_parent_id: Optional[UUID] = None       # set when the parent chain ends in a deleted/inaccessible conversation
    branch_message_id: Optional[UUID] = None
    children: list[UUID] = field(default_factory=list)


@dataclass
class ExportBundle:
    selected_id: UUID
    root_id: UUID
    conversations: list[ExportConversation]        # tree order, each exactly once
    artifacts: dict[str, ArtifactRecord]           # canonical manim spec -> registry artifact (only those that exist)
    generated_at: datetime

    def artifact_for_spec(self, spec) -> Optional[ArtifactRecord]:
        """The registry artifact for a Manim spec found in a message (same canonicalisation as registration)."""
        try:
            return self.artifacts.get(canonical_manim_spec(spec, 20000))
        except ArtifactSpecError:
            return None


def _latest_marker(messages_desc: list[Message]) -> str:
    return f"{messages_desc[0].message_id}:{messages_desc[0].status.value}" if messages_desc else ""


class ExportCollector:
    def __init__(self, conversations, message_repo, artifacts, max_messages: int) -> None:
        self.conversations = conversations          # ConversationService
        self.messages = message_repo
        self.artifacts = artifacts                  # ArtifactService
        self.max_messages = max_messages

    async def snapshot(self, ids: list[UUID]) -> dict[str, str]:
        """{conversation_id: "<latest message id>:<status>"}: identifies the exact source an export was made from."""
        async def one(cid: UUID) -> tuple[str, str]:
            latest = await asyncio.to_thread(self.messages.history, cid, 1)
            return str(cid), _latest_marker(latest)
        return dict(await asyncio.gather(*[one(c) for c in ids]))

    async def _messages(self, cid: UUID, snapshot_marker: str) -> list[Message]:
        """Chronological (oldest first), sent messages only, never beyond the snapshot's latest message."""
        collected: list[Message] = []
        cursor: Optional[UUID] = None
        while len(collected) < self.max_messages:
            page = await asyncio.to_thread(self.messages.history, cid, PAGE, cursor)
            if not page:
                break
            collected.extend(page)
            cursor = page[-1].message_id          # a short page does not mean the end: soft-deleted rows are filtered
        collected.reverse()                                       # storage order is newest first
        limit_id = snapshot_marker.split(":")[0] if snapshot_marker else ""
        if limit_id:
            ids = [str(m.message_id) for m in collected]
            if limit_id in ids:
                collected = collected[: ids.index(limit_id) + 1]
        seen: set[UUID] = set()
        out = []
        for m in collected:
            if m.status != MessageStatus.SENT or not (m.content or "").strip() or m.message_id in seen:
                continue
            seen.add(m.message_id)
            out.append(m)
        return out

    async def collect(self, owner: UUID, hierarchy: Hierarchy, selected_id: UUID, ids: list[UUID],
                      snapshot: dict[str, str], renderer_version: str, quality: str, progress=None) -> ExportBundle:
        if selected_id not in hierarchy.nodes:
            raise SourceMissing(str(selected_id))
        root_id, missing_parent = hierarchy.root_of(selected_id)
        in_scope = [c for c in ids if c in hierarchy.nodes]       # deleted since the snapshot: simply absent
        convs: list[ExportConversation] = []
        wanted: set[str] = set()                                  # canonical manim specs referenced by messages
        for cid in in_scope:
            node = hierarchy.nodes[cid]
            full = await self.conversations.get(cid)              # carries branch_message_id (the list rows do not)
            if full is not None and full.user_id != owner:
                continue                                          # never export what the caller does not own
            msgs = await self._messages(cid, snapshot.get(str(cid), ""))
            parent = node.parent_conversation_id
            missing = None
            if parent is not None and parent not in hierarchy.nodes:
                missing = parent
            if progress is not None:
                await progress(len(convs) + 1, len(in_scope))
            convs.append(ExportConversation(
                conversation=full or node, depth=hierarchy.depth_of(cid, root_id), messages=msgs,
                parent_id=parent if parent in hierarchy.nodes else None, missing_parent_id=missing,
                branch_message_id=getattr(full, "branch_message_id", None),
                children=[c for c in hierarchy.children.get(cid, []) if c in in_scope]))
            for m in msgs:
                for block in find_blocks(m.content):
                    if block.kind != "manim":
                        continue
                    spec = block.manim_spec()
                    if spec is None:
                        continue
                    try:
                        canonical = canonical_manim_spec(spec, 20000)
                    except ArtifactSpecError:
                        continue
                    wanted.add(canonical)
        found: dict[str, ArtifactRecord] = {}
        for canonical in sorted(wanted):                          # lookups only: an export never starts a render
            row = await self.artifacts.lookup(owner, "manim", canonical, renderer_version, quality)
            if row is not None:
                found[canonical] = row
        return ExportBundle(selected_id=selected_id, root_id=root_id, conversations=convs, artifacts=found,
                            generated_at=datetime.now(timezone.utc))
