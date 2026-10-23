"""Conversation hierarchy resolution (the conversation catalog is the only source of truth).

The artifact registry and the export pipeline both need "the tree this conversation belongs to". It is derived at read
time from the caller's own, non-deleted conversations, so:
  * nothing the caller does not own can ever appear (no cross-user leakage);
  * a deleted/missing parent simply ends the chain (the child is reported as an orphan branch, never dropped);
  * cycles in corrupt data cannot loop (every walk carries a visited set);
  * each conversation appears exactly once.
"""

import asyncio
from dataclasses import dataclass, field
from typing import Optional
from uuid import UUID

from app.models.conversation import Conversation


@dataclass
class Hierarchy:
    nodes: dict[UUID, Conversation]
    children: dict[UUID, list[UUID]] = field(default_factory=dict)

    def root_of(self, conversation_id: UUID) -> tuple[UUID, Optional[UUID]]:
        """(root id, missing parent id). The second item is set when the chain ends because a parent is missing,
        deleted or inaccessible, or when a cycle was cut."""
        seen: set[UUID] = set()
        current = conversation_id
        while True:
            seen.add(current)
            parent = self.nodes[current].parent_conversation_id
            if parent is None:
                return current, None
            if parent not in self.nodes:
                return current, parent
            if parent in seen:                       # cycle: stop at the node whose parent would close it
                return current, parent
            current = parent

    def subtree(self, root_id: UUID) -> list[UUID]:
        """Pre-order (parents before children, siblings oldest first), each id once."""
        order: list[UUID] = []
        seen: set[UUID] = set()
        stack = [root_id]
        while stack:
            cid = stack.pop()
            if cid in seen or cid not in self.nodes:
                continue
            seen.add(cid)
            order.append(cid)
            stack.extend(reversed(self.children.get(cid, [])))
        return order

    def depth_of(self, conversation_id: UUID, root_id: UUID) -> int:
        depth, current, seen = 0, conversation_id, set()
        while current != root_id and current not in seen:
            seen.add(current)
            parent = self.nodes[current].parent_conversation_id
            if parent is None or parent not in self.nodes:
                break
            current, depth = parent, depth + 1
        return depth


def build_hierarchy(conversations: list[Conversation]) -> Hierarchy:
    nodes = {c.conversation_id: c for c in conversations}
    children: dict[UUID, list[UUID]] = {}
    for c in sorted(conversations, key=lambda c: (c.created_at, str(c.conversation_id))):
        parent = c.parent_conversation_id
        if parent is not None and parent in nodes and parent != c.conversation_id:
            children.setdefault(parent, []).append(c.conversation_id)
    return Hierarchy(nodes=nodes, children=children)


class HierarchyService:
    def __init__(self, conversations, max_conversations: int = 2000) -> None:
        self.conversations = conversations            # ConversationService
        self.max = max_conversations

    async def load(self, user_id: UUID) -> Hierarchy:
        items = await asyncio.to_thread(self.conversations.list, user_id, self.max)
        return build_hierarchy([c for c in items if c.user_id == user_id])

    @staticmethod
    def scope_ids(h: Hierarchy, conversation_id: UUID, scope: str) -> list[UUID]:
        """`self` = just this conversation; `hierarchy` = the whole tree it belongs to (root, branches and all
        descendants); `auto` = whole tree for a root, only itself for a branch."""
        if conversation_id not in h.nodes:
            return [conversation_id] if scope == "self" else []
        if scope == "auto":
            scope = "hierarchy" if h.nodes[conversation_id].parent_conversation_id is None else "self"
        if scope == "self":
            return [conversation_id]
        root, _ = h.root_of(conversation_id)
        return h.subtree(root)
