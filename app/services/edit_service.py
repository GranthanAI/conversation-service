"""Edit a sent message (Q1): the edited question becomes a NEW sibling branch, so the original and its answer stay in the tree.

Where the branch goes (C = the conversation holding the message, mid = the edited message, P = the message of C just
before mid; history is clustered newest first, so P is the next older row):

  1. P exists                       parent = C, branch point = P      (the edit continues from the turn before it)
  2. no P, C is a branch            parent = C's parent, branch point = C's own branch point (a sibling of C)
  3. no P, C is a root              a new root conversation (no parent)

The order leaves nothing behind when something is refused: every check that send() would run is answered BEFORE the
conversation is created (plan limits, the branch limit, the think limit), then the conversation, the message_edits row and
send() (which reserves with billing and writes the outbox). If send() raises, the new conversation is soft-deleted (that emits
conversation.deleted) and the edits row removed, and the ORIGINAL error is re-raised unchanged.
"""

from typing import Optional
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import logger
from app.models.conversation import Conversation
from app.models.message import Message
from app.telemetry import metrics

PAGE_BEFORE = 8


class EditRefused(Exception):
    """A 409 problem: `code` is one of not_user_message, message_not_sent, temporary_conversation, conversation_busy."""

    MESSAGES = {
        "not_user_message": "Only your own messages can be edited.",
        "message_not_sent": "That message was not sent, so it cannot be edited.",
        "temporary_conversation": "Temporary chats can't be edited. Start a new message instead.",
        "conversation_busy": "Wait for the current answer to finish.",
    }

    def __init__(self, code: str):
        self.code = code
        super().__init__(self.MESSAGES[code])


class MessageNotFound(LookupError):
    pass


class EditService:
    def __init__(self, session: AsyncSession, conversations, messages, message_service, plan_limits=None):
        self.session = session
        self.conversations = conversations          # ConversationService
        self.messages = messages                    # CassandraMessageRepository
        self.message_service = message_service
        self.plan_limits = plan_limits

    # ---- helpers ----------------------------------------------------------------------------------------------

    def _previous(self, conversation_id: UUID, message_id: UUID) -> Optional[Message]:
        """P: the message right before `message_id` (any sender), skipping deleted rows."""
        older = self.messages.history(conversation_id, PAGE_BEFORE, message_id)
        return older[0] if older else None

    def _busy(self, conversation_id: UUID) -> bool:
        """An answer in this conversation is still pending (its placeholder row has not been finished yet)."""
        recent = self.messages.history(conversation_id, PAGE_BEFORE)
        return any(m.sender == "assistant" and getattr(m.status, "value", m.status) == "pending" for m in recent)

    # ---- edit ---------------------------------------------------------------------------------------------------

    async def edit(self, user_id: UUID, conv: Conversation, message_id: UUID, *, content: str, mode=None,
                   selected_tools=None, include_diagrams: bool = False, file_ids=None, model=None,
                   think: bool = False) -> tuple[Conversation, Message]:
        original = self.messages.get_message(conv.conversation_id, message_id)
        if original is None:
            raise MessageNotFound(str(message_id))
        if original.sender != "user":
            raise EditRefused("not_user_message")
        if getattr(original.status, "value", original.status) != "sent":
            raise EditRefused("message_not_sent")
        if conv.temporary:
            raise EditRefused("temporary_conversation")
        if self._busy(conv.conversation_id):
            raise EditRefused("conversation_busy")

        previous = self._previous(conv.conversation_id, message_id)
        if previous is not None:
            case, parent, branch_point = "after_previous", conv.conversation_id, previous.message_id
        elif conv.parent_conversation_id is not None:
            case, parent, branch_point = "sibling", conv.parent_conversation_id, conv.branch_message_id
        else:
            case, parent, branch_point = "new_root", None, None

        # a. every check send() would run, before anything is created
        if self.plan_limits is not None:
            if parent is not None:
                await self.plan_limits.check_new_branch(user_id, parent)       # 403 plan_limit branches_per_tree
            if think:
                await self.plan_limits.check_think(user_id)                    # 403 plan_limit think_longer_per_day

        # b. the new conversation (create() validates the parent again and emits conversation.created)
        keep_files = list(original.file_ids) if file_ids is None else list(file_ids)
        new_conv = await self.conversations.create(
            user_id=user_id, title=conv.title, parent_conversation_id=parent, branch_message_id=branch_point,
            message_repo=self.messages if (parent is None or branch_point is not None) else None,
            plan_limits=None,                       # already checked in (a)
            edited_from_message_id=message_id)
        # c. the edits row
        await self.session.execute(
            text("INSERT INTO message_edits (branch_conversation_id, original_message_id, original_conversation_id, user_id) "
                 "VALUES (:b, :m, :c, :u)"),
            {"b": new_conv.conversation_id, "m": message_id, "c": conv.conversation_id, "u": user_id})
        # d. send in the new conversation (billing reserve + outbox)
        try:
            from app.utils.helpers import uuidv7
            sent = await self.message_service.send(
                conversation_id=new_conv.conversation_id, message_id=uuidv7(), sender="user", content=content,
                user_id=user_id, mode=mode, selected_tools=selected_tools, file_ids=keep_files or None,
                include_diagrams=include_diagrams, model=model, think=think)
        except BaseException:
            await self._undo(new_conv)              # e. leave nothing behind, then re-raise the original error
            raise
        metrics.message_edits_total.inc(case=case)
        return new_conv, sent

    async def _undo(self, new_conv: Conversation) -> None:
        try:
            await self.session.execute(text("DELETE FROM message_edits WHERE branch_conversation_id = :b"),
                                       {"b": new_conv.conversation_id})
        except Exception as e:  # noqa: BLE001 - the request transaction is rolled back anyway
            logger.warning("edit_undo_edits_row_failed", error=str(e))
        try:
            await self.conversations.delete(new_conv.conversation_id)       # emits conversation.deleted
        except Exception as e:  # noqa: BLE001 - never mask the original error
            logger.error("edit_undo_conversation_failed", conversation_id=str(new_conv.conversation_id), error=str(e))

    # ---- versions ------------------------------------------------------------------------------------------------

    async def versions(self, user_id: UUID, message_id: UUID) -> list[dict]:
        """The edited versions made from `message_id`: [{conversation_id, title, created_at}], deleted ones excluded."""
        rows = (await self.session.execute(
            text("SELECT branch_conversation_id, created_at FROM message_edits "
                 "WHERE user_id = :u AND original_message_id = :m ORDER BY created_at"),
            {"u": user_id, "m": message_id})).all()
        out = []
        for branch_id, created_at in rows:
            conv = await self.conversations.get(branch_id)
            if conv is None or conv.user_id != user_id or getattr(conv.status, "value", conv.status) == "deleted":
                continue
            out.append({"conversation_id": branch_id, "title": conv.title, "created_at": created_at})
        return out
