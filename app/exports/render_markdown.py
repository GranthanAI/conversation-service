"""Markdown rendering of persisted conversations. Messages are written verbatim (roles, code, tables, math, Mermaid
fences are all preserved); the only transformation is that a ```manim spec block becomes a reference to its stored
video, because a JSON spec is not meaningful to a reader and a video cannot be embedded in text."""

import re
from datetime import timezone
from dataclasses import dataclass
from typing import Callable, Optional
from uuid import UUID

from app.exports.blocks import find_blocks
from app.exports.collector import ExportBundle, ExportConversation
from app.models.artifact_models import ArtifactRecord
from app.models.message import Message

_WS = re.compile(r"\s+")
_MD_SPECIAL = re.compile(r"([\\`*_{}\[\]<>#|])")


def md_inline(text: str, limit: int = 160) -> str:
    """Single-line, markdown-neutral text for titles that come from user/model content."""
    return _MD_SPECIAL.sub(r"\\\1", _WS.sub(" ", text or "").strip())[:limit]


def fmt_time(value) -> str:
    """Always UTC, so an export is identical whichever server produced it."""
    if not value:
        return ""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


@dataclass
class AssetLinks:
    """Where (relative to the Markdown file) an artifact's media was bundled, or None when it was not."""
    video: Callable[[ArtifactRecord], Optional[str]] = lambda a: None
    thumbnail: Callable[[ArtifactRecord], Optional[str]] = lambda a: None


def manim_reference(title: str, artifact: Optional[ArtifactRecord], links: AssetLinks) -> str:
    label = md_inline(title or "Animation")
    if artifact is None or artifact.status != "ready":
        state = "was not rendered" if artifact is None else f"is {artifact.status}"
        return f"> **Animation: {label}**\n> This animation {state}; no video is available for it."
    lines = [f"> **Animation: {label}**"]
    thumb = links.thumbnail(artifact)
    if thumb:
        lines.append(f"> ![Preview of {label}]({thumb})")
    video = links.video(artifact)
    if video:
        lines.append(f"> Video: [{label}.mp4]({video}) (artifact `{artifact.artifact_id}`)")
    else:
        lines.append(f"> The video is not included in this export (artifact `{artifact.artifact_id}`); "
                     "open it in Granthan to play or download it.")
    return "\n".join(lines)


def message_body(conv_id: UUID, message: Message, bundle: ExportBundle, links: AssetLinks) -> str:
    """Message text with manim fences replaced by references; every other byte is kept as stored."""
    content = message.content or ""
    out, cursor = [], 0
    for block in find_blocks(content):
        if block.kind != "manim":
            continue
        spec = block.manim_spec() or {}
        artifact = bundle.artifact_for_spec(spec)
        title = spec.get("title") if isinstance(spec.get("title"), str) else (artifact.title if artifact else "Animation")
        out.append(content[cursor:block.start])
        out.append(manim_reference(title, artifact, links))
        cursor = block.end
    out.append(content[cursor:])
    return "".join(out).strip("\n")


def role_label(sender: str) -> str:
    return {"user": "User", "assistant": "Assistant"}.get((sender or "").lower(), (sender or "Message").title())


def render_conversation_markdown(ec: ExportConversation, bundle: ExportBundle, links: AssetLinks,
                                 parent_title: Optional[str] = None) -> str:
    conv = ec.conversation
    lines = [f"# {md_inline(conv.title, 200) or 'Untitled conversation'}", ""]
    meta = [f"- **Conversation ID:** `{conv.conversation_id}`", f"- **Created:** {fmt_time(conv.created_at)}",
            f"- **Messages:** {len(ec.messages)}"]
    if ec.parent_id:
        parent = f" ({md_inline(parent_title)})" if parent_title else ""
        meta.append(f"- **Branched from:** `{ec.parent_id}`{parent}")
        if ec.branch_message_id:
            meta.append(f"- **Branch point:** after message `{ec.branch_message_id}`")
    elif ec.missing_parent_id:
        meta.append(f"- **Branched from:** `{ec.missing_parent_id}` (not available: deleted or inaccessible)")
    lines += meta + ["", "---", ""]
    if not ec.messages:
        lines += ["*This conversation has no messages.*", ""]
    for m in ec.messages:
        stamp = fmt_time(m.created_at)
        lines += [f"## {role_label(m.sender)}" + (f" · {stamp}" if stamp else ""), "",
                  message_body(conv.conversation_id, m, bundle, links), ""]
    return "\n".join(lines).rstrip() + "\n"
