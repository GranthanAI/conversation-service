"""PDF export: Markdown -> sanitised HTML -> WeasyPrint.

Safety: raw HTML inside messages is disabled in the Markdown parser (it is escaped, never interpreted), WeasyPrint does not
execute JavaScript, and its URL fetcher refuses everything except inline `data:` images, so a message cannot make the
renderer read local files or call internal services.

Honesty: a PDF cannot play video and this renderer does not run Mermaid or KaTeX. Mermaid diagrams are printed as labelled
source, math stays in its TeX notation, and an animation is shown as a titled card with its preview frame (when one
exists) and a reference to the stored video.
"""

import asyncio
import base64
import html
import json
import os
from typing import Optional

from markdown_it import MarkdownIt

from app.core.config import settings
from app.exports.collector import ExportBundle, ExportConversation
from app.exports.render_markdown import fmt_time, role_label
from app.models.artifact_models import ArtifactRecord

CSS = """
@page { size: A4; margin: 22mm 18mm 20mm 18mm;
  @top-center { content: string(doc-title); font: 9pt sans-serif; color: #666; }
  @bottom-center { content: "Page " counter(page) " of " counter(pages); font: 9pt sans-serif; color: #666; } }
html { font: 10.5pt/1.5 "DejaVu Sans", "Helvetica", sans-serif; color: #1a1a1a; }
h1.doc { string-set: doc-title content(); font-size: 22pt; margin: 0 0 4mm; }
section.conversation { break-before: page; }
section.conversation:first-of-type { break-before: auto; }
section.conversation h1 { string-set: doc-title content(); font-size: 17pt; margin: 0 0 3mm; }
.meta { color: #555; font-size: 9pt; margin: 0 0 6mm; padding-bottom: 3mm; border-bottom: 1px solid #ccc; }
.meta div { margin: 0.5mm 0; }
article.msg { margin: 0 0 5mm; padding: 2.5mm 3.5mm; border-left: 3px solid #999; background: #f6f6f6; }
article.msg.user { border-left-color: #3b6fd4; background: #eef3fc; }
article.msg h2 { font-size: 9.5pt; margin: 0 0 2mm; color: #333; break-after: avoid; }
article.msg h2 span { font-weight: normal; color: #777; }
pre { background: #f0f0f0; border: 1px solid #ddd; padding: 2mm; font: 8.5pt/1.35 "DejaVu Sans Mono", monospace;
  white-space: pre-wrap; overflow-wrap: anywhere; break-inside: avoid-page; }
code { font: 9pt "DejaVu Sans Mono", monospace; background: #eee; padding: 0 1mm; }
pre code { background: none; padding: 0; }
table { border-collapse: collapse; margin: 2mm 0; width: 100%; }
th, td { border: 1px solid #bbb; padding: 1.2mm 2mm; font-size: 9.5pt; vertical-align: top; }
th { background: #e8e8e8; }
blockquote { margin: 2mm 0; padding-left: 3mm; border-left: 2px solid #bbb; color: #444; }
figure { margin: 3mm 0; padding: 2.5mm; border: 1px solid #bbb; background: #fff; break-inside: avoid; }
figcaption { font-size: 9pt; font-weight: bold; margin-bottom: 1.5mm; }
figure img { max-width: 100%; }
figure .note { font-size: 8.5pt; color: #666; }
p { margin: 0 0 2.5mm; overflow-wrap: anywhere; }
a { color: #1a4fa0; }
.toc { margin: 4mm 0 8mm; } .toc li { margin: 1mm 0; }
"""


def _md() -> MarkdownIt:
    md = MarkdownIt("commonmark", {"html": False, "linkify": False, "typographer": False}).enable("table")
    default_fence = md.renderer.rules.get("fence")

    def fence(tokens, idx, options, env):
        token = tokens[idx]
        lang = (token.info or "").strip().split()[0].lower() if (token.info or "").strip() else ""
        if lang == "mermaid":
            return ('<figure><figcaption>Mermaid diagram (source)</figcaption>'
                    f'<pre><code>{html.escape(token.content)}</code></pre>'
                    '<div class="note">The diagram is shown as its source; open the conversation in Granthan to see it rendered.</div></figure>')
        if lang == "manim":
            return env["manim"](idx, token, env)
        return default_fence(tokens, idx, options, env) if default_fence else f"<pre><code>{html.escape(token.content)}</code></pre>"

    md.renderer.rules["fence"] = fence
    return md


_MD = _md()


def _data_uri(png: bytes) -> str:
    return "data:image/png;base64," + base64.b64encode(png).decode("ascii")


def _manim_card(title: str, artifact: Optional[ArtifactRecord], thumb_png: Optional[bytes]) -> str:
    cap = html.escape(title or "Animation")
    if artifact is None or artifact.status != "ready":
        state = "was not rendered" if artifact is None else f"is {html.escape(artifact.status)}"
        return (f'<figure><figcaption>Animation: {cap}</figcaption>'
                f'<div class="note">This animation {state}; no video is available for it.</div></figure>')
    img = f'<img alt="Preview of {cap}" src="{_data_uri(thumb_png)}"/>' if thumb_png else ""
    return (f'<figure><figcaption>Animation: {cap}</figcaption>{img}'
            '<div class="note">A PDF cannot play video. The animation is stored in Granthan '
            f'(artifact {html.escape(str(artifact.artifact_id))}); open the conversation to play or download it.</div></figure>')


def message_html(ec: ExportConversation, message, bundle: ExportBundle, thumbs: dict) -> str:
    def manim(_idx, token, _env):
        try:
            spec = json.loads(token.content)
        except ValueError:
            spec = None
        spec = spec if isinstance(spec, dict) else {}
        artifact = bundle.artifact_for_spec(spec)
        title = spec.get("title") if isinstance(spec.get("title"), str) else (artifact.title if artifact else "Animation")
        return _manim_card(title, artifact, thumbs.get(artifact.artifact_id) if artifact else None)

    return _MD.render(message.content or "", {"manim": manim})


def build_html(bundle: ExportBundle, thumbs: Optional[dict] = None, title: Optional[str] = None) -> str:
    thumbs = thumbs or {}
    first = bundle.conversations[0].conversation.title if bundle.conversations else "Conversation"
    doc_title = title or (first if len(bundle.conversations) == 1 else f"{first} — conversation hierarchy")
    parts = [f'<!doctype html><html lang="en"><head><meta charset="utf-8"><title>{html.escape(doc_title)}</title>'
             f"<style>{CSS}</style></head><body>"]
    if len(bundle.conversations) > 1:
        parts.append(f'<h1 class="doc">{html.escape(doc_title)}</h1><p class="meta">Generated {html.escape(fmt_time(bundle.generated_at))}'
                     f" · {len(bundle.conversations)} conversations</p><ol class=\"toc\">")
        for ec in bundle.conversations:
            parts.append(f'<li style="margin-left:{ec.depth * 6}mm">{html.escape(ec.conversation.title or "Untitled")}'
                         f" — {len(ec.messages)} messages</li>")
        parts.append("</ol>")
    titles = {e.conversation.conversation_id: e.conversation.title for e in bundle.conversations}
    for ec in bundle.conversations:
        c = ec.conversation
        meta = [f"<div>Conversation ID: {html.escape(str(c.conversation_id))}</div>",
                f"<div>Created: {html.escape(fmt_time(c.created_at))}</div>"]
        if ec.parent_id:
            meta.append(f"<div>Branched from: {html.escape(titles.get(ec.parent_id) or str(ec.parent_id))}"
                        + (f" (after message {html.escape(str(ec.branch_message_id))})" if ec.branch_message_id else "") + "</div>")
        elif ec.missing_parent_id:
            meta.append(f"<div>Branched from a conversation that is no longer available ({html.escape(str(ec.missing_parent_id))})</div>")
        parts.append(f'<section class="conversation"><h1>{html.escape(c.title or "Untitled conversation")}</h1>'
                     f'<div class="meta">{"".join(meta)}</div>')
        if not ec.messages:
            parts.append("<p><em>This conversation has no messages.</em></p>")
        for m in ec.messages:
            role = (m.sender or "").lower()
            parts.append(f'<article class="msg {"user" if role == "user" else "assistant"}">'
                         f"<h2>{html.escape(role_label(m.sender))} <span>{html.escape(fmt_time(m.created_at))}</span></h2>"
                         f"{message_html(ec, m, bundle, thumbs)}</article>")
        parts.append("</section>")
    parts.append("</body></html>")
    return "".join(parts)


def _deny_fetcher(url: str, *args, **kwargs):
    """Only inline data: images; no files, no network."""
    if url.startswith("data:image/png;base64,"):
        from weasyprint import default_url_fetcher
        return default_url_fetcher(url)
    raise ValueError("external resources are not allowed in exports")


class PdfUnavailable(RuntimeError):
    """WeasyPrint (or its native Pango/Cairo libraries) is not installed in this environment."""


def _render_sync(document: str) -> bytes:
    if settings.WEASYPRINT_DLL_DIRECTORIES and not os.environ.get("WEASYPRINT_DLL_DIRECTORIES"):
        os.environ["WEASYPRINT_DLL_DIRECTORIES"] = settings.WEASYPRINT_DLL_DIRECTORIES   # must be set before the import
    try:
        from weasyprint import HTML
    except Exception as exc:            # ImportError, or OSError when the native libraries are missing
        raise PdfUnavailable(type(exc).__name__) from exc
    return HTML(string=document, url_fetcher=_deny_fetcher).write_pdf()


async def render_pdf(bundle: ExportBundle, thumbs: Optional[dict] = None, title: Optional[str] = None) -> bytes:
    document = build_html(bundle, thumbs, title)
    return await asyncio.to_thread(_render_sync, document)
