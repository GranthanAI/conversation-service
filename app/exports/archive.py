"""The hierarchy ZIP: one Markdown file per conversation in a directory tree that mirrors the branches, shared media bundled
once under assets/, a machine-readable manifest and a README. Built on disk (never in memory) from validated paths."""

import json
import posixpath
import tempfile
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
from uuid import UUID

from app.artifacts.storage import ObjectStore, sha256_file
from app.exports.collector import ExportBundle, ExportConversation
from app.exports.naming import conversation_dir, slug, validate_archive_path
from app.exports.render_markdown import AssetLinks, fmt_time, render_conversation_markdown
from app.models.artifact_models import ArtifactRecord

FORMAT_VERSION = 1


@dataclass
class ArchiveResult:
    path: Path
    filename: str
    omitted: list[dict] = field(default_factory=list)
    members: list[str] = field(default_factory=list)


def plan_paths(bundle: ExportBundle) -> dict[UUID, str]:
    """conversation id -> archive path of its Markdown file. Children live under `<parent dir>/branches/`."""
    by_id = {ec.conversation.conversation_id: ec for ec in bundle.conversations}
    dirs: dict[UUID, str] = {}
    used: set[str] = set()

    def place(cid: UUID, base: str) -> None:
        ec = by_id[cid]
        name = conversation_dir(ec.conversation.title, cid)
        path = f"{base}/{name}"
        while path in used:                                  # id8 collision on a degenerate dataset: stay unique
            name += "x"
            path = f"{base}/{name}"
        used.add(path)
        dirs[cid] = path
        for child in ec.children:
            if child in by_id and child not in dirs:
                place(child, f"{path}/branches")

    roots = [ec.conversation.conversation_id for ec in bundle.conversations
             if ec.parent_id is None or ec.parent_id not in by_id]
    for r in roots:
        if r not in dirs:
            place(r, "conversations")
    for ec in bundle.conversations:                          # anything unreachable (cycle remnants) still gets a place
        if ec.conversation.conversation_id not in dirs:
            place(ec.conversation.conversation_id, "conversations")
    return {cid: f"{d}/conversation.md" for cid, d in dirs.items()}


async def build_hierarchy_zip(bundle: ExportBundle, store: ObjectStore, include_media: bool, workdir: Path,
                              max_bytes: int, options: dict, snapshot: dict[str, str],
                              max_video_bytes: int) -> ArchiveResult:
    paths = plan_paths(bundle)
    root_ec = next((e for e in bundle.conversations if e.conversation.conversation_id == bundle.root_id),
                   bundle.conversations[0])
    top = f"granthan-export-{slug(root_ec.conversation.title)}-{str(bundle.root_id).replace('-', '')[:8]}"
    titles = {e.conversation.conversation_id: e.conversation.title for e in bundle.conversations}

    # Which artifacts are referenced, once each (a shared animation is bundled a single time).
    referenced: dict[UUID, ArtifactRecord] = {}
    for art in bundle.artifacts.values():
        referenced.setdefault(art.artifact_id, art)

    zip_path = workdir / "export.zip"
    omitted: list[dict] = []
    bundled_video: dict[UUID, str] = {}
    bundled_thumb: dict[UUID, str] = {}
    total = 0
    members: list[str] = []

    def add_bytes(zf: zipfile.ZipFile, path: str, data: bytes) -> None:
        nonlocal total
        validate_archive_path(path)
        zf.writestr(zipfile.ZipInfo(f"{top}/{path}", date_time=bundle.generated_at.timetuple()[:6]), data,
                    compress_type=zipfile.ZIP_DEFLATED)
        total += len(data)
        members.append(path)

    def add_file(zf: zipfile.ZipFile, path: str, source: Path) -> None:
        nonlocal total
        validate_archive_path(path)
        zf.write(source, f"{top}/{path}", compress_type=zipfile.ZIP_STORED)
        total += source.stat().st_size
        members.append(path)

    with zipfile.ZipFile(zip_path, "w", allowZip64=True) as zf:
        if include_media:
            for art in referenced.values():
                if art.status != "ready" or not art.object_key:
                    omitted.append({"artifact_id": str(art.artifact_id), "reason": f"not_ready:{art.status}"})
                    continue
                if total + (art.size_bytes or 0) > max_bytes:
                    omitted.append({"artifact_id": str(art.artifact_id), "reason": "archive_size_limit"})
                    continue
                local = workdir / f"{art.artifact_id}.mp4"
                try:
                    await store.get_to_file(art.object_key, local)
                except Exception:
                    omitted.append({"artifact_id": str(art.artifact_id), "reason": "object_unavailable"})
                    continue
                if local.stat().st_size > max_video_bytes or sha256_file(local) != art.checksum_sha256:
                    omitted.append({"artifact_id": str(art.artifact_id), "reason": "integrity_check_failed"})
                    local.unlink(missing_ok=True)
                    continue
                path = f"assets/videos/{art.artifact_id}.mp4"
                add_file(zf, path, local)
                local.unlink(missing_ok=True)
                bundled_video[art.artifact_id] = path
                if art.thumbnail_key:
                    thumb = workdir / f"{art.artifact_id}.png"
                    try:
                        await store.get_to_file(art.thumbnail_key, thumb)
                        tpath = f"assets/thumbnails/{art.artifact_id}.png"
                        add_file(zf, tpath, thumb)
                        bundled_thumb[art.artifact_id] = tpath
                    except Exception:
                        pass                                  # a thumbnail is optional; the video is what matters
                    finally:
                        thumb.unlink(missing_ok=True)

        manifest_convs = []
        for ec in bundle.conversations:
            cid = ec.conversation.conversation_id
            here = posixpath.dirname(paths[cid])

            def rel(target: Optional[str], _here: str = here) -> Optional[str]:
                return posixpath.relpath(target, _here) if target else None

            links = AssetLinks(video=lambda a, r=rel: r(bundled_video.get(a.artifact_id)),
                               thumbnail=lambda a, r=rel: r(bundled_thumb.get(a.artifact_id)))
            md = render_conversation_markdown(ec, bundle, links, titles.get(ec.parent_id) if ec.parent_id else None)
            add_bytes(zf, paths[cid], md.encode("utf-8"))
            manifest_convs.append({
                "conversation_id": str(cid), "title": ec.conversation.title, "path": paths[cid],
                "parent_conversation_id": str(ec.parent_id) if ec.parent_id else None,
                "branch_message_id": str(ec.branch_message_id) if ec.branch_message_id else None,
                "created_at": ec.conversation.created_at.isoformat() if ec.conversation.created_at else None,
                "message_count": len(ec.messages), "depth": ec.depth,
                "orphaned_from": str(ec.missing_parent_id) if ec.missing_parent_id else None,
                "status": getattr(ec.conversation.status, "value", str(ec.conversation.status)),
            })

        artifacts_manifest = [{
            "artifact_id": str(a.artifact_id), "type": a.artifact_type, "title": a.title, "status": a.status,
            "video": bundled_video.get(a.artifact_id), "thumbnail": bundled_thumb.get(a.artifact_id),
        } for a in referenced.values()]
        manifest = {
            "format_version": FORMAT_VERSION, "generated_at": bundle.generated_at.isoformat(),
            "selected_conversation_id": str(bundle.selected_id), "root_conversation_id": str(bundle.root_id),
            "options": options, "snapshot": snapshot, "conversations": manifest_convs,
            "artifacts": artifacts_manifest, "omitted_media": omitted,
        }
        add_bytes(zf, "manifest.json", json.dumps(manifest, indent=2, ensure_ascii=False).encode("utf-8"))
        add_bytes(zf, "README.md", render_readme(bundle, manifest_convs, options, omitted).encode("utf-8"))
    return ArchiveResult(zip_path, f"{top}.zip", omitted, members)


def render_readme(bundle: ExportBundle, convs: list[dict], options: dict, omitted: list[dict]) -> str:
    lines = [
        "# Granthan conversation export", "",
        f"Generated {fmt_time(bundle.generated_at)}. This archive contains the stored messages exactly as they were "
        "saved; nothing was summarised or regenerated.", "",
        "## Layout", "",
        "- `conversations/<title>-<id>/conversation.md` — one Markdown file per conversation.",
        "- `.../branches/<title>-<id>/conversation.md` — branches live inside their parent's directory.",
        "- `assets/videos/` and `assets/thumbnails/` — animation media, each file stored once and shared by every "
        "message that references it.",
        "- `manifest.json` — machine-readable index (ids, parents, branch points, message counts, media).", "",
        f"## Contents ({len(convs)} conversations)", ""]
    for c in convs:
        lines.append(f"{'  ' * c['depth']}- [{c['title'] or 'Untitled'}]({c['path']}) — {c['message_count']} messages")
    lines += ["", "## Notes", "",
              "- Mermaid diagrams are kept as ```mermaid source blocks.",
              "- Animations are videos; a text document cannot play them. They are linked from the message that "
              + ("contained them." if options.get("include_media") else "contained them only by reference "
                 "(media was not included in this export).")]
    if omitted:
        lines += ["", "## Media that could not be included", ""]
        lines += [f"- artifact `{o['artifact_id']}`: {o['reason']}" for o in omitted]
    orphans = [c for c in convs if c["orphaned_from"]]
    if orphans:
        lines += ["", "## Branches whose parent is unavailable", ""]
        lines += [f"- {c['title'] or 'Untitled'} (parent `{c['orphaned_from']}` was deleted or is inaccessible)" for c in orphans]
    return "\n".join(lines) + "\n"
