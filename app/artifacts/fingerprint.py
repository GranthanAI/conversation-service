"""Deterministic artifact fingerprints.

The fingerprint is what makes rendering idempotent: two requests for the same animation (same canonical spec, same
renderer version, same render-affecting configuration) produce the same fingerprint, so they resolve to the same
registry row and the same stored video. It deliberately excludes everything that does not change the output
(timestamps, request ids, message/conversation ids, UI state) and is versioned so the algorithm can evolve.
"""

import hashlib
import json
import re
import unicodedata
import uuid
from typing import Any

FINGERPRINT_VERSION = 1
SPEC_VERSION = "1.0"                       # the Manim spec schema version (llm-service manim_templates.SPEC_VERSION)
_ALLOWED_KEYS = {"schema_version", "template", "title", "params"}
_NAMESPACE = uuid.UUID("6f1c2d5e-3a47-4b9e-9d0a-5b7c1e8a2f64")
_WS = re.compile(r"\s+")


class ArtifactSpecError(ValueError):
    """The submitted source/spec cannot be an artifact (wrong shape, unknown fields, too large)."""


def _normalise(value: Any, depth: int = 0) -> Any:
    if depth > 12:
        raise ArtifactSpecError("the specification is nested too deeply")
    if isinstance(value, str):
        return unicodedata.normalize("NFC", value)
    if isinstance(value, dict):
        return {unicodedata.normalize("NFC", str(k)): _normalise(v, depth + 1) for k, v in sorted(value.items())}
    if isinstance(value, list):
        return [_normalise(v, depth + 1) for v in value]
    if value is None or isinstance(value, (bool, int, float)):
        return value
    raise ArtifactSpecError("the specification contains an unsupported value")


def canonical_manim_spec(spec: Any, max_chars: int) -> str:
    """Canonical JSON of a Manim spec: sorted keys, NFC text, collapsed title whitespace, default schema_version filled
    in. Raises ArtifactSpecError for anything llm-service would also reject structurally."""
    if not isinstance(spec, dict):
        raise ArtifactSpecError("the specification must be a JSON object")
    extra = set(spec) - _ALLOWED_KEYS
    if extra:
        raise ArtifactSpecError("unknown field(s) in the specification")
    if not isinstance(spec.get("template"), str) or not spec["template"].strip():
        raise ArtifactSpecError("the specification needs a template")
    params = spec.get("params", {})
    if not isinstance(params, dict):
        raise ArtifactSpecError("params must be an object")
    title = spec.get("title", "")
    if not isinstance(title, str):
        raise ArtifactSpecError("title must be text")
    canonical = {
        "schema_version": str(spec.get("schema_version", SPEC_VERSION)),
        "template": spec["template"].strip(),
        "title": _WS.sub(" ", unicodedata.normalize("NFC", title)).strip(),
        "params": _normalise(params),
    }
    text = json.dumps(canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    if len(text) > max_chars:
        raise ArtifactSpecError("the specification is too large")
    return text


def canonical_mermaid_source(source: Any, max_chars: int) -> str:
    """Line endings unified, trailing whitespace and outer blank lines removed. Indentation is meaningful to Mermaid and
    is preserved."""
    if not isinstance(source, str) or not source.strip():
        raise ArtifactSpecError("the diagram source is empty")
    text = unicodedata.normalize("NFC", source).replace("\r\n", "\n").replace("\r", "\n")
    text = "\n".join(line.rstrip() for line in text.split("\n")).strip("\n")
    if not text.strip():
        raise ArtifactSpecError("the diagram source is empty")
    if len(text) > max_chars:
        raise ArtifactSpecError("the diagram source is too large")
    return text


def compute_fingerprint(artifact_type: str, canonical_text: str, renderer_version: str = "", quality: str = "") -> str:
    """sha256 over the algorithm version, artifact type, renderer/template version, render-affecting configuration and
    the canonical content. Mermaid is rendered in the browser, so it only depends on its source."""
    if artifact_type == "mermaid":
        renderer_version, quality = "", ""
    payload = "\x00".join([f"fp{FINGERPRINT_VERSION}", artifact_type, renderer_version, quality, canonical_text])
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def artifact_id_for(owner: uuid.UUID, artifact_type: str, fingerprint: str) -> uuid.UUID:
    """Deterministic id: the same owner + content always maps to the same id, and therefore the same object key, even
    before any database row exists (which is what lets a crash after upload be reconciled without re-rendering)."""
    return uuid.uuid5(_NAMESPACE, f"{owner}:{artifact_type}:{fingerprint}")


def video_object_key(artifact_id: uuid.UUID) -> str:
    return f"artifacts/{artifact_id}/video.mp4"


def thumbnail_object_key(artifact_id: uuid.UUID) -> str:
    return f"artifacts/{artifact_id}/thumb.png"


def title_for(artifact_type: str, canonical_text: str, supplied: str | None) -> str:
    if supplied and supplied.strip():
        return _WS.sub(" ", supplied).strip()[:200]
    if artifact_type == "manim":
        try:
            return (json.loads(canonical_text).get("title") or "Animation")[:200]
        except ValueError:
            return "Animation"
    first = next((ln.strip() for ln in canonical_text.split("\n") if ln.strip() and not ln.strip().startswith("%%")), "")
    kind = first.split()[0] if first else "diagram"
    return f"Mermaid {kind}"[:200]
