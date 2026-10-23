"""Locating Mermaid / Manim fences inside persisted message text (the wire/storage form is plain Markdown, see
docs/VISUALIZATIONS.md). Mirrors the fence rules of the frontend parser: a recognised fence is ``mermaid or ``manim
with a closing fence of the same character and at least the same length; anything else is ordinary text."""

import json
import re
from dataclasses import dataclass
from typing import Optional

_FENCE = re.compile(
    r"^(?P<indent>[ ]{0,3})(?P<fence>`{3,}|~{3,})[ \t]*(?P<lang>mermaid|manim)(?![A-Za-z0-9_-])[^\n`]*\n(?P<body>.*?)^[ ]{0,3}(?P=fence)[`~]*[ \t]*$",
    re.DOTALL | re.MULTILINE | re.IGNORECASE)


@dataclass(frozen=True)
class FencedBlock:
    kind: str                      # mermaid | manim
    body: str
    start: int
    end: int

    def manim_spec(self) -> Optional[dict]:
        if self.kind != "manim":
            return None
        try:
            spec = json.loads(self.body)
        except ValueError:
            return None
        return spec if isinstance(spec, dict) else None


def find_blocks(content: str) -> list[FencedBlock]:
    return [FencedBlock(m.group("lang").lower(), m.group("body").rstrip("\n"), m.start(), m.end())
            for m in _FENCE.finditer(content or "")]
