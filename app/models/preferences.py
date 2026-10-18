"""
User AI / application preferences (authoritative owner: conversation-service).

These are persistent *defaults* for new messages. A message that states its own
`mode` / `selected_tools` overrides the defaults for that message only; saved
preferences are never changed by sending a message.

Purely visual settings (theme, sidebar, fonts) are NOT here: they stay in the
browser. Memory on/off lives in memory-service, which enforces it.
"""

import re
from datetime import datetime
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


class AnswerLength(str, Enum):
    CONCISE = "concise"
    BALANCED = "balanced"
    DETAILED = "detailed"


class Tone(str, Enum):
    PROFESSIONAL = "professional"
    FRIENDLY = "friendly"
    SIMPLE = "simple"


class CitationPreference(str, Enum):
    # Show sources when the answer is grounded in documents or web results.
    SHOW = "show"
    # Do not attach citations or add source references to answers.
    HIDE = "hide"


class DocumentContext(str, Enum):
    # Documents attached earlier in a conversation stay in scope for follow-ups.
    CONVERSATION = "conversation"
    # Only the files attached to the message itself are used.
    ATTACHED_ONLY = "attached_only"


# Short name used in the generation prompt when it differs from the display name.
_LANGUAGE_PROMPT_NAME = {"hinglish": "Hinglish"}

# Response languages offered to users. "auto" = answer in the language of the question.
SUPPORTED_LANGUAGES: dict[str, str] = {
    "auto": "Auto (match my message)",
    "en": "English",
    "hi": "Hindi",
    "hinglish": "Hinglish (Hindi in English script, mixed with English)",
    "bn": "Bengali",
    "mr": "Marathi",
    "ta": "Tamil",
    "te": "Telugu",
    "gu": "Gujarati",
    "kn": "Kannada",
    "ml": "Malayalam",
    "pa": "Punjabi",
    "ur": "Urdu",
    "es": "Spanish",
    "fr": "French",
    "de": "German",
    "pt": "Portuguese",
    "it": "Italian",
    "nl": "Dutch",
    "ru": "Russian",
    "ar": "Arabic",
    "tr": "Turkish",
    "zh": "Chinese",
    "ja": "Japanese",
    "ko": "Korean",
    "id": "Indonesian",
    "vi": "Vietnamese",
}

PERSONAL_INSTRUCTIONS_MAX = 1000
MAX_DEFAULT_TOOLS = 10  # shape bound; the live limit comes from llm-service discovery
_TOOL_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,63}$")
_MODE_ID = re.compile(r"^[A-Za-z0-9_-]{1,32}$")
# Control characters other than tab / newline are stripped from free text.
_CONTROL = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")


def _clean_mode(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    v = v.strip().lower()
    if v in ("", "auto", "default"):
        return None  # None = Auto: the planner decides
    if not _MODE_ID.fullmatch(v):
        raise ValueError("Invalid mode.")
    return v


def _clean_tools(v: Optional[List[str]]) -> Optional[List[str]]:
    if v is None:
        return None
    cleaned: List[str] = []
    for raw in v:
        name = raw.strip()
        if not _TOOL_ID.match(name):
            raise ValueError(f"Invalid tool id: {raw[:64]!r}")
        if name not in cleaned:
            cleaned.append(name)
    if len(cleaned) > MAX_DEFAULT_TOOLS:
        raise ValueError(f"At most {MAX_DEFAULT_TOOLS} tools can be selected.")
    return cleaned


def _clean_instructions(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    v = _CONTROL.sub("", v).strip()
    if len(v) > PERSONAL_INSTRUCTIONS_MAX:
        raise ValueError(f"Personal instructions can be at most {PERSONAL_INSTRUCTIONS_MAX} characters.")
    return v


class UserPreferences(BaseModel):
    """The resolved preference document (defaults filled in)."""

    default_mode: Optional[str] = None  # None = Auto
    default_tools: List[str] = Field(default_factory=list)
    answer_length: AnswerLength = AnswerLength.BALANCED
    tone: Tone = Tone.PROFESSIONAL
    language: str = "auto"
    personal_instructions: str = ""
    citations: CitationPreference = CitationPreference.SHOW
    document_context: DocumentContext = DocumentContext.CONVERSATION

    @classmethod
    def from_row(cls, row: Optional[dict]) -> "UserPreferences":
        """Builds from a stored row, tolerating values that are no longer valid
        (e.g. an enum member that was removed): each invalid field falls back to
        its default instead of failing the whole document."""
        if not row:
            return cls()
        valid: dict = {}
        for field in cls.model_fields:
            value = row.get(field)
            if value is None:
                continue
            try:
                cls.model_validate({field: value})
            except Exception:
                continue  # keep the default for this one field
            valid[field] = value
        return cls.model_validate(valid)

    @field_validator("language")
    @classmethod
    def _language(cls, v: str) -> str:
        if v not in SUPPORTED_LANGUAGES:
            raise ValueError("Unsupported language.")
        return v

    @field_validator("default_mode", mode="before")
    @classmethod
    def _mode(cls, v):
        return _clean_mode(v)

    @field_validator("default_tools", mode="before")
    @classmethod
    def _tools(cls, v):
        return _clean_tools(v) or []

    @field_validator("personal_instructions", mode="before")
    @classmethod
    def _instructions(cls, v):
        return _clean_instructions(v) or ""


class PreferencesUpdate(BaseModel):
    """PATCH body: only the fields present are changed. `version` (from the last GET)
    enables optimistic concurrency: a stale version is rejected with 409 instead of
    silently overwriting a newer change made elsewhere."""

    model_config = ConfigDict(extra="forbid")

    version: Optional[int] = Field(None, ge=0)
    default_mode: Optional[str] = None
    default_tools: Optional[List[str]] = None
    answer_length: Optional[AnswerLength] = None
    tone: Optional[Tone] = None
    language: Optional[str] = None
    personal_instructions: Optional[str] = None
    citations: Optional[CitationPreference] = None
    document_context: Optional[DocumentContext] = None

    @field_validator("default_mode")
    @classmethod
    def _mode(cls, v):
        # Explicit null and "auto" both mean Auto; normalised to "" so it is
        # distinguishable from "field not sent" (None).
        cleaned = _clean_mode(v)
        return "" if cleaned is None else cleaned

    @field_validator("default_tools")
    @classmethod
    def _tools(cls, v):
        return _clean_tools(v)

    @field_validator("language")
    @classmethod
    def _language(cls, v):
        if v is not None and v not in SUPPORTED_LANGUAGES:
            raise ValueError("Unsupported language.")
        return v

    @field_validator("personal_instructions")
    @classmethod
    def _instructions(cls, v):
        return _clean_instructions(v)

    def changes(self) -> dict:
        """Fields explicitly sent by the client (excluding `version`)."""
        sent = self.model_dump(exclude_unset=True, exclude={"version"})
        if "default_mode" in sent:
            sent["default_mode"] = sent["default_mode"] or None
        return sent


class PreferencesResponse(UserPreferences):
    version: int = 0
    updated_at: Optional[datetime] = None
    options: dict = Field(default_factory=dict)


def preference_options() -> dict:
    """Static choices the UI renders. Modes and tools are NOT listed here: they are
    discovered live from llm-service via GET /tools."""
    return {
        "answer_length": [e.value for e in AnswerLength],
        "tone": [e.value for e in Tone],
        "citations": [e.value for e in CitationPreference],
        "document_context": [e.value for e in DocumentContext],
        "languages": [{"code": code, "name": name} for code, name in SUPPORTED_LANGUAGES.items()],
        "personal_instructions_max": PERSONAL_INSTRUCTIONS_MAX,
    }


def generation_preferences(prefs: UserPreferences) -> dict:
    """The part of the preferences llm-service applies to generation. Sent on the
    chat.message.created event; never includes anything authorization-related."""
    return {
        "answer_length": prefs.answer_length.value,
        "tone": prefs.tone.value,
        "language": prefs.language,
        "language_name": _LANGUAGE_PROMPT_NAME.get(prefs.language) or SUPPORTED_LANGUAGES.get(prefs.language, prefs.language),
        "personal_instructions": prefs.personal_instructions,
        "citations": prefs.citations.value,
    }
