"""Live catalog of built-in modes and tools, owned by llm-service (never hard-coded here).

Used to reject an unknown built-in mode / tool when a preference is SAVED. The answer is cached briefly. If
llm-service cannot be reached the catalog is "unknown" (None): the save is accepted and the pipeline validates
again at send time (saved defaults that are no longer available are skipped, explicit choices are rejected)."""

import time
from dataclasses import dataclass
from typing import Optional, Set

import httpx

from app.core.config import settings
from app.core.logging import logger


@dataclass
class Catalog:
    modes: Set[str]
    tools: Set[str]


class ModeCatalog:
    def __init__(self, fetch=None):
        self._fetch = fetch or self._http_fetch
        self._cached: Optional[Catalog] = None
        self._at = 0.0

    @staticmethod
    async def _http_fetch() -> Catalog:
        async with httpx.AsyncClient(timeout=settings.LLM_CATALOG_TIMEOUT_SECONDS) as client:
            resp = await client.get(f"{settings.LLM_SERVICE_URL.rstrip('/')}/api/v1/tools")
            resp.raise_for_status()
            data = resp.json()
        return Catalog(modes={m["id"] for m in data.get("modes", [])}, tools={t["id"] for t in data.get("tools", [])})

    async def get(self) -> Optional[Catalog]:
        now = time.monotonic()
        if self._cached is not None and now - self._at < settings.LLM_CATALOG_TTL_SECONDS:
            return self._cached
        try:
            self._cached, self._at = await self._fetch(), now
        except Exception as exc:
            logger.warning("Mode catalog unavailable; saving without catalog validation", error=str(exc))
            return self._cached     # a stale catalog is better than none; None = unknown
        return self._cached


mode_catalog = ModeCatalog()
