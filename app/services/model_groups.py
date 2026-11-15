"""Model id -> group (and label), from billing's GET /billing/v1/models.

Cached in Redis for 10 minutes and refreshed in the background. The bundled `plan_defaults.MODEL_GROUPS` is only the fallback
for when billing has never been reachable (a cold start with billing down)."""

import asyncio
import json
import time
from typing import Awaitable, Callable, Optional

import httpx

from app.core.config import settings
from app.core.logging import logger
from app.core.plan_defaults import MODEL_GROUPS

CACHE_KEY = "billing:model_groups"
CACHE_TTL_SECONDS = 600
REFRESH_SECONDS = 300
LOCAL_SECONDS = 60.0


async def fetch_models(base_url: str, timeout_s: float = 3.0) -> list[dict]:
    async with httpx.AsyncClient(timeout=timeout_s) as client:
        resp = await client.get(f"{base_url.rstrip('/')}/billing/v1/models")
        resp.raise_for_status()
        return list(resp.json()["models"])


class ModelGroups:
    def __init__(self, redis_client=None, fetch: Callable[[str], Awaitable[list[dict]]] = fetch_models,
                 base_url: Optional[str] = None, monotonic: Callable[[], float] = time.monotonic):
        self.redis, self._fetch, self._mono = redis_client, fetch, monotonic
        self.base_url = base_url
        self._local: Optional[tuple[float, dict]] = None
        self._task: Optional[asyncio.Task] = None

    def _url(self) -> str:
        return self.base_url or settings.BILLING_SERVICE_URL

    @staticmethod
    def _index(models: list[dict]) -> dict:
        return {m["id"]: {"group": m["group"], "label": m.get("label") or m["id"]} for m in models}

    async def refresh(self) -> Optional[dict]:
        """Fetch from billing and store; None (cache untouched) when billing cannot be reached."""
        try:
            index = self._index(await self._fetch(self._url()))
        except Exception as e:
            logger.warning("model_groups_refresh_failed", error=type(e).__name__)
            return None
        self._local = (self._mono(), index)
        if self.redis:
            try:
                await self.redis.set(CACHE_KEY, json.dumps(index), ex=CACHE_TTL_SECONDS)
            except Exception as e:
                logger.warning("model_groups_cache_write_failed", error=type(e).__name__)
        return index

    async def index(self) -> dict:
        if self._local and self._mono() - self._local[0] < LOCAL_SECONDS:
            return self._local[1]
        if self.redis:
            try:
                raw = await self.redis.get(CACHE_KEY)
                if raw:
                    index = json.loads(raw)
                    self._local = (self._mono(), index)
                    return index
            except Exception as e:
                logger.warning("model_groups_cache_read_failed", error=type(e).__name__)
        fresh = await self.refresh()
        if fresh is not None:
            return fresh
        if self._local:                                   # stale beats bundled
            return self._local[1]
        return {m: {"group": g, "label": m} for m, g in MODEL_GROUPS.items()}   # bundled fallback

    async def group_of(self, model: str) -> Optional[str]:
        entry = (await self.index()).get(model)
        return entry["group"] if entry else None

    async def label_of(self, model: str) -> str:
        entry = (await self.index()).get(model)
        return entry["label"] if entry else model

    async def _loop(self) -> None:
        while True:
            await asyncio.sleep(REFRESH_SECONDS)
            await self.refresh()

    async def start(self) -> None:
        """One try now (never blocks startup on failure), then a refresh every 5 minutes."""
        await self.refresh()
        if self._task is None:
            self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None


model_groups = ModelGroups()
