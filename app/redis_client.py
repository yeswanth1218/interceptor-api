"""TCP Redis client using redis.asyncio with JSON helpers."""

from __future__ import annotations

import json
import logging
from typing import Any, Optional

import redis.asyncio as aioredis


logger = logging.getLogger(__name__)


class RedisError(RuntimeError):
    """Raised when a Redis operation fails unexpectedly."""


class RedisClient:
    def __init__(self, client: aioredis.Redis) -> None:
        self._r = client

    @classmethod
    def from_url(cls, url: str, max_connections: int = 5) -> "RedisClient":
        client = aioredis.from_url(
            url, decode_responses=True, max_connections=max_connections
        )
        return cls(client)

    async def close(self) -> None:
        if hasattr(self._r, "aclose"):
            await self._r.aclose()
        else:
            await self._r.close()

    async def ping(self) -> None:
        await self._r.ping()

    async def lpush_json(self, key: str, value: dict[str, Any]) -> int:
        """Push a JSON item to the left (head) of a list."""
        return await self._r.lpush(key, json.dumps(value, separators=(",", ":")))

    async def brpop_json(
        self, key: str, timeout: int = 30
    ) -> Optional[dict[str, Any]]:
        """
        Block until an item appears at the right (tail) of the list, then pop and
        return it. Returns None if timeout elapses with no item.
        """
        result = await self._r.brpop(key, timeout=timeout)
        if result is None:
            return None
        _, raw = result
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as error:
            raise RedisError("Queue item was not valid JSON") from error
        if not isinstance(payload, dict):
            raise RedisError("Queue item must be a JSON object")
        return payload
