"""Small asynchronous client for the Upstash Redis HTTPS REST API."""

from __future__ import annotations

import json
from typing import Any, Optional

import httpx


class UpstashRedisError(RuntimeError):
    """Raised when the Upstash REST API rejects a Redis command."""


class UpstashRedis:
    def __init__(self, client: httpx.AsyncClient, rest_url: str, token: str) -> None:
        self._client = client
        self._rest_url = rest_url.rstrip("/")
        self._headers = {"Authorization": f"Bearer {token}"}

    async def execute(self, *command: Any) -> Any:
        """Execute a Redis command through Upstash's JSON command endpoint."""
        response = await self._client.post(
            self._rest_url,
            headers=self._headers,
            json=list(command),
        )
        response.raise_for_status()
        payload = response.json()
        if "error" in payload:
            raise UpstashRedisError(str(payload["error"]))
        if "result" not in payload:
            raise UpstashRedisError("Upstash response did not contain a result")
        return payload["result"]

    async def ping(self) -> Any:
        return await self.execute("PING")

    async def lpush_json(self, queue_name: str, item: dict[str, Any]) -> int:
        result = await self.execute(
            "LPUSH", queue_name, json.dumps(item, separators=(",", ":"))
        )
        return int(result)

    async def rpop_json(self, queue_name: str) -> Optional[dict[str, Any]]:
        result = await self.execute("RPOP", queue_name)
        if result is None:
            return None
        if not isinstance(result, str):
            raise UpstashRedisError("Queue item was not a JSON string")
        try:
            payload = json.loads(result)
        except json.JSONDecodeError as error:
            raise UpstashRedisError("Queue item was not valid JSON") from error
        if not isinstance(payload, dict):
            raise UpstashRedisError("Queue item must be a JSON object")
        return payload
