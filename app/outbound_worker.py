"""Outbound worker — blocks on whatsapp:outbound and delivers via Meta Graph API."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from .config import Settings
from .meta_client import MetaWhatsAppClient
from .redis_client import RedisClient


logger = logging.getLogger(__name__)


class OutboundWorker:
    def __init__(
        self,
        redis: RedisClient,
        meta: MetaWhatsAppClient,
        settings: Settings,
    ) -> None:
        self._redis = redis
        self._meta = meta
        self._settings = settings
        self._last_error_log_at = 0.0

    async def run(self) -> None:
        """Block on whatsapp:outbound using BRPOP and deliver each job to Meta."""
        logger.info(
            "Outbound worker started (BRPOP) on queue %s",
            self._settings.outbound_queue_name,
        )
        while True:
            try:
                job = await self._redis.brpop_json(
                    self._settings.outbound_queue_name, timeout=30
                )
                if job is not None:
                    await self._process_job(job)
            except asyncio.CancelledError:
                logger.info("Outbound worker stopped")
                raise
            except Exception as error:
                self._log_error_throttled(error)
                await asyncio.sleep(1)

    async def _process_job(self, job: dict[str, Any]) -> None:
        job.setdefault("id", str(uuid4()))
        logger.info("Outbound job %s picked up", job["id"])
        try:
            recipient, text = self._validate_text_job(job)
            if self._settings.testing:
                logger.info(
                    "Outbound job %s: [TESTING MODE] Bypassing Meta Graph API call. "
                    "Recipient: %s, Text: %r",
                    job["id"], recipient, text
                )
                meta_message_id = f"mock-wamid-{uuid4()}"
            else:
                response = await self._meta.send_text(recipient, text)
                meta_message_id = self._meta_message_id(response)
            logger.info(
                "Outbound job %s sent%s",
                job["id"],
                f" as {meta_message_id}" if meta_message_id else "",
            )
        except Exception as error:
            await self._handle_failure(job, error)

    def _log_error_throttled(self, error: Exception) -> None:
        now = time.monotonic()
        if now - self._last_error_log_at >= self._settings.outbound_error_log_cooldown_seconds:
            logger.exception("Outbound worker error: %s", error)
            self._last_error_log_at = now

    @staticmethod
    def _validate_text_job(job: dict[str, Any]) -> tuple[str, str]:
        if job.get("message_type", "text") != "text":
            raise ValueError("Only text outbound jobs are supported in this POC")
        recipient = job.get("to")
        text = job.get("text")
        if not isinstance(recipient, str) or not recipient.strip():
            raise ValueError("Outbound job requires a non-empty 'to' phone number")
        if not isinstance(text, str) or not text.strip():
            raise ValueError("Outbound job requires non-empty 'text'")
        return recipient, text

    @staticmethod
    def _meta_message_id(response: dict[str, Any]) -> str | None:
        messages = response.get("messages")
        if isinstance(messages, list) and messages and isinstance(messages[0], dict):
            mid = messages[0].get("id")
            return mid if isinstance(mid, str) else None
        return None

    async def _handle_failure(self, job: dict[str, Any], error: Exception) -> None:
        attempt = int(job.get("attempt", 0)) + 1
        job["attempt"] = attempt
        job["last_error"] = str(error)
        job["last_failed_at"] = datetime.now(timezone.utc).isoformat()

        if attempt >= self._settings.outbound_max_attempts:
            await self._redis.lpush_json(self._settings.outbound_failed_queue_name, job)
            logger.error(
                "Outbound job %s moved to failed queue after %s attempts",
                job["id"],
                attempt,
            )
            return

        await self._redis.lpush_json(self._settings.outbound_queue_name, job)
        logger.warning(
            "Outbound job %s failed (attempt %s/%s); queued for retry",
            job["id"],
            attempt,
            self._settings.outbound_max_attempts,
        )
        await asyncio.sleep(min(2**attempt, 30))
