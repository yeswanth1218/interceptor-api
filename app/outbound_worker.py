"""Polling worker that sends outbound queue jobs to Meta."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from .config import Settings
from .meta_client import MetaWhatsAppClient
from .redis_client import UpstashRedis


logger = logging.getLogger(__name__)


class OutboundWorker:
    def __init__(
        self,
        redis: UpstashRedis,
        meta: MetaWhatsAppClient,
        settings: Settings,
    ) -> None:
        self._redis = redis
        self._meta = meta
        self._settings = settings
        self._last_iteration_error_log_at = 0.0

    async def run(self) -> None:
        """Continuously poll the configured outbound queue until cancelled."""
        logger.info("Outbound worker started for queue %s", self._settings.outbound_queue_name)
        while True:
            try:
                did_process = await self.process_next()
                if not did_process:
                    await asyncio.sleep(self._settings.outbound_poll_interval_seconds)
            except asyncio.CancelledError:
                logger.info("Outbound worker stopped")
                raise
            except Exception as error:
                self._log_iteration_error(error)
                await asyncio.sleep(self._settings.outbound_poll_interval_seconds)

    async def process_next(self) -> bool:
        """Process at most one job; exposed separately for deterministic tests."""
        job = await self._redis.rpop_json(self._settings.outbound_queue_name)
        if job is None:
            return False

        job.setdefault("id", str(uuid4()))
        logger.info(
            "Outbound job %s picked from %s",
            job["id"],
            self._settings.outbound_queue_name,
        )
        try:
            recipient, text = self._validate_text_job(job)
            response = await self._meta.send_text(recipient, text)
            meta_message_id = self._meta_message_id(response)
            logger.info(
                "Outbound job %s sent%s",
                job["id"],
                f" as {meta_message_id}" if meta_message_id else "",
            )
        except Exception as error:
            await self._handle_failure(job, error)
        return True

    def _log_iteration_error(self, error: Exception) -> None:
        """Avoid emitting one infrastructure error for every polling interval."""
        now = time.monotonic()
        if (
            now - self._last_iteration_error_log_at
            >= self._settings.outbound_error_log_cooldown_seconds
        ):
            logger.exception("Outbound worker iteration failed: %s", error)
            self._last_iteration_error_log_at = now

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
            message_id = messages[0].get("id")
            return message_id if isinstance(message_id, str) else None
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
        await asyncio.sleep(
            min(self._settings.outbound_poll_interval_seconds * (2**attempt), 30)
        )
