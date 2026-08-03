"""FastAPI application for Meta webhook interception and outbound delivery."""

from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import AsyncIterator, Optional
from uuid import uuid4

import httpx
from fastapi import FastAPI, HTTPException, Query, Request, Response, status

from .config import Settings
from .logging_config import configure_logging
from .meta_client import MetaWhatsAppClient, signature_is_valid
from .outbound_worker import OutboundWorker
from .redis_client import RedisClient


logger = logging.getLogger(__name__)


def create_app(settings: Optional[Settings] = None) -> FastAPI:
    service_settings = settings or Settings.from_env()
    configure_logging(service_settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        redis = RedisClient.from_url(service_settings.redis_url)
        app.state.redis = redis
        outbound_task: Optional[asyncio.Task[None]] = None

        if service_settings.outbound_worker_enabled and service_settings.outbound_is_configured:
            http_client = httpx.AsyncClient(timeout=httpx.Timeout(15.0))
            meta_client = MetaWhatsAppClient(
                http_client,
                service_settings.meta_access_token or "",
                service_settings.meta_phone_number_id or "",
                service_settings.meta_graph_api_version or "",
            )
            outbound_task = asyncio.create_task(
                OutboundWorker(redis, meta_client, service_settings).run(),
                name="whatsapp-outbound-worker",
            )
        elif service_settings.outbound_worker_enabled:
            logger.warning(
                "Outbound worker disabled — META_ACCESS_TOKEN, "
                "META_PHONE_NUMBER_ID, and META_GRAPH_API_VERSION must all be set"
            )
            http_client = None
        else:
            http_client = None

        try:
            yield
        finally:
            if outbound_task:
                outbound_task.cancel()
                try:
                    await outbound_task
                except asyncio.CancelledError:
                    pass
            if http_client:
                await http_client.aclose()
            await redis.close()

    app = FastAPI(
        title="Simpliby Interceptor API",
        version="0.1.0",
        lifespan=lifespan,
    )

    @app.get("/")
    async def index() -> dict[str, str]:
        return {"message": "Simpliby interceptor service is running"}

    @app.get("/health")
    async def health() -> dict[str, str]:
        try:
            await app.state.redis.ping()
        except Exception as error:
            logger.exception("Redis health check failed")
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Redis unavailable",
            ) from error
        return {"status": "ok"}

    @app.get("/webhooks/meta")
    async def verify_meta_webhook(
        mode: Optional[str] = Query(None, alias="hub.mode"),
        verify_token: Optional[str] = Query(None, alias="hub.verify_token"),
        challenge: Optional[str] = Query(None, alias="hub.challenge"),
    ) -> Response:
        if not service_settings.meta_verify_token:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="META_VERIFY_TOKEN is not configured",
            )
        if (
            mode != "subscribe"
            or verify_token != service_settings.meta_verify_token
            or challenge is None
        ):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Verification failed")
        return Response(content=challenge, media_type="text/plain")

    @app.post("/webhooks/meta", status_code=status.HTTP_200_OK)
    async def receive_meta_webhook(request: Request) -> dict[str, str]:
        if not service_settings.meta_app_secret:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="META_APP_SECRET is not configured",
            )

        raw_body = await request.body()
        signature = request.headers.get("X-Hub-Signature-256")
        if not signature_is_valid(raw_body, signature, service_settings.meta_app_secret):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid signature")

        try:
            payload = json.loads(raw_body)
        except json.JSONDecodeError as error:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid JSON payload"
            ) from error
        if not isinstance(payload, dict):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="Payload must be an object"
            )

        event_id = str(uuid4())
        event = {
            "id": event_id,
            "provider": "meta_whatsapp",
            "received_at": datetime.now(timezone.utc).isoformat(),
            "payload": payload,
        }
        try:
            await app.state.redis.lpush_json(service_settings.inbound_queue_name, event)
        except Exception as error:
            logger.exception("Failed to queue Meta webhook %s", event_id)
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Queue unavailable",
            ) from error

        logger.info("Queued Meta webhook %s", event_id)
        return {"status": "accepted", "event_id": event_id}

    return app


app = create_app()
