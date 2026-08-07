"""Meta WhatsApp webhook verification and outbound Graph API client."""

from __future__ import annotations

import hashlib
import hmac
from typing import Any

import httpx


class MetaApiError(RuntimeError):
    """A failed outbound request to Meta's Graph API."""


def signature_is_valid(raw_body: bytes, signature: str | None, app_secret: str) -> bool:
    """Validate Meta's X-Hub-Signature-256 against the untouched request body."""
    if not signature:
        return False
    digest = hmac.new(
        app_secret.encode("utf-8"), raw_body, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(signature, f"sha256={digest}")


class MetaWhatsAppClient:
    def __init__(
        self,
        client: httpx.AsyncClient,
        access_token: str,
        phone_number_id: str,
        graph_api_version: str,
    ) -> None:
        self._client = client
        self._access_token = access_token
        self._endpoint = (
            f"https://graph.facebook.com/{graph_api_version}/{phone_number_id}/messages"
        )

    async def send_text(self, recipient_phone_e164: str, text: str) -> dict[str, Any]:
        """Send a text message through the WhatsApp Cloud API."""
        response = await self._client.post(
            self._endpoint,
            headers={
                "Authorization": f"Bearer {self._access_token}",
                "Content-Type": "application/json",
            },
            json={
                "messaging_product": "whatsapp",
                "recipient_type": "individual",
                "to": recipient_phone_e164.lstrip("+"),
                "type": "text",
                "text": {"preview_url": False, "body": text},
            },
        )
        if response.is_error:
            try:
                details: Any = response.json()
            except ValueError:
                details = response.text
            raise MetaApiError(f"Meta API returned HTTP {response.status_code}: {details}")
        return response.json()

    async def send_image(self, recipient_phone_e164: str, image_url: str, caption: str | None = None) -> dict[str, Any]:
        """Send an image through the WhatsApp Cloud API using a public URL."""
        payload = {
            "messaging_product": "whatsapp",
            "recipient_type": "individual",
            "to": recipient_phone_e164.lstrip("+"),
            "type": "image",
            "image": {
                "link": image_url,
            }
        }
        if caption:
            payload["image"]["caption"] = caption

        response = await self._client.post(
            self._endpoint,
            headers={
                "Authorization": f"Bearer {self._access_token}",
                "Content-Type": "application/json",
            },
            json=payload,
        )
        if response.is_error:
            try:
                details: Any = response.json()
            except ValueError:
                details = response.text
            raise MetaApiError(f"Meta API returned HTTP {response.status_code}: {details}")
        return response.json()

    async def send_document(
        self, recipient_phone_e164: str, document_url: str, filename: str, caption: str | None = None
    ) -> dict[str, Any]:
        """Send a document (e.g. PDF) through the WhatsApp Cloud API using a public URL."""
        payload = {
            "messaging_product": "whatsapp",
            "recipient_type": "individual",
            "to": recipient_phone_e164.lstrip("+"),
            "type": "document",
            "document": {
                "link": document_url,
                "filename": filename,
            }
        }
        if caption:
            payload["document"]["caption"] = caption

        response = await self._client.post(
            self._endpoint,
            headers={
                "Authorization": f"Bearer {self._access_token}",
                "Content-Type": "application/json",
            },
            json=payload,
        )
        if response.is_error:
            try:
                details: Any = response.json()
            except ValueError:
                details = response.text
            raise MetaApiError(f"Meta API returned HTTP {response.status_code}: {details}")
        return response.json()
