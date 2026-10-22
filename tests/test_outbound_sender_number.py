import unittest
from unittest.mock import AsyncMock, MagicMock

import httpx

from app.meta_client import MetaWhatsAppClient
from app.outbound_worker import OutboundWorker


def _ok_response() -> MagicMock:
    response = MagicMock(spec=httpx.Response)
    response.is_error = False
    response.json.return_value = {"messages": [{"id": "wamid.sent"}]}
    return response


class MetaClientSenderNumberTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.http = AsyncMock(spec=httpx.AsyncClient)
        self.http.post.return_value = _ok_response()
        self.meta = MetaWhatsAppClient(self.http, "token", "default-pnid", "v25.0")

    async def test_defaults_to_configured_number(self) -> None:
        await self.meta.send_text("+911234567890", "hi")
        self.assertEqual(
            self.http.post.call_args.args[0],
            "https://graph.facebook.com/v25.0/default-pnid/messages",
        )

    async def test_sends_from_given_number(self) -> None:
        for send in (
            lambda: self.meta.send_text("+911234567890", "hi", phone_number_id="tenant-pnid"),
            lambda: self.meta.send_image("+911234567890", "https://x/i.png", phone_number_id="tenant-pnid"),
            lambda: self.meta.send_document("+911234567890", "https://x/d.pdf", "d.pdf", phone_number_id="tenant-pnid"),
        ):
            self.http.post.reset_mock()
            await send()
            self.assertEqual(
                self.http.post.call_args.args[0],
                "https://graph.facebook.com/v25.0/tenant-pnid/messages",
            )


class OutboundWorkerSenderNumberTests(unittest.IsolatedAsyncioTestCase):
    async def test_passes_job_phone_number_id_to_meta(self) -> None:
        meta = AsyncMock()
        meta.send_text.return_value = {"messages": [{"id": "wamid.sent"}]}
        settings = MagicMock()
        settings.testing = False
        worker = OutboundWorker(AsyncMock(), meta, settings)

        await worker._process_job(
            {"id": "job-1", "to": "+911234567890", "text": "hi", "phone_number_id": "tenant-pnid"}
        )

        meta.send_text.assert_awaited_once_with(
            "+911234567890", "hi", phone_number_id="tenant-pnid"
        )

    async def test_job_without_phone_number_id_uses_default(self) -> None:
        meta = AsyncMock()
        meta.send_text.return_value = {"messages": [{"id": "wamid.sent"}]}
        settings = MagicMock()
        settings.testing = False
        worker = OutboundWorker(AsyncMock(), meta, settings)

        await worker._process_job({"id": "job-2", "to": "+911234567890", "text": "hi"})

        meta.send_text.assert_awaited_once_with("+911234567890", "hi", phone_number_id=None)


if __name__ == "__main__":
    unittest.main()
