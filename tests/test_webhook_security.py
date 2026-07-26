import hashlib
import hmac
import unittest

from app.meta_client import signature_is_valid


class WebhookSignatureTests(unittest.TestCase):
    def test_accepts_correct_signature(self) -> None:
        secret = "test-secret"
        body = b'{"object":"whatsapp_business_account"}'
        digest = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()

        self.assertTrue(signature_is_valid(body, f"sha256={digest}", secret))

    def test_rejects_incorrect_signature(self) -> None:
        self.assertFalse(signature_is_valid(b"payload", "sha256=incorrect", "test-secret"))
        self.assertFalse(signature_is_valid(b"payload", None, "test-secret"))
