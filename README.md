# Simpliby Interceptor API

The deployed service has two POC responsibilities:

1. Receive verified Meta WhatsApp webhooks at `POST /webhooks/meta` and push them to `whatsapp:inbound` in Upstash Redis.
2. Run one background worker that polls `whatsapp:outbound`, then sends text replies through the Meta Graph API.

## Setup

Copy the keys from [`.env.example`](.env.example) into the existing `.env`. The Upstash URL and standard (read/write) token are already present; add the required `META_*` variables before configuring the Meta callback.

Install and run:

```bash
cd interceptor-api
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 6000
```

For local tests:

```bash
cd interceptor-api
python3 -m unittest discover -s tests
```

## Meta setup

Configure this deployed HTTPS path as Meta's callback URL:

```text
https://your-domain.example/webhooks/meta
```

Use the exact `META_VERIFY_TOKEN` value in Meta's webhook configuration. The application validates every POST against `X-Hub-Signature-256` using `META_APP_SECRET`; do not disable this check.

## Queue formats

### Inbound (`whatsapp:inbound`)

The interceptor adds a wrapper around the untouched Meta payload:

```json
{
  "id": "event UUID",
  "provider": "meta_whatsapp",
  "received_at": "2026-07-25T10:00:00+00:00",
  "payload": { "object": "whatsapp_business_account" }
}
```

### Outbound (`whatsapp:outbound`)

Your local AI/API worker should push this JSON object to Upstash:

```json
{
  "id": "job UUID",
  "tenant_id": "tenant UUID",
  "patient_id": "patient UUID",
  "to": "+919999911111",
  "message_type": "text",
  "text": "Glad to hear that. Have you taken your medicines today?",
  "reply_to_message_id": "wamid...",
  "attempt": 0
}
```

The worker supports text messages in this POC. A job that fails fewer than `OUTBOUND_MAX_ATTEMPTS` is returned to the outbound queue after a short exponential pause; after that it is moved to `whatsapp:outbound:failed` with `last_error` and `last_failed_at`.

## Logging

The worker does not log when a queue poll finds no job. It logs only when it starts, picks a job, sends/retries/fails a job, stops, or encounters an infrastructure error. Repeated worker-level errors are throttled to one log every `OUTBOUND_ERROR_LOG_COOLDOWN_SECONDS` (60 seconds by default).

Use `LOG_LEVEL=INFO` for normal operation. Do not log webhook bodies, access tokens, Redis tokens, or patient message text.

## POC limits

- Upstash's REST API does not provide blocking list pops, so this worker polls once per `OUTBOUND_POLL_INTERVAL_SECONDS`.
- Queue delivery is at-least-once. A network failure after Meta receives a request can make it unclear whether the message was accepted. Keep outbound jobs identifiable and add persistent delivery records before production.
- Run one application instance for this POC. Multiple instances would each start an outbound consumer.
