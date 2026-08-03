"""Configuration loading for the interceptor service."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional


SERVICE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_ENV_FILE = SERVICE_DIR / ".env"


def _read_env_file(path: Path) -> Dict[str, str]:
    if not path.is_file():
        return {}
    values: Dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key:
            values[key] = value.strip().strip('"').strip("'")
    return values


def _as_bool(value: str, default: bool) -> bool:
    if not value:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    redis_url: str
    meta_verify_token: Optional[str]
    meta_app_secret: Optional[str]
    meta_access_token: Optional[str]
    meta_phone_number_id: Optional[str]
    meta_graph_api_version: Optional[str]
    inbound_queue_name: str
    outbound_queue_name: str
    outbound_failed_queue_name: str
    outbound_max_attempts: int
    outbound_worker_enabled: bool
    outbound_error_log_cooldown_seconds: float
    log_level: str

    @property
    def outbound_is_configured(self) -> bool:
        return bool(
            self.meta_access_token
            and self.meta_phone_number_id
            and self.meta_graph_api_version
        )

    @classmethod
    def from_env(cls, env_file: Path = DEFAULT_ENV_FILE) -> "Settings":
        file_values = _read_env_file(env_file)

        def value(key: str, default: str = "") -> str:
            val = os.environ.get(key, file_values.get(key, default)).strip()
            if (val.startswith('"') and val.endswith('"')) or (
                val.startswith("'") and val.endswith("'")
            ):
                val = val[1:-1].strip()
            return val

        required = {"REDIS_URL": value("REDIS_URL")}
        missing = [k for k, v in required.items() if not v]
        if missing:
            raise ValueError("Missing required settings: " + ", ".join(missing))

        max_attempts = int(value("OUTBOUND_MAX_ATTEMPTS", "3"))
        if max_attempts < 1:
            raise ValueError("OUTBOUND_MAX_ATTEMPTS must be at least one")

        error_log_cooldown = float(value("OUTBOUND_ERROR_LOG_COOLDOWN_SECONDS", "60"))
        if error_log_cooldown <= 0:
            raise ValueError("OUTBOUND_ERROR_LOG_COOLDOWN_SECONDS must be greater than zero")

        return cls(
            redis_url=required["REDIS_URL"],
            meta_verify_token=value("META_VERIFY_TOKEN") or None,
            meta_app_secret=value("META_APP_SECRET") or None,
            meta_access_token=value("META_ACCESS_TOKEN") or None,
            meta_phone_number_id=value("META_PHONE_NUMBER_ID") or None,
            meta_graph_api_version=value("META_GRAPH_API_VERSION") or None,
            inbound_queue_name=value("INBOUND_QUEUE_NAME", "whatsapp:inbound"),
            outbound_queue_name=value("OUTBOUND_QUEUE_NAME", "whatsapp:outbound"),
            outbound_failed_queue_name=value(
                "OUTBOUND_FAILED_QUEUE_NAME", "whatsapp:outbound:failed"
            ),
            outbound_max_attempts=max_attempts,
            outbound_worker_enabled=_as_bool(value("OUTBOUND_WORKER_ENABLED", "true"), True),
            outbound_error_log_cooldown_seconds=error_log_cooldown,
            log_level=value("LOG_LEVEL", "INFO"),
        )
