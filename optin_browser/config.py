"""Configuration loader for the opt-in Instagram browser toolkit."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
import pyotp


def _to_bool(value: Optional[str], default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _to_int(value: Optional[str], default: int) -> int:
    if value is None or value == "":
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _first_of(*values: Optional[str]) -> Optional[str]:
    for candidate in values:
        if candidate:
            return candidate
    return None


@dataclass(frozen=True)
class OptInConfig:
    """Dataclass with all runtime configuration used by the toolkit."""

    headless: bool
    proxy_url: Optional[str]
    ig_totp_secret: Optional[str]
    send_cooldown_seconds: int
    typing_min_ms: int
    typing_max_ms: int
    parallel_limit: int
    session_encryption_key: Optional[bytes]
    user_agent: Optional[str]
    locale: Optional[str]
    timezone_id: Optional[str]
    sessions_dir: Path
    audit_log_path: Path
    flows_dir: Path

    def generate_totp(self) -> Optional[str]:
        """Return a current TOTP code if a secret has been configured."""
        if not self.ig_totp_secret:
            return None
        totp = pyotp.TOTP(self.ig_totp_secret)
        return totp.now()


def _load_config() -> OptInConfig:
    base_dir = Path(__file__).resolve().parent.parent
    load_dotenv(dotenv_path=base_dir / ".env", override=False)

    sessions_dir = base_dir / "data" / "optin_sessions"
    flows_dir = base_dir / "data" / "flows"
    audit_log_path = base_dir / "logs" / "optin_audit.jsonl"
    sessions_dir.mkdir(parents=True, exist_ok=True)
    flows_dir.mkdir(parents=True, exist_ok=True)
    audit_log_path.parent.mkdir(parents=True, exist_ok=True)

    session_key = os.getenv("SESSION_ENCRYPTION_KEY")
    key_bytes = session_key.encode("utf-8") if session_key else None

    timezone_id = _first_of(
        os.getenv("OPTIN_TIMEZONE"),
        os.getenv("OPTIN_TIMEZONE_ID"),
        os.getenv("OPTIN_TZ_ID"),
        os.getenv("OPTIN_TZ"),
    )

    cfg = OptInConfig(
        headless=_to_bool(os.getenv("OPTIN_HEADLESS"), default=False),
        proxy_url=os.getenv("OPTIN_PROXY_URL") or None,
        ig_totp_secret=os.getenv("OPTIN_IG_TOTP") or None,
        send_cooldown_seconds=_to_int(
            os.getenv("OPTIN_SEND_COOLDOWN_SECONDS"), default=90
        ),
        typing_min_ms=_to_int(os.getenv("OPTIN_TYPING_MIN_MS"), default=60),
        typing_max_ms=_to_int(os.getenv("OPTIN_TYPING_MAX_MS"), default=180),
        parallel_limit=_to_int(os.getenv("OPTIN_PARALLEL_LIMIT"), default=3),
        session_encryption_key=key_bytes,
        user_agent=os.getenv("OPTIN_USER_AGENT") or None,
        locale=os.getenv("OPTIN_LOCALE") or None,
        timezone_id=timezone_id,
        sessions_dir=sessions_dir,
        audit_log_path=audit_log_path,
        flows_dir=flows_dir,
    )

    if cfg.typing_min_ms > cfg.typing_max_ms:
        object.__setattr__(cfg, "typing_min_ms", cfg.typing_max_ms)

    return cfg


cfg = _load_config()
