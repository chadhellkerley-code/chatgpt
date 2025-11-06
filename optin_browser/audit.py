"""Audit logging utilities for the opt-in toolkit."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict

from .config import cfg

_SENSITIVE_KEYS = {"password", "otp", "totp", "code", "token", "secret"}


def _sanitize_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    sanitized: Dict[str, Any] = {}
    for key, value in payload.items():
        if any(part in key.lower() for part in _SENSITIVE_KEYS):
            sanitized[key] = "***"
        else:
            sanitized[key] = value
    return sanitized


def log_event(event_type: str, **payload: Any) -> None:
    """Append a JSON line describing an event to the audit log."""
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event": event_type,
        "payload": _sanitize_payload(payload),
    }
    cfg.audit_log_path.parent.mkdir(parents=True, exist_ok=True)
    with cfg.audit_log_path.open("a", encoding="utf-8") as log_file:
        log_file.write(json.dumps(entry, ensure_ascii=False))
        log_file.write("\n")
