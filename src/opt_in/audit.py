"""Simple JSONL audit logger for opt-in Instagram workflows."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

_LOG_PATH = Path("logs/optin_audit.jsonl")


def _ensure_parent() -> None:
    _LOG_PATH.parent.mkdir(parents=True, exist_ok=True)


def record_event(
    account: str,
    event: str,
    status: str,
    message: str = "",
    meta: Optional[Dict[str, Any]] = None,
) -> None:
    """Record a structured audit entry.

    The payload follows a tiny schema so that downstream tooling can parse the log and
    compute daily summaries.  Secrets must *never* be persisted, therefore the helper
    filters out common sensitive keys defensively.
    """

    if not account:
        raise ValueError("account alias is required")
    if not event:
        raise ValueError("event name is required")
    if not status:
        raise ValueError("status is required")

    safe_meta: Dict[str, Any] = {}
    if meta:
        for key, value in meta.items():
            if key and key.lower() in {"password", "otp", "totp", "secret"}:
                continue
            safe_meta[key] = value

    payload = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "account": account,
        "event": event,
        "status": status,
    }
    if message:
        payload["message"] = message
    if safe_meta:
        payload["meta"] = safe_meta

    _ensure_parent()
    with _LOG_PATH.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=False) + "\n")
