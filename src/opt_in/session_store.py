"""Helpers for persisting Playwright storage states for the opt-in Instagram flow."""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Optional

from cryptography.fernet import Fernet, InvalidToken

_STORAGE_DIR = Path("data/optin_sessions")


def _get_fernet() -> Optional[Fernet]:
    key = os.getenv("SESSION_ENCRYPTION_KEY")
    if not key:
        return None
    try:
        return Fernet(key.encode("utf-8"))
    except Exception as exc:  # pragma: no cover - defensive programming
        raise ValueError("Invalid SESSION_ENCRYPTION_KEY provided") from exc


def _ensure_storage_dir() -> None:
    _STORAGE_DIR.mkdir(parents=True, exist_ok=True)


async def save_storage_state(context: Any, account: str) -> Path:
    """Persist the storage state of ``context`` for the given ``account``.

    Parameters
    ----------
    context:
        A Playwright ``BrowserContext``.
    account:
        Alias used to namespace the session on disk.

    Returns
    -------
    Path
        Location where the storage state was persisted.
    """

    if not hasattr(context, "storage_state"):
        raise TypeError("context object does not expose storage_state method")

    _ensure_storage_dir()
    path = _STORAGE_DIR / f"{account}.json"
    data = await context.storage_state()
    blob = json.dumps(data, ensure_ascii=False).encode("utf-8")

    fernet = _get_fernet()
    if fernet:
        blob = fernet.encrypt(blob)
        path.write_bytes(blob)
    else:
        path.write_text(blob.decode("utf-8"), encoding="utf-8")
    return path


async def load_storage_state_dict(account: str) -> Optional[Dict[str, Any]]:
    """Load the storage state stored for ``account`` if available.

    The function returns ``None`` when the state is absent or cannot be decoded.  The
    caller may choose to re-run the login flow in such case.
    """

    path = _STORAGE_DIR / f"{account}.json"
    if not path.exists():
        return None

    raw = path.read_bytes()
    fernet = _get_fernet()
    if fernet:
        try:
            raw = fernet.decrypt(raw)
        except InvalidToken:
            return None
    try:
        return json.loads(raw.decode("utf-8"))
    except json.JSONDecodeError:
        return None


def mark_session_invalid(account: str) -> None:
    """Remove the persisted session for ``account``.

    Playwright contexts perform validation lazily.  When downstream logic detects a
    session is no longer valid we simply delete it so that future runs can fall back
    to the login flow.
    """

    path = _STORAGE_DIR / f"{account}.json"
    if path.exists():
        path.unlink()
