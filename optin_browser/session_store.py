"""Encrypted session storage for Playwright storage state objects."""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, Optional, Union

from cryptography.fernet import Fernet, InvalidToken

from .config import cfg

StorageState = Union[str, Dict[str, Any]]


class SessionStoreError(RuntimeError):
    """Raised when a storage state cannot be saved or loaded."""


def _filename_for_account(account: str) -> Path:
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", account)
    return cfg.sessions_dir / f"{safe}.json"


def save_state(account: str, state: StorageState) -> None:
    """Persist a Playwright storage state for an account."""
    path = _filename_for_account(account)
    if isinstance(state, str):
        payload = state.encode("utf-8")
    else:
        payload = json.dumps(state).encode("utf-8")
    if cfg.session_encryption_key:
        fernet = Fernet(cfg.session_encryption_key)
        payload = fernet.encrypt(payload)
    path.write_bytes(payload)


def load_state(account: str) -> Optional[Dict[str, Any]]:
    """Load and decrypt a storage state for the account if present."""
    path = _filename_for_account(account)
    if not path.exists():
        return None
    payload = path.read_bytes()
    if cfg.session_encryption_key:
        fernet = Fernet(cfg.session_encryption_key)
        try:
            payload = fernet.decrypt(payload)
        except InvalidToken as exc:
            raise SessionStoreError("No se pudo descifrar la sesión almacenada.") from exc
    try:
        return json.loads(payload.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise SessionStoreError("La sesión almacenada está dañada.") from exc


def delete_state(account: str) -> None:
    """Remove the stored state for the given account if it exists."""
    path = _filename_for_account(account)
    if path.exists():
        path.unlink()
