# session_store.py
# -*- coding: utf-8 -*-
"""Compatibilidad con formatos de sesiones antiguas y nuevas."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Iterable, List

_BASE = Path(__file__).resolve().parent
_OLD_DIR = _BASE / ".sessions"
_NEW_DIR = _BASE / "storage" / "sessions"
_CLIENT_ALIAS_RE = re.compile(r"[^a-z0-9_-]+")


def _session_dirs() -> Iterable[Path]:
    return (_NEW_DIR, _OLD_DIR)


def _client_sessions_root() -> Path | None:
    root = os.environ.get("CLIENT_SESSIONS_ROOT")
    if not root:
        return None
    return Path(root).expanduser()


def _client_alias() -> str | None:
    alias = os.environ.get("CLIENT_ALIAS", "").strip().lower()
    if not alias:
        return None
    alias = alias.replace(" ", "-")
    alias = _CLIENT_ALIAS_RE.sub("-", alias)
    alias = alias.strip("-")
    return alias or None


def _client_session_dir() -> Path | None:
    root = _client_sessions_root()
    alias = _client_alias()
    if not root or not alias:
        return None
    return root / alias


def _client_candidates(username: str) -> List[Path]:
    directory = _client_session_dir()
    if not directory:
        return []
    return [
        directory / f"session_{username}.json",
        directory / f"{username}.json",
    ]


def session_candidates(username: str) -> list[Path]:
    username = username.strip().lstrip("@")
    candidates: List[Path] = []
    candidates.extend(_client_candidates(username))
    candidates.extend(directory / f"{username}.json" for directory in _session_dirs())
    return candidates


def has_session(username: str) -> bool:
    return any(path.exists() for path in session_candidates(username))


def load_into(client, username: str) -> Path:
    """Carga la primera sesión disponible en el cliente."""
    for path in session_candidates(username):
        if path.exists():
            client.load_settings(str(path))
            return path
    raise FileNotFoundError(f"No existe sesión guardada para {username}.")


def ensure_dirs() -> None:
    for directory in _session_dirs():
        directory.mkdir(parents=True, exist_ok=True)
    client_dir = _client_session_dir()
    if client_dir:
        client_dir.mkdir(parents=True, exist_ok=True)


def save_from(client, username: str) -> Path:
    """Guarda la sesión en el nuevo formato y replica en el legado."""
    ensure_dirs()
    username = username.strip().lstrip("@")
    client_path = None
    directory = _client_session_dir()
    if directory:
        client_path = directory / f"session_{username}.json"
        client.dump_settings(str(client_path))
    new_path = _NEW_DIR / f"{username}.json"
    client.dump_settings(str(new_path))
    # replica para mantener compatibilidad con scripts antiguos
    legacy_path = _OLD_DIR / f"{username}.json"
    client.dump_settings(str(legacy_path))
    return client_path or new_path


def remove(username: str) -> None:
    for path in session_candidates(username):
        try:
            if path.exists():
                path.unlink()
        except Exception:
            pass


def validate(client, username: str) -> bool:
    """Carga la sesión y realiza una llamada liviana para verificar validez."""
    try:
        load_into(client, username)
    except FileNotFoundError:
        return False
    try:
        client.get_timeline_feed()
    except Exception:
        return False
    return True
