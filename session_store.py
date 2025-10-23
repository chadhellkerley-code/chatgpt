# session_store.py
# -*- coding: utf-8 -*-
"""Compatibilidad con formatos de sesiones antiguas y nuevas."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

_BASE = Path(__file__).resolve().parent
_OLD_DIR = _BASE / ".sessions"
_NEW_DIR = _BASE / "storage" / "sessions"


def _session_dirs() -> Iterable[Path]:
    return (_NEW_DIR, _OLD_DIR)


def session_candidates(username: str) -> list[Path]:
    username = username.strip().lstrip("@")
    return [directory / f"{username}.json" for directory in _session_dirs()]


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


def save_from(client, username: str) -> Path:
    """Guarda la sesión en el nuevo formato y replica en el legado."""
    ensure_dirs()
    username = username.strip().lstrip("@")
    new_path = _NEW_DIR / f"{username}.json"
    client.dump_settings(str(new_path))
    # replica para mantener compatibilidad con scripts antiguos
    legacy_path = _OLD_DIR / f"{username}.json"
    client.dump_settings(str(legacy_path))
    return new_path


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
