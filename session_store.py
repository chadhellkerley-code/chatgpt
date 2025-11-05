# session_store.py
# -*- coding: utf-8 -*-
"""Compatibilidad con formatos de sesiones antiguas y nuevas."""

from __future__ import annotations

import base64
import contextlib
import hashlib
import json
import logging
import os
import re
import tempfile
import time
from pathlib import Path
from typing import Iterable, List, Optional

from cryptography.fernet import Fernet, InvalidToken

from paths import runtime_base

logger = logging.getLogger(__name__)

_BASE = runtime_base(Path(__file__).resolve().parent)
_BASE.mkdir(parents=True, exist_ok=True)
_OLD_DIR = _BASE / ".sessions"
_NEW_DIR = _BASE / "storage" / "sessions"
_CLIENT_ALIAS_RE = re.compile(r"[^a-z0-9_-]+")
_LOCK_SUFFIX = ".lock"
_LOCK_TIMEOUT = float(os.environ.get("SESSION_LOCK_TIMEOUT", "10"))
_MAGIC = b"IGSESS1"

_ENCRYPTION_KEY = os.environ.get("SESSION_ENCRYPTION_KEY", "").strip()


def _build_fernet(raw_key: str) -> Optional[Fernet]:
    if not raw_key:
        return None
    key_bytes = raw_key.strip().encode("utf-8")
    try:
        return Fernet(key_bytes)
    except Exception:
        digest = hashlib.sha256(key_bytes).digest()
        derived = base64.urlsafe_b64encode(digest)
        logger.warning("La clave de sesión no es un token válido; se derivó un secreto seguro.")
        return Fernet(derived)


_FERNET = _build_fernet(_ENCRYPTION_KEY)


def _fernet_instance() -> Optional[Fernet]:
    return _FERNET


def _lock_path(path: Path) -> Path:
    return path.with_suffix(path.suffix + _LOCK_SUFFIX)


class _FileLock:
    def __init__(self, path: Path, timeout: float) -> None:
        self._path = path
        self._timeout = timeout
        self._handle: Optional[object] = None

    def __enter__(self):
        start = time.time()
        delay = 0.05
        while True:
            self._handle = open(self._path, "a+b")
            try:
                if os.name == "nt":
                    import msvcrt  # type: ignore

                    msvcrt.locking(self._handle.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl  # type: ignore

                    fcntl.flock(self._handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                return self
            except OSError:
                self._handle.close()
                if time.time() - start >= self._timeout:
                    raise TimeoutError(f"No se pudo adquirir lock para {self._path}")
                time.sleep(delay)
                delay = min(delay * 1.7, 0.5)

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._handle is None:
            return
        try:
            if os.name == "nt":
                import msvcrt  # type: ignore

                msvcrt.locking(self._handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl  # type: ignore

                fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
        finally:
            self._handle.close()


def _atomic_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as tmp:
        tmp.write(data)
        tmp.flush()
        os.fsync(tmp.fileno())
        temp_name = tmp.name
    os.replace(temp_name, path)
    with contextlib.suppress(OSError):
        os.chmod(path, 0o600)


def _encrypt_payload(data: bytes) -> bytes:
    fernet = _fernet_instance()
    if not fernet:
        return data
    token = fernet.encrypt(data)
    return _MAGIC + token


def _decrypt_payload(data: bytes) -> bytes:
    if data.startswith(_MAGIC):
        fernet = _fernet_instance()
        if not fernet:
            raise RuntimeError(
                "Se encontró una sesión cifrada pero SESSION_ENCRYPTION_KEY no está configurada."
            )
        token = data[len(_MAGIC) :]
        return fernet.decrypt(token)
    return data


def _collect_settings_bytes(client) -> bytes:
    if hasattr(client, "get_settings"):
        settings = client.get_settings()
        if isinstance(settings, (bytes, bytearray)):
            return bytes(settings)
        if isinstance(settings, str):
            return settings.encode("utf-8")
        return json.dumps(settings, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    temp = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
    try:
        temp.close()
        client.dump_settings(temp.name)
        return Path(temp.name).read_bytes()
    finally:
        with contextlib.suppress(FileNotFoundError):
            os.remove(temp.name)


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
    if not root:
        return None
    alias = _client_alias()
    if alias:
        return root / alias
    return root


def _client_candidates(username: str) -> List[Path]:
    directory = _client_session_dir()
    if not directory:
        return []
    candidates: List[Path] = [
        directory / f"session_{username}.json",
        directory / f"{username}.json",
    ]
    root = _client_sessions_root()
    if root and root != directory:
        candidates.append(root / f"session_{username}.json")
        candidates.append(root / f"{username}.json")
    return candidates


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
        if not path.exists():
            continue
        lock_file = _lock_path(path)
        lock_file.parent.mkdir(parents=True, exist_ok=True)
        try:
            with _FileLock(lock_file, _LOCK_TIMEOUT):
                raw = path.read_bytes()
        except TimeoutError as exc:
            logger.warning("No se pudo obtener lock para la sesión %s: %s", path, exc)
            continue
        except OSError as exc:
            logger.warning("No se pudo leer la sesión %s: %s", path, exc)
            continue
        try:
            payload = _decrypt_payload(raw)
        except (InvalidToken, RuntimeError) as exc:
            logger.error("No se pudo descifrar la sesión %s: %s", path, exc)
            continue
        with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as tmp:
            tmp.write(payload)
            tmp.flush()
            temp_name = tmp.name
        try:
            client.load_settings(temp_name)
        finally:
            with contextlib.suppress(FileNotFoundError):
                os.remove(temp_name)
        return path
    raise FileNotFoundError(f"No existe sesión guardada para {username}.")


def ensure_dirs() -> None:
    for directory in _session_dirs():
        directory.mkdir(parents=True, exist_ok=True)
    client_dir = _client_session_dir()
    if client_dir:
        client_dir.mkdir(parents=True, exist_ok=True)
    else:
        root = _client_sessions_root()
        if root:
            root.mkdir(parents=True, exist_ok=True)


def save_from(client, username: str) -> Path:
    """Guarda la sesión en el nuevo formato y replica en el legado."""
    ensure_dirs()
    username = username.strip().lstrip("@")
    payload = _collect_settings_bytes(client)
    prepared = _encrypt_payload(payload)

    paths: List[Path] = []
    directory = _client_session_dir()
    if directory:
        paths.append(directory / f"session_{username}.json")
    new_path = _NEW_DIR / f"{username}.json"
    paths.append(new_path)
    paths.append(_OLD_DIR / f"{username}.json")

    saved_path: Optional[Path] = None
    for path in paths:
        path.parent.mkdir(parents=True, exist_ok=True)
        lock_file = _lock_path(path)
        lock_file.parent.mkdir(parents=True, exist_ok=True)
        try:
            with _FileLock(lock_file, _LOCK_TIMEOUT):
                _atomic_write(path, prepared)
        except TimeoutError as exc:
            logger.warning("No se pudo guardar la sesión en %s: %s", path, exc)
            continue
        except OSError as exc:
            logger.warning("Error escribiendo la sesión en %s: %s", path, exc)
            continue
        if saved_path is None:
            saved_path = path
    return saved_path or new_path


def remove(username: str) -> None:
    for path in session_candidates(username):
        try:
            if path.exists():
                path.unlink()
            lock_file = _lock_path(path)
            if lock_file.exists():
                lock_file.unlink()
        except Exception:
            pass


def list_saved_sessions() -> dict[str, Path]:
    """Devuelve un mapeo username -> Path con las sesiones detectadas."""

    def _directories() -> Iterable[Path]:
        yielded = set()
        for directory in _session_dirs():
            if directory not in yielded:
                yielded.add(directory)
                yield directory
        client_dir = _client_session_dir()
        if client_dir and client_dir not in yielded:
            yielded.add(client_dir)
            yield client_dir
        root = _client_sessions_root()
        if root and root not in yielded:
            yielded.add(root)
            yield root

    found: dict[str, Path] = {}
    for directory in _directories():
        try:
            entries = list(directory.glob("*.json"))
        except Exception:
            continue
        for path in entries:
            if not path.is_file():
                continue
            stem = path.stem
            if stem.startswith("session_"):
                username = stem[len("session_") :]
            else:
                username = stem
            username = username.strip().lstrip("@").lower()
            if not username or username in found:
                continue
            found[username] = path
    return found


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
