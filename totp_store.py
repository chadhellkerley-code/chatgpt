# totp_store.py
# -*- coding: utf-8 -*-
"""Secure storage and retrieval for TOTP secrets per account."""

from __future__ import annotations

import base64
import json
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pyotp
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from paths import runtime_base

logger = logging.getLogger(__name__)

_BASE = runtime_base(Path(__file__).resolve().parent)
_BASE.mkdir(parents=True, exist_ok=True)
_STORE = _BASE / "storage" / "totp"
_STORE.mkdir(parents=True, exist_ok=True)
_MASTER_FILE = _STORE / ".master_key"

_ITERATIONS = 390_000
_SALT_BYTES = 16


@dataclass(frozen=True)
class SecretRecord:
    salt: bytes
    ciphertext: bytes


def _passphrase() -> bytes:
    env_value = os.environ.get("TOTP_MASTER_KEY")
    if env_value:
        return env_value.encode("utf-8")
    if _MASTER_FILE.exists():
        return _MASTER_FILE.read_text(encoding="utf-8").strip().encode("utf-8")
    random_key = base64.urlsafe_b64encode(os.urandom(32)).decode("utf-8")
    _MASTER_FILE.write_text(random_key, encoding="utf-8")
    try:
        os.chmod(_MASTER_FILE, 0o600)
    except OSError:
        pass
    logger.info(
        "Se generó una passphrase local para cifrar secretos TOTP en %s.",
        _MASTER_FILE,
    )
    return random_key.encode("utf-8")


def _derive_key(salt: bytes) -> bytes:
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=_ITERATIONS,
    )
    return base64.urlsafe_b64encode(kdf.derive(_passphrase()))


def _fernet(salt: bytes) -> Fernet:
    return Fernet(_derive_key(salt))


def _path_for(username: str) -> Path:
    safe = re.sub(r"[^a-z0-9_-]", "_", username.lower())
    return _STORE / f"{safe}.json"


def _encode(record: SecretRecord) -> str:
    payload = {
        "salt": base64.urlsafe_b64encode(record.salt).decode("utf-8"),
        "ciphertext": base64.urlsafe_b64encode(record.ciphertext).decode("utf-8"),
    }
    return json.dumps(payload)


def _decode(raw: str) -> SecretRecord:
    data = json.loads(raw)
    salt = base64.urlsafe_b64decode(data["salt"])
    ciphertext = base64.urlsafe_b64decode(data["ciphertext"])
    return SecretRecord(salt=salt, ciphertext=ciphertext)


def _normalize_secret(raw: str) -> str:
    candidate = raw.strip()
    if not candidate:
        raise ValueError("Secreto vacío.")
    if candidate.lower().startswith("otpauth://"):
        try:
            parsed = pyotp.parse_uri(candidate)
            candidate = parsed.secret
        except Exception as exc:  # pragma: no cover - parse errors depend on pyotp internals
            raise ValueError("URI otpauth inválida.") from exc
    candidate = candidate.replace(" ", "")
    try:
        secret = pyotp.TOTP(candidate).secret
    except Exception as exc:
        raise ValueError("Secreto TOTP inválido.") from exc
    return secret


def save_secret(username: str, raw_secret: str) -> None:
    secret = _normalize_secret(raw_secret)
    salt = os.urandom(_SALT_BYTES)
    token = _fernet(salt).encrypt(secret.encode("utf-8"))
    record = SecretRecord(salt=salt, ciphertext=token)
    path = _path_for(username)
    path.write_text(_encode(record), encoding="utf-8")
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass
    logger.debug("Se almacenó el secreto TOTP cifrado para @%s", username)


def remove_secret(username: str) -> None:
    path = _path_for(username)
    if path.exists():
        path.unlink()
        logger.debug("Se eliminó el secreto TOTP de @%s", username)


def rename_secret(old_username: str, new_username: str) -> None:
    old_path = _path_for(old_username)
    new_path = _path_for(new_username)
    if not old_path.exists():
        return

    old_normalized = (old_username or "").strip().lstrip("@").lower()
    new_normalized = (new_username or "").strip().lstrip("@").lower()
    if not new_normalized or old_normalized == new_normalized:
        return

    try:
        new_path.parent.mkdir(parents=True, exist_ok=True)
        if new_path.exists():
            new_path.unlink()
        old_path.replace(new_path)
        try:
            os.chmod(new_path, 0o600)
        except OSError:
            pass
        logger.debug(
            "Se renombró el secreto TOTP de @%s a @%s",
            old_username,
            new_username,
        )
    except Exception as exc:  # pragma: no cover - operaciones de disco
        logger.warning(
            "No se pudo renombrar el TOTP de @%s a @%s: %s",
            old_username,
            new_username,
            exc,
        )


def has_secret(username: str) -> bool:
    return _path_for(username).exists()


def _load_secret(username: str) -> Optional[str]:
    path = _path_for(username)
    if not path.exists():
        return None
    try:
        record = _decode(path.read_text(encoding="utf-8"))
        decrypted = _fernet(record.salt).decrypt(record.ciphertext)
        return decrypted.decode("utf-8")
    except Exception as exc:  # pragma: no cover - unexpected corruption
        logger.error("No se pudo desencriptar el TOTP de @%s: %s", username, exc)
        return None


def generate_code(username: str) -> Optional[str]:
    secret = _load_secret(username)
    if not secret:
        return None
    try:
        return pyotp.TOTP(secret).now()
    except Exception as exc:  # pragma: no cover - pyotp errors
        logger.error("Error generando código TOTP para @%s: %s", username, exc)
        return None
