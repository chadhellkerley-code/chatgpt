# config.py
# -*- coding: utf-8 -*-
"""Carga y validación de parámetros de configuración."""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Tuple

from dotenv import dotenv_values, load_dotenv

_ROOT = Path(__file__).resolve().parent
_ENV_FILENAMES = (".env", ".env.local")
_CONFIG_FILE = _ROOT / "storage" / "config.json"
_LICENSE_PAYLOAD = _ROOT / "storage" / "license_payload.json"


def _load_file_values() -> Dict[str, str]:
    values: Dict[str, str] = {}
    for filename in _ENV_FILENAMES:
        file_path = _ROOT / filename
        if not file_path.exists():
            continue
        load_dotenv(file_path, override=filename != ".env")
        file_values = {
            key: value
            for key, value in dotenv_values(file_path).items()
            if value is not None
        }
        values.update(file_values)
    return values


def _coerce_int(value: str | None, default: int) -> int:
    if value is None or value == "":
        return default
    try:
        return int(value)
    except Exception:
        return default


def _coerce_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _coerce_path(value: str | None, default: Path) -> Path:
    if not value:
        return default
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = (_ROOT / path).resolve()
    return path


@dataclass(frozen=True)
class Settings:
    max_per_account: int = 10
    max_concurrency: int = 5
    delay_min: int = 45
    delay_max: int = 55
    autoresponder_delay: int = 10
    quiet: bool = False
    log_dir: Path = _ROOT / "storage" / "logs"
    log_file: str = "app.log"
    supabase_url: str = ""
    supabase_key: str = ""
    openai_api_key: str = ""
    client_distribution: bool = False
    proxy_default_url: str = ""
    proxy_default_user: str = ""
    proxy_default_pass: str = ""
    proxy_sticky_minutes: int = 10


def _validated_ranges(values: Dict[str, str]) -> Tuple[int, int, int, int]:
    max_per_account = _coerce_int(values.get("MAX_PER_ACCOUNT"), 10)
    if max_per_account < 2:
        logging.warning("MAX_PER_ACCOUNT debe ser >=2. Se ajusta a 2.")
        max_per_account = 2
    elif max_per_account > 50:
        logging.warning("MAX_PER_ACCOUNT máximo 50. Se ajusta a 50.")
        max_per_account = 50

    max_concurrency = _coerce_int(values.get("MAX_CONCURRENCY"), 5)
    if max_concurrency < 1:
        logging.warning("MAX_CONCURRENCY debe ser >=1. Se ajusta a 1.")
        max_concurrency = 1
    elif max_concurrency > 20:
        logging.warning("MAX_CONCURRENCY máximo 20. Se ajusta a 20.")
        max_concurrency = 20

    delay_min = _coerce_int(values.get("DELAY_MIN"), 45)
    if delay_min < 10:
        logging.warning("DELAY_MIN debe ser >=10. Se ajusta a 10.")
        delay_min = 10

    delay_max = _coerce_int(values.get("DELAY_MAX"), 55)
    if delay_max < delay_min:
        logging.warning("DELAY_MAX debe ser >= DELAY_MIN. Se ajusta a %s.", delay_min)
        delay_max = delay_min

    return max_per_account, max_concurrency, delay_min, delay_max


def _client_distribution_flag(env_values: Dict[str, str], default: bool) -> bool:
    env_value = env_values.get("CLIENT_DISTRIBUTION")
    if env_value is not None:
        return _coerce_bool(env_value, default)

    if _LICENSE_PAYLOAD.exists():
        try:
            payload = json.loads(_LICENSE_PAYLOAD.read_text(encoding="utf-8"))
        except Exception:
            return True

        edition = str(payload.get("edition", "")).strip().lower()
        if edition:
            return edition == "client"

        return bool(payload)

    return default


def load_settings() -> Settings:
    file_values = _load_file_values()
    env_values = {**file_values, **os.environ}
    max_per_account, max_concurrency, delay_min, delay_max = _validated_ranges(env_values)

    defaults = Settings()
    client_distribution = _client_distribution_flag(env_values, defaults.client_distribution)

    return Settings(
        max_per_account=max_per_account,
        max_concurrency=max_concurrency,
        delay_min=delay_min,
        delay_max=delay_max,
        autoresponder_delay=max(1, _coerce_int(env_values.get("AUTORESPONDER_DELAY"), defaults.autoresponder_delay)),
        quiet=_coerce_bool(env_values.get("QUIET"), defaults.quiet),
        log_dir=_coerce_path(env_values.get("LOG_DIR"), defaults.log_dir),
        log_file=env_values.get("LOG_FILE", defaults.log_file),
        supabase_url=env_values.get("SUPABASE_URL", ""),
        supabase_key=env_values.get("SUPABASE_KEY", ""),
        openai_api_key=env_values.get("OPENAI_API_KEY", ""),
        client_distribution=client_distribution,
        proxy_default_url=env_values.get("PROXY_DEFAULT_URL", defaults.proxy_default_url),
        proxy_default_user=env_values.get("PROXY_DEFAULT_USER", defaults.proxy_default_user),
        proxy_default_pass=env_values.get("PROXY_DEFAULT_PASS", defaults.proxy_default_pass),
        proxy_sticky_minutes=max(
            1,
            _coerce_int(
                env_values.get("PROXY_STICKY_MINUTES"), defaults.proxy_sticky_minutes
            ),
        ),
    )


def read_env_local() -> Dict[str, str]:
    path = _ROOT / ".env.local"
    if not path.exists():
        return {}
    data = {}
    for key, value in dotenv_values(path).items():
        if value is not None:
            data[key] = value
    return data


def update_env_local(updates: Dict[str, str]) -> Path:
    path = _ROOT / ".env.local"
    current = read_env_local()
    current.update({k: v for k, v in updates.items() if v is not None})
    lines = [f"{key}={value}" for key, value in sorted(current.items())]
    path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    return path


def read_app_config() -> Dict[str, str]:
    if not _CONFIG_FILE.exists():
        return {}
    try:
        return json.loads(_CONFIG_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def update_app_config(updates: Dict[str, str]) -> Dict[str, str]:
    current = read_app_config()
    current.update({k: v for k, v in updates.items() if v is not None})
    _CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
    _CONFIG_FILE.write_text(
        json.dumps(current, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return current


def refresh_settings() -> Settings:
    global SETTINGS
    SETTINGS = load_settings()
    return SETTINGS


SETTINGS = load_settings()
