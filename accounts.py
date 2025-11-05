# accounts.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import csv
import getpass
import io
import json
import random
import re
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import logging
from urllib.parse import urlparse

from config import SETTINGS
from proxy_manager import (
    ProxyConfig,
    apply_proxy_to_client,
    clear_proxy,
    default_proxy_settings,
    record_proxy_failure,
    should_retry_proxy,
    test_proxy_connection,
)
from session_store import has_session, load_into, remove as remove_session, save_from
from totp_store import generate_code as generate_totp_code
from totp_store import has_secret as has_totp_secret
from totp_store import remove_secret as remove_totp_secret
from totp_store import rename_secret as rename_totp_secret
from totp_store import save_secret as save_totp_secret
from utils import ask, banner, em, ok, press_enter, title, warn
from paths import runtime_base

BASE = runtime_base(Path(__file__).resolve().parent)
BASE.mkdir(parents=True, exist_ok=True)
DATA = BASE / "data"
DATA.mkdir(exist_ok=True)
FILE = DATA / "accounts.json"
_PASSWORD_FILE = DATA / "passwords.json"

_LOGIN_FAILURE_BACKOFF = timedelta(minutes=5)
_LOGIN_FAILURES: Dict[str, datetime] = {}
_LOGIN_FAILURE_LOCK = Lock()


def _password_key(username: str | None) -> str:
    if not username:
        return ""
    return username.strip().lstrip("@").lower()


def _load_password_cache() -> Dict[str, str]:
    if not _PASSWORD_FILE.exists():
        return {}
    try:
        raw = json.loads(_PASSWORD_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}
    cache: Dict[str, str] = {}
    if isinstance(raw, dict):
        for key, value in raw.items():
            if not isinstance(key, str) or not isinstance(value, str):
                continue
            normalized = key.strip().lower()
            if not normalized or not value:
                continue
            cache[normalized] = value
    return cache


def _save_password_cache(cache: Dict[str, str]) -> None:
    try:
        _PASSWORD_FILE.write_text(
            json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    except Exception:
        pass


def _record_login_failure(username: str) -> None:
    key = _password_key(username)
    if not key:
        return
    with _LOGIN_FAILURE_LOCK:
        _LOGIN_FAILURES[key] = datetime.utcnow()


def _clear_login_failure(username: str) -> None:
    key = _password_key(username)
    if not key:
        return
    with _LOGIN_FAILURE_LOCK:
        _LOGIN_FAILURES.pop(key, None)


def _login_backoff_remaining(username: str) -> float:
    key = _password_key(username)
    if not key:
        return 0.0
    with _LOGIN_FAILURE_LOCK:
        timestamp = _LOGIN_FAILURES.get(key)
        if not timestamp:
            return 0.0
        elapsed = datetime.utcnow() - timestamp
        if elapsed >= _LOGIN_FAILURE_BACKOFF:
            _LOGIN_FAILURES.pop(key, None)
            return 0.0
        return (_LOGIN_FAILURE_BACKOFF - elapsed).total_seconds()


_PASSWORD_CACHE: Dict[str, str] = _load_password_cache()

logger = logging.getLogger(__name__)

_HEALTH_CACHE_TTL = timedelta(minutes=15)
_HEALTH_CACHE: Dict[str, tuple[datetime, str]] = {}
_HEALTH_CACHE_LOCK = Lock()
_HEALTH_CACHE_FILE = DATA / "account_health.json"
_HEALTH_REFRESH_PENDING: set[str] = set()
_HEALTH_REFRESH_EXECUTOR = ThreadPoolExecutor(
    max_workers=3, thread_name_prefix="health-refresh"
)


_SENT_LOG = BASE / "storage" / "sent_log.jsonl"
_ACTIVITY_CACHE_TTL = timedelta(minutes=5)
_ACTIVITY_CACHE: Optional[Tuple[int, datetime, Dict[str, int]]] = None


_CSV_HEADERS = [
    "username",
    "password",
    "2fa code",
    "proxy id",
    "proxy port",
    "proxy username",
    "proxy password",
]


def _load_health_cache_from_disk() -> None:
    if not _HEALTH_CACHE_FILE.exists():
        return
    try:
        raw = json.loads(_HEALTH_CACHE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return
    entries: Dict[str, tuple[datetime, str]] = {}
    for key, entry in raw.items():
        if not isinstance(entry, dict):
            continue
        ts_raw = entry.get("timestamp")
        badge = entry.get("badge")
        if not ts_raw or not badge:
            continue
        try:
            ts = datetime.fromisoformat(ts_raw)
        except Exception:
            continue
        entries[key] = (ts, badge)
    if not entries:
        return
    with _HEALTH_CACHE_LOCK:
        _HEALTH_CACHE.update(entries)


def _persist_health_cache() -> None:
    try:
        with _HEALTH_CACHE_LOCK:
            serializable = {
                key: {"timestamp": ts.isoformat(), "badge": badge}
                for key, (ts, badge) in _HEALTH_CACHE.items()
            }
        _HEALTH_CACHE_FILE.write_text(
            json.dumps(serializable, ensure_ascii=False), encoding="utf-8"
        )
    except Exception:
        pass


_load_health_cache_from_disk()


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _isoformat_utc(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_datetime(value: object) -> Optional[datetime]:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _settings_value(name: str, default: int) -> int:
    try:
        value = getattr(SETTINGS, name)
    except AttributeError:
        return default
    try:
        return int(value)
    except Exception:
        return default


def _ensure_timestamp(record: Dict, key: str) -> Tuple[Optional[datetime], bool]:
    original = record.get(key)
    parsed = _parse_datetime(original)
    if parsed is None:
        record.pop(key, None)
        return None, bool(original)
    formatted = _isoformat_utc(parsed)
    if formatted != original:
        record[key] = formatted
        return parsed, True
    return parsed, False


def _ensure_first_seen(record: Dict) -> bool:
    first_seen, changed = _ensure_timestamp(record, "first_seen")
    if first_seen is not None:
        return changed
    now_iso = _isoformat_utc(_now_utc())
    record["first_seen"] = now_iso
    return True


def _normalize_profile_edit_metadata(record: Dict) -> None:
    try:
        count = int(record.get("profile_edit_count", 0))
    except Exception:
        count = 0
    record["profile_edit_count"] = max(0, count)

    types_raw = record.get("profile_edit_types")
    if isinstance(types_raw, list):
        normalized = sorted({str(item).strip() for item in types_raw if str(item).strip()})
    else:
        normalized = []
    record["profile_edit_types"] = normalized

    _ensure_timestamp(record, "last_profile_edit")


def _recent_activity_counts() -> Dict[str, int]:
    global _ACTIVITY_CACHE
    window_hours = max(1, _settings_value("low_profile_activity_window_hours", 48))
    now = _now_utc()
    if _ACTIVITY_CACHE is not None:
        cached_window, timestamp, cached_counts = _ACTIVITY_CACHE
        if cached_window == window_hours and now - timestamp < _ACTIVITY_CACHE_TTL:
            return dict(cached_counts)

    counts: Dict[str, int] = defaultdict(int)
    cutoff = now - timedelta(hours=window_hours)
    if _SENT_LOG.exists():
        try:
            with _SENT_LOG.open("r", encoding="utf-8") as handle:
                for line in handle:
                    if not line.strip():
                        continue
                    try:
                        entry = json.loads(line)
                    except Exception:
                        continue
                    ts_raw = entry.get("ts")
                    if ts_raw is None:
                        continue
                    try:
                        ts = datetime.fromtimestamp(float(ts_raw), tz=timezone.utc)
                    except Exception:
                        continue
                    if ts < cutoff:
                        continue
                    username = str(entry.get("account") or "").strip().lstrip("@").lower()
                    if not username:
                        continue
                    counts[username] += 1
        except Exception:
            counts = defaultdict(int)

    frozen = dict(counts)
    _ACTIVITY_CACHE = (window_hours, now, frozen)
    return frozen


def _auto_low_profile(record: Dict) -> Tuple[bool, str, int]:
    username = (record.get("username") or "").strip().lstrip("@").lower()
    if not username:
        return False, "", 0

    _ensure_first_seen(record)
    first_seen = _parse_datetime(record.get("first_seen"))
    age_days = 0.0
    if first_seen is not None:
        age_days = (_now_utc() - first_seen).total_seconds() / 86400

    recent_activity_map = _recent_activity_counts()
    recent_activity = int(recent_activity_map.get(username, 0))
    record["recent_activity_count"] = recent_activity

    age_limit = max(1, _settings_value("low_profile_age_days", 14))
    edits_threshold = max(1, _settings_value("low_profile_profile_edit_threshold", 3))
    activity_threshold = max(0, _settings_value("low_profile_activity_threshold", 30))

    is_new = age_days < age_limit
    edit_count = int(record.get("profile_edit_count", 0) or 0)
    has_many_edits = edit_count >= edits_threshold
    has_high_activity = activity_threshold > 0 and recent_activity >= activity_threshold

    reasons: list[str] = []
    if is_new:
        reasons.append(f"cuenta nueva ({int(age_days)}d)")
    if has_many_edits:
        reasons.append(f"{edit_count} cambios de perfil")
    if has_high_activity:
        window_hours = max(1, _settings_value("low_profile_activity_window_hours", 48))
        reasons.append(f"{recent_activity} envíos/{window_hours}h")

    should_flag = is_new and (has_many_edits or has_high_activity)
    reason_text = "; ".join(reasons) if should_flag else ""
    return should_flag, reason_text, recent_activity


def _record_profile_edit(username: str, kind: str) -> None:
    normalized = username.strip().lstrip("@").lower()
    if not normalized:
        return

    items = _load()
    updated = False
    for idx, item in enumerate(items):
        stored = (item.get("username") or "").strip().lstrip("@").lower()
        if stored != normalized:
            continue
        record = dict(item)
        try:
            count = int(record.get("profile_edit_count", 0))
        except Exception:
            count = 0
        record["profile_edit_count"] = max(0, count) + 1

        types_raw = record.get("profile_edit_types")
        types: list[str]
        if isinstance(types_raw, list):
            types = [str(entry).strip() for entry in types_raw if str(entry).strip()]
        else:
            types = []
        kind_clean = kind.strip()
        if kind_clean and kind_clean not in types:
            types.append(kind_clean)
        record["profile_edit_types"] = sorted(set(types))
        record["last_profile_edit"] = _isoformat_utc(_now_utc())
        items[idx] = record
        updated = True
        break

    if updated:
        _save(items)

def _normalize_account(record: Dict) -> Dict:
    result = dict(record)
    result.setdefault("alias", "default")
    result.setdefault("active", True)
    result.setdefault("connected", False)
    result.setdefault("password", "")
    result.setdefault("proxy_url", "")
    result.setdefault("proxy_user", "")
    result.setdefault("proxy_pass", "")
    sticky_default = SETTINGS.proxy_sticky_minutes or 10
    try:
        sticky_value = int(result.get("proxy_sticky_minutes", sticky_default))
    except Exception:
        sticky_value = sticky_default
    result["proxy_sticky_minutes"] = max(1, sticky_value)

    _normalize_profile_edit_metadata(result)
    _ensure_first_seen(result)

    username = result.get("username")
    if username:
        key = _password_key(username)
        if not result.get("password") and key:
            cached = _PASSWORD_CACHE.get(key)
            if cached:
                result["password"] = cached
        result["has_totp"] = has_totp_secret(username)
    else:
        result.setdefault("has_totp", False)

    manual_override = bool(result.get("low_profile_manual"))
    auto_flag, auto_reason, recent_activity = _auto_low_profile(result)
    result["recent_activity_count"] = recent_activity
    result["low_profile_auto"] = auto_flag

    if manual_override:
        manual_value = bool(result.get("low_profile"))
        result["low_profile"] = manual_value
        existing_reason = str(result.get("low_profile_reason") or "")
        if manual_value and not existing_reason:
            result["low_profile_reason"] = "Marcado manualmente"
        elif not manual_value:
            result["low_profile_reason"] = existing_reason
        result["low_profile_source"] = "manual" if manual_value else ""
    else:
        result["low_profile"] = auto_flag
        result["low_profile_reason"] = auto_reason if auto_flag else ""
        result["low_profile_source"] = "auto" if auto_flag else ""
        result.setdefault("low_profile_manual", False)

    return result


def _prepare_for_save(record: Dict) -> Dict:
    stored = dict(record)
    if stored.get("proxy_url"):
        try:
            stored["proxy_sticky_minutes"] = int(
                stored.get("proxy_sticky_minutes", SETTINGS.proxy_sticky_minutes)
            )
        except Exception:
            stored["proxy_sticky_minutes"] = SETTINGS.proxy_sticky_minutes
    else:
        stored.pop("proxy_url", None)
        stored.pop("proxy_user", None)
        stored.pop("proxy_pass", None)
        stored.pop("proxy_sticky_minutes", None)
    stored.pop("has_totp", None)
    stored.pop("recent_activity_count", None)
    stored.pop("low_profile_auto", None)
    stored.pop("low_profile_source", None)

    manual_override = bool(stored.get("low_profile_manual"))
    if manual_override:
        stored["low_profile"] = bool(stored.get("low_profile"))
        reason = str(stored.get("low_profile_reason") or "")
        if reason:
            stored["low_profile_reason"] = reason
        else:
            stored.pop("low_profile_reason", None)
    else:
        stored.pop("low_profile", None)
        stored.pop("low_profile_reason", None)
        stored.pop("low_profile_manual", None)

    return stored


def _load() -> List[Dict]:
    if not FILE.exists():
        return []
    try:
        data = json.loads(FILE.read_text(encoding="utf-8"))
    except Exception:
        return []
    normalized: List[Dict] = []
    changed = False
    for item in data:
        if not isinstance(item, dict):
            continue
        normalized_item = _normalize_account(item)
        normalized.append(normalized_item)
        if item.get("first_seen") != normalized_item.get("first_seen"):
            changed = True
        if int(item.get("profile_edit_count", 0) or 0) != normalized_item.get(
            "profile_edit_count", 0
        ):
            changed = True
        original_types = item.get("profile_edit_types") if isinstance(item, dict) else []
        if isinstance(original_types, list):
            original_sorted = sorted({str(v).strip() for v in original_types if str(v).strip()})
        else:
            original_sorted = []
        if original_sorted != normalized_item.get("profile_edit_types", []):
            changed = True
    if changed:
        try:
            _save(normalized)
        except Exception:
            pass
    return normalized


def _save(items: List[Dict]) -> None:
    cleaned = [_prepare_for_save(_normalize_account(it)) for it in items]
    FILE.write_text(json.dumps(cleaned, ensure_ascii=False, indent=2), encoding="utf-8")


def _parse_accounts_csv(path: Path) -> List[Dict[str, str]]:
    raw_text = path.read_text(encoding="utf-8-sig")
    if not raw_text.strip():
        return []

    buffer = io.StringIO(raw_text)
    reader = csv.DictReader(buffer)
    normalized_rows: List[Dict[str, str]] = []
    mapping: Dict[str, str] = {}

    if reader.fieldnames:
        lowered = {name.strip().lower(): name for name in reader.fieldnames if name}
        if all(header in lowered for header in _CSV_HEADERS):
            mapping = {header: lowered[header] for header in _CSV_HEADERS}

    if mapping:
        for row in reader:
            normalized = {
                header: (row.get(actual) or "").strip()
                for header, actual in mapping.items()
            }
            if not any(normalized.values()):
                continue
            normalized_rows.append(normalized)
        return normalized_rows

    buffer = io.StringIO(raw_text)
    plain_reader = csv.reader(buffer)
    for row_index, row in enumerate(plain_reader):
        if not row:
            continue
        candidate = [cell.strip().lower() for cell in row[: len(_CSV_HEADERS)]]
        if row_index == 0 and candidate == _CSV_HEADERS:
            continue
        normalized = {
            header: row[idx].strip() if idx < len(row) else ""
            for idx, header in enumerate(_CSV_HEADERS)
        }
        if not any(normalized.values()):
            continue
        normalized_rows.append(normalized)
    return normalized_rows


def _compose_proxy_url(identifier: str, port: str) -> str:
    base = identifier.strip()
    if not base:
        return ""
    if "://" not in base:
        base = f"http://{base}"
    if port:
        trimmed = base.rstrip("/")
        if trimmed.count(":") <= 1:
            base = f"{trimmed}:{port}"
        else:
            base = trimmed
    return base


def list_all() -> List[Dict]:
    return _load()


def _find(items: List[Dict], username: str) -> Optional[Dict]:
    username = username.lower()
    for it in items:
        if it.get("username", "").lower() == username:
            return it
    return None


def get_account(username: str) -> Optional[Dict]:
    items = _load()
    account = _find(items, username)
    return _normalize_account(account) if account else None


def update_account(username: str, updates: Dict) -> bool:
    items = _load()
    username_norm = username.lower()
    for idx, item in enumerate(items):
        if item.get("username", "").lower() == username_norm:
            updated = dict(item)
            updated.update(updates)
            items[idx] = _normalize_account(updated)
            _save(items)
            _invalidate_health(username)
            return True
    return False


def _prompt_totp(username: str) -> bool:
    while True:
        raw = ask("TOTP Secret / otpauth URI (opcional): ").strip()
        if not raw:
            return False
        try:
            save_totp_secret(username, raw)
            ok("Se guardó el TOTP cifrado para esta cuenta.")
            return True
        except ValueError as exc:
            warn(f"No se pudo guardar el TOTP: {exc}")
            retry = ask("¿Reintentar ingreso de TOTP? (s/N): ").strip().lower()
            if retry != "s":
                return False


@dataclass(frozen=True)
class _LoginTwoFactorPayload:
    code: str
    mode: str
    source: str


_TOTP_SECRET_KEYS = (
    "totp_secret",
    "totp seed",
    "totp_seed",
    "totp key",
    "totp_key",
    "totp uri",
    "totp_uri",
    "authenticator_secret",
    "authenticator",
    "2fa_secret",
    "two_factor_secret",
)


def _ingest_totp_secret_from_account(account: Dict) -> None:
    username = (account.get("username") or "").strip()
    if not username or has_totp_secret(username):
        return

    for key in _TOTP_SECRET_KEYS:
        raw = account.get(key)
        if not raw:
            continue
        candidate = str(raw).strip()
        if not candidate:
            continue
        try:
            save_totp_secret(username, candidate)
            logger.debug(
                "Se almacenó el secreto TOTP definido en '%s' para @%s durante el login.",
                key,
                username,
            )
        except ValueError as exc:
            logger.warning(
                "Se ignoró el secreto TOTP incluido en '%s' para @%s: %s",
                key,
                username,
                exc,
            )
        finally:
            break

    account["has_totp"] = has_totp_secret(username)


def _two_factor_payload_for_login(account: Dict) -> Optional[_LoginTwoFactorPayload]:
    username = (account.get("username") or "").strip()
    if username:
        _ingest_totp_secret_from_account(account)
        if has_totp_secret(username):
            code = generate_totp_code(username)
            if code:
                return _LoginTwoFactorPayload(code=code, mode="totp", source="totp_store")
            logger.warning(
                "No se pudo generar el código TOTP automático para @%s. Revisá el secreto almacenado.",
                username,
            )

    for key in ("totp_code", "two_factor_code", "2fa_code"):
        value = str(account.get(key) or "").strip()
        if value:
            return _LoginTwoFactorPayload(code=value, mode=key, source="manual")

    return None


def _two_factor_mode_from_info(info: Dict[str, Any]) -> str:
    if not isinstance(info, dict):
        return "unknown"

    if info.get("totp_two_factor_on") or info.get("is_totp_two_factor_enabled"):
        return "totp"
    if info.get("whatsapp_two_factor_on") or info.get("should_use_whatsapp_token"):
        return "whatsapp"
    if info.get("sms_two_factor_on") or info.get("is_sms_two_factor_enabled"):
        return "sms"

    method = str(info.get("verification_method") or "").strip()
    if method == "3":
        return "totp"
    if method == "5":
        return "whatsapp"
    if method == "1":
        return "sms"
    return "unknown"


def add_account(username: str, alias: str, proxy: Optional[Dict] = None) -> bool:
    items = _load()
    if _find(items, username):
        warn("Ya existe.")
        return False
    record = {
        "username": username.strip().lstrip("@"),
        "alias": alias,
        "active": True,
        "connected": False,
    }
    if proxy:
        record.update(proxy)
    items.append(_normalize_account(record))
    _save(items)
    _invalidate_health(username)
    ok("Agregada.")
    return True


def remove_account(username: str) -> None:
    items = _load()
    new_items = [it for it in items if it.get("username", "").lower() != username.lower()]
    _save(new_items)
    remove_session(username)
    remove_totp_secret(username)
    clear_proxy(username)
    _invalidate_health(username)
    key = _password_key(username)
    if key and key in _PASSWORD_CACHE:
        _PASSWORD_CACHE.pop(key, None)
        _save_password_cache(_PASSWORD_CACHE)
    ok("Eliminada (si existía).")


def set_active(username: str, is_active: bool = True) -> None:
    if update_account(username, {"active": is_active}):
        _invalidate_health(username)
        ok("Actualizada.")
    else:
        warn("No existe.")


def mark_connected(username: str, connected: bool) -> None:
    update_account(username, {"connected": connected})
    _invalidate_health(username)


def _proxy_config_from_inputs(data: Dict) -> ProxyConfig:
    return ProxyConfig(
        url=data.get("proxy_url", ""),
        user=data.get("proxy_user") or None,
        password=data.get("proxy_pass") or None,
        sticky_minutes=int(data.get("proxy_sticky_minutes", SETTINGS.proxy_sticky_minutes)),
    )


def _prompt_proxy_settings(existing: Optional[Dict] = None) -> Dict:
    defaults = default_proxy_settings()
    current = existing or {}
    print("\nConfiguración de proxy (opcional)")
    base_default = current.get("proxy_url") or defaults["url"]
    prompt_default = base_default or "sin proxy"
    raw_url = ask(f"Proxy URL [{prompt_default}]: ").strip()
    if raw_url.lower() in {"-", "none", "sin", "no"}:
        url = ""
    elif not raw_url and base_default:
        url = base_default
    else:
        url = raw_url

    user_default = current.get("proxy_user") or defaults["user"]
    user_prompt = user_default or "(sin definir)"
    proxy_user = ask(f"Usuario (opcional) [{user_prompt}]: ").strip() or user_default

    pass_default = current.get("proxy_pass") or defaults["password"]
    pass_prompt = "***" if pass_default else "(sin definir)"
    proxy_pass = ask(f"Password (opcional) [{pass_prompt}]: ").strip() or pass_default

    sticky_default = current.get("proxy_sticky_minutes") or defaults["sticky"]
    sticky_input = ask(f"Sticky minutes [{sticky_default}]: ").strip()
    try:
        sticky = int(sticky_input) if sticky_input else int(sticky_default)
    except Exception:
        sticky = int(defaults["sticky"] or 10)
    sticky = max(1, sticky)

    proxy_url = url.strip()
    data = {
        "proxy_url": proxy_url,
        "proxy_user": (proxy_user or "").strip(),
        "proxy_pass": (proxy_pass or "").strip(),
        "proxy_sticky_minutes": sticky,
    }

    if not proxy_url:
        return {"proxy_url": "", "proxy_user": "", "proxy_pass": "", "proxy_sticky_minutes": sticky}

    if ask("¿Probar proxy ahora? (s/N): ").strip().lower() == "s":
        try:
            result = test_proxy_connection(_proxy_config_from_inputs(data))
            ok(f"Proxy OK. IP detectada: {result.public_ip} (latencia {result.latency:.2f}s)")
        except Exception as exc:
            warn(f"Proxy falló: {exc}")
            retry = ask("¿Reintentar configuración? (s/N): ").strip().lower()
            if retry == "s":
                return _prompt_proxy_settings(existing)
    return data


def _test_existing_proxy(account: Dict) -> None:
    if not account.get("proxy_url"):
        warn("La cuenta no tiene proxy configurado.")
        return
    try:
        result = test_proxy_connection(_proxy_config_from_inputs(account))
        ok(f"Proxy OK. IP detectada: {result.public_ip} (latencia {result.latency:.2f}s)")
    except Exception as exc:
        warn(f"Error probando proxy: {exc}")


def _launch_hashtag_mode(alias: str) -> None:
    try:
        from actions import hashtag_mode
    except Exception as exc:  # pragma: no cover - módulo opcional
        warn(f"No se pudo iniciar el modo hashtag: {exc}")
        press_enter()
        return

    accounts = [acct for acct in _load() if acct.get("alias") == alias]
    active_accounts = [acct for acct in accounts if acct.get("active")]
    if not active_accounts:
        warn("No hay cuentas activas en este alias para ejecutar el modo hashtag.")
        press_enter()
        return

    print("Seleccioná cuentas activas (coma separada, * para todas):")
    for idx, acct in enumerate(active_accounts, start=1):
        sess = "[sesión]" if has_session(acct["username"]) else "[sin sesión]"
        proxy_flag = _proxy_indicator(acct)
        low_flag = _low_profile_indicator(acct)
        totp_flag = _totp_indicator(acct)
        print(f" {idx}) @{acct['username']} {sess} {proxy_flag}{low_flag}{totp_flag}")
        if low_flag and acct.get("low_profile_reason"):
            print(f"    ↳ {acct['low_profile_reason']}")
    raw = ask("Selección: ").strip()
    if not raw:
        warn("Sin selección.")
        press_enter()
        return

    if raw == "*":
        chosen = [acct["username"] for acct in active_accounts]
    else:
        selected: set[str] = set()
        for part in raw.split(","):
            part = part.strip()
            if not part:
                continue
            if part.isdigit():
                idx = int(part)
                if 1 <= idx <= len(active_accounts):
                    selected.add(active_accounts[idx - 1]["username"])
            else:
                selected.add(part.lstrip("@"))
        chosen = [acct["username"] for acct in active_accounts if acct["username"] in selected]

    if not chosen:
        warn("No se encontraron cuentas con esos datos.")
        press_enter()
        return

    hashtag_mode.run_from_menu(chosen)


def _launch_content_publisher(alias: str) -> None:
    try:
        from actions import content_publisher
    except Exception as exc:  # pragma: no cover - módulo opcional
        warn(f"No se pudo iniciar el módulo de publicaciones: {exc}")
        press_enter()
        return

    content_publisher.run_from_menu(alias)


def _launch_interactions(alias: str) -> None:
    try:
        from actions import interactions
    except Exception as exc:  # pragma: no cover - módulo opcional
        warn(f"No se pudo iniciar el módulo de interacciones: {exc}")
        press_enter()
        return

    interactions.run_from_menu(alias)


def _login_and_save_session(
    account: Dict, password: str, *, respect_backoff: bool = True
) -> bool:
    """Login con instagrapi y guarda sesión en storage/sessions."""

    username = account["username"]
    if respect_backoff:
        remaining = _login_backoff_remaining(username)
        if remaining > 0:
            logger.debug(
                "Omitiendo login automático para @%s (reintentar en %.0fs)",
                username,
                remaining,
            )
            return False
    try:
        from instagrapi import Client, exceptions as ig_exceptions
    except Exception as exc:
        logger.debug("No se pudo crear el cliente de Instagram: %s", exc)
        return False

    try:
        cl = Client()
    except Exception as exc:
        logger.debug("Error inicializando instagrapi para @%s: %s", username, exc)
        return False

    binding = None
    try:
        binding = apply_proxy_to_client(cl, username, account, reason="login")
    except Exception as exc:
        if account.get("proxy_url"):
            record_proxy_failure(username, exc)
        logger.debug("No se pudo aplicar el proxy de @%s: %s", username, exc)

    payload = _two_factor_payload_for_login(account)
    verification_code = payload.code if payload else ""
    if payload and payload.mode == "totp":
        logger.debug("Aplicando TOTP automático para @%s", username)

    jitter = random.uniform(1.5, 3.5)
    time.sleep(jitter)

    try:
        cl.login(username, password, verification_code=verification_code)
        save_from(cl, username)
        mark_connected(username, True)
        _clear_login_failure(username)
        account["has_totp"] = has_totp_secret(username)
        ok(f"Sesión guardada para {username}.")
        return True
    except ig_exceptions.BadPassword:
        warn(f"Contraseña incorrecta para @{username}. Actualizá la clave y reintentá.")
    except ig_exceptions.UserNotFound:
        warn(
            f"Instagram indicó que la cuenta @{username} no existe o fue deshabilitada."
        )
    except ig_exceptions.TwoFactorRequired:
        info = getattr(cl, "last_json", {}).get("two_factor_info", {})
        mode = _two_factor_mode_from_info(info)
        if mode == "whatsapp":
            warn(
                "Esta cuenta requiere verificación externa por WhatsApp. El login automático no es posible."
            )
        elif mode == "sms":
            warn(
                "Esta cuenta requiere verificación externa por SMS. El login automático no es posible."
            )
        elif payload and payload.mode == "totp":
            warn(
                f"Instagram rechazó el código TOTP automático para @{username}. Revisá el secreto en tu autenticador."
            )
        else:
            warn(
                f"Instagram solicitó un código 2FA para @{username}. Guardá el secreto TOTP o ingresá el código manualmente."
            )
    except ig_exceptions.ChallengeRequired:
        warn(
            f"Instagram solicitó un challenge adicional para @{username}. Resolvilo manualmente para continuar."
        )
    except ig_exceptions.CheckpointChallengeRequired:
        warn(
            f"Instagram bloqueó el login de @{username} con un checkpoint. Debés verificar la cuenta manualmente."
        )
    except Exception as exc:
        if should_retry_proxy(exc):
            record_proxy_failure(username, exc)
            warn(f"Problema con el proxy de @{username}: {exc}")
        else:
            warn(f"No se pudo iniciar sesión para {username}: {exc}")
        mark_connected(username, False)
        _record_login_failure(username)
        return False

    mark_connected(username, False)
    _record_login_failure(username)
    return False


def _authorization_payload(client: Any) -> Dict[str, Any]:
    """Extract the authorization payload from an instagrapi client."""

    candidates: list[dict[str, Any] | None] = []

    auth = getattr(client, "authorization_data", None)
    if isinstance(auth, dict):
        candidates.append(auth)

    try:
        settings = client.get_settings()
        if isinstance(settings, dict):
            candidates.append(settings.get("authorization_data"))
    except Exception:
        pass

    for payload in candidates:
        if isinstance(payload, dict):
            return payload

    return {}


def has_valid_session_settings(client: Any) -> bool:
    """Return True if the loaded client contains a usable session token."""

    payload = _authorization_payload(client)
    session_id = str(payload.get("sessionid") or payload.get("session_id") or "").strip()
    user_id = str(payload.get("user_id") or payload.get("ds_user_id") or "").strip()
    return bool(session_id and user_id)


def _session_active(
    username: str,
    *,
    account: Optional[Dict] = None,
    reason: str = "session-check",
) -> bool:
    if not username or not has_session(username):
        return False

    account = account or get_account(username)

    try:
        from instagrapi import Client
    except Exception as exc:  # pragma: no cover - dependencia externa opcional
        logger.debug("No se pudo importar instagrapi para validar sesión: %s", exc)
        return False

    try:
        cl = Client()
    except Exception as exc:  # pragma: no cover - inicialización opcional
        logger.debug("No se pudo crear el cliente de Instagram: %s", exc)
        return False

    binding = None
    try:
        binding = apply_proxy_to_client(cl, username, account, reason=reason)
    except Exception as exc:
        if account and account.get("proxy_url"):
            record_proxy_failure(username, exc)
        logger.debug("No se pudo aplicar el proxy de @%s: %s", username, exc)

    try:
        load_into(cl, username)
    except FileNotFoundError:
        mark_connected(username, False)
        return False
    except Exception as exc:
        if binding and should_retry_proxy(exc):
            record_proxy_failure(username, exc)
        mark_connected(username, False)
        logger.debug("Error cargando sesión para @%s: %s", username, exc)
        return False

    if has_valid_session_settings(cl):
        mark_connected(username, True)
        return True

    mark_connected(username, False)
    logger.debug("La sesión cargada para @%s no contiene credenciales activas.", username)
    return False


def auto_login_with_saved_password(
    username: str, *, account: Optional[Dict] = None
) -> bool:
    """Intenta iniciar sesión reutilizando la contraseña almacenada."""

    account = account or get_account(username)
    if not account:
        return False

    if _session_active(username, account=account, reason="auto-login-check"):
        return True

    stored_password = _account_password(account).strip()
    if not stored_password:
        return False

    return _login_and_save_session(account, stored_password)


def prompt_login(username: str, *, interactive: bool = True) -> bool:
    account = get_account(username)
    if not account:
        warn("No existe la cuenta indicada.")
        return False

    if _session_active(username, account=account, reason="prompt-login"):
        return True
    stored_password = _account_password(account).strip()
    original_stored = stored_password
    attempted_auto = False

    if stored_password:
        attempted_auto = True
        if auto_login_with_saved_password(username, account=account):
            return True

    while True:
        if attempted_auto and stored_password:
            changed = (
                ask("¿Cambiaste la contraseña de esta cuenta? (s/N): ")
                .strip()
                .lower()
            )
            if changed != "s":
                warn(
                    "Instagram rechazó la sesión guardada. Posiblemente haya un challenge o chequeo de seguridad pendiente."
                )
                return False
            password = getpass.getpass(
                f"Nueva password @{account['username']}: "
            )
        else:
            password = getpass.getpass(
                f"Password @{account['username']}: "
            )

        if not password:
            warn("Se canceló el inicio de sesión.")
            return False

        success = _login_and_save_session(
            account, password, respect_backoff=False
        )
        if success:
            if password != original_stored:
                _store_account_password(username, password)
            return True

        attempted_auto = False
        stored_password = ""
        if interactive and (
            ask("¿Intentar ingresar nuevamente? (s/N): ")
            .strip()
            .lower()
            == "s"
        ):
            continue
        return False


def _low_profile_indicator(account: Dict) -> str:
    return f" {em('🌱 bajo perfil')}" if account.get("low_profile") else ""


def _proxy_indicator(account: Dict) -> str:
    return f" {em('🛡️')}" if account.get("proxy_url") else ""


def _totp_indicator(account: Dict) -> str:
    return f" {em('🔐')}" if account.get("has_totp") else ""


def _health_cache_key(username: str) -> str:
    return username.strip().lstrip("@").lower()


def _invalidate_health(username: str) -> None:
    key = _health_cache_key(username)
    if key:
        with _HEALTH_CACHE_LOCK:
            if key in _HEALTH_CACHE:
                _HEALTH_CACHE.pop(key, None)
        _persist_health_cache()


def _health_cached(username: str) -> tuple[str | None, bool]:
    key = _health_cache_key(username)
    if not key:
        return None, True
    with _HEALTH_CACHE_LOCK:
        cached = _HEALTH_CACHE.get(key)
    if not cached:
        return None, True
    timestamp, badge = cached
    expired = datetime.utcnow() - timestamp >= _HEALTH_CACHE_TTL
    return badge, expired


def _store_health(username: str, badge: str) -> str:
    key = _health_cache_key(username)
    if key:
        with _HEALTH_CACHE_LOCK:
            _HEALTH_CACHE[key] = (datetime.utcnow(), badge)
        _persist_health_cache()
    return badge


def _badge_for_display(account: Dict) -> tuple[str, bool]:
    username = account.get("username", "")
    cached_badge, expired = _health_cached(username)
    if cached_badge:
        return cached_badge, expired
    return "[🟡 En riesgo: unknown]", True


def _account_status_from_badge(account: Dict, badge: str) -> str:
    if not account.get("active"):
        return "inactiva"

    lowered = (badge or "").lower()
    if "desactivada" in lowered:
        return "baneada"
    if any(keyword in lowered for keyword in ("action_block", "challenge", "checkpoint")):
        return "bloqueada"
    if "sesión expirada" in lowered or "sesion expirada" in lowered:
        return "no se puede iniciar sesión"
    if not account.get("connected"):
        return "no se puede iniciar sesión"
    return "activa"


def _proxy_status_from_badge(account: Dict, badge: str) -> str:
    lowered = (badge or "").lower()
    if "proxy" in lowered and any(term in lowered for term in ("caído", "caido", "bloqueado")):
        return "bloqueado"
    return "activo"


def _current_totp_code(username: str) -> str:
    if not username:
        return ""
    try:
        code = generate_totp_code(username)
    except Exception:
        return ""
    return code or ""


def _proxy_components(account: Dict) -> tuple[str, str, str, str]:
    raw_url = (account.get("proxy_url") or "").strip()
    ip = ""
    port = ""
    if raw_url:
        parsed = urlparse(raw_url if "://" in raw_url else f"http://{raw_url}")
        ip = (parsed.hostname or "").strip()
        port = str(parsed.port) if parsed.port else ""
    proxy_user = (account.get("proxy_user") or "").strip()
    proxy_pass = (account.get("proxy_pass") or "").strip()
    return ip, port, proxy_user, proxy_pass


def _alias_slug(alias: str) -> str:
    candidate = re.sub(r"[^0-9A-Za-z_-]", "_", alias.strip())
    candidate = candidate.strip("_")
    return candidate or "default"


def _export_path(alias: str) -> Path:
    base_dir = Path.home() / "Desktop" / "archivos CSV"
    base_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{_alias_slug(alias)}_accounts_{timestamp}.csv"
    return base_dir / filename


def _account_password(account: Dict) -> str:
    value = account.get("password")
    if isinstance(value, str) and value:
        return value
    key = _password_key(account.get("username"))
    if key:
        cached = _PASSWORD_CACHE.get(key)
        if cached:
            return cached
    return ""


def _store_account_password(username: str, password: str) -> None:
    if not password:
        return
    update_account(username, {"password": password})
    key = _password_key(username)
    if not key:
        return
    if _PASSWORD_CACHE.get(key) == password:
        return
    _PASSWORD_CACHE[key] = password
    _save_password_cache(_PASSWORD_CACHE)


def _export_accounts_csv(alias: str) -> None:
    accounts = [acct for acct in _load() if acct.get("alias") == alias]
    destination = _export_path(alias)
    headers = [
        "Username",
        "Contraseña",
        "Código 2FA",
        "Proxy IP",
        "Proxy Puerto",
        "Proxy Usuario",
        "Proxy Contraseña",
        "Estado de la cuenta",
        "Estado del proxy",
    ]

    with destination.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(headers)
        for account in accounts:
            username = (account.get("username") or "").strip()
            badge, _ = _badge_for_display(account)
            account_status = _account_status_from_badge(account, badge)
            proxy_status = _proxy_status_from_badge(account, badge)
            proxy_ip, proxy_port, proxy_user, proxy_pass = _proxy_components(account)
            writer.writerow(
                [
                    username,
                    _account_password(account),
                    _current_totp_code(username),
                    proxy_ip,
                    proxy_port,
                    proxy_user,
                    proxy_pass,
                    account_status,
                    proxy_status,
                ]
            )

    ok(f"Archivo CSV generado en: {destination}")
    press_enter()


def _prompt_destination_alias(current_alias: str) -> Optional[str]:
    items = _load()
    aliases = sorted({(it.get("alias") or "default") for it in items} | {"default"})
    alias_lookup = {alias.lower(): alias for alias in aliases}
    normalized_current = current_alias.lower()
    if normalized_current not in alias_lookup:
        alias_lookup[normalized_current] = current_alias
        aliases.append(current_alias)

    if aliases:
        print("\nAlias disponibles para mover: " + ", ".join(sorted(set(aliases))))

    while True:
        destination = ask("Alias destino (Enter para cancelar): ").strip()
        if not destination:
            return None

        normalized = destination.lower()
        if normalized == normalized_current:
            warn("El alias destino es el mismo que el origen. Seleccioná otro alias.")
            continue

        if normalized in alias_lookup:
            return alias_lookup[normalized]

        create = (
            ask(
                f"El alias '{destination}' no existe. ¿Crear automáticamente y continuar? (s/N): "
            )
            .strip()
            .lower()
        )
        if create == "s":
            ok(f"Alias '{destination}' creado.")
            return destination


def _move_accounts_to_alias(alias: str) -> None:
    usernames = _select_usernames_for_modifications(alias)
    if not usernames:
        return

    destination = _prompt_destination_alias(alias)
    if not destination:
        warn("Operación cancelada.")
        press_enter()
        return

    selected = {username.lower() for username in usernames if username}
    if not selected:
        warn("No se seleccionaron cuentas válidas.")
        press_enter()
        return

    items = _load()
    moved: set[str] = set()
    for idx, item in enumerate(items):
        username = (item.get("username") or "").strip()
        if not username:
            continue
        if item.get("alias") != alias:
            continue
        if username.lower() not in selected:
            continue
        updated = dict(item)
        updated["alias"] = destination
        items[idx] = _normalize_account(updated)
        moved.add(username)

    if not moved:
        warn("No se movieron cuentas.")
        press_enter()
        return

    _save(items)
    for username in moved:
        _invalidate_health(username)

    ok(f"Se movieron {len(moved)} cuenta(s) al alias '{destination}'.")
    press_enter()


def _schedule_health_refresh(accounts_to_refresh: List[Dict]) -> None:
    if SETTINGS.client_distribution:
        return
    for account in accounts_to_refresh:
        username = account.get("username", "")
        key = _health_cache_key(username)
        if not key:
            continue
        with _HEALTH_CACHE_LOCK:
            if key in _HEALTH_REFRESH_PENDING:
                continue
            _HEALTH_REFRESH_PENDING.add(key)

        def _task(acc: Dict, cache_key: str, uname: str):
            try:
                badge = _compute_health_badge(acc)
                _store_health(uname, badge)
            finally:
                with _HEALTH_CACHE_LOCK:
                    _HEALTH_REFRESH_PENDING.discard(cache_key)

        try:
            _HEALTH_REFRESH_EXECUTOR.submit(_task, dict(account), key, username)
        except Exception:
            with _HEALTH_CACHE_LOCK:
                _HEALTH_REFRESH_PENDING.discard(key)


def _import_accounts_from_csv(alias: str) -> None:
    path_input = ask("Ruta del archivo CSV: ").strip()
    if not path_input:
        warn("No se indicó la ruta del archivo.")
        press_enter()
        return

    path = Path(path_input).expanduser()
    if not path.exists() or not path.is_file():
        warn("El archivo CSV indicado no existe o no es un archivo válido.")
        press_enter()
        return

    try:
        rows = _parse_accounts_csv(path)
    except Exception as exc:
        warn(f"No se pudo leer el CSV: {exc}")
        press_enter()
        return

    if not rows:
        warn("El archivo CSV no contiene registros válidos.")
        press_enter()
        return

    proxy_defaults = default_proxy_settings()
    sticky_value = proxy_defaults.get("sticky") or SETTINGS.proxy_sticky_minutes or 10
    try:
        sticky_minutes = int(sticky_value)
    except Exception:
        sticky_minutes = SETTINGS.proxy_sticky_minutes or 10
    sticky_minutes = max(1, sticky_minutes)

    total = len(rows)
    successes = 0
    errors: List[tuple[int, str]] = []

    for idx, row in enumerate(rows, start=1):
        username = (row.get("username") or "").strip().lstrip("@")
        password = (row.get("password") or "").strip()
        totp_value = (row.get("2fa code") or "").strip()
        proxy_id = (row.get("proxy id") or "").strip()
        proxy_port = (row.get("proxy port") or "").strip()
        proxy_user = (row.get("proxy username") or "").strip()
        proxy_pass = (row.get("proxy password") or "").strip()

        fields = [
            ("Username", username),
            ("Password", password),
            ("2FA Code", totp_value),
            ("Proxy ID", proxy_id),
            ("Proxy Port", proxy_port),
            ("Proxy Username", proxy_user),
            ("Proxy Password", proxy_pass),
        ]
        missing = [label for label, value in fields if not value]
        if missing:
            errors.append((idx, f"Campos incompletos: {', '.join(missing)}"))
            continue

        proxy_url = _compose_proxy_url(proxy_id, proxy_port)
        if not proxy_url:
            errors.append((idx, "Configuración de proxy inválida."))
            continue

        proxy_data = {
            "proxy_url": proxy_url,
            "proxy_user": proxy_user,
            "proxy_pass": proxy_pass,
            "proxy_sticky_minutes": sticky_minutes,
        }

        added = add_account(username, alias, proxy_data)
        if not added:
            errors.append((idx, "No se pudo agregar la cuenta (posible duplicado)."))
            continue

        if totp_value:
            try:
                save_totp_secret(username, totp_value)
            except ValueError as exc:
                remove_account(username)
                errors.append((idx, f"2FA inválido: {exc}"))
                continue

        successes += 1

        _store_account_password(username, password)

        account = get_account(username)
        if account and not _login_and_save_session(account, password):
            errors.append((idx, "La cuenta se agregó pero el inicio de sesión falló."))

    print("\nResumen de importación:")
    print(f"Total de cuentas procesadas: {total}")
    print(f"Cuentas agregadas correctamente: {successes}")
    print(f"Cuentas con error: {len(errors)}")
    if errors:
        warn("Detalle de errores:")
        for row_number, message in errors:
            print(f" - Fila {row_number}: {message}")
    press_enter()


def _select_usernames_for_modifications(alias: str) -> List[str]:
    group = [acct for acct in _load() if acct.get("alias") == alias]
    if not group:
        warn("No hay cuentas disponibles en este alias.")
        press_enter()
        return []

    print("Seleccioná cuentas por número o username (coma separada, * para todas):")
    alias_map: Dict[str, str] = {}
    for idx, acct in enumerate(group, start=1):
        username = (acct.get("username") or "").strip()
        if not username:
            continue
        alias_map[username.lower()] = username
        sess = "[sesión]" if has_session(username) else "[sin sesión]"
        proxy_flag = _proxy_indicator(acct)
        low_flag = _low_profile_indicator(acct)
        totp_flag = _totp_indicator(acct)
        print(f" {idx}) @{username} {sess} {proxy_flag}{low_flag}{totp_flag}")
        if low_flag and acct.get("low_profile_reason"):
            print(f"    ↳ {acct['low_profile_reason']}")

    raw = ask("Selección: ").strip()
    if not raw:
        warn("Sin selección.")
        press_enter()
        return []

    if raw == "*":
        return [acct.get("username") for acct in group if acct.get("username")]

    chosen: List[str] = []
    seen: set[str] = set()
    for part in raw.split(","):
        chunk = part.strip()
        if not chunk:
            continue
        if chunk.isdigit():
            idx = int(chunk)
            if 1 <= idx <= len(group):
                username = group[idx - 1].get("username")
                if username:
                    key = username.lower()
                    if key not in seen:
                        seen.add(key)
                        chosen.append(username)
        else:
            normalized = chunk.lstrip("@").lower()
            username = alias_map.get(normalized)
            if username and normalized not in seen:
                seen.add(normalized)
                chosen.append(username)

    if not chosen:
        warn("No se encontraron cuentas con esos datos.")
        press_enter()
        return []

    return chosen


def _resolve_accounts_for_modifications(
    alias: str, usernames: List[str]
) -> List[Optional[Dict]]:
    if not usernames:
        return []

    records = [acct for acct in _load() if acct.get("alias") == alias]
    mapping: Dict[str, Dict] = {}
    for acct in records:
        username = (acct.get("username") or "").strip()
        if username:
            mapping[username.lower()] = acct

    resolved: List[Optional[Dict]] = []
    missing: List[str] = []
    for username in usernames:
        key = (username or "").strip().lstrip("@").lower()
        acct = mapping.get(key)
        if acct:
            resolved.append(acct)
        else:
            resolved.append(None)
            missing.append(username)

    if missing:
        formatted = ", ".join(f"@{name}" for name in missing if name)
        if formatted:
            warn(f"No se encontraron estas cuentas: {formatted}")

    return resolved


def _ask_delay_seconds(default: float = 5.0) -> float:
    prompt = ask(f"Delay entre cuentas en segundos [{default:.0f}]: ").strip()
    if not prompt:
        return max(1.0, default)
    try:
        value = float(prompt.replace(",", "."))
    except ValueError:
        warn("Valor inválido, se utilizará el delay por defecto.")
        return max(1.0, default)
    return max(1.0, value)


def _client_for_account_action(account: Dict, *, reason: str):
    username = (account.get("username") or "").strip()
    if not username:
        return None

    try:
        from instagrapi import Client
    except Exception as exc:  # pragma: no cover - dependencia opcional
        warn(f"No se pudo importar instagrapi para @{username}: {exc}")
        return None

    try:
        cl = Client()
    except Exception as exc:
        warn(f"No se pudo crear el cliente de Instagram para @{username}: {exc}")
        return None

    binding = None
    try:
        binding = apply_proxy_to_client(cl, username, account, reason=reason)
    except Exception as exc:
        logger.warning("No se pudo aplicar el proxy para @%s: %s", username, exc)
        binding = None

    try:
        load_into(cl, username)
    except FileNotFoundError:
        warn(
            f"No hay sesión guardada para @{username}. Iniciá sesión antes de modificar."
        )
        return None
    except Exception as exc:
        if binding and should_retry_proxy(exc):
            record_proxy_failure(username, exc)
        warn(f"No se pudo cargar la sesión de @{username}: {exc}")
        return None

    if not has_valid_session_settings(cl):
        mark_connected(username, False)
        warn(
            f"La sesión guardada para @{username} no contiene credenciales activas. Iniciá sesión nuevamente."
        )
        return None

    mark_connected(username, True)
    return cl


def _rename_account_record(old_username: str, new_username: str) -> str:
    old_clean = (old_username or "").strip().lstrip("@")
    new_clean = (new_username or "").strip().lstrip("@")
    if not new_clean:
        return old_clean

    items = _load()
    old_norm = old_clean.lower()
    changed = False
    for idx, item in enumerate(items):
        stored = (item.get("username") or "").strip().lstrip("@").lower()
        if stored == old_norm:
            updated = dict(item)
            updated["username"] = new_clean
            items[idx] = _normalize_account(updated)
            changed = True
            break

    if changed:
        _save(items)

    if old_clean:
        _invalidate_health(old_clean)
    if new_clean:
        _invalidate_health(new_clean)

    try:
        rename_totp_secret(old_clean, new_clean)
    except Exception as exc:  # pragma: no cover - operaciones de disco
        logger.warning(
            "No se pudo trasladar el TOTP de @%s a @%s: %s", old_clean, new_clean, exc
        )

    return new_clean


def _apply_username_change(account: Dict, desired_username: str, delay: float) -> Optional[str]:
    username = (account.get("username") or "").strip()
    client = _client_for_account_action(account, reason="mod-username")
    if not client:
        time.sleep(delay)
        return None

    desired_clean = desired_username.strip().lstrip("@")
    if not desired_clean:
        time.sleep(delay)
        return None

    actual_username = desired_clean
    try:
        result = client.account_edit(username=desired_clean)
        actual_username = getattr(result, "username", None) or desired_clean
        ok(f"@{username} → @{actual_username}")
        try:
            save_from(client, actual_username)
        except Exception as exc:
            logger.warning(
                "No se pudo guardar la sesión actualizada de @%s: %s",
                actual_username,
                exc,
            )
        normalized = _rename_account_record(username, actual_username)
        if username.strip().lower() != normalized.lower():
            remove_session(username)
        mark_connected(normalized, True)
        _record_profile_edit(normalized, "username")
        return normalized
    except Exception as exc:
        if should_retry_proxy(exc):
            record_proxy_failure(username, exc)
        warn(f"No se pudo actualizar el username de @{username}: {exc}")
        mark_connected(username, False)
        return None
    finally:
        time.sleep(delay)


def _change_usernames_flow(alias: str, selected: List[str]) -> List[str]:
    resolved = _resolve_accounts_for_modifications(alias, selected)
    if not any(resolved):
        warn("No hay cuentas válidas seleccionadas.")
        press_enter()
        return selected

    inputs: List[str] = []
    for acct in resolved:
        if acct:
            value = ask(f"Nuevo username para @{acct['username']} (vacío para omitir): ")
            inputs.append(value)
        else:
            inputs.append("")

    if not any(inp.strip() for inp, acct in zip(inputs, resolved) if acct):
        warn("No se ingresaron nuevos usernames.")
        press_enter()
        return selected

    delay = _ask_delay_seconds()
    total_targets = 0
    successes = 0
    for idx, acct in enumerate(resolved):
        if not acct:
            continue
        desired = inputs[idx].strip()
        if not desired:
            continue
        total_targets += 1
        updated = _apply_username_change(acct, desired, delay)
        if updated:
            successes += 1
            selected[idx] = updated

    print(f"Usernames actualizados: {successes}/{total_targets}")
    press_enter()
    return selected


def _apply_full_name_change(account: Dict, full_name: str, delay: float) -> bool:
    username = (account.get("username") or "").strip()
    client = _client_for_account_action(account, reason="mod-full-name")
    if not client:
        time.sleep(delay)
        return False

    try:
        client.account_edit(full_name=full_name)
        ok(f"Nombre actualizado para @{username}.")
        mark_connected(username, True)
        _record_profile_edit(username, "full_name")
        return True
    except Exception as exc:
        if should_retry_proxy(exc):
            record_proxy_failure(username, exc)
        warn(f"No se pudo actualizar el nombre completo de @{username}: {exc}")
        mark_connected(username, False)
        return False
    finally:
        time.sleep(delay)


def _change_full_name_flow(alias: str, selected: List[str]) -> None:
    resolved = _resolve_accounts_for_modifications(alias, selected)
    if not any(resolved):
        warn("No hay cuentas válidas seleccionadas.")
        press_enter()
        return

    values: List[str] = []
    for acct in resolved:
        if acct:
            values.append(ask(f"Nombre completo para @{acct['username']} (vacío para mantener): "))
        else:
            values.append("")

    if not any(val.strip() for val, acct in zip(values, resolved) if acct):
        warn("No se ingresaron nombres para actualizar.")
        press_enter()
        return

    delay = _ask_delay_seconds()
    total = 0
    successes = 0
    for acct, value in zip(resolved, values):
        if not acct:
            continue
        value = value.strip()
        if not value:
            continue
        total += 1
        if _apply_full_name_change(acct, value, delay):
            successes += 1

    print(f"Nombres completos actualizados: {successes}/{total}")
    press_enter()


def _apply_bio_change(account: Dict, biography: str, delay: float) -> bool:
    username = (account.get("username") or "").strip()
    client = _client_for_account_action(account, reason="mod-bio")
    if not client:
        time.sleep(delay)
        return False

    try:
        client.account_set_biography(biography)
        action = "eliminada" if not biography else "actualizada"
        ok(f"Bio {action} para @{username}.")
        mark_connected(username, True)
        _record_profile_edit(username, "bio")
        return True
    except Exception as exc:
        if should_retry_proxy(exc):
            record_proxy_failure(username, exc)
        warn(f"No se pudo actualizar la bio de @{username}: {exc}")
        mark_connected(username, False)
        return False
    finally:
        time.sleep(delay)


def _change_bio_flow(alias: str, selected: List[str]) -> None:
    resolved = _resolve_accounts_for_modifications(alias, selected)
    if not any(resolved):
        warn("No hay cuentas válidas seleccionadas.")
        press_enter()
        return

    bios: List[str] = []
    for acct in resolved:
        if acct:
            bios.append(ask(f"Bio para @{acct['username']} (vacío = eliminar): "))
        else:
            bios.append("")

    if not any(acct for acct in resolved):
        warn("No se encontraron cuentas para actualizar.")
        press_enter()
        return

    delay = _ask_delay_seconds()
    total = 0
    successes = 0
    for acct, bio in zip(resolved, bios):
        if not acct:
            continue
        biography = bio or ""
        total += 1
        if _apply_bio_change(acct, biography, delay):
            successes += 1

    print(f"Bios actualizadas/eliminadas: {successes}/{total}")
    press_enter()


def _apply_profile_picture(account: Dict, image_path: Path, delay: float) -> bool:
    username = (account.get("username") or "").strip()
    client = _client_for_account_action(account, reason="mod-profile-picture")
    if not client:
        time.sleep(delay)
        return False

    try:
        client.account_change_picture(image_path)
        ok(f"Foto de perfil actualizada para @{username}.")
        mark_connected(username, True)
        _record_profile_edit(username, "profile_picture")
        return True
    except Exception as exc:
        if should_retry_proxy(exc):
            record_proxy_failure(username, exc)
        warn(f"No se pudo cambiar la foto de @{username}: {exc}")
        mark_connected(username, False)
        return False
    finally:
        time.sleep(delay)


def _apply_profile_picture_removal(account: Dict, delay: float) -> bool:
    username = (account.get("username") or "").strip()
    client = _client_for_account_action(account, reason="mod-profile-picture")
    if not client:
        time.sleep(delay)
        return False

    try:
        client.private_request(
            "accounts/remove_profile_picture/", client.with_default_data({})
        )
        ok(f"Foto de perfil eliminada para @{username}.")
        mark_connected(username, True)
        _record_profile_edit(username, "profile_picture")
        return True
    except Exception as exc:
        if should_retry_proxy(exc):
            record_proxy_failure(username, exc)
        warn(f"No se pudo eliminar la foto de @{username}: {exc}")
        mark_connected(username, False)
        return False
    finally:
        time.sleep(delay)


def _profile_photo_flow(alias: str, selected: List[str]) -> None:
    resolved = _resolve_accounts_for_modifications(alias, selected)
    if not any(resolved):
        warn("No hay cuentas válidas seleccionadas.")
        press_enter()
        return

    while True:
        print("\n1) Subir una imagen para todas las cuentas")
        print("2) Subir imágenes individuales")
        print("3) Eliminar la foto actual")
        print("4) Volver")
        choice = ask("Opción: ").strip() or "4"

        if choice == "1":
            path_input = ask("Ruta de la imagen: ").strip()
            if not path_input:
                warn("No se indicó la ruta del archivo.")
                press_enter()
                continue
            path = Path(path_input).expanduser()
            if not path.exists() or not path.is_file():
                warn("La imagen indicada no existe o no es un archivo válido.")
                press_enter()
                continue
            delay = _ask_delay_seconds()
            total = 0
            successes = 0
            for acct in resolved:
                if not acct:
                    continue
                total += 1
                if _apply_profile_picture(acct, path, delay):
                    successes += 1
            print(f"Fotos actualizadas: {successes}/{total}")
            press_enter()
        elif choice == "2":
            delay = _ask_delay_seconds()
            total = 0
            successes = 0
            for acct in resolved:
                if not acct:
                    continue
                raw = ask(
                    f"Ruta de imagen para @{acct['username']} (vacío para omitir): "
                ).strip()
                if not raw:
                    continue
                path = Path(raw).expanduser()
                if not path.exists() or not path.is_file():
                    warn(
                        f"Archivo inválido para @{acct['username']}. Se omitirá esta cuenta."
                    )
                    continue
                total += 1
                if _apply_profile_picture(acct, path, delay):
                    successes += 1
            print(f"Fotos actualizadas: {successes}/{total}")
            press_enter()
        elif choice == "3":
            delay = _ask_delay_seconds()
            total = 0
            successes = 0
            for acct in resolved:
                if not acct:
                    continue
                total += 1
                if _apply_profile_picture_removal(acct, delay):
                    successes += 1
            print(f"Fotos eliminadas: {successes}/{total}")
            press_enter()
        elif choice == "4":
            break
        else:
            warn("Opción inválida.")
            press_enter()


def _apply_highlight_cleanup(account: Dict, delay: float) -> bool:
    username = (account.get("username") or "").strip()
    client = _client_for_account_action(account, reason="mod-highlights")
    if not client:
        time.sleep(delay)
        return False

    try:
        user_id = client.user_id or client.user_id_from_username(username)
        highlights = client.user_highlights(user_id)
        deleted = 0
        for item in highlights:
            try:
                if client.highlight_delete(item.id):
                    deleted += 1
            except Exception as exc:
                if should_retry_proxy(exc):
                    record_proxy_failure(username, exc)
                logger.warning(
                    "Error eliminando historia destacada de @%s: %s", username, exc
                )
        ok(f"Historias destacadas eliminadas para @{username}: {deleted}")
        mark_connected(username, True)
        return True
    except Exception as exc:
        if should_retry_proxy(exc):
            record_proxy_failure(username, exc)
        warn(f"No se pudieron eliminar las destacadas de @{username}: {exc}")
        mark_connected(username, False)
        return False
    finally:
        time.sleep(delay)


def _delete_highlights_flow(alias: str, selected: List[str]) -> None:
    resolved = _resolve_accounts_for_modifications(alias, selected)
    if not any(resolved):
        warn("No hay cuentas válidas seleccionadas.")
        press_enter()
        return

    delay = _ask_delay_seconds()
    total = 0
    successes = 0
    for acct in resolved:
        if not acct:
            continue
        total += 1
        if _apply_highlight_cleanup(acct, delay):
            successes += 1

    print(f"Cuentas con historias destacadas eliminadas: {successes}/{total}")
    press_enter()


def _apply_posts_cleanup(account: Dict, delay: float) -> bool:
    username = (account.get("username") or "").strip()
    client = _client_for_account_action(account, reason="mod-posts")
    if not client:
        time.sleep(delay)
        return False

    try:
        user_id = client.user_id or client.user_id_from_username(username)
        medias = client.user_medias(user_id, amount=0)
        deleted = 0
        failures = 0
        for media in medias:
            try:
                if client.media_delete(media.id):
                    deleted += 1
                else:
                    failures += 1
            except Exception as exc:
                if should_retry_proxy(exc):
                    record_proxy_failure(username, exc)
                failures += 1
                logger.warning(
                    "Error eliminando publicación de @%s: %s", username, exc
                )
        ok(f"Publicaciones eliminadas para @{username}: {deleted}")
        if failures:
            warn(
                f"@{username}: {failures} publicaciones no pudieron eliminarse automáticamente."
            )
        mark_connected(username, True)
        return True
    except Exception as exc:
        if should_retry_proxy(exc):
            record_proxy_failure(username, exc)
        warn(f"No se pudieron eliminar las publicaciones de @{username}: {exc}")
        mark_connected(username, False)
        return False
    finally:
        time.sleep(delay)


def _delete_posts_flow(alias: str, selected: List[str]) -> None:
    resolved = _resolve_accounts_for_modifications(alias, selected)
    if not any(resolved):
        warn("No hay cuentas válidas seleccionadas.")
        press_enter()
        return

    delay = _ask_delay_seconds()
    total = 0
    successes = 0
    for acct in resolved:
        if not acct:
            continue
        total += 1
        if _apply_posts_cleanup(acct, delay):
            successes += 1

    print(f"Cuentas con publicaciones eliminadas: {successes}/{total}")
    press_enter()


def _modification_menu(alias: str) -> None:
    selected: List[str] = []
    while True:
        banner()
        title(f"Modificación de cuentas de Instagram - Alias: {alias}")
        if selected:
            print("Cuentas seleccionadas: " + ", ".join(f"@{name}" for name in selected))
        else:
            print("Cuentas seleccionadas: (ninguna)")

        print("\n1) Seleccionar cuentas a modificar")
        print("2) Cambiar usernames")
        print("3) Cambiar nombres completos (Full name)")
        print("4) Cambiar o eliminar biografía (bio)")
        print("5) Cambiar o eliminar foto de perfil")
        print("6) Eliminar historias destacadas")
        print("7) Eliminar publicaciones existentes")
        print("8) Volver\n")

        choice = ask("Opción: ").strip() or "8"

        if choice == "1":
            selected = _select_usernames_for_modifications(alias)
        elif choice == "2":
            if not selected:
                warn("Seleccioná cuentas primero.")
                press_enter()
                continue
            selected = _change_usernames_flow(alias, selected)
        elif choice == "3":
            if not selected:
                warn("Seleccioná cuentas primero.")
                press_enter()
                continue
            _change_full_name_flow(alias, selected)
        elif choice == "4":
            if not selected:
                warn("Seleccioná cuentas primero.")
                press_enter()
                continue
            _change_bio_flow(alias, selected)
        elif choice == "5":
            if not selected:
                warn("Seleccioná cuentas primero.")
                press_enter()
                continue
            _profile_photo_flow(alias, selected)
        elif choice == "6":
            if not selected:
                warn("Seleccioná cuentas primero.")
                press_enter()
                continue
            _delete_highlights_flow(alias, selected)
        elif choice == "7":
            if not selected:
                warn("Seleccioná cuentas primero.")
                press_enter()
                continue
            _delete_posts_flow(alias, selected)
        elif choice == "8":
            break
        else:
            warn("Opción inválida.")
            press_enter()


def _format_health_error(exc: Exception, ig_exceptions) -> str:
    msg = str(exc).lower()

    def _has_attr(name: str):
        return getattr(ig_exceptions, name, None) if ig_exceptions else None

    login_types = tuple(
        tp
        for tp in (
            _has_attr("ClientLoginRequired"),
            _has_attr("LoginRequired"),
            _has_attr("TwoFactorRequired"),
        )
        if tp is not None
    )
    if login_types and isinstance(exc, login_types):
        return "[⚠️ Sesión expirada]"

    challenge_type = _has_attr("ChallengeRequired")
    if challenge_type and isinstance(exc, challenge_type):
        return "[🟡 En riesgo: challenge]"

    checkpoint_type = _has_attr("CheckpointRequired")
    if checkpoint_type and isinstance(exc, checkpoint_type):
        return "[🟡 En riesgo: checkpoint]"

    proxy_block_type = _has_attr("ProxyAddressIsBlocked")
    if proxy_block_type and isinstance(exc, proxy_block_type):
        return "[🌐 Proxy caído]"

    action_block_types = tuple(
        tp
        for tp in (
            _has_attr("FeedbackRequired"),
            _has_attr("SentryBlock"),
        )
        if tp is not None
    )
    if action_block_types and isinstance(exc, action_block_types):
        return "[🟡 En riesgo: action_block]"

    rate_limit_types = tuple(
        tp
        for tp in (
            _has_attr("RateLimitError"),
            _has_attr("PleaseWaitFewMinutes"),
        )
        if tp is not None
    )
    if rate_limit_types and isinstance(exc, rate_limit_types):
        return "[🟡 En riesgo: rate_limit]"

    disabled_types = tuple(
        tp
        for tp in (
            _has_attr("UserNotFound"),
            _has_attr("NotFoundError"),
        )
        if tp is not None
    )
    if disabled_types and isinstance(exc, disabled_types):
        return "[🔴 Desactivada]"

    if "proxy" in msg or "timed out" in msg or "dns" in msg or "connection" in msg:
        if any(word in msg for word in ("refused", "timeout", "timed out", "timedout", "unreachable", "name or service")):
            return "[🌐 Proxy caído]"

    if "login required" in msg or "sessionid" in msg or "401" in msg:
        return "[⚠️ Sesión expirada]"

    if "challenge" in msg:
        return "[🟡 En riesgo: challenge]"

    if "checkpoint" in msg:
        return "[🟡 En riesgo: checkpoint]"

    if any(keyword in msg for keyword in ("few minutes", "rate limit", "try again later")):
        return "[🟡 En riesgo: rate_limit]"

    if "feedback" in msg or "action block" in msg or "sentry" in msg:
        return "[🟡 En riesgo: action_block]"

    if "disabled" in msg or "desactiv" in msg:
        return "[🔴 Desactivada]"

    return "[🟡 En riesgo: unknown]"


def _compute_health_badge(account: Dict) -> str:
    username = account.get("username", "").strip().lstrip("@")
    if not username:
        return "[🟡 En riesgo: unknown]"

    if not has_session(username):
        return "[⚠️ Sesión expirada]"

    try:
        from instagrapi import Client, exceptions as ig_exceptions
    except Exception:
        return "[🟡 En riesgo: unknown]"

    try:
        cl = Client()
    except Exception as exc:
        return _format_health_error(exc, None)

    try:
        apply_proxy_to_client(cl, username, account, reason="healthcheck")
    except Exception as exc:
        return _format_health_error(exc, None)

    try:
        load_into(cl, username)
    except FileNotFoundError:
        return "[⚠️ Sesión expirada]"
    except Exception as exc:
        return _format_health_error(exc, ig_exceptions)

    info = None
    try:
        info = cl.account_info()
    except Exception as exc:
        badge = _format_health_error(exc, ig_exceptions)
        if "unknown" not in badge:
            return badge
        try:
            if getattr(cl, "user_id", None):
                info = cl.user_info(cl.user_id)
        except Exception as inner_exc:
            return _format_health_error(inner_exc, ig_exceptions)

    if info and getattr(info, "username", None):
        return "[✅ OK]"

    return "[🟡 En riesgo: unknown]"


def _health_badge(account: Dict) -> str:
    badge = _compute_health_badge(account)
    return _store_health(account.get("username", ""), badge)


def menu_accounts():
    while True:
        banner()
        items = _load()
        aliases = sorted(set([it.get("alias", "default") for it in items]) | {"default"})
        title("Alias disponibles: " + ", ".join(aliases))
        alias = ask("Alias / grupo (ej default, ventas, matias): ").strip() or "default"

        print(f"\nCuentas del alias: {alias}")
        group = [it for it in items if it.get("alias") == alias]
        if not group:
            print("(no hay cuentas aún)")
        else:
            pending_refresh: List[Dict] = []
            for it in group:
                flag = em("🟢") if it.get("active") else em("⚪")
                conn = "[conectada]" if it.get("connected") else "[no conectada]"
                sess = "[sesión]" if has_session(it["username"]) else "[sin sesión]"
                proxy_flag = _proxy_indicator(it)
                totp_flag = _totp_indicator(it)
                badge, needs_refresh = _badge_for_display(it)
                if needs_refresh:
                    pending_refresh.append(it)
                print(
                    f" - @{it['username']} {conn} {sess} {flag} {proxy_flag}{totp_flag} • {badge}"
                )
            if pending_refresh:
                _schedule_health_refresh(pending_refresh)

        print("\n1) Agregar cuenta")
        print("2) Agregar cuentas mediante archivo CSV")
        print("3) Eliminar cuenta")
        print("4) Activar/Desactivar / Proxy")
        print("5) Iniciar sesión y guardar sesiónid (auto en TODAS del alias)")
        print("6) Iniciar sesión y guardar sesión ID (seleccionar cuenta)")
        print("7) Modo de exploración automática por hashtag (nuevo)")
        print("8) Subir contenidos (Historias / Post / Reels)")
        print("9) Interacciones (Comentar / Ver & Like Reels)")
        print("10) Modificación de cuentas de Instagram")
        print("11) Exportar cuentas a CSV")
        print("12) Mover cuentas a otro alias")
        print("13) Volver\n")

        op = ask("Opción: ").strip()
        if op == "1":
            u = ask("Username (sin @): ").strip().lstrip("@")
            if not u:
                continue
            if get_account(u):
                warn("Ya existe.")
                press_enter()
                continue
            proxy_data = _prompt_proxy_settings()
            totp_saved = _prompt_totp(u)
            if add_account(u, alias, proxy_data):
                if not totp_saved:
                    remove_totp_secret(u)
                prompt_login(u)
            else:
                if totp_saved:
                    remove_totp_secret(u)
            press_enter()
        elif op == "2":
            _import_accounts_from_csv(alias)
        elif op == "3":
            if not group:
                warn("No hay cuentas para eliminar en este alias.")
                press_enter()
                continue
            print("\n¿Querés eliminar una cuenta, varias o todas las del alias?")
            print("1) Una")
            print("2) Varias (selección múltiple)")
            print("3) Todas las del alias")
            mode = ask("Opción: ").strip() or "1"
            if mode == "1":
                u = ask("Username a eliminar: ").strip().lstrip("@")
                if not u:
                    warn("No se ingresó username.")
                else:
                    remove_account(u)
                press_enter()
            elif mode == "2":
                print("Seleccioná cuentas por número o username (coma separada):")
                for idx, acct in enumerate(group, start=1):
                    low_flag = _low_profile_indicator(acct)
                    label = f" {idx}) @{acct['username']}"
                    if low_flag:
                        label += f" {low_flag}"
                    print(label)
                    if low_flag and acct.get("low_profile_reason"):
                        print(f"    ↳ {acct['low_profile_reason']}")
                raw = ask("Selección: ").strip()
                if not raw:
                    warn("Sin selección.")
                    press_enter()
                    continue
                chosen = set()
                for part in raw.split(","):
                    part = part.strip()
                    if not part:
                        continue
                    if part.isdigit():
                        idx = int(part)
                        if 1 <= idx <= len(group):
                            chosen.add(group[idx - 1]["username"])
                    else:
                        chosen.add(part.lstrip("@"))
                if not chosen:
                    warn("No se encontraron cuentas con esos datos.")
                    press_enter()
                    continue
                for acct in group:
                    if acct["username"] in chosen:
                        remove_account(acct["username"])
                press_enter()
            elif mode == "3":
                confirm = ask(
                    "¿Confirmás eliminar TODAS las cuentas de este alias? (s/N): "
                ).strip().lower()
                if confirm == "s":
                    for acct in group:
                        remove_account(acct["username"])
                else:
                    warn("Operación cancelada.")
                press_enter()
            else:
                warn("Opción inválida.")
                press_enter()
        elif op == "4":
            u = ask("Username: ").strip().lstrip("@")
            account = get_account(u)
            if not account:
                warn("No existe la cuenta.")
                press_enter()
                continue
            print("\n1) Activar/Desactivar")
            print("2) Editar proxy")
            print("3) Probar proxy")
            print("4) Configurar/Reemplazar TOTP")
            print("5) Eliminar TOTP")
            print("6) Volver")
            choice = ask("Opción: ").strip() or "6"
            if choice == "1":
                val = ask("1=activar, 0=desactivar: ").strip()
                set_active(u, val == "1")
                press_enter()
            elif choice == "2":
                updates = _prompt_proxy_settings(account)
                update_account(u, updates)
                record_proxy_failure(u)
                ok("Proxy actualizado.")
                press_enter()
            elif choice == "3":
                _test_existing_proxy(account)
                press_enter()
            elif choice == "4":
                configured = _prompt_totp(u)
                if not configured:
                    warn("No se configuró TOTP.")
                press_enter()
                account = get_account(u) or account
            elif choice == "5":
                if has_totp_secret(u):
                    remove_totp_secret(u)
                    ok("Se eliminó el TOTP almacenado.")
                else:
                    warn("La cuenta no tenía TOTP guardado.")
                press_enter()
                account = get_account(u) or account
            else:
                continue
        elif op == "5":
            print(
                "Se reutilizará la contraseña guardada cuando esté disponible; "
                "se solicitará solo si es necesario."
            )
            for it in [x for x in _load() if x.get("alias") == alias]:
                username = it["username"]
                if auto_login_with_saved_password(username, account=it) and has_session(username):
                    continue
                prompt_login(username, interactive=False)
            press_enter()
        elif op == "6":
            group = [x for x in _load() if x.get("alias") == alias]
            if not group:
                warn("No hay cuentas para iniciar sesión.")
                press_enter()
                continue
            print("Seleccioná cuentas por número o username (coma separada, * para todas):")
            for idx, acct in enumerate(group, start=1):
                sess = "[sesión]" if has_session(acct["username"]) else "[sin sesión]"
                proxy_flag = _proxy_indicator(acct)
                low_flag = _low_profile_indicator(acct)
                totp_flag = _totp_indicator(acct)
                print(f" {idx}) @{acct['username']} {sess} {proxy_flag}{low_flag}{totp_flag}")
                if low_flag and acct.get("low_profile_reason"):
                    print(f"    ↳ {acct['low_profile_reason']}")
            raw = ask("Selección: ").strip()
            if not raw:
                warn("Sin selección.")
                press_enter()
                continue
            targets: List[Dict] = []
            if raw == "*":
                targets = group
            else:
                chosen = set()
                for part in raw.split(","):
                    part = part.strip()
                    if not part:
                        continue
                    if part.isdigit():
                        idx = int(part)
                        if 1 <= idx <= len(group):
                            chosen.add(group[idx - 1]["username"])
                    else:
                        chosen.add(part.lstrip("@"))
                targets = [acct for acct in group if acct["username"] in chosen]
            if not targets:
                warn("No se encontraron cuentas con esos datos.")
                press_enter()
                continue
            for acct in targets:
                username = acct["username"]
                if auto_login_with_saved_password(username, account=acct) and has_session(username):
                    continue
                prompt_login(username, interactive=False)
            press_enter()
        elif op == "7":
            _launch_hashtag_mode(alias)
        elif op == "8":
            _launch_content_publisher(alias)
        elif op == "9":
            _launch_interactions(alias)
        elif op == "10":
            _modification_menu(alias)
        elif op == "11":
            _export_accounts_csv(alias)
        elif op == "12":
            _move_accounts_to_alias(alias)
        elif op == "13":
            break
        else:
            warn("Opción inválida.")
            press_enter()


# Mantener compatibilidad con importación dinámica
mark_connected.__doc__ = "Actualiza el flag de conexión en almacenamiento"
