# accounts.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import csv
import getpass
import io
import json
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from pathlib import Path
from typing import Dict, List, Optional

import logging

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

logger = logging.getLogger(__name__)

_HEALTH_CACHE_TTL = timedelta(minutes=15)
_HEALTH_CACHE: Dict[str, tuple[datetime, str]] = {}
_HEALTH_CACHE_LOCK = Lock()
_HEALTH_CACHE_FILE = DATA / "account_health.json"
_HEALTH_REFRESH_PENDING: set[str] = set()
_HEALTH_REFRESH_EXECUTOR = ThreadPoolExecutor(
    max_workers=3, thread_name_prefix="health-refresh"
)


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


def _normalize_account(record: Dict) -> Dict:
    result = dict(record)
    result.setdefault("alias", "default")
    result.setdefault("active", True)
    result.setdefault("connected", False)
    result.setdefault("proxy_url", "")
    result.setdefault("proxy_user", "")
    result.setdefault("proxy_pass", "")
    sticky_default = SETTINGS.proxy_sticky_minutes or 10
    try:
        sticky_value = int(result.get("proxy_sticky_minutes", sticky_default))
    except Exception:
        sticky_value = sticky_default
    result["proxy_sticky_minutes"] = max(1, sticky_value)
    username = result.get("username")
    if username:
        result["has_totp"] = has_totp_secret(username)
    else:
        result.setdefault("has_totp", False)
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
    return stored


def _load() -> List[Dict]:
    if not FILE.exists():
        return []
    try:
        data = json.loads(FILE.read_text(encoding="utf-8"))
    except Exception:
        return []
    normalized: List[Dict] = []
    for item in data:
        if isinstance(item, dict):
            normalized.append(_normalize_account(item))
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
        totp_flag = _totp_indicator(acct)
        print(f" {idx}) @{acct['username']} {sess} {proxy_flag}{totp_flag}")
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


def _login_and_save_session(account: Dict, password: str) -> bool:
    """Login con instagrapi y guarda sesión en storage/sessions."""

    username = account["username"]
    try:
        from instagrapi import Client

        cl = Client()
        binding = apply_proxy_to_client(cl, username, account, reason="login")
        verification_code = ""
        if has_totp_secret(username):
            code = generate_totp_code(username)
            if code:
                verification_code = code
                logger.debug("Aplicando TOTP automático para @%s", username)
            else:
                warn(
                    "No se pudo generar el código 2FA automático. Intentá reconfigurar el TOTP."
                )
        cl.login(username, password, verification_code=verification_code)
        save_from(cl, username)
        mark_connected(username, True)
        ok(f"Sesión guardada para {username}.")
        return True
    except Exception as exc:
        if should_retry_proxy(exc):
            record_proxy_failure(username, exc)
            warn(f"Problema con el proxy de @{username}: {exc}")
        else:
            warn(f"No se pudo iniciar sesión para {username}: {exc}")
        mark_connected(username, False)
        return False


def prompt_login(username: str) -> bool:
    account = get_account(username)
    if not account:
        warn("No existe la cuenta indicada.")
        return False
    pwd = getpass.getpass(f"Password @{account['username']}: ")
    if not pwd:
        warn("Se canceló el inicio de sesión.")
        return False
    return _login_and_save_session(account, pwd)


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


def _schedule_health_refresh(accounts_to_refresh: List[Dict]) -> None:
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
        totp_flag = _totp_indicator(acct)
        print(f" {idx}) @{username} {sess} {proxy_flag}{totp_flag}")

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

    try:
        cl.account_info()
        mark_connected(username, True)
    except Exception as exc:
        if binding and should_retry_proxy(exc):
            record_proxy_failure(username, exc)
        mark_connected(username, False)
        warn(f"Instagram rechazó la sesión de @{username}: {exc}")
        return None

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
        print("11) Volver\n")

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
            u = ask("Username a eliminar: ").strip().lstrip("@")
            remove_account(u)
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
            print("Se pedirá contraseña por cada cuenta...")
            for it in [x for x in _load() if x.get("alias") == alias]:
                prompt_login(it["username"])
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
                totp_flag = _totp_indicator(acct)
                print(f" {idx}) @{acct['username']} {sess} {proxy_flag}{totp_flag}")
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
                prompt_login(acct["username"])
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
            break
        else:
            warn("Opción inválida.")
            press_enter()


# Mantener compatibilidad con importación dinámica
mark_connected.__doc__ = "Actualiza el flag de conexión en almacenamiento"
