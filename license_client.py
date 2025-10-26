# license_client.py
# -*- coding: utf-8 -*-
"""Lanzador para builds de cliente con validación de licencia."""

from __future__ import annotations

import glob
import json
import os
import re
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

from licensekit import validate_license_payload
from ui import Fore, banner, full_line, style_text

PAYLOAD_NAME = "storage/license_payload.json"

SESSION_PATTERNS = [
    "session_*.json",
    "v1_settings_*.json",
    "settings_*.json",
    "*.session.json",
]

CANDIDATE_SESSION_DIRS = [
    "Station ID",
    "session_id",
    "station_id",
    "Session ID",
]

_DEBUG_ROOT_PRINTED = False


def _resource_path(relative: str) -> Path:
    base = getattr(sys, "_MEIPASS", None)
    if base:
        return Path(base) / relative
    return Path(__file__).resolve().parent / relative


def _load_payload() -> Dict[str, str]:
    path = _resource_path(PAYLOAD_NAME)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _print_section(title: str, *, color: str = Fore.CYAN) -> None:
    banner()
    print(style_text(title, color=color, bold=True))
    print(full_line(color=color))
    print()


def _print_error(msg: str) -> None:
    print(full_line(color=Fore.RED))
    print(style_text("Licencia inválida", color=Fore.RED, bold=True))
    print(msg)
    print(full_line(color=Fore.RED))
    print()


def _slugify(value: str, fallback: str = "cliente") -> str:
    value = (value or "").strip().lower()
    if not value:
        return fallback
    value = value.replace(" ", "-")
    value = re.sub(r"[^a-z0-9_-]+", "-", value)
    value = value.strip("-")
    return value or fallback


def _get_app_root() -> Path:
    """Determina el directorio raíz del bundle/ejecutable."""

    meipass = getattr(sys, "_MEIPASS", None)
    if meipass:
        return Path(meipass).resolve()
    if sys.argv and sys.argv[0]:
        return Path(os.path.abspath(sys.argv[0])).resolve().parent
    return Path(__file__).resolve().parent


def _resolve_sessions_dir() -> Path:
    base_dir = _get_app_root()
    for name in CANDIDATE_SESSION_DIRS:
        candidate = base_dir / name
        if candidate.is_dir():
            candidate.mkdir(parents=True, exist_ok=True)
            return candidate
    target = base_dir / "Station ID"
    target.mkdir(parents=True, exist_ok=True)
    return target


def _iter_session_files(sess_dir: Path) -> Iterable[Path]:
    seen: set[str] = set()
    for pattern in SESSION_PATTERNS:
        for path in glob.glob(str(sess_dir / pattern)):
            base = os.path.basename(path)
            if base in seen:
                continue
            seen.add(base)
            candidate = Path(path)
            if candidate.is_file():
                yield candidate
        for path in glob.glob(str(sess_dir / "*" / pattern)):
            base = os.path.basename(path)
            if base in seen:
                continue
            seen.add(base)
            candidate = Path(path)
            if candidate.is_file():
                yield candidate


def _prepare_client_environment(record: Dict[str, str]) -> None:
    alias = record.get("client_alias") or record.get("client_slug") or record.get("client_name")
    alias = _slugify(alias)
    sessions_root = _resolve_sessions_dir()
    os.environ.setdefault("CLIENT_DISTRIBUTION", "1")
    os.environ["CLIENT_SESSIONS_ROOT"] = str(sessions_root)
    os.environ["CLIENT_ALIAS"] = alias
    os.environ["LICENSE_ALREADY_VALIDATED"] = "1"


def _load_sessions_on_boot() -> Tuple[int, int, List[str]]:
    global _DEBUG_ROOT_PRINTED

    sessions_dir = _resolve_sessions_dir()
    found_files = list(_iter_session_files(sessions_dir))
    print(f"📦 Sesiones detectadas en '{sessions_dir.name}': {len(found_files)}")
    try:
        names_preview = ", ".join(path.name for path in found_files[:5])
        if len(found_files) > 5:
            names_preview += ", ..."
        if names_preview:
            print(f"🗂️ Archivos: {names_preview}")
    except Exception:
        pass

    try:
        from instagrapi import Client
    except Exception:
        print("🔄 Sesiones restauradas: 0")
        return 0, len(found_files), []

    try:
        from accounts import list_all, mark_connected
        from proxy_manager import apply_proxy_to_client, record_proxy_failure, should_retry_proxy
    except Exception:
        print("🔄 Sesiones restauradas: 0")
        return 0, len(found_files), []

    accounts = list_all()
    account_map = {
        (acct.get("username") or "").strip().lstrip("@").lower(): acct
        for acct in accounts
        if acct.get("username")
    }

    for acct in accounts:
        username = acct.get("username")
        if username:
            mark_connected(username, False)

    loaded = 0
    errors = 0
    loaded_users: List[str] = []

    for path in found_files:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            errors += 1
            print(f"⚠️ No se pudo cargar la sesión desde: {path.name}")
            continue

        username = (
            (data.get("username") or data.get("user") or data.get("account") or "")
            .strip()
            .lstrip("@")
        )
        if not username:
            stem = path.stem
            for prefix in ("session_", "v1_settings_", "settings_"):
                if stem.startswith(prefix):
                    username = stem[len(prefix) :]
                    break
            if not username:
                username = stem
        username = username.strip().lstrip("@")
        if not username:
            errors += 1
            print(f"⚠️ No se pudo cargar la sesión desde: {path.name}")
            continue

        lower_username = username.lower()
        account = account_map.get(lower_username)
        if not account:
            errors += 1
            print(f"⚠️ Sesión de @{username} no vinculada a una cuenta guardada.")
            continue

        raw_cookies = data.get("cookies") or {}
        cookies: Dict[str, str] = {}
        if isinstance(raw_cookies, dict):
            cookies = {str(k): raw_cookies[k] for k in raw_cookies if raw_cookies[k]}
        elif isinstance(raw_cookies, list):
            for item in raw_cookies:
                if not isinstance(item, dict):
                    continue
                name = item.get("name")
                value = item.get("value")
                if name and value:
                    cookies[str(name)] = value

        session_id = (
            data.get("sessionid")
            or cookies.get("sessionid")
            or cookies.get("session_id")
            or (data.get("authorization_data") or {}).get("sessionid")
        )
        if not session_id:
            errors += 1
            mark_connected(username, False)
            print(f"⚠️ Sesión de @{username} inválida, por favor volvé a iniciar sesión.")
            continue

        client = Client()
        binding = None
        try:
            binding = apply_proxy_to_client(client, username, account, reason="sesion")
        except Exception as exc:
            if account.get("proxy_url"):
                record_proxy_failure(username, exc)

        if hasattr(client, "set_settings"):
            try:
                client.set_settings(data)
            except Exception:
                # seguimos; se intentará iniciar sesión igualmente
                pass

        try:
            if hasattr(client, "login_by_sessionid"):
                client.login_by_sessionid(session_id)
            else:
                raise RuntimeError("login_by_sessionid no disponible en el cliente instagrapi.")
            client.get_timeline_feed()
        except Exception as exc:
            errors += 1
            mark_connected(username, False)
            print(f"⚠️ @{username}: sesión expirada, iniciá sesión nuevamente.")
            if binding and should_retry_proxy(exc):
                record_proxy_failure(username, exc)
            continue

        mark_connected(username, True)
        loaded += 1
        loaded_users.append(username)

    if found_files and loaded == 0 and not _DEBUG_ROOT_PRINTED:
        _DEBUG_ROOT_PRINTED = True
        print(f"ROOT={_get_app_root()}")
        print(f"SESS_DIR={sessions_dir}")
        print(f"EXISTS={sessions_dir.exists()}")

    print(f"🔄 Sesiones restauradas: {loaded}")
    return loaded, errors, loaded_users


def launch_with_license() -> None:
    payload = _load_payload()
    if not payload:
        _print_section("Validación de licencia", color=Fore.RED)
        _print_error("No se encontró la licencia incluida en el paquete.")
        sys.exit(2)

    attempts = 3
    record: Dict[str, str] = {}
    _print_section("Validación de licencia")
    print(style_text("Ingresá tu código de licencia para continuar.", color=Fore.WHITE))
    print()
    for remaining in range(attempts, 0, -1):
        provided = input("Ingresá tu código de licencia: ").strip()
        ok, message, record = validate_license_payload(provided, payload)
        if ok:
            break
        _print_error(message or "Licencia inválida.")
        if remaining - 1:
            print(style_text(f"Intentos restantes: {remaining - 1}", color=Fore.YELLOW))
            print()
    else:
        sys.exit(2)

    _prepare_client_environment(record)
    _load_sessions_on_boot()

    _print_section("Licencia validada", color=Fore.GREEN)
    client = record.get("client_name", "Cliente")
    print(style_text(f"Licencia válida para {client}", color=Fore.GREEN, bold=True))
    expires = record.get("expires_at")
    if expires:
        print(style_text(f"Vence: {expires}", color=Fore.GREEN))
    print(full_line(color=Fore.GREEN))
    print()

    from app import menu  # import tardío para evitar ciclos

    menu()


if __name__ == "__main__":
    launch_with_license()

