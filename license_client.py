# license_client.py
# -*- coding: utf-8 -*-
"""Lanzador para builds de cliente con validación de licencia."""

from __future__ import annotations

import glob
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Dict, Iterable, List, Tuple


def _initial_app_root() -> Path:
    """Determina un directorio estable para guardar datos del cliente."""

    candidates: List[Path] = []

    if sys.argv and sys.argv[0]:
        try:
            candidates.append(Path(os.path.abspath(sys.argv[0])).resolve().parent)
        except Exception:
            pass

    # Distribuciones congeladas (PyInstaller/zipapp) exponen ``sys.executable``
    # apuntando al binario real en el bundle. Solo lo usamos cuando realmente
    # estamos en modo congelado para evitar rutas del intérprete del sistema
    # (por ejemplo, /usr/bin).
    if getattr(sys, "frozen", False):
        executable = getattr(sys, "executable", None)
        if executable:
            try:
                candidates.append(Path(executable).resolve().parent)
            except Exception:
                pass

    for candidate in candidates:
        if candidate and candidate.exists():
            return candidate

    meipass = getattr(sys, "_MEIPASS", None)
    if meipass:
        try:
            return Path(meipass).resolve()
        except Exception:
            pass

    return Path(__file__).resolve().parent


os.environ.setdefault("APP_DATA_ROOT", str(_initial_app_root()))

import config
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

    return _initial_app_root()


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


def _migrate_legacy_data(app_root: Path, data_root: Path) -> None:
    """Replica datos existentes cuando cambiamos el directorio base."""

    if data_root == app_root:
        return

    legacy_candidates = [
        app_root / "data",
        app_root / "storage",
        app_root / "conversation_state.db",
    ]

    for candidate in legacy_candidates:
        try:
            if not candidate.exists():
                continue
            target = data_root / candidate.name
            if candidate.is_file():
                if not target.exists():
                    target.write_bytes(candidate.read_bytes())
                continue
            target.mkdir(parents=True, exist_ok=True)
            for child in candidate.glob("**/*"):
                if not child.is_file():
                    continue
                relative = child.relative_to(candidate)
                destination = target / relative
                if destination.exists():
                    continue
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(child.read_bytes())
        except Exception:
            continue


def _prepare_client_environment(record: Dict[str, str]) -> None:
    alias = record.get("client_alias") or record.get("client_slug") or record.get("client_name")
    alias = _slugify(alias)
    app_root = _get_app_root()
    sessions_root = _resolve_sessions_dir()
    data_root = sessions_root / alias if alias else sessions_root
    data_root.mkdir(parents=True, exist_ok=True)
    _migrate_legacy_data(app_root, data_root)
    os.environ.setdefault("CLIENT_DISTRIBUTION", "1")
    os.environ["CLIENT_SESSIONS_ROOT"] = str(sessions_root)
    os.environ["CLIENT_ALIAS"] = alias
    os.environ["LICENSE_ALREADY_VALIDATED"] = "1"
    os.environ["APP_DATA_ROOT"] = str(data_root)


def _client_integrity_marker_path() -> Path:
    base = Path(os.environ.get("APP_DATA_ROOT", str(_get_app_root())))
    return base / "storage" / ".client_integrity_check"


def _run_client_integrity_check() -> None:
    if not config.SETTINGS.client_distribution:
        return

    marker = _client_integrity_marker_path()
    if marker.exists():
        return

    try:
        from app import current_menu_option_labels
        from responder import autoresponder_menu_options, autoresponder_prompt_length
    except Exception:
        return

    print(full_line(color=Fore.CYAN))
    print(style_text("Verificación de integridad del cliente", color=Fore.CYAN, bold=True))
    print(full_line(color=Fore.CYAN))
    print(style_text("Versión detectada: cliente", color=Fore.GREEN, bold=True))

    options = current_menu_option_labels()
    sin_siete = all(not opt.strip().startswith("7)") for opt in options)
    estado_menu = "sí" if sin_siete else "no"
    print(f"Menú sin opción 7: {estado_menu}")
    if options:
        print("Opciones visibles:")
        for opt in options:
            print(f" • {opt.strip()}")

    prompt_length = autoresponder_prompt_length()
    print(
        f"Autoresponder 5.2 listo. System Prompt actual: {prompt_length} caracteres."
    )

    calendar_disponible = any(
        "Conectar con Google Calendar" in opt for opt in autoresponder_menu_options()
    )
    estado_calendar = "sí" if calendar_disponible else "no"
    print(f"Submenú 'Conectar con Google Calendar' disponible: {estado_calendar}")

    gohighlevel_disponible = any(
        "Conectar con GoHighLevel" in opt for opt in autoresponder_menu_options()
    )
    estado_gohighlevel = "sí" if gohighlevel_disponible else "no"
    print(f"Submenú 'Conectar con GoHighLevel' disponible: {estado_gohighlevel}")

    try:
        marker.parent.mkdir(parents=True, exist_ok=True)
        marker.write_text(str(int(time.time())), encoding="utf-8")
    except Exception:
        pass

    print(full_line(color=Fore.CYAN))
    print()


def _ensure_account_record(username: str, accounts: List[Dict]) -> Dict | None:
    """Garantiza que exista un registro básico de cuenta para el usuario."""

    normalized_username = username.lower()
    for record in accounts:
        existing = (record.get("username") or "").strip().lower()
        if existing == normalized_username:
            return record

    try:
        from accounts import _normalize_account as normalize_account  # type: ignore[attr-defined]
        from accounts import _save as save_accounts  # type: ignore[attr-defined]
    except Exception:
        return None

    alias = os.environ.get("CLIENT_ALIAS") or "default"
    base_record = {
        "username": username,
        "alias": alias,
        "active": True,
        "connected": False,
    }
    try:
        normalized = normalize_account(base_record)
    except Exception:
        normalized = base_record

    accounts.append(normalized)
    try:
        save_accounts(accounts)
    except Exception:
        pass
    return normalized


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
        from accounts import list_all, mark_connected
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
            account = _ensure_account_record(username, accounts)
            if account:
                account_map[lower_username] = account
                try:
                    mark_connected(username, False)
                except Exception:
                    pass
            else:
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

        mark_connected(username, True)
        try:
            account["connected"] = True
        except Exception:
            pass
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
    config.refresh_settings()
    _load_sessions_on_boot()

    _print_section("Licencia validada", color=Fore.GREEN)
    client = record.get("client_name", "Cliente")
    print(style_text(f"Licencia válida para {client}", color=Fore.GREEN, bold=True))
    expires = record.get("expires_at")
    if expires:
        print(style_text(f"Vence: {expires}", color=Fore.GREEN))
    print(full_line(color=Fore.GREEN))
    print()

    _run_client_integrity_check()

    from app import menu  # import tardío para evitar ciclos

    menu()


if __name__ == "__main__":
    launch_with_license()

