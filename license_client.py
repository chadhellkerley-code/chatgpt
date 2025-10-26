# license_client.py
# -*- coding: utf-8 -*-
"""Lanzador para builds de cliente con validación de licencia."""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path
from typing import Dict

from licensekit import validate_license_payload
from ui import Fore, banner, full_line, style_text

PAYLOAD_NAME = "storage/license_payload.json"


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


def _prepare_client_environment(record: Dict[str, str]) -> None:
    alias = record.get("client_alias") or record.get("client_slug") or record.get("client_name")
    alias = _slugify(alias)
    base_dir = Path(sys.argv[0]).resolve().parent
    sessions_root = base_dir / "sessions"
    os.environ.setdefault("CLIENT_DISTRIBUTION", "1")
    os.environ["CLIENT_SESSIONS_ROOT"] = str(sessions_root)
    os.environ["CLIENT_ALIAS"] = alias
    os.environ["LICENSE_ALREADY_VALIDATED"] = "1"


def _bootstrap_sessions(record: Dict[str, str]) -> None:
    try:
        from accounts import list_all, mark_connected
        from proxy_manager import apply_proxy_to_client, record_proxy_failure, should_retry_proxy
        from session_store import ensure_dirs, has_session, load_into
    except Exception:
        return

    try:
        from instagrapi import Client
    except Exception:
        return

    ensure_dirs()
    accounts = list_all()
    if not accounts:
        return

    for account in accounts:
        username = account.get("username")
        if not username:
            continue
        if not has_session(username):
            mark_connected(username, False)
            continue
        client = Client()
        binding = None
        try:
            binding = apply_proxy_to_client(client, username, account, reason="sesion")
        except Exception as exc:
            if account.get("proxy_url"):
                record_proxy_failure(username, exc)
        try:
            load_into(client, username)
            client.get_timeline_feed()
        except Exception as exc:
            mark_connected(username, False)
            print(f"⚠️ La sesión de @{username} ha expirado. Iniciá sesión nuevamente para reactivarla.")
            if binding and should_retry_proxy(exc):
                record_proxy_failure(username, exc)
        else:
            mark_connected(username, True)


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
    _bootstrap_sessions(record)

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

