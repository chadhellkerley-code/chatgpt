# license_client.py
# -*- coding: utf-8 -*-
"""Lanzador para builds de cliente con validación de licencia."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Dict

from licensekit import verify_license_remote
from ui import Fore, full_line, style_text

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


def _print_error(msg: str) -> None:
    print(full_line(color=Fore.RED))
    print(style_text("Licencia inválida", color=Fore.RED, bold=True))
    print(msg)
    print(full_line(color=Fore.RED))


def launch_with_license() -> None:
    payload = _load_payload()
    license_key = payload.get("license_key", "")
    supabase_url = payload.get("supabase_url", "")
    supabase_key = payload.get("supabase_key", "")

    ok, message, record = verify_license_remote(license_key, supabase_url, supabase_key)
    if not ok:
        _print_error(message or "No se pudo validar la licencia.")
        sys.exit(2)

    print(full_line(color=Fore.GREEN))
    client = record.get("client_name", "Cliente")
    print(style_text(f"Licencia válida para {client}", color=Fore.GREEN, bold=True))
    expires = record.get("expires_at")
    if expires:
        print(style_text(f"Vence: {expires}", color=Fore.GREEN))
    print(full_line(color=Fore.GREEN))

    os.environ.setdefault("CLIENT_DISTRIBUTION", "1")
    os.environ["LICENSE_ALREADY_VALIDATED"] = "1"
    from app import menu  # import tardío para evitar ciclos

    menu()


if __name__ == "__main__":
    launch_with_license()

