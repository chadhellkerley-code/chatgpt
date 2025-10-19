#!/usr/bin/env python3
"""CLI para empaquetar builds de cliente asociadas a una licencia."""
from __future__ import annotations

import argparse
import sys
from typing import List

from config import SETTINGS
from licensekit import (
    ensure_supabase_credentials,
    ensure_table_exists,
    fetch_license,
    list_licenses,
    package_license,
)


def _format_record(record: dict) -> str:
    client = record.get("client_name", "-")
    key = record.get("license_key", "-")
    status = record.get("status", "-")
    expires = record.get("expires_at", "-")
    return f"{client} | {key} | {status} | {expires}"


def _select_license_interactive(records: List[dict]) -> str:
    print("Licencias disponibles:")
    for idx, record in enumerate(records, start=1):
        print(f"{idx}) {_format_record(record)}")
    choice = input("Seleccioná número de licencia: ").strip()
    if not choice:
        raise SystemExit("Operación cancelada.")
    try:
        idx = int(choice)
    except ValueError as exc:  # pragma: no cover - input inválido
        raise SystemExit("Entrada inválida.") from exc
    if not 1 <= idx <= len(records):
        raise SystemExit("Número fuera de rango.")
    return records[idx - 1]["license_key"]


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--license", help="Clave de licencia a empaquetar")
    parser.add_argument("--supabase-url", help="Override de SUPABASE_URL")
    parser.add_argument("--supabase-key", help="Override de SUPABASE_KEY")
    args = parser.parse_args(argv)

    _, url_found, key_found = ensure_supabase_credentials(interactive=False)
    url = args.supabase_url or url_found or SETTINGS.supabase_url
    key = args.supabase_key or key_found or SETTINGS.supabase_key

    if not url or not key:
        parser.error("Debes definir SUPABASE_URL y SUPABASE_KEY (argumentos o .env.local).")

    if not ensure_table_exists(url, key, interactive=False):
        parser.error("La tabla de licencias no está disponible. Ejecutá el menú 7 primero.")

    license_key = args.license
    if not license_key:
        records = list_licenses()
        if not records:
            parser.error("No hay licencias para empaquetar.")
        license_key = _select_license_interactive(records)

    if not fetch_license(license_key):
        parser.error("Licencia no encontrada en Supabase.")

    success, message = package_license(license_key, url, key)
    if not success:
        parser.error(message)

    print(message)
    return 0


if __name__ == "__main__":
    sys.exit(main())
