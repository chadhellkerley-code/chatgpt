#!/usr/bin/env python3
"""CLI para empaquetar builds de cliente asociadas a una licencia."""
from __future__ import annotations

import argparse
import sys
from typing import List

from licensekit import fetch_license, list_licenses, package_license


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
    args = parser.parse_args(argv)

    license_key = args.license
    if not license_key:
        records = list_licenses()
        if not records:
            parser.error("No hay licencias para empaquetar.")
        license_key = _select_license_interactive(records)

    if not fetch_license(license_key):
        parser.error("Licencia no encontrada en el registro local.")

    success, bundle_path, message = package_license(license_key)
    if not success:
        parser.error(message)

    print(f"{message}. ZIP generado en: {bundle_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
