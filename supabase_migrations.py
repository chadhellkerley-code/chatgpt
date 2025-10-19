"""Helpers para ejecutar migraciones mínimas en Supabase."""
from __future__ import annotations

from typing import Tuple

import requests

LICENSES_TABLE_SQL = """
create table if not exists public.licenses (
    id uuid primary key default gen_random_uuid(),
    client_name text not null,
    license_key text not null unique,
    expires_at timestamptz not null,
    status text not null default 'active',
    created_at timestamptz not null default now()
);
""".strip()


def ensure_licenses_table(url: str, key: str) -> Tuple[bool, str]:
    """Crea la tabla de licencias usando los endpoints RPC de Supabase."""

    if not url or not key:
        return False, "Faltan credenciales de Supabase."

    base = url.rstrip("/") + "/rest/v1/"
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }
    payload = {"sql": LICENSES_TABLE_SQL}
    endpoints = ("rpc/run_sql", "rpc/execute_sql", "rpc/pg_execute_sql")
    last_error = "No se pudo crear la tabla de licencias."

    for endpoint in endpoints:
        try:
            response = requests.post(
                base + endpoint,
                headers=headers,
                json=payload,
                timeout=30,
            )
        except requests.RequestException as exc:  # pragma: no cover - red externa
            last_error = str(exc)
            continue
        if response.status_code < 400:
            return True, "Tabla de licencias verificada/creada correctamente."
        try:
            detail = response.json()
        except Exception:  # pragma: no cover - fallback
            detail = response.text
        last_error = f"{response.status_code}: {detail}"

    return False, last_error
