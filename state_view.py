"""Panel de estado de conversaciones."""

from __future__ import annotations

from datetime import datetime, time as dtime
from typing import Optional

from storage import TZ, conversation_rows, menu_supabase
from ui import Fore, banner, format_table, full_line, style_text
from utils import ask, press_enter, warn


def _parse_date(value: str) -> Optional[datetime]:
    value = value.strip()
    if not value:
        return None
    try:
        date = datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        warn("Formato inválido. Usá YYYY-MM-DD.")
        press_enter()
        return None
    return datetime.combine(date, dtime.min, tzinfo=TZ)


def _format_rows(rows: list[dict]) -> list[str]:
    table = [["Fecha y hora", "Cuenta emisora", "Cuenta receptora", "Estado"]]
    for row in rows:
        ts = row["timestamp"].strftime("%Y-%m-%d %H:%M")
        table.append([ts, f"@{row['account']}", f"@{row['recipient']}", row["status"]])
    return format_table(table)


def menu_conversation_state() -> None:
    account_filter: Optional[str] = None
    start: Optional[datetime] = None
    end: Optional[datetime] = None

    while True:
        banner()
        print(style_text("📊 ESTADO DE LA CONVERSACIÓN", color=Fore.CYAN, bold=True))
        print(full_line())

        rows = conversation_rows(account_filter=account_filter, start=start, end=end)
        if rows:
            rendered = _format_rows(rows)
            for line in rendered:
                print(style_text(line, bold=False))
            print(full_line())
            print(style_text(f"Total filas: {len(rows)}", color=Fore.GREEN, bold=True))
        else:
            print(style_text("No hay conversaciones registradas.", color=Fore.YELLOW, bold=True))
        print(full_line())

        filters = []
        if account_filter:
            filters.append(f"Cuenta: @{account_filter}")
        if start:
            filters.append(f"Desde: {start.strftime('%Y-%m-%d')}")
        if end:
            filters.append(f"Hasta: {end.strftime('%Y-%m-%d')}")
        filters_text = ", ".join(filters) if filters else "(sin filtros)"
        print(style_text(f"Filtros activos: {filters_text}", color=Fore.WHITE, bold=True))
        print()
        print("[Enter] Refrescar  |  [F] Filtrar  |  [L] Limpiar filtros  |  [C] Configurar Supabase  |  [V] Volver")
        choice = ask("Acción: ").strip().lower()

        if choice in {"", "r"}:
            continue
        if choice == "v":
            break
        if choice == "l":
            account_filter = None
            start = None
            end = None
            continue
        if choice == "c":
            menu_supabase()
            continue
        if choice == "f":
            account = ask("Filtrar por cuenta (vacío = todas): ").strip()
            account_filter = account or None
            start_candidate = ask("Desde (YYYY-MM-DD, vacío = sin límite): ")
            parsed_start = _parse_date(start_candidate)
            if start_candidate and not parsed_start:
                continue
            end_candidate = ask("Hasta (YYYY-MM-DD, vacío = sin límite): ")
            if end_candidate:
                parsed_end = _parse_date(end_candidate)
                if not parsed_end:
                    continue
                end = datetime.combine(parsed_end.date(), dtime.max, tzinfo=TZ)
            else:
                end = None
            start = parsed_start
            continue
        warn("Opción inválida.")
        press_enter()

