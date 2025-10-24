"""Panel de estado de conversaciones."""

from __future__ import annotations

from datetime import datetime, time as dtime, timedelta
from math import ceil
from typing import Optional

from storage import (
    TZ,
    conversation_rows,
    export_conversation_state,
    menu_supabase,
    purge_conversations_before,
)
from ui import Fore, banner, full_line, style_text
from utils import ask, ok, press_enter, warn

_PAGE_SIZE = 12
_DEFAULT_RANGE_DAYS = 7


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
    rendered: list[str] = []
    for row in rows:
        ts = row["timestamp"].strftime("%Y-%m-%d %H:%M")
        account = row["account"].lstrip("@")
        recipient = row["recipient"].lstrip("@")
        status = row["status"]
        line = (
            f"📅 {ts} | 📨 Emisor: @{account} | 📩 Receptor: @{recipient} | 💬 Estado: {status}"
        )
        rendered.append(line)
    return rendered


def _default_start() -> datetime:
    return datetime.now(TZ) - timedelta(days=_DEFAULT_RANGE_DAYS)


def menu_conversation_state() -> None:
    account_filter: Optional[str] = None
    start: Optional[datetime] = _default_start()
    end: Optional[datetime] = None
    page = 0

    while True:
        banner()
        print(style_text("📊 ESTADO DE LA CONVERSACIÓN", color=Fore.CYAN, bold=True))
        print(full_line())

        rows = conversation_rows(account_filter=account_filter, start=start, end=end)
        total_rows = len(rows)
        total_pages = max(1, ceil(total_rows / _PAGE_SIZE))
        if page >= total_pages:
            page = total_pages - 1
        offset = page * _PAGE_SIZE
        visible = rows[offset : offset + _PAGE_SIZE]
        if rows:
            rendered = _format_rows(visible)
            for line in rendered:
                print(style_text(line, bold=False))
            print(full_line())
            print(
                style_text(
                    f"Página {page + 1}/{total_pages} · Total filas: {total_rows}",
                    color=Fore.GREEN,
                    bold=True,
                )
            )
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
        if start:
            print(
                style_text(
                    f"Rango por defecto: últimos {_DEFAULT_RANGE_DAYS} días",
                    color=Fore.BLUE,
                    bold=False,
                )
            )
        print()
        print(style_text("Seleccioná una opción:", color=Fore.WHITE, bold=True))
        options = [
            "[Enter] Refrescar",
            "1) Filtrar",
            "2) Limpiar filtros",
            "3) Página siguiente",
            "4) Página anterior",
            "5) Exportar CSV",
            "6) Eliminar datos antiguos",
            "7) Configurar Supabase",
            "8) Volver",
        ]
        for line in options:
            print(style_text(line, color=Fore.CYAN if line.startswith("[") else Fore.WHITE, bold=True))

        choice = ask("Acción: ").strip().lower()

        if choice in {"", "r"}:
            continue
        if choice in {"8", "v"}:
            break
        if choice in {"3", "n"}:
            if page + 1 < total_pages:
                page += 1
            else:
                warn("Ya estás en la última página.")
                press_enter()
            continue
        if choice in {"4", "p"}:
            if page > 0:
                page -= 1
            else:
                warn("Ya estás en la primera página.")
                press_enter()
            continue
        if choice in {"2", "l"}:
            account_filter = None
            start = _default_start()
            end = None
            page = 0
            continue
        if choice in {"7", "c"}:
            menu_supabase()
            continue
        if choice in {"1", "f"}:
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
            page = 0
            continue
        if choice in {"5", "e"}:
            if not rows:
                warn("No hay datos para exportar.")
                press_enter()
                continue
            path = export_conversation_state(rows)
            ok(f"CSV generado en: {path}")
            press_enter()
            continue
        if choice in {"6", "d"}:
            cutoff = datetime.now(TZ) - timedelta(days=30)
            removed = purge_conversations_before(cutoff)
            ok(f"Se eliminaron {removed} registros anteriores al último mes.")
            page = 0
            press_enter()
            continue
        warn("Opción inválida.")
        press_enter()

