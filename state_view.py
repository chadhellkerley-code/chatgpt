"""Visualización del estado de conversaciones en inbox y pendientes."""

from __future__ import annotations

import sqlite3
import unicodedata
import csv
import math
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional
from zoneinfo import ZoneInfo

from accounts import list_all, mark_connected
from proxy_manager import apply_proxy_to_client, record_proxy_failure, should_retry_proxy
from session_store import has_session, load_into
from storage import TZ
from utils import ask

_STATUS_FALLO = "FALLO"
_STATUS_DESCONOCIDO = "DESCONOCIDO"
_ALLOWED_STATUSES = {
    "INTERESADO_AHORA",
    "INTERESADO_FUTURO",
    "QUIERE_MAS_INFO",
    "OBJECCION",
    "NO_INTERESADO",
    "EN_CURSO",
    "SIN_RESPUESTA_48H",
    "MENSAJE_ENVIADO",
    _STATUS_FALLO,
    _STATUS_DESCONOCIDO,
}

_DB_PATH = Path(__file__).resolve().parent / "storage" / "conversation_state.db"
_THREAD_LIMIT = 40
_CONTEXT_MESSAGES = 12
_PAGE_SIZE = 20

_POSITIVE_NOW = (
    "lo quiero",
    "me interesa",
    "cuando cerramos",
    "hagamoslo",
    "donde firmo",
    "arranquemos",
    "listo",
    "dale",
)
_POSITIVE_FUTURE = (
    "mas adelante",
    "más adelante",
    "el mes que viene",
    "en unas semanas",
    "despues vemos",
    "después vemos",
    "cuando pueda",
    "en otro momento",
)
_INFO_REQUEST = (
    "info",
    "informacion",
    "información",
    "detalle",
    "detalles",
    "precio",
    "cuanto",
    "cuánto",
    "valor",
    "cost",
    "tarifa",
)
_OBJECTIONS = (
    "caro",
    "carísimo",
    "carisimo",
    "no puedo",
    "no tengo",
    "difícil",
    "dificil",
    "complicado",
    "problema",
    "no llego",
)
_NO_INTEREST = (
    "no me interesa",
    "no estoy interesado",
    "no quiero",
    "no gracias",
    "dejame en paz",
    "no insistas",
    "baja",
    "stop",
)


@dataclass
class CachedState:
    last_item: str
    status: str
    other_username: str
    message_ts: int


@dataclass
class ThreadSnapshot:
    timestamp: datetime
    emitter: str
    recipient: str
    status: str


class ConversationCache:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._conn = sqlite3.connect(str(path))
        self._ensure()

    def _ensure(self) -> None:
        with self._conn:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS conversation_state (
                    account TEXT NOT NULL,
                    thread_id TEXT NOT NULL,
                    last_item TEXT NOT NULL,
                    status TEXT NOT NULL,
                    other_username TEXT NOT NULL,
                    message_ts INTEGER NOT NULL DEFAULT 0,
                    updated_at INTEGER NOT NULL,
                    PRIMARY KEY (account, thread_id)
                )
                """
            )
        try:
            cols = {row[1] for row in self._conn.execute("PRAGMA table_info(conversation_state)")}
            if "message_ts" not in cols:
                self._conn.execute(
                    "ALTER TABLE conversation_state ADD COLUMN message_ts INTEGER NOT NULL DEFAULT 0"
                )
            if "emitter" not in cols:
                self._conn.execute(
                    "ALTER TABLE conversation_state ADD COLUMN emitter TEXT NOT NULL DEFAULT ''"
                )
            if "recipient" not in cols:
                self._conn.execute(
                    "ALTER TABLE conversation_state ADD COLUMN recipient TEXT NOT NULL DEFAULT ''"
                )
        except Exception:
            pass

    def lookup(self, account: str, thread_id: str) -> Optional[CachedState]:
        cur = self._conn.execute(
            "SELECT last_item, status, other_username, message_ts FROM conversation_state WHERE account=? AND thread_id=?",
            (account, thread_id),
        )
        row = cur.fetchone()
        if not row:
            return None
        return CachedState(last_item=row[0], status=row[1], other_username=row[2], message_ts=int(row[3] or 0))

    def store(
        self,
        account: str,
        thread_id: str,
        last_item: str,
        status: str,
        other_username: str,
        message_ts: int,
        updated_at: int,
        emitter: str,
        recipient: str,
    ) -> None:
        status = status if status in _ALLOWED_STATUSES else _STATUS_DESCONOCIDO
        emitter = emitter or account
        recipient = recipient or other_username or account
        with self._conn:
            self._conn.execute(
                """
                INSERT INTO conversation_state (account, thread_id, last_item, status, other_username, message_ts, updated_at, emitter, recipient)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(account, thread_id)
                DO UPDATE SET last_item=excluded.last_item, status=excluded.status,
                              other_username=excluded.other_username, message_ts=excluded.message_ts,
                              updated_at=excluded.updated_at, emitter=excluded.emitter,
                              recipient=excluded.recipient
                """,
                (
                    account,
                    thread_id,
                    last_item,
                    status,
                    other_username,
                    message_ts,
                    updated_at,
                    emitter,
                    recipient,
                ),
            )

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:
            pass

    def delete_between(self, start_ts: int, end_ts: int) -> int:
        with self._conn:
            cur = self._conn.execute(
                "DELETE FROM conversation_state WHERE message_ts BETWEEN ? AND ?",
                (start_ts, end_ts),
            )
        return cur.rowcount or 0

    def iter_all(self) -> Iterable[tuple[str, str, str, int, int, str, str]]:
        cur = self._conn.execute(
            """
            SELECT account, other_username, status, message_ts, updated_at, emitter, recipient
            FROM conversation_state
            """
        )
        for row in cur.fetchall():
            yield (
                row[0],
                row[1],
                row[2],
                int(row[3] or 0),
                int(row[4] or 0),
                row[5] or "",
                row[6] or "",
            )

    def cached_snapshots(self) -> list[ThreadSnapshot]:
        snapshots: list[ThreadSnapshot] = []
        for account, other_user, status, message_ts, updated_at, emitter, recipient in self.iter_all():
            ts_source = message_ts or updated_at
            if ts_source:
                timestamp = datetime.fromtimestamp(ts_source, TZ)
            else:
                timestamp = datetime.now(TZ)
            emitter_val = emitter or account
            recipient_val = recipient or other_user or account
            snapshots.append(
                ThreadSnapshot(
                    timestamp=timestamp,
                    emitter=emitter_val,
                    recipient=recipient_val,
                    status=status if status in _ALLOWED_STATUSES else _STATUS_DESCONOCIDO,
                )
            )
        snapshots.sort(key=lambda snap: snap.timestamp, reverse=True)
        return snapshots


def _format_handle(value: str) -> str:
    value = (value or "-").strip()
    if not value:
        return "@-"
    if value.startswith("@"):
        return value
    return f"@{value}"


def _normalize_text(text: str) -> str:
    normalized = unicodedata.normalize("NFD", text.lower())
    return "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")


def _contains_any(text: str, keywords: Iterable[str]) -> bool:
    for keyword in keywords:
        if keyword and keyword in text:
            return True
    return False


_UTC = ZoneInfo("UTC")


def _to_local(dt: Optional[datetime]) -> datetime:
    if dt is None:
        return datetime.now(TZ)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=_UTC).astimezone(TZ)
    return dt.astimezone(TZ)


def _classify_messages(messages: List, self_user_id: int | str, now: datetime) -> str:
    if not messages:
        return _STATUS_DESCONOCIDO

    last_msg = messages[0]
    last_dt = _to_local(getattr(last_msg, "timestamp", None))
    self_id = str(self_user_id)

    last_inbound = None
    for msg in messages:
        if str(getattr(msg, "user_id", "")) != self_id and (msg.text or "").strip():
            last_inbound = msg
            break

    if last_inbound and last_inbound.text:
        inbound_norm = _normalize_text(last_inbound.text)
        if _contains_any(inbound_norm, _NO_INTEREST):
            return "NO_INTERESADO"
        if _contains_any(inbound_norm, _OBJECTIONS):
            return "OBJECCION"
        if _contains_any(inbound_norm, _POSITIVE_FUTURE):
            return "INTERESADO_FUTURO"
        if _contains_any(inbound_norm, _INFO_REQUEST):
            return "QUIERE_MAS_INFO"
        if _contains_any(inbound_norm, _POSITIVE_NOW):
            return "INTERESADO_AHORA"

    recent_cutoff = now - timedelta(hours=6)
    recent_messages = [
        msg
        for msg in messages
        if getattr(msg, "timestamp", None) and _to_local(getattr(msg, "timestamp")) >= recent_cutoff
    ]
    if len({str(getattr(msg, "user_id", "")) for msg in recent_messages}) >= 2 and len(recent_messages) >= 4:
        return "EN_CURSO"

    if str(getattr(last_msg, "user_id", "")) == self_id:
        if last_dt <= now - timedelta(hours=48):
            return "SIN_RESPUESTA_48H"
        return "MENSAJE_ENVIADO"

    if last_inbound and last_inbound.text:
        if len(recent_messages) >= 2:
            return "EN_CURSO"

    return _STATUS_DESCONOCIDO


def _resolve_username(client, user_id: int | str) -> str:
    try:
        info = client.user_info(int(user_id))
        username = getattr(info, "username", None)
        if username:
            return username
    except Exception:
        pass
    return str(user_id)


def _other_username(client, thread, self_user_id: int | str, fallback: str) -> str:
    self_id = str(self_user_id)
    for participant in getattr(thread, "users", []) or []:
        pk = str(getattr(participant, "pk", getattr(participant, "id", "")))
        if pk != self_id:
            username = getattr(participant, "username", None)
            if username:
                return username
    for message in getattr(thread, "messages", []) or []:
        if str(getattr(message, "user_id", "")) != self_id:
            user_obj = getattr(message, "user", None)
            username = getattr(user_obj, "username", None)
            if username:
                return username
            target_id = getattr(message, "user_id", None)
            if target_id:
                return _resolve_username(client, target_id)
    return fallback


def _message_identifier(message) -> str:
    return str(getattr(message, "id", getattr(message, "pk", "")))


def _thread_timestamp(thread, last_message) -> datetime:
    ts = getattr(last_message, "timestamp", None)
    if ts:
        return _to_local(ts)
    return _to_local(getattr(thread, "last_activity_at", None))


def _snapshot_from_thread(client, account: str, thread, cache: ConversationCache, now: datetime) -> Optional[ThreadSnapshot]:
    if getattr(thread, "is_group", False):
        return None

    messages = list(getattr(thread, "messages", []) or [])
    if not messages:
        return None

    messages = messages[:_CONTEXT_MESSAGES]
    last_message = messages[0]
    last_item_id = _message_identifier(last_message)
    if not last_item_id:
        return None

    thread_id = str(getattr(thread, "id", getattr(thread, "pk", "")))
    if not thread_id:
        return None

    timestamp = _thread_timestamp(thread, last_message)
    message_ts_actual = int(timestamp.timestamp())
    cache_entry = cache.lookup(account, thread_id)
    other_username: str
    status: str
    message_ts_for_age = message_ts_actual
    store_message_ts = message_ts_actual
    if cache_entry and cache_entry.last_item == last_item_id:
        status = cache_entry.status
        other_username = cache_entry.other_username
        cached_ts = cache_entry.message_ts or message_ts_actual
        message_ts_for_age = cached_ts
        if cache_entry.message_ts != message_ts_actual:
            store_message_ts = message_ts_actual
        else:
            store_message_ts = cached_ts
        if (
            status == "MENSAJE_ENVIADO"
            and now - datetime.fromtimestamp(message_ts_for_age, TZ) >= timedelta(hours=48)
        ):
            status = "SIN_RESPUESTA_48H"
            store_message_ts = message_ts_for_age
    else:
        other_username = _other_username(
            client,
            thread,
            client.user_id,
            cache_entry.other_username if cache_entry else "-",
        )
        status = _classify_messages(messages, client.user_id, now)
        store_message_ts = message_ts_actual

    other_username = other_username or "-"
    if str(getattr(last_message, "user_id", "")) == str(client.user_id):
        emitter = account
        recipient = other_username
    else:
        emitter = other_username
        recipient = account

    cache.store(
        account,
        thread_id,
        last_item_id,
        status,
        other_username,
        store_message_ts,
        int(now.timestamp()),
        emitter,
        recipient,
    )

    return ThreadSnapshot(timestamp=timestamp, emitter=emitter, recipient=recipient, status=status)


def _failure_snapshot(account: str, when: datetime) -> ThreadSnapshot:
    return ThreadSnapshot(timestamp=when, emitter=account, recipient="-", status=_STATUS_FALLO)


def _client_for(account_record: Dict) -> Optional:
    from instagrapi import Client

    username = account_record.get("username")
    if not username:
        return None

    cl = Client()
    binding = None
    try:
        binding = apply_proxy_to_client(
            cl,
            username,
            account_record,
            reason="estado_conversacion",
        )
    except Exception as exc:
        if account_record.get("proxy_url"):
            record_proxy_failure(username, exc)
            raise

    try:
        load_into(cl, username)
    except FileNotFoundError:
        mark_connected(username, False)
        raise
    except Exception as exc:
        if binding and should_retry_proxy(exc):
            record_proxy_failure(username, exc)
        mark_connected(username, False)
        raise

    try:
        cl.get_timeline_feed()
        mark_connected(username, True)
    except Exception as exc:
        if binding and should_retry_proxy(exc):
            record_proxy_failure(username, exc)
        mark_connected(username, False)
        raise
    return cl


def _gather_snapshots(cache: ConversationCache) -> list[ThreadSnapshot]:
    snapshots: list[ThreadSnapshot] = []
    now = datetime.now(TZ)

    for account in list_all():
        username = account.get("username")
        if not username:
            continue
        if not has_session(username):
            snapshots.append(_failure_snapshot(username, now))
            continue
        try:
            client = _client_for(account)
        except Exception:
            snapshots.append(_failure_snapshot(username, now))
            continue

        threads: Dict[str, object] = {}
        try:
            inbox_threads = client.direct_threads(amount=_THREAD_LIMIT, thread_message_limit=_CONTEXT_MESSAGES)
            for thread in inbox_threads:
                threads[str(getattr(thread, "id", getattr(thread, "pk", "")))] = thread
        except Exception:
            snapshots.append(_failure_snapshot(username, now))
            continue

        try:
            pending_threads = client.direct_pending_inbox(amount=_THREAD_LIMIT)
            for thread in pending_threads:
                threads[str(getattr(thread, "id", getattr(thread, "pk", "")))] = thread
        except Exception:
            snapshots.append(_failure_snapshot(username, now))

        for thread in threads.values():
            snapshot = _snapshot_from_thread(client, username, thread, cache, now)
            if snapshot:
                snapshots.append(snapshot)

    return snapshots


def _render_snapshots(cache: ConversationCache) -> tuple[list[ThreadSnapshot], Counter]:
    snapshots = _gather_snapshots(cache)
    snapshots.sort(key=lambda snap: snap.timestamp, reverse=True)
    summary = Counter()
    for snap in snapshots:
        summary[snap.status] += 1
    return snapshots, summary


def _cached_snapshots(cache: ConversationCache) -> tuple[list[ThreadSnapshot], Counter]:
    snapshots = cache.cached_snapshots()
    summary = Counter()
    for snap in snapshots:
        summary[snap.status] += 1
    return snapshots, summary


def _truncate(value: str, width: int) -> str:
    if len(value) <= width:
        return value.ljust(width)
    if width <= 1:
        return value[:width]
    return value[: width - 1] + "…"


def _format_rows(snapshots: list[ThreadSnapshot]) -> list[tuple[str, str, str, str]]:
    rows: list[tuple[str, str, str, str]] = []
    for snap in snapshots:
        rows.append(
            (
                snap.timestamp.strftime("%Y-%m-%d %H:%M"),
                _format_handle(snap.emitter),
                _format_handle(snap.recipient),
                snap.status,
            )
        )
    return rows


def _print_table(rows: list[tuple[str, str, str, str]], page: int) -> tuple[int, int]:
    total = len(rows)
    total_pages = max(1, math.ceil(total / _PAGE_SIZE))
    page = max(0, min(page, total_pages - 1))
    start = page * _PAGE_SIZE
    end = start + _PAGE_SIZE
    page_rows = rows[start:end]

    headers = ("Fecha y hora", "Emisor", "Receptor", "Estado")
    widths = [17, 22, 22, 18]

    print("\n" + "=" * 72)
    header_line = " | ".join(_truncate(h, w) for h, w in zip(headers, widths))
    print(header_line)
    print("-" * 72)

    if not page_rows:
        print("(Sin conversaciones registradas)")
    else:
        for row in page_rows:
            print(" | ".join(_truncate(cell, width) for cell, width in zip(row, widths)))

    print("-" * 72)
    print(f"Página {page + 1} de {total_pages}  (Total: {total})")
    return total_pages, page


def _handle_delete(cache: ConversationCache) -> None:
    print("Ingrese el rango de fechas a eliminar (formato YYYY-MM-DD). Deje vacío para cancelar.")
    start_str = ask("Desde: ").strip()
    if not start_str:
        print("Operación cancelada.")
        return
    end_str = ask("Hasta: ").strip()
    if not end_str:
        print("Operación cancelada.")
        return
    try:
        start_dt = datetime.strptime(start_str, "%Y-%m-%d").replace(tzinfo=TZ)
        end_dt = datetime.strptime(end_str, "%Y-%m-%d").replace(tzinfo=TZ) + timedelta(days=1) - timedelta(seconds=1)
    except ValueError:
        print("Fechas inválidas. Utilice el formato YYYY-MM-DD.")
        return
    if end_dt < start_dt:
        print("El rango es inválido (la fecha final es anterior a la inicial).")
        return
    removed = cache.delete_between(int(start_dt.timestamp()), int(end_dt.timestamp()))
    print(f"Se eliminaron {removed} registros.")


def _handle_export(rows: list[tuple[str, str, str, str]]) -> None:
    if not rows:
        print("No hay datos para exportar.")
        return
    target_dir = Path.home() / "Desktop"
    if not target_dir.exists():
        target_dir = Path.cwd()
    filename = f"estado_conversacion_{datetime.now(TZ).strftime('%Y%m%d_%H%M%S')}.csv"
    export_path = target_dir / filename
    try:
        with export_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(["fecha_hora", "emisor", "receptor", "estado"])
            for row in rows:
                writer.writerow(row)
    except Exception as exc:
        print(f"No se pudo generar el CSV: {exc}")
        return
    print(f"CSV guardado en: {export_path}")


def _print_summary(summary: Counter) -> None:
    print("Resumen de estados:")
    for status in sorted(_ALLOWED_STATUSES):
        count = summary.get(status, 0)
        print(f" - {status}: {count}")


def menu_conversation_state() -> None:
    cache = ConversationCache(_DB_PATH)
    try:
        snapshots, summary = _cached_snapshots(cache)
        if not snapshots:
            snapshots, summary = _render_snapshots(cache)
        rows = _format_rows(snapshots)
        page = 0
        while True:
            total_pages, page = _print_table(rows, page)
            print("1) página anterior")
            print("2) página siguiente")
            print("3) borrar todos los datos")
            print("4) descargar CSV")
            print("5) actualizar")
            print("6) volver")

            choice = ask("> ").strip().lower()
            if choice == "1":
                if page > 0:
                    page -= 1
                else:
                    print("Ya estás en la primera página.")
            elif choice == "2":
                if page + 1 < total_pages:
                    page += 1
                else:
                    print("Ya estás en la última página.")
            elif choice == "3":
                _handle_delete(cache)
                snapshots, summary = _cached_snapshots(cache)
                rows = _format_rows(snapshots)
                page = 0
            elif choice == "4":
                _handle_export(rows)
            elif choice in {"", "5"}:
                snapshots, summary = _render_snapshots(cache)
                rows = _format_rows(snapshots)
                page = 0
            elif choice == "6":
                _print_summary(summary)
                break
            else:
                print("Opción no válida. Seleccione un número del 1 al 6.")
    finally:
        cache.close()
