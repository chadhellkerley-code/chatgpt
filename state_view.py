"""Visualización del estado de conversaciones en inbox y pendientes."""

from __future__ import annotations

import sqlite3
import unicodedata
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
    ) -> None:
        status = status if status in _ALLOWED_STATUSES else _STATUS_DESCONOCIDO
        with self._conn:
            self._conn.execute(
                """
                INSERT INTO conversation_state (account, thread_id, last_item, status, other_username, message_ts, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(account, thread_id)
                DO UPDATE SET last_item=excluded.last_item, status=excluded.status,
                              other_username=excluded.other_username, message_ts=excluded.message_ts,
                              updated_at=excluded.updated_at
                """,
                (account, thread_id, last_item, status, other_username, message_ts, updated_at),
            )

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:
            pass


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
    message_ts_val = int(timestamp.timestamp())
    cache_entry = cache.lookup(account, thread_id)
    if cache_entry and cache_entry.last_item == last_item_id:
        status = cache_entry.status
        other_username = cache_entry.other_username
        cached_ts = cache_entry.message_ts or message_ts_val
        if cache_entry.message_ts != message_ts_val:
            cache.store(
                account,
                thread_id,
                last_item_id,
                status,
                other_username,
                message_ts_val,
                int(now.timestamp()),
            )
        message_ts_val = cached_ts
        if (
            status == "MENSAJE_ENVIADO"
            and now - datetime.fromtimestamp(message_ts_val, TZ) >= timedelta(hours=48)
        ):
            status = "SIN_RESPUESTA_48H"
            cache.store(
                account,
                thread_id,
                last_item_id,
                status,
                other_username,
                message_ts_val,
                int(now.timestamp()),
            )
    else:
        other_username = _other_username(
            client,
            thread,
            client.user_id,
            cache_entry.other_username if cache_entry else "-",
        )
        status = _classify_messages(messages, client.user_id, now)
        cache.store(
            account,
            thread_id,
            last_item_id,
            status,
            other_username,
            message_ts_val,
            int(now.timestamp()),
        )

    other_username = other_username or "-"
    emitter: str
    recipient: str
    if str(getattr(last_message, "user_id", "")) == str(client.user_id):
        emitter = account
        recipient = other_username
    else:
        emitter = other_username
        recipient = account

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


def _render_snapshots(cache: ConversationCache) -> tuple[list[str], Counter]:
    snapshots = _gather_snapshots(cache)
    snapshots.sort(key=lambda snap: snap.timestamp, reverse=True)
    summary = Counter()
    lines: list[str] = []
    for snap in snapshots:
        summary[snap.status] += 1
        formatted = (
            f"[{snap.timestamp.strftime('%Y-%m-%d %H:%M')}] "
            f"Emisor:{_format_handle(snap.emitter)}  "
            f"Receptor:{_format_handle(snap.recipient)}  "
            f"Estado:{snap.status}"
        )
        lines.append(formatted)
    return lines, summary


def _print_summary(summary: Counter) -> None:
    print("Resumen de estados:")
    for status in sorted(_ALLOWED_STATUSES):
        count = summary.get(status, 0)
        print(f" - {status}: {count}")


def menu_conversation_state() -> None:
    cache = ConversationCache(_DB_PATH)
    try:
        while True:
            lines, summary = _render_snapshots(cache)
            for line in lines:
                print(line)
            print("R = refrescar; Q = salir con resumen.")
            choice = ask("> ").strip().lower()
            if choice in {"", "r"}:
                continue
            if choice == "q":
                _print_summary(summary)
                break
    finally:
        cache.close()
