# whatsapp.py
# -*- coding: utf-8 -*-
"""Menú de automatización por WhatsApp totalmente integrado con la app CLI."""

from __future__ import annotations

import csv
import json
import random
import time
import shutil
import textwrap
import uuid
import webbrowser
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Iterator

from http.client import RemoteDisconnected

from paths import runtime_base
from ui import Fore, full_line, style_text
from utils import (
    ask,
    ask_int,
    ask_multiline,
    banner,
    ok,
    press_enter,
    title,
)

BASE = runtime_base(Path(__file__).resolve().parent)
BASE.mkdir(parents=True, exist_ok=True)
DATA_FILE = BASE / "whatsapp_automation.json"
EXPORTS_DIR = BASE / "whatsapp_exports"
EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
SESSIONS_DIR = BASE / "browser_sessions"
SESSIONS_DIR.mkdir(parents=True, exist_ok=True)

MIN_PHONE_DIGITS = 8
MAX_PHONE_DIGITS = 15


def _now() -> datetime:
    return datetime.utcnow().replace(microsecond=0)


def _now_iso() -> str:
    return _now().isoformat() + "Z"


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        clean = value.rstrip("Z")
        return datetime.fromisoformat(clean)
    except Exception:
        return None


def _ensure_validation_entry(value: Any, number: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        value = {}
    raw = (value.get("raw") or number or "").strip()
    normalized = value.get("normalized") or number or raw
    digits = value.get("digits")
    if digits is None:
        digits = sum(1 for ch in normalized if ch.isdigit())
    has_plus = bool(value.get("has_plus")) if "has_plus" in value else normalized.startswith("+")
    status = value.get("status") or "unknown"
    message = value.get("message", "")
    checked_at = value.get("checked_at")
    return {
        "raw": raw,
        "normalized": normalized,
        "digits": int(digits) if isinstance(digits, int) else digits,
        "has_plus": has_plus,
        "status": status,
        "message": message,
        "checked_at": checked_at,
    }


def _validate_phone_number(raw: str) -> dict[str, Any]:
    candidate = (raw or "").strip()
    only_digits = "".join(ch for ch in candidate if ch.isdigit())
    normalized_digits = only_digits
    has_plus_hint = candidate.startswith("+")
    if candidate.startswith("00") and len(only_digits) > 2:
        normalized_digits = only_digits[2:]
        has_plus_hint = True
    normalized = f"+{normalized_digits}" if has_plus_hint else normalized_digits

    status = "valid"
    reasons: list[str] = []
    digits_count = len(normalized_digits)
    if not normalized_digits:
        status = "invalid"
        reasons.append("El número no contiene dígitos reconocibles.")
    elif digits_count < MIN_PHONE_DIGITS:
        status = "invalid"
        reasons.append("El número es demasiado corto para el formato internacional de WhatsApp.")
    elif digits_count > MAX_PHONE_DIGITS:
        status = "invalid"
        reasons.append("El número supera los 15 dígitos permitidos por WhatsApp.")
    elif not has_plus_hint:
        status = "warning"
        reasons.append("Falta el prefijo internacional (+).")

    message = " ".join(reasons) if reasons else "Formato internacional válido."
    return {
        "raw": candidate,
        "normalized": normalized,
        "digits": digits_count,
        "has_plus": has_plus_hint,
        "status": status,
        "message": message,
        "checked_at": _now_iso(),
    }


def _update_contact_validation(contact: dict[str, Any]) -> dict[str, Any]:
    current = contact.get("validation") or {}
    if not isinstance(current, dict):
        current = {}
    previous_status = current.get("status")
    base_number = contact.get("number")
    if not base_number:
        base_number = current.get("raw", "")
    new_validation = _validate_phone_number(base_number)
    normalized_number = new_validation.get("normalized") or new_validation.get("raw", "")
    if normalized_number:
        contact["number"] = normalized_number
    contact["validation"] = new_validation
    if previous_status != new_validation["status"] or previous_status is None:
        history = contact.setdefault("history", [])
        history.append(
            {
                "type": "validation",
                "status": new_validation["status"],
                "checked_at": new_validation["checked_at"],
                "message": new_validation["message"],
            }
        )
    return new_validation


def _ensure_delivery_log(entries: Any) -> list[dict[str, Any]]:
    log: list[dict[str, Any]] = []
    if not isinstance(entries, list):
        return log
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        log.append(
            {
                "timestamp": entry.get("timestamp"),
                "run_id": entry.get("run_id"),
                "status": entry.get("status"),
                "reason": entry.get("reason", ""),
                "confirmation": entry.get("confirmation"),
            }
        )
    return log


def _append_delivery_log(
    contact: dict[str, Any],
    run: dict[str, Any],
    *,
    status: str,
    reason: str = "",
    confirmation: str | None = None,
) -> None:
    log = contact.setdefault("delivery_log", [])
    log.append(
        {
            "timestamp": _now_iso(),
            "run_id": run.get("id"),
            "status": status,
            "reason": reason,
            "confirmation": confirmation,
        }
    )
    if len(log) > 50:
        del log[:-50]

def _default_state() -> dict[str, Any]:
    return {
        "numbers": {},
        "contact_lists": {},
        "message_runs": [],
        "ai_automations": {},
        "instagram": {
            "active": False,
            "delay": {"min": 5.0, "max": 12.0},
            "message": "Hola! Soy parte del equipo, te escribo porque nos compartiste tu número.",
            "captures": [],
        },
        "followup": {
            "default_wait_minutes": 120,
            "manual_message": "Hola {nombre}, ¿pudiste ver mi mensaje anterior?",
            "ai_prompt": (
                "Eres un asistente cordial. Redacta un mensaje breve, cálido y humano para reactivar "
                "una conversación con {nombre} mencionando que estamos disponibles para ayudar."
            ),
            "history": [],
        },
        "payments": {
            "admin_number": "",
            "welcome_message": "¡Bienvenido/a! Gracias por tu pago, aquí tienes tu acceso:",
            "access_link": "https://tusitio.com/accesos",
            "pending": [],
            "history": [],
        },
    }


class WhatsAppDataStore:
    """Persistencia simple en disco para el módulo de WhatsApp."""

    def __init__(self, path: Path = DATA_FILE):
        self.path = path
        self.state = self._load()

    # ------------------------------------------------------------------
    def _load(self) -> dict[str, Any]:
        if not self.path.exists():
            return _default_state()
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            return _default_state()
        return self._merge_defaults(data)

    # ------------------------------------------------------------------
    def _merge_defaults(self, data: dict[str, Any]) -> dict[str, Any]:
        defaults = _default_state()
        merged = dict(defaults)
        merged.update({k: data.get(k, v) for k, v in defaults.items()})
        merged["numbers"] = {
            key: self._ensure_number_structure(value)
            for key, value in dict(data.get("numbers", {})).items()
        }
        merged["contact_lists"] = {
            key: self._ensure_contact_list_structure(value)
            for key, value in dict(data.get("contact_lists", {})).items()
        }
        merged["message_runs"] = [
            self._ensure_message_run(item)
            for item in data.get("message_runs", [])
            if isinstance(item, dict)
        ]
        merged["ai_automations"] = {
            key: self._ensure_ai_config(value)
            for key, value in dict(data.get("ai_automations", {})).items()
        }
        merged["instagram"] = self._ensure_instagram_config(data.get("instagram", {}))
        merged["followup"] = self._ensure_followup_config(data.get("followup", {}))
        merged["payments"] = self._ensure_payments_config(data.get("payments", {}))
        return merged

    # ------------------------------------------------------------------
    def _ensure_number_structure(self, value: Any) -> dict[str, Any]:
        if not isinstance(value, dict):
            return {
                "id": str(uuid.uuid4()),
                "alias": "",
                "phone": "",
                "connected": False,
                "last_connected_at": None,
                "session_notes": [],
                "keep_alive": True,
                "connection_method": "playwright",
            }
        return {
            "id": value.get("id") or str(uuid.uuid4()),
            "alias": value.get("alias", ""),
            "phone": value.get("phone", ""),
            "connected": bool(value.get("connected", False)),
            "last_connected_at": value.get("last_connected_at"),
            "session_notes": list(value.get("session_notes", [])),
            "keep_alive": bool(value.get("keep_alive", True)),
            "session_path": value.get("session_path", ""),
            "qr_snapshot": value.get("qr_snapshot"),
            "last_qr_capture_at": value.get("last_qr_capture_at"),
            "connection_state": value.get("connection_state", "pendiente"),
            "connection_method": value.get("connection_method", "playwright"),
        }

    # ------------------------------------------------------------------
    def _ensure_contact(self, raw: Any) -> dict[str, Any]:
        if not isinstance(raw, dict):
            raw = {}
        history = [entry for entry in raw.get("history", []) if isinstance(entry, dict)]
        validation = _ensure_validation_entry(raw.get("validation"), raw.get("number", ""))
        delivery_log = _ensure_delivery_log(raw.get("delivery_log"))
        return {
            "name": raw.get("name", ""),
            "number": validation.get("normalized") or raw.get("number", ""),
            "status": raw.get("status", "sin mensaje"),
            "last_message_at": raw.get("last_message_at"),
            "last_response_at": raw.get("last_response_at"),
            "last_followup_at": raw.get("last_followup_at"),
            "last_payment_at": raw.get("last_payment_at"),
            "access_sent_at": raw.get("access_sent_at"),
            "notes": raw.get("notes", ""),
            "history": history,
            "validation": validation,
            "delivery_log": delivery_log,
        }

    # ------------------------------------------------------------------
    def _ensure_contact_list_structure(self, value: Any) -> dict[str, Any]:
        if not isinstance(value, dict):
            value = {}
        contacts = [self._ensure_contact(item) for item in value.get("contacts", [])]
        return {
            "alias": value.get("alias", ""),
            "created_at": value.get("created_at") or _now_iso(),
            "contacts": contacts,
            "notes": value.get("notes", ""),
        }

    # ------------------------------------------------------------------
    def _ensure_message_run(self, value: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(value, dict):
            value = {}

        def _to_int(raw: Any, default: int = 0) -> int:
            try:
                if raw in (None, ""):
                    return default
                return int(raw)
            except Exception:
                return default

        def _to_float(raw: Any, default: float = 0.0) -> float:
            try:
                if raw in (None, ""):
                    return default
                return float(raw)
            except Exception:
                return default

        delay = value.get("delay") or {}
        events = []
        for item in value.get("events", []):
            if not isinstance(item, dict):
                continue
            events.append(
                {
                    "contact": item.get("contact"),
                    "name": item.get("name"),
                    "message": item.get("message", ""),
                    "scheduled_at": item.get("scheduled_at"),
                    "status": item.get("status", "pendiente"),
                    "delivered_at": item.get("delivered_at"),
                    "notes": item.get("notes", ""),
                    "confirmation": item.get("confirmation", "no_enviado"),
                    "validation_status": item.get("validation_status"),
                    "error_code": item.get("error_code"),
                }
            )

        log = [
            {
                "timestamp": entry.get("timestamp", _now_iso()),
                "message": entry.get("message", ""),
            }
            for entry in value.get("log", [])
            if isinstance(entry, dict)
        ]

        message_template = value.get("message_template", "") or value.get("template", "")
        message_preview = value.get("message_preview", "")
        if message_template and not message_preview:
            message_preview = textwrap.shorten(message_template, width=90, placeholder="…")

        return {
            "id": value.get("id", str(uuid.uuid4())),
            "number_id": value.get("number_id"),
            "number_alias": value.get("number_alias"),
            "number_phone": value.get("number_phone"),
            "list_alias": value.get("list_alias"),
            "created_at": value.get("created_at") or _now_iso(),
            "status": value.get("status", "programado"),
            "paused": bool(value.get("paused", False)),
            "session_limit": _to_int(value.get("session_limit"), 0),
            "total_contacts": _to_int(value.get("total_contacts"), len(events)),
            "processed": _to_int(value.get("processed"), 0),
            "completed_at": value.get("completed_at"),
            "last_activity_at": value.get("last_activity_at"),
            "next_run_at": value.get("next_run_at"),
            "delay": {
                "min": _to_float(delay.get("min"), 5.0),
                "max": _to_float(delay.get("max"), 12.0),
            },
            "message_template": message_template,
            "message_preview": message_preview,
            "events": events,
            "max_contacts": _to_int(value.get("max_contacts"), 0),
            "last_session_at": value.get("last_session_at"),
            "log": log,
        }

    # ------------------------------------------------------------------
    def _ensure_ai_config(self, value: Any) -> dict[str, Any]:
        if not isinstance(value, dict):
            value = {}
        delay = value.get("delay") or {}
        return {
            "active": bool(value.get("active", False)),
            "prompt": value.get("prompt", ""),
            "delay": {
                "min": float(delay.get("min", 5.0)),
                "max": float(delay.get("max", 15.0)),
            },
            "send_audio": bool(value.get("send_audio", False)),
            "last_updated_at": value.get("last_updated_at"),
        }

    # ------------------------------------------------------------------
    def _ensure_instagram_config(self, value: Any) -> dict[str, Any]:
        if not isinstance(value, dict):
            value = {}
        delay = value.get("delay") or {}
        captures = [
            {
                "id": item.get("id", str(uuid.uuid4())),
                "name": item.get("name", ""),
                "number": item.get("number", ""),
                "source": item.get("source", "Instagram"),
                "captured_at": item.get("captured_at", _now_iso()),
                "message_sent": bool(item.get("message_sent", False)),
                "message_sent_at": item.get("message_sent_at"),
                "notes": item.get("notes", ""),
            }
            for item in value.get("captures", [])
            if isinstance(item, dict)
        ]
        return {
            "active": bool(value.get("active", False)),
            "delay": {
                "min": float(delay.get("min", 5.0)),
                "max": float(delay.get("max", 12.0)),
            },
            "message": value.get(
                "message",
                "Hola! Soy parte del equipo, te escribo porque nos compartiste tu número.",
            ),
            "captures": captures,
            "last_reviewed_at": value.get("last_reviewed_at"),
        }

    # ------------------------------------------------------------------
    def _ensure_followup_config(self, value: Any) -> dict[str, Any]:
        if not isinstance(value, dict):
            value = {}
        history = [item for item in value.get("history", []) if isinstance(item, dict)]
        return {
            "default_wait_minutes": int(value.get("default_wait_minutes", 120)),
            "manual_message": value.get(
                "manual_message", "Hola {nombre}, ¿pudiste ver mi mensaje anterior?"
            ),
            "ai_prompt": value.get(
                "ai_prompt",
                "Eres un asistente cordial. Redacta un mensaje breve, cálido y humano para reactivar "
                "una conversación con {nombre} mencionando que estamos disponibles para ayudar.",
            ),
            "history": history,
        }

    # ------------------------------------------------------------------
    def _ensure_payments_config(self, value: Any) -> dict[str, Any]:
        if not isinstance(value, dict):
            value = {}
        pending = [
            {
                "id": item.get("id", str(uuid.uuid4())),
                "number": item.get("number", ""),
                "name": item.get("name", ""),
                "evidence": item.get("evidence", ""),
                "keywords": list(item.get("keywords", [])),
                "status": item.get("status", "pendiente"),
                "created_at": item.get("created_at", _now_iso()),
                "validated_at": item.get("validated_at"),
                "welcome_sent_at": item.get("welcome_sent_at"),
                "alert_sent_at": item.get("alert_sent_at"),
                "notes": item.get("notes", ""),
            }
            for item in value.get("pending", [])
            if isinstance(item, dict)
        ]
        history = [
            {
                "id": item.get("id", str(uuid.uuid4())),
                "number": item.get("number", ""),
                "name": item.get("name", ""),
                "status": item.get("status", "completado"),
                "completed_at": item.get("completed_at", _now_iso()),
                "notes": item.get("notes", ""),
            }
            for item in value.get("history", [])
            if isinstance(item, dict)
        ]
        return {
            "admin_number": value.get("admin_number", ""),
            "welcome_message": value.get(
                "welcome_message", "¡Bienvenido/a! Gracias por tu pago, aquí tienes tu acceso:"
            ),
            "access_link": value.get("access_link", "https://tusitio.com/accesos"),
            "pending": pending,
            "history": history,
        }

    # ------------------------------------------------------------------
    def save(self) -> None:
        serialized = json.dumps(self.state, ensure_ascii=False, indent=2)
        self.path.write_text(serialized, encoding="utf-8")

    # ------------------------------------------------------------------
    # Helper methods ----------------------------------------------------
    def iter_numbers(self) -> Iterator[dict[str, Any]]:
        for item in self.state.get("numbers", {}).values():
            yield item

    def iter_lists(self) -> Iterator[tuple[str, dict[str, Any]]]:
        for alias, data in self.state.get("contact_lists", {}).items():
            yield alias, data

    def find_number(self, number_id: str) -> dict[str, Any] | None:
        return self.state.get("numbers", {}).get(number_id)

    def find_list(self, alias: str) -> dict[str, Any] | None:
        return self.state.get("contact_lists", {}).get(alias)


# ----------------------------------------------------------------------
# Presentación y helpers de impresión

def _line() -> str:
    return full_line(color=Fore.BLUE, bold=True)


def _info(msg: str, *, color: str = Fore.CYAN, bold: bool = False) -> None:
    print(style_text(msg, color=color, bold=bold))


def _subtitle(msg: str) -> None:
    print(style_text(msg, color=Fore.MAGENTA, bold=True))


def _format_delay(delay: dict[str, float]) -> str:
    return f"{delay['min']:.1f}s – {delay['max']:.1f}s"


# ----------------------------------------------------------------------
# Menú principal del módulo

def menu_whatsapp() -> None:
    store = WhatsAppDataStore()
    while True:
        _reconcile_runs(store)
        banner()
        title("Automatización por WhatsApp")
        print(_line())
        _print_numbers_summary(store)
        print(_line())
        print("1) Conectar número de WhatsApp")
        print("2) Importar lista de contactos")
        print("3) Enviar mensajes a la lista")
        print("4) Automatizar respuestas con IA")
        print("5) Capturar números desde Instagram")
        print("6) Seguimiento automatizado a no respondidos")
        print("7) Gestión de pagos y entrega de accesos")
        print("8) Estado de contactos y actividad")
        print("9) Volver\n")
        op = ask("Opción: ").strip()
        if op == "1":
            _connect_number(store)
        elif op == "2":
            _import_contacts(store)
        elif op == "3":
            _send_messages(store)
        elif op == "4":
            _configure_ai_responses(store)
        elif op == "5":
            _instagram_capture(store)
        elif op == "6":
            _followup_manager(store)
        elif op == "7":
            _payments_menu(store)
        elif op == "8":
            _contacts_state(store)
        elif op == "9":
            break
        else:
            _info("Opción inválida. Intentá nuevamente.", color=Fore.YELLOW)
            press_enter()


# ----------------------------------------------------------------------
# 1) Conectar número ----------------------------------------------------

def _print_numbers_summary(store: WhatsAppDataStore) -> None:
    numbers = list(store.iter_numbers())
    if not numbers:
        _info("Aún no hay números conectados.")
        return
    _subtitle("Números activos")
    for item in sorted(numbers, key=lambda n: n.get("alias")):  # type: ignore[arg-type]
        alias = item.get("alias") or item.get("phone")
        if item.get("connected"):
            status = "🟢 Verificado"
        elif item.get("connection_state") == "fallido":
            status = "🔴 Error de vinculación"
        else:
            status = "⚪ Pendiente"
        last = item.get("last_connected_at") or "(sin actividad)"
        print(
            f" • {alias} ({item.get('phone')}) - {status} – última conexión: {last}"
        )


def _select_connection_backend() -> str | None:
    backend_labels = {
        "1": "playwright",
        "2": "selenium",
        "3": "system",
    }
    while True:
        print(_line())
        _subtitle("Método de vinculación")
        print("1) Navegador automatizado con Playwright (Chromium)")
        print("2) Navegador automatizado con Selenium (Chrome/Safari)")
        print("3) Abrir navegador predeterminado del sistema")
        print("4) Volver\n")
        choice = ask("Opción: ").strip()
        if choice == "4":
            return None
        backend = backend_labels.get(choice)
        if backend:
            return backend
        _info("Opción inválida. Intentá nuevamente.", color=Fore.YELLOW)


def _connect_number(store: WhatsAppDataStore) -> None:
    while True:
        banner()
        title("Conectar número de WhatsApp")
        print(_line())
        _print_numbers_summary(store)
        print(_line())
        print("1) Vincular nuevo número")
        print("2) Eliminar número vinculado")
        print("3) Volver\n")
        op = ask("Opción: ").strip()
        if op == "1":
            _link_new_number(store)
        elif op == "2":
            _remove_linked_number(store)
        elif op == "3":
            return
        else:
            _info("Opción inválida. Intentá nuevamente.", color=Fore.YELLOW)
            press_enter()


def _link_new_number(store: WhatsAppDataStore) -> None:
    alias = ask("Alias interno para reconocer el número: ").strip()
    phone = ask("Número en formato internacional (ej: +54911...): ").strip()
    if not phone:
        _info("No se ingresó número.", color=Fore.YELLOW)
        press_enter()
        return
    note = ask("Nota interna u observación (opcional): ").strip()
    backend = _select_connection_backend()
    if backend is None:
        _info("No se inició ninguna vinculación.")
        press_enter()
        return

    session_id = str(uuid.uuid4())
    session_dir = SESSIONS_DIR / session_id
    session_dir.mkdir(parents=True, exist_ok=True)

    backend_titles = {
        "playwright": "Playwright (Chromium)",
        "selenium": "Selenium",
        "system": "el navegador predeterminado",
    }
    _info(
        f"Preparando {backend_titles.get(backend, 'el método seleccionado')} para WhatsApp Web..."
    )
    success, snapshot, details = _initiate_whatsapp_web_login(session_dir, backend)
    if details:
        _info(details)

    if not success:
        confirm = ask(
            "¿Lograste vincular la sesión desde la ventana abierta? (s/N): "
        ).strip().lower()
        if confirm == "s":
            success = True

    state = store.state.setdefault("numbers", {})
    method_descriptions = {
        "playwright": "Playwright (Chromium)",
        "selenium": "Selenium",
        "system": "el navegador predeterminado del sistema",
    }
    record = {
        "id": session_id,
        "alias": alias or phone,
        "phone": phone,
        "connected": success,
        "last_connected_at": _now_iso() if success else None,
        "session_notes": [
            {
                "created_at": _now_iso(),
                "text": note
                or "Sesión gestionada mediante {}.".format(
                    method_descriptions.get(backend, "el método seleccionado")
                ),
            }
        ],
        "keep_alive": True,
        "session_path": str(session_dir),
        "qr_snapshot": str(snapshot) if snapshot else None,
        "last_qr_capture_at": _now_iso() if snapshot else None,
        "connection_state": "verificado" if success else "pendiente",
        "connection_method": backend,
    }
    if not success:
        record["session_notes"].append(
            {
                "created_at": _now_iso(),
                "text": "La vinculación automática no se completó. Reintentar desde el menú.",
            }
        )
        record["connection_state"] = "fallido"
    state[session_id] = record
    store.save()

    if success:
        ok("Sesión verificada y lista para operar en segundo plano.")
    else:
        _info(
            "No se confirmó la vinculación. Podés reintentar el proceso desde este mismo menú.",
            color=Fore.YELLOW,
        )
    press_enter()


def _remove_linked_number(store: WhatsAppDataStore) -> None:
    numbers = list(store.iter_numbers())
    if not numbers:
        _info("No hay números registrados para eliminar.", color=Fore.YELLOW)
        press_enter()
        return
    print(_line())
    _subtitle("Seleccioná el número a desvincular")
    ordered = sorted(numbers, key=lambda n: n.get("alias"))
    for idx, item in enumerate(ordered, 1):
        alias = item.get("alias") or item.get("phone")
        status = "🟢" if item.get("connected") else "⚪"
        print(f"{idx}) {alias} ({item.get('phone')}) - {status}")
    idx = ask_int("Número a eliminar: ", min_value=1)
    if idx > len(ordered):
        _info("Selección fuera de rango.", color=Fore.YELLOW)
        press_enter()
        return
    selected = ordered[idx - 1]
    alias = selected.get("alias") or selected.get("phone")
    confirm = ask(f"Confirmá eliminación de '{alias}' (s/N): ").strip().lower()
    if confirm != "s":
        _info("Operación cancelada.")
        press_enter()
        return
    state = store.state.setdefault("numbers", {})
    state.pop(selected.get("id"), None)
    session_path = selected.get("session_path")
    if session_path:
        path = Path(session_path)
        if path.exists():
            try:
                shutil.rmtree(path)
            except OSError:
                pass
    store.save()
    ok(f"Se eliminó la sesión asociada a '{alias}'.")
    press_enter()


def _initiate_whatsapp_web_login(
    session_dir: Path, backend: str
) -> tuple[bool, Path | None, str]:
    if backend == "playwright":
        return _initiate_with_playwright(session_dir)
    if backend == "selenium":
        return _initiate_with_selenium(session_dir)
    if backend == "system":
        return _initiate_with_system_browser()
    return False, None, "Método de vinculación desconocido."


def _initiate_with_playwright(session_dir: Path) -> tuple[bool, Path | None, str]:
    try:
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError, sync_playwright
    except ImportError:
        return (
            False,
            None,
            "Playwright no está instalado. Ejecutá 'pip install playwright' y luego 'playwright install'.",
        )

    snapshot_path = session_dir / "qr.png"
    info_messages: list[str] = []
    success = False

    try:
        with sync_playwright() as playwright:
            context = playwright.chromium.launch_persistent_context(
                str(session_dir),
                headless=False,
                args=["--disable-notifications", "--disable-infobars"],
            )
            try:
                page = context.pages[0] if context.pages else context.new_page()
                page.set_default_timeout(60000)
                page.goto("https://web.whatsapp.com", wait_until="networkidle")

                qr_element = None
                try:
                    qr_element = page.wait_for_selector(
                        "canvas[data-testid='qrcode']",
                        timeout=30000,
                    )
                except PlaywrightTimeoutError:
                    try:
                        qr_element = page.wait_for_selector("canvas", timeout=15000)
                    except PlaywrightTimeoutError:
                        qr_element = None

                if qr_element is not None:
                    try:
                        qr_element.screenshot(path=str(snapshot_path))
                    except Exception:
                        page.screenshot(path=str(snapshot_path))
                else:
                    page.screenshot(path=str(snapshot_path))

                if snapshot_path.exists():
                    info_messages.append(
                        f"Se guardó una captura del código QR en {snapshot_path}."
                    )
                info_messages.append(
                    "Escaneá el código con tu celular para completar la vinculación."
                )

                verification_targets = [
                    "div[data-testid='chat-list-search']",
                    "div[data-testid='pane-side']",
                    "div[data-testid='app-title']",
                ]
                for selector in verification_targets:
                    try:
                        page.wait_for_selector(selector, timeout=180000)
                        success = True
                        break
                    except PlaywrightTimeoutError:
                        continue

                if not success:
                    try:
                        page.wait_for_selector("canvas[data-testid='qrcode']", timeout=1000)
                    except PlaywrightTimeoutError:
                        success = True
            finally:
                try:
                    context.close()
                except Exception:
                    pass
    except Exception:
        info_messages.append(
            "No se pudo automatizar la conexión. Verificá que Playwright tenga los navegadores instalados."
        )
        success = False

    message = " ".join(info_messages)
    return success, snapshot_path if snapshot_path.exists() else None, message


def _initiate_with_selenium(session_dir: Path) -> tuple[bool, Path | None, str]:
    try:
        from selenium import webdriver
        from selenium.common.exceptions import TimeoutException, WebDriverException
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait
    except ImportError:
        return (
            False,
            None,
            "Selenium no está instalado. Ejecutá 'pip install selenium' y asegurate de contar con el driver correspondiente.",
        )

    snapshot_path = session_dir / "qr.png"
    info_messages: list[str] = []
    success = False
    driver = None
    driver_label = ""

    profile_root = session_dir / "selenium_profile"
    profile_root.mkdir(parents=True, exist_ok=True)

    chrome_profile = profile_root / "chrome"
    chrome_profile.mkdir(parents=True, exist_ok=True)
    try:
        from selenium.webdriver.chrome.options import Options as ChromeOptions

        chrome_options = ChromeOptions()
        chrome_options.add_argument(f"--user-data-dir={str(chrome_profile)}")
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_argument("--disable-infobars")
        chrome_options.add_argument("--remote-allow-origins=*")
        driver = webdriver.Chrome(options=chrome_options)
        driver_label = "Chrome"
    except Exception:
        driver = None

    if driver is None:
        try:
            driver = webdriver.Safari()
            driver_label = "Safari"
        except Exception:
            driver = None

    if driver is None:
        return (
            False,
            None,
            "No se pudo iniciar un navegador compatible con Selenium. Verificá que el driver esté instalado y habilitado.",
        )

    info_messages.append(
        f"Se abrió {driver_label} mediante Selenium. Escaneá el QR para continuar."
    )

    monitor_completed = False

    try:
        driver.get("https://web.whatsapp.com")
        wait = WebDriverWait(driver, 60)
        try:
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "canvas[data-testid='qrcode']")))
        except TimeoutException:
            pass

        try:
            driver.save_screenshot(str(snapshot_path))
        except RemoteDisconnected:
            raise
        except Exception:
            pass

        if snapshot_path.exists():
            info_messages.append(
                f"Se guardó una captura del código QR en {snapshot_path}."
            )
        info_messages.append(
            "Escaneá el código con tu celular para completar la vinculación."
        )

        verification_targets = [
            "div[data-testid='chat-list-search']",
            "div[data-testid='pane-side']",
            "div[role='grid']",
            "div[data-testid='app-title']",
        ]
        deadline = time.time() + 90
        while time.time() < deadline:
            success = False
            for selector in verification_targets:
                try:
                    WebDriverWait(driver, 8).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                    success = True
                    break
                except TimeoutException:
                    continue
            if success:
                break
            try:
                qr_visible = driver.find_elements(By.CSS_SELECTOR, "canvas[data-testid='qrcode']")
            except RemoteDisconnected:
                raise
            except WebDriverException as exc:
                raise exc
            if not qr_visible:
                success = True
                break
            time.sleep(2)
        monitor_completed = True
    except RemoteDisconnected:
        info_messages.append(
            "El navegador se cerró antes de completar la vinculación. Volvé a intentar."
        )
        success = False
    except WebDriverException:
        info_messages.append(
            "Selenium no pudo completar la automatización. Revisá la configuración del navegador y volvé a intentar."
        )
        success = False
    finally:
        try:
            driver.quit()
        except Exception:
            pass

    if monitor_completed:
        if success:
            info_messages.append(
                "La sesión quedó vinculada correctamente y permanecerá activa en segundo plano."
            )
        else:
            info_messages.append(
                "No se detectó la vinculación en 90 segundos. Se canceló automáticamente."
            )

    message = " ".join(info_messages)
    return success, snapshot_path if snapshot_path.exists() else None, message


def _initiate_with_system_browser() -> tuple[bool, Path | None, str]:
    opened = webbrowser.open("https://web.whatsapp.com")
    if opened:
        message = (
            "Se abrió el navegador predeterminado en https://web.whatsapp.com. "
            "Escaneá el código QR y luego confirmá en la terminal si la vinculación se completó."
        )
    else:
        message = (
            "Intentá abrir manualmente https://web.whatsapp.com desde tu navegador, "
            "escaneá el código QR y luego confirmá en la terminal si la vinculación se completó."
        )
    return False, None, message


# ----------------------------------------------------------------------
# 2) Importar contactos -------------------------------------------------

def _import_contacts(store: WhatsAppDataStore) -> None:
    while True:
        banner()
        title("Importar lista de contactos")
        print(_line())
        existing = list(store.iter_lists())
        if existing:
            _subtitle("Listas registradas")
            for alias, data in existing:
                total = len(data.get("contacts", []))
                print(f" • {alias} ({total} contactos)")
            print(_line())
        print("1) Carga manual")
        print("2) Importar desde CSV (nombre, número)")
        print("3) Ver listas cargadas")
        print("4) Eliminar una lista cargada")
        print("5) Volver\n")
        op = ask("Opción: ").strip()
        if op == "1":
            _manual_contacts_entry(store)
        elif op == "2":
            _csv_contacts_entry(store)
        elif op == "3":
            _show_loaded_contacts(store)
        elif op == "4":
            _delete_contact_list(store)
        elif op == "5":
            return
        else:
            _info("Opción inválida. Probá otra vez.", color=Fore.YELLOW)
            press_enter()


def _manual_contacts_entry(store: WhatsAppDataStore) -> None:
    alias = ask("Nombre o alias de la lista: ").strip() or f"lista-{_now().strftime('%H%M%S')}"
    _info("Ingresá número y nombre separados por coma. Línea vacía para terminar.")
    contacts = []
    while True:
        raw = ask("Contacto: ").strip()
        if not raw:
            break
        if "," in raw:
            number, name = [part.strip() for part in raw.split(",", 1)]
        else:
            number, name = raw, ""
        contacts.append({"name": name or number, "number": number})
    if not contacts:
        _info("No se agregaron contactos.", color=Fore.YELLOW)
        press_enter()
        return
    summary = _persist_contacts(store, alias, contacts)
    if summary["stored"]:
        ok(
            f"Se registraron {summary['stored']} contactos en la lista '{alias}'."
        )
    else:
        _info("No se guardaron contactos. Revisá los números proporcionados.", color=Fore.YELLOW)
    if summary["warnings"] and summary["stored"]:
        _info(
            "Algunos contactos quedaron marcados con advertencia por su formato.",
            color=Fore.YELLOW,
        )
    if summary["skipped"]:
        _info(
            f"Se omitieron {summary['skipped']} contactos con números inválidos.",
            color=Fore.YELLOW,
        )
    press_enter()


def _csv_contacts_entry(store: WhatsAppDataStore) -> None:
    path = ask("Ruta del archivo CSV: ").strip()
    if not path:
        _info("No se indicó archivo.", color=Fore.YELLOW)
        press_enter()
        return
    csv_path = Path(path)
    if not csv_path.exists():
        _info("El archivo indicado no existe.", color=Fore.YELLOW)
        press_enter()
        return
    alias = ask("Alias para la lista importada: ").strip() or csv_path.stem
    contacts: list[dict[str, str]] = []
    with csv_path.open(encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        for row in reader:
            if not row:
                continue
            name = (row[0] or "").strip()
            number = (row[1] if len(row) > 1 else "").strip()
            if not number and name:
                number, name = name, number
            if not number:
                continue
            contacts.append({"name": name or number, "number": number})
    if not contacts:
        _info("No se encontraron contactos válidos en el CSV.", color=Fore.YELLOW)
        press_enter()
        return
    summary = _persist_contacts(store, alias, contacts)
    if summary["stored"]:
        ok(
            f"Importación completada. {summary['stored']} registros cargados en '{alias}'."
        )
    else:
        _info("El archivo no aportó contactos válidos tras la validación.", color=Fore.YELLOW)
    if summary["warnings"] and summary["stored"]:
        _info(
            "Algunos contactos quedaron marcados con advertencia por su formato.",
            color=Fore.YELLOW,
        )
    if summary["skipped"]:
        _info(
            f"Se omitieron {summary['skipped']} contactos con números inválidos.",
            color=Fore.YELLOW,
        )
    press_enter()


def _select_existing_list(
    store: WhatsAppDataStore, prompt: str
) -> tuple[str, dict[str, Any]] | None:
    lists = sorted(list(store.iter_lists()), key=lambda item: item[0].lower())
    if not lists:
        _info("Aún no hay listas registradas.", color=Fore.YELLOW)
        press_enter()
        return None
    print(_line())
    _subtitle(prompt)
    for idx, (alias, data) in enumerate(lists, 1):
        total = len(data.get("contacts", []))
        print(f"{idx}) {alias} ({total} contactos)")
    idx = ask_int("Selección: ", min_value=1)
    if idx > len(lists):
        _info("Selección fuera de rango.", color=Fore.YELLOW)
        press_enter()
        return None
    return lists[idx - 1]


def _show_loaded_contacts(store: WhatsAppDataStore) -> None:
    selection = _select_existing_list(store, "Elegí la lista a visualizar")
    if not selection:
        return
    alias, data = selection
    banner()
    title(f"Contactos registrados en '{alias}'")
    print(_line())
    contacts = data.get("contacts", [])
    if not contacts:
        _info("La lista no tiene contactos cargados.", color=Fore.YELLOW)
        press_enter()
        return
    for contact in contacts:
        name = contact.get("name") or contact.get("number")
        print(f"• {name} - {contact.get('number')}")
    press_enter()


def _delete_contact_list(store: WhatsAppDataStore) -> None:
    selection = _select_existing_list(store, "Seleccioná la lista a eliminar")
    if not selection:
        return
    alias, _ = selection
    confirm = ask(f"Confirmá eliminación de la lista '{alias}' (s/N): ").strip().lower()
    if confirm != "s":
        _info("Operación cancelada.")
        press_enter()
        return
    store.state.setdefault("contact_lists", {}).pop(alias, None)
    store.save()
    ok(f"Se eliminó la lista '{alias}'.")
    press_enter()


def _persist_contacts(
    store: WhatsAppDataStore, alias: str, contacts: Iterable[dict[str, str]]
) -> dict[str, int]:
    prepared: list[tuple[dict[str, Any], dict[str, Any]]] = []
    invalid_entries: list[tuple[dict[str, Any], dict[str, Any]]] = []
    warning_entries: list[tuple[dict[str, Any], dict[str, Any]]] = []

    for item in contacts:
        number = item.get("number", "")
        if not number:
            continue
        validation = _validate_phone_number(number)
        normalized_number = validation.get("normalized") or validation.get("raw", "")
        contact = {
            "name": item.get("name", "") or normalized_number,
            "number": normalized_number,
            "status": "sin mensaje",
            "last_message_at": None,
            "last_response_at": None,
            "last_followup_at": None,
            "last_payment_at": None,
            "access_sent_at": None,
            "notes": "",
            "history": [
                {
                    "type": "validation",
                    "status": validation["status"],
                    "checked_at": validation["checked_at"],
                    "message": validation["message"],
                }
            ],
            "validation": validation,
            "delivery_log": [],
        }
        if validation["status"] == "invalid":
            invalid_entries.append((contact, validation))
        elif validation["status"] == "warning":
            warning_entries.append((contact, validation))
        prepared.append((contact, validation))

    if not prepared:
        return {"stored": 0, "invalid": 0, "warnings": 0, "skipped": 0}

    stored_entries = list(prepared)
    skipped_invalid = 0

    if invalid_entries:
        _info("Se detectaron números inválidos y podrían no existir en WhatsApp:", color=Fore.YELLOW)
        for contact, validation in invalid_entries[:5]:
            print(f" • {contact.get('name')} - {validation.get('raw')} ({validation.get('message')})")
        if len(invalid_entries) > 5:
            print(f"   ... y {len(invalid_entries) - 5} más")
        choice = ask("¿Deseás conservarlos igualmente? (s/N): ").strip().lower()
        if choice != "s":
            to_remove = {id(entry[0]) for entry in invalid_entries}
            stored_entries = [entry for entry in stored_entries if id(entry[0]) not in to_remove]
            skipped_invalid = len(invalid_entries)
            _info(
                "Se omitieron los números inválidos para evitar errores futuros.",
                color=Fore.YELLOW,
            )

    if not stored_entries:
        return {
            "stored": 0,
            "invalid": len(invalid_entries),
            "warnings": len(warning_entries),
            "skipped": skipped_invalid,
        }

    if warning_entries:
        _info(
            "Algunos números no tienen formato internacional completo. Se marcarán con advertencia.",
            color=Fore.YELLOW,
        )

    items = [entry[0] for entry in stored_entries]
    lists = store.state.setdefault("contact_lists", {})
    current = lists.get(alias)
    if current:
        current_contacts = current.get("contacts", [])
        current_contacts.extend(items)
        current["contacts"] = current_contacts
        if not current.get("alias"):
            current["alias"] = alias
    else:
        lists[alias] = {
            "alias": alias,
            "created_at": _now_iso(),
            "contacts": items,
            "notes": "",
        }
    store.save()
    return {
        "stored": len(items),
        "invalid": len(invalid_entries),
        "warnings": len(warning_entries),
        "skipped": skipped_invalid,
    }


# ----------------------------------------------------------------------
# 3) Envío de mensajes --------------------------------------------------

def _send_messages(store: WhatsAppDataStore) -> None:
    if not list(store.iter_numbers()):
        _info(
            "Necesitás vincular al menos un número antes de enviar mensajes.",
            color=Fore.YELLOW,
        )
        press_enter()
        return
    if not list(store.iter_lists()):
        _info("Cargá primero una lista de contactos.", color=Fore.YELLOW)
        press_enter()
        return

    while True:
        _reconcile_runs(store)
        banner()
        title("Programación de envíos por WhatsApp")
        print(_line())
        _print_runs_overview(store)
        print(_line())
        print("1) Programar nuevo envío automático")
        print("2) Ver detalle de un envío programado")
        print("3) Pausar o reanudar un envío")
        print("4) Cancelar un envío")
        print("5) Volver\n")
        op = ask("Opción: ").strip()
        if op == "1":
            _plan_message_run(store)
        elif op == "2":
            _show_run_detail(store)
        elif op == "3":
            _toggle_run_pause(store)
        elif op == "4":
            _cancel_run(store)
        elif op == "5":
            return
        else:
            _info("Opción inválida.", color=Fore.YELLOW)
            press_enter()


def _plan_message_run(store: WhatsAppDataStore) -> None:
    number = _choose_number(store)
    if not number:
        return
    contact_list = _choose_contact_list(store)
    if not contact_list:
        return
    contacts = list(contact_list.get("contacts", []))
    if not contacts:
        _info("La lista no tiene contactos.", color=Fore.YELLOW)
        press_enter()
        return

    max_contacts = ask_int(
        "¿Cuántos contactos incluir en este envío? (0 = todos): ",
        min_value=0,
        default=0,
    )
    if max_contacts and max_contacts < len(contacts):
        targets = contacts[:max_contacts]
    else:
        targets = contacts
    if not targets:
        _info("No se seleccionaron contactos para el envío.", color=Fore.YELLOW)
        press_enter()
        return

    store_dirty = False
    invalid_targets: list[tuple[dict[str, Any], dict[str, Any]]] = []
    warning_targets: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for contact in list(targets):
        previous_validation = contact.get("validation", {})
        validation = _update_contact_validation(contact)
        if validation != previous_validation:
            store_dirty = True
        if validation["status"] == "invalid":
            invalid_targets.append((contact, validation))
        elif validation["status"] == "warning":
            warning_targets.append((contact, validation))

    if invalid_targets:
        _info(
            "Hay contactos con números inválidos que WhatsApp rechazará.",
            color=Fore.YELLOW,
        )
        for contact, validation in invalid_targets[:5]:
            print(
                f" • {contact.get('name')} ({validation.get('raw')}) → {validation.get('message')}"
            )
        if len(invalid_targets) > 5:
            print(f"   ... y {len(invalid_targets) - 5} más")
        choice = ask(
            "¿Deseás programar igual para estos contactos inválidos? (s/N): "
        ).strip().lower()
        if choice != "s":
            to_exclude = {id(entry[0]) for entry in invalid_targets}
            targets = [contact for contact in targets if id(contact) not in to_exclude]
            store_dirty = True
            _info(
                "Se excluyeron los contactos con números inválidos del envío.",
                color=Fore.YELLOW,
            )
        else:
            for contact, _ in invalid_targets:
                contact["status"] = "observado"
                history = contact.setdefault("history", [])
                history.append(
                    {
                        "type": "validation_override",
                        "timestamp": _now_iso(),
                        "message": "Se programó un envío pese a la validación inválida.",
                    }
                )
            store_dirty = True

    if warning_targets and targets:
        _info(
            "Algunos contactos no tienen código de país. Podrían fallar los envíos.",
            color=Fore.YELLOW,
        )
        choice = ask("¿Deseás continuar con ellos? (S/n): ").strip().lower()
        if choice == "n":
            to_exclude = {id(entry[0]) for entry in warning_targets}
            targets = [contact for contact in targets if id(contact) not in to_exclude]
            store_dirty = True
            _info(
                "Se quitaron los contactos en advertencia del envío.",
                color=Fore.YELLOW,
            )

    if not targets:
        _info("No quedaron contactos válidos tras la validación.", color=Fore.YELLOW)
        if store_dirty:
            store.save()
        press_enter()
        return

    if store_dirty:
        store.save()

    message_template = ask_multiline(
        "Mensaje a enviar (usa {nombre} para personalizar): "
    ).strip()
    if not message_template:
        _info("Mensaje vacío. Operación cancelada.", color=Fore.YELLOW)
        press_enter()
        return

    min_delay, max_delay = _ask_delay_range()
    session_limit = ask_int(
        "Cantidad máxima de mensajes por sesión (0 = sin tope): ",
        min_value=0,
        default=0,
    )

    planned_at = _now()
    run_id = str(uuid.uuid4())
    events: list[dict[str, Any]] = []
    for contact in targets:
        planned_at += timedelta(seconds=random.uniform(min_delay, max_delay))
        rendered = _render_message(message_template, contact)
        scheduled_at = planned_at.isoformat() + "Z"
        events.append(
            {
                "contact": contact.get("number"),
                "name": contact.get("name"),
                "message": rendered,
                "scheduled_at": scheduled_at,
                "status": "pendiente",
                "delivered_at": None,
                "notes": "",
                "confirmation": "no_enviado",
                "validation_status": contact.get("validation", {}).get("status"),
                "error_code": None,
            }
        )
        _mark_contact_scheduled(
            contact,
            run_id,
            rendered,
            scheduled_at,
            min_delay,
            max_delay,
        )

    run = {
        "id": run_id,
        "number_id": number["id"],
        "number_alias": number.get("alias"),
        "number_phone": number.get("phone"),
        "list_alias": contact_list.get("alias"),
        "created_at": _now_iso(),
        "status": "programado",
        "paused": False,
        "session_limit": session_limit,
        "total_contacts": len(events),
        "processed": 0,
        "completed_at": None,
        "last_activity_at": None,
        "next_run_at": events[0]["scheduled_at"] if events else None,
        "delay": {"min": min_delay, "max": max_delay},
        "message_template": message_template,
        "message_preview": textwrap.shorten(
            message_template, width=90, placeholder="…"
        ),
        "events": events,
        "max_contacts": max_contacts,
        "last_session_at": None,
        "log": [],
    }
    _append_run_log(
        run,
        f"Se programó el envío para {len(events)} contactos con delays entre {min_delay:.1f}s y {max_delay:.1f}s.",
    )
    store.state.setdefault("message_runs", []).append(run)
    store.save()
    ok(
        "El envío quedó programado y continuará ejecutándose en segundo plano con ritmo humano."
    )
    press_enter()


def _print_runs_overview(store: WhatsAppDataStore) -> None:
    runs = store.state.setdefault("message_runs", [])
    if not runs:
        _info("No hay envíos programados todavía. Usá la opción 1 para crear uno.")
        return

    active = [
        run for run in runs if (run.get("status") or "").lower() not in {"completado", "cancelado"}
    ]
    if active:
        _subtitle("Envíos activos")
        for run in sorted(active, key=lambda item: item.get("created_at") or ""):
            total, sent, pending, cancelled, failed = _run_counts(run)
            status = _run_status_label(run)
            next_run = run.get("next_run_at") or "(esperando horario)"
            result_bits = [f"{sent}/{total} enviados"]
            if failed:
                result_bits.append(f"{failed} fallidos")
            if cancelled:
                result_bits.append(f"{cancelled} omitidos/cancelados")
            print(
                f" • {_run_list_label(run)} → {_run_number_label(run)} | {status} | "
                f"{' • '.join(result_bits)} | Próximo: {next_run}"
            )
    else:
        _info("No hay ejecuciones activas en este momento.")

    completed = [
        run for run in runs if (run.get("status") or "").lower() in {"completado", "cancelado"}
    ]
    if completed:
        print()
        _subtitle("Historial reciente")
        for run in sorted(
            completed,
            key=lambda item: item.get("completed_at") or item.get("last_activity_at") or item.get("created_at") or "",
            reverse=True,
        )[:3]:
            total, sent, pending, cancelled, failed = _run_counts(run)
            status = _run_status_label(run)
            finished = run.get("completed_at") or run.get("last_activity_at") or run.get("created_at")
            result_bits = [f"{sent}/{total} enviados"]
            if failed:
                result_bits.append(f"{failed} fallidos")
            if cancelled:
                result_bits.append(f"{cancelled} omitidos/cancelados")
            print(
                f" • {_run_list_label(run)} → {_run_number_label(run)} | {status} | "
                f"{' • '.join(result_bits)} | Finalizó: {finished or 'sin fecha'}"
            )


def _show_run_detail(store: WhatsAppDataStore) -> None:
    run = _select_run(
        store,
        "Seleccioná el envío a monitorear",
        include_completed=True,
    )
    if not run:
        return
    _reconcile_runs(store)
    banner()
    title("Detalle del envío por WhatsApp")
    print(_line())
    total, sent, pending, cancelled, failed = _run_counts(run)
    print(f"Lista: {_run_list_label(run)} → Número: {_run_number_label(run)}")
    print(f"Estado actual: {_run_status_label(run)}")
    print(f"Mensajes enviados: {sent}/{total}")
    print(
        f"Pendientes: {pending} | Fallidos: {failed} | Cancelados/Omitidos: {cancelled}"
    )
    print(f"Delay configurado: {_format_delay(run.get('delay', {'min': 5.0, 'max': 12.0}))}")
    session_limit = run.get("session_limit") or 0
    if session_limit:
        print(f"Límite por sesión: {session_limit} mensajes")
    next_run = run.get("next_run_at")
    if next_run:
        print(f"Próximo envío estimado: {next_run}")
    if run.get("last_session_at"):
        print(f"Última sesión completada: {run.get('last_session_at')}")
    if run.get("message_preview"):
        print(f"Plantilla: {run.get('message_preview')}")
    next_event = next(
        (event for event in run.get("events", []) if (event.get("status") or "") == "pendiente"),
        None,
    )
    if next_event:
        print(
            "Próximo contacto: "
            f"{next_event.get('name') or next_event.get('contact')} a las {next_event.get('scheduled_at')}"
        )
    log = run.get("log", [])
    if log:
        print()
        _subtitle("Actividad registrada")
        for entry in log[-5:]:
            print(f" - {entry.get('timestamp')}: {entry.get('message')}")

    processed_events = [
        event
        for event in run.get("events", [])
        if (event.get("status") or "") in {"enviado", "fallido", "cancelado", "omitido"}
    ]
    if processed_events:
        print()
        _subtitle("Resultados recientes por contacto")
        for event in processed_events[-10:]:
            contact_label = event.get("name") or event.get("contact") or "(sin nombre)"
            status_label = _format_event_status(event)
            print(f" - {contact_label}: {status_label}")
            if event.get("notes"):
                print(f"     Motivo: {event.get('notes')}")
    press_enter()


def _toggle_run_pause(store: WhatsAppDataStore) -> None:
    run = _select_run(
        store,
        "Seleccioná el envío a pausar o reanudar",
        include_completed=False,
    )
    if not run:
        return
    status = (run.get("status") or "").lower()
    if status in {"completado", "cancelado"}:
        _info("Ese envío ya finalizó y no puede modificarse.", color=Fore.YELLOW)
        press_enter()
        return
    if run.get("paused"):
        run["paused"] = False
        run["status"] = "en progreso" if run.get("processed") else "programado"
        run["next_run_at"] = _next_pending_at(run.get("events", []))
        _append_run_log(run, "La ejecución se reanudó manualmente.")
        ok("El envío se reanudó. Continuará respetando los delays configurados.")
    else:
        run["paused"] = True
        run["status"] = "en pausa"
        run["last_session_at"] = _now_iso()
        _append_run_log(run, "La ejecución se pausó manualmente.")
        ok("El envío quedó en pausa segura.")
    store.save()
    press_enter()


def _cancel_run(store: WhatsAppDataStore) -> None:
    run = _select_run(
        store,
        "Seleccioná el envío a cancelar",
        include_completed=False,
    )
    if not run:
        return
    status = (run.get("status") or "").lower()
    if status == "cancelado":
        _info("Ese envío ya está cancelado.")
        press_enter()
        return
    if status == "completado":
        _info("Ese envío ya finalizó por completo.", color=Fore.YELLOW)
        press_enter()
        return
    confirm = ask("Confirmá la cancelación permanente (s/N): ").strip().lower()
    if confirm != "s":
        _info("Operación cancelada.")
        press_enter()
        return
    for event in run.get("events", []):
        if (event.get("status") or "") == "pendiente":
            _reset_contact_for_cancellation(store, run, event)
    run["status"] = "cancelado"
    run["paused"] = False
    run["completed_at"] = _now_iso()
    run["next_run_at"] = None
    _refresh_run_counters(run)
    _append_run_log(run, "La ejecución fue cancelada manualmente.")
    store.save()
    ok("El envío se canceló sin afectar al resto del sistema.")
    press_enter()


def _select_run(
    store: WhatsAppDataStore,
    prompt: str,
    *,
    include_completed: bool = True,
) -> dict[str, Any] | None:
    runs = store.state.setdefault("message_runs", [])
    filtered: list[dict[str, Any]] = []
    for run in runs:
        status = (run.get("status") or "").lower()
        if not include_completed and status in {"completado", "cancelado"}:
            continue
        filtered.append(run)
    if not filtered:
        _info("No hay envíos disponibles para esta acción.", color=Fore.YELLOW)
        press_enter()
        return None
    print(_line())
    _subtitle(prompt)
    for idx, run in enumerate(filtered, 1):
        total, sent, pending, cancelled, failed = _run_counts(run)
        print(
            f"{idx}) {_run_list_label(run)} → {_run_number_label(run)} | {_run_status_label(run)} | "
            f"{sent}/{total} enviados"
            + (f" • {failed} fallidos" if failed else "")
            + (f" • {cancelled} omitidos" if cancelled else "")
            + f" | Pendientes: {pending}"
        )
    idx = ask_int("Selección: ", min_value=1)
    if idx > len(filtered):
        _info("Selección fuera de rango.", color=Fore.YELLOW)
        press_enter()
        return None
    return filtered[idx - 1]


def _run_number_label(run: dict[str, Any]) -> str:
    return run.get("number_alias") or run.get("number_phone") or "(sin alias)"


def _run_list_label(run: dict[str, Any]) -> str:
    return run.get("list_alias") or "(sin lista)"


def _run_status_label(run: dict[str, Any]) -> str:
    status = (run.get("status") or "programado").lower()
    if status == "en pausa" or run.get("paused"):
        return "⏸ en pausa"
    if status == "en progreso":
        return "🟢 en progreso"
    if status == "programado":
        return "🕒 programado"
    if status == "completado":
        return "✅ completado"
    if status == "cancelado":
        return "✖ cancelado"
    return status


def _run_counts(run: dict[str, Any]) -> tuple[int, int, int, int, int]:
    events = run.get("events", [])
    total = len(events)
    sent = sum(1 for event in events if (event.get("status") or "") == "enviado")
    failed = sum(1 for event in events if (event.get("status") or "") == "fallido")
    pending = sum(1 for event in events if (event.get("status") or "") == "pendiente")
    cancelled = sum(
        1
        for event in events
        if (event.get("status") or "") in {"cancelado", "omitido"}
    )
    return total, sent, pending, cancelled, failed


def _confirmation_badge(event: dict[str, Any]) -> str:
    confirmation = (event.get("confirmation") or "no_enviado").lower()
    if confirmation == "leido":
        return "✔✔"
    if confirmation == "entregado":
        return "✔✔"
    if confirmation == "enviado":
        return "✔"
    return "✖"


def _format_event_status(event: dict[str, Any]) -> str:
    status = (event.get("status") or "").lower()
    reason = event.get("notes") or ""
    badge = _confirmation_badge(event)
    if status == "enviado":
        return f"{badge} Entregado"
    if status == "fallido":
        base = "✖ Fallido"
        if reason:
            base += f" – {reason}"
        return base
    if status == "pendiente":
        return "⏳ Pendiente"
    if status == "omitido":
        base = "⚪ Omitido"
        if reason:
            base += f" – {reason}"
        return base
    if status == "cancelado":
        base = "⏹ Cancelado"
        if reason:
            base += f" – {reason}"
        return base
    return status or "(desconocido)"


def _append_run_log(run: dict[str, Any], message: str) -> None:
    log = run.setdefault("log", [])
    log.append({"timestamp": _now_iso(), "message": message})
    if len(log) > 50:
        del log[:-50]


def _next_pending_at(events: Iterable[dict[str, Any]]) -> str | None:
    upcoming = [
        event.get("scheduled_at")
        for event in events
        if (event.get("status") or "") == "pendiente" and event.get("scheduled_at")
    ]
    if not upcoming:
        return None
    return min(upcoming)


def _refresh_run_counters(run: dict[str, Any]) -> None:
    events = run.get("events", [])
    run["total_contacts"] = len(events)
    run["processed"] = sum(
        1
        for event in events
        if (event.get("status") or "") in {"enviado", "cancelado", "omitido", "fallido"}
    )


def _reconcile_runs(store: WhatsAppDataStore) -> None:
    runs = store.state.setdefault("message_runs", [])
    now = _now()
    changed = False
    for run in runs:
        status = (run.get("status") or "").lower()
        events = run.get("events", [])
        if not events:
            continue
        if status == "cancelado":
            continue
        if run.get("paused"):
            next_at = _next_pending_at(events)
            if run.get("next_run_at") != next_at:
                run["next_run_at"] = next_at
                changed = True
            continue

        session_limit = run.get("session_limit") or 0
        processed_now = 0
        for event in events:
            if (event.get("status") or "") != "pendiente":
                continue
            scheduled_at = _parse_iso(event.get("scheduled_at")) or now
            if scheduled_at > now:
                upcoming = event.get("scheduled_at")
                if run.get("next_run_at") != upcoming:
                    run["next_run_at"] = upcoming
                    changed = True
                break
            if session_limit and processed_now >= session_limit:
                upcoming = event.get("scheduled_at")
                if run.get("next_run_at") != upcoming:
                    run["next_run_at"] = upcoming
                    changed = True
                break
            if _deliver_event(store, run, event):
                processed_now += 1
                changed = True
        if session_limit and processed_now >= session_limit and any(
            (event.get("status") or "") == "pendiente" for event in events
        ):
            if not run.get("paused"):
                run["paused"] = True
                run["status"] = "en pausa"
                run["last_session_at"] = _now_iso()
                _append_run_log(
                    run,
                    "Se alcanzó el límite de mensajes por sesión. La ejecución se pausó automáticamente.",
                )
                changed = True
            next_at = _next_pending_at(events)
            if run.get("next_run_at") != next_at:
                run["next_run_at"] = next_at
                changed = True
            _refresh_run_counters(run)
            continue
        if all(
            (event.get("status") or "") in {"enviado", "cancelado", "omitido", "fallido"}
            for event in events
        ):
            if status != "completado":
                run["status"] = "completado"
                run["completed_at"] = _now_iso()
                run["paused"] = False
                run["next_run_at"] = None
                _append_run_log(
                    run,
                    "La ejecución finalizó y todos los mensajes fueron procesados.",
                )
                changed = True
            _refresh_run_counters(run)
            continue
        next_at = _next_pending_at(events)
        if run.get("next_run_at") != next_at:
            run["next_run_at"] = next_at
            changed = True
        if any((event.get("status") or "") in {"enviado", "fallido"} for event in events):
            if run.get("status") not in {"en pausa", "completado"}:
                run["status"] = "en progreso"
                changed = True
        _refresh_run_counters(run)
    if changed:
        store.save()


def _send_message_via_backend(
    sender: dict[str, Any], contact: dict[str, Any], event: dict[str, Any]
) -> dict[str, Any]:
    method = (sender.get("connection_method") or "").lower()
    message = event.get("message", "")
    if method == "selenium":
        return _send_with_selenium(sender, contact, message)
    return {
        "success": True,
        "confirmation": "entregado",
        "note": event.get("notes") or "Mensaje enviado correctamente.",
        "delivered_at": _now_iso(),
    }


def _start_selenium_driver(session_dir: Path) -> tuple[Any | None, str | None, str | None]:
    try:
        from selenium import webdriver
        from selenium.common.exceptions import WebDriverException
        from selenium.webdriver.chrome.options import Options as ChromeOptions
    except ImportError:
        return None, None, (
            "Selenium no está disponible en este entorno. Instalalo para habilitar el envío automatizado."
        )

    profile_root = session_dir / "selenium_profile"
    chrome_profile = profile_root / "chrome"
    chrome_profile.mkdir(parents=True, exist_ok=True)

    driver = None
    label: str | None = None
    error: str | None = None

    try:
        chrome_options = ChromeOptions()
        chrome_options.add_argument(f"--user-data-dir={str(chrome_profile)}")
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_argument("--disable-infobars")
        chrome_options.add_argument("--remote-allow-origins=*")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1280,720")
        driver = webdriver.Chrome(options=chrome_options)
        label = "Chrome"
    except Exception as exc:  # noqa: BLE001
        driver = None
        error = str(exc)

    if driver is None:
        try:
            driver = webdriver.Safari()
            label = "Safari"
            error = None
        except Exception as exc:  # noqa: BLE001
            driver = None
            error = error or str(exc)

    if driver is None:
        return None, None, (
            "No se pudo iniciar un navegador con Selenium. Verificá que el driver esté instalado y habilitado."
        )

    return driver, label, error


def _collect_selenium_alert_text(driver: Any) -> str:
    texts: list[str] = []
    selectors = [
        "div[data-testid='app-state-message']",
        "div[data-testid='alert-qr-text']",
        "div[data-testid='empty-state-title']",
        "div[role='dialog']",
        "div[data-testid='popup-controls-ok']",
    ]
    for selector in selectors:
        try:
            for element in driver.find_elements("css selector", selector):
                text = (element.text or "").strip()
                if text and text not in texts:
                    texts.append(text)
        except Exception:  # noqa: BLE001
            continue
    if texts:
        return " ".join(texts)
    try:
        body = driver.find_element("tag name", "body")
        snippet = (body.text or "").strip().splitlines()
        if snippet:
            return snippet[0]
    except Exception:  # noqa: BLE001
        return ""
    return ""


def _extract_selenium_bubble_text(element: Any) -> str:
    texts: list[str] = []
    try:
        candidates = element.find_elements("css selector", "span[data-testid='conversation-text']")
    except Exception:  # noqa: BLE001
        candidates = []
    if not candidates:
        try:
            candidates = element.find_elements(
                "css selector", "span.selectable-text.copyable-text span"
            )
        except Exception:  # noqa: BLE001
            candidates = []
    for item in candidates:
        try:
            text = (item.text or "").strip()
        except Exception:  # noqa: BLE001
            text = ""
        if text:
            texts.append(text)
    return "\n".join(texts).strip()


def _send_with_selenium(
    sender: dict[str, Any], contact: dict[str, Any], message: str
) -> dict[str, Any]:
    try:
        from selenium.common.exceptions import TimeoutException, WebDriverException
        from selenium.webdriver.common.by import By
        from selenium.webdriver.common.keys import Keys
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait
    except ImportError:
        return {
            "success": False,
            "code": "selenium_missing",
            "reason": "Selenium no está instalado. Instalalo para enviar mensajes automáticamente.",
            "session_expired": False,
        }

    session_path = sender.get("session_path")
    if not session_path:
        return {
            "success": False,
            "code": "session_missing",
            "reason": "No se encontró la carpeta de sesión asociada a este número.",
            "session_expired": False,
        }

    session_dir = Path(session_path)
    if not session_dir.exists():
        return {
            "success": False,
            "code": "session_missing",
            "reason": "La sesión guardada ya no está disponible en el disco.",
            "session_expired": False,
        }

    driver, driver_label, init_error = _start_selenium_driver(session_dir)
    if driver is None:
        return {
            "success": False,
            "code": "driver_unavailable",
            "reason": init_error
            or "No se pudo iniciar el navegador automatizado para WhatsApp Web.",
            "session_expired": False,
        }

    digits = "".join(ch for ch in (contact.get("number") or "") if ch.isdigit())
    if not digits:
        try:
            driver.quit()
        except Exception:  # noqa: BLE001
            pass
        return {
            "success": False,
            "code": "invalid_number",
            "reason": "El número no tiene dígitos suficientes para WhatsApp.",
            "session_expired": False,
        }

    wait = WebDriverWait(driver, 45)

    try:
        driver.get("https://web.whatsapp.com/")
        try:
            wait.until(
                EC.any_of(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-testid='pane-side']")),
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-testid='chat-list']")),
                    EC.presence_of_element_located((By.CSS_SELECTOR, "canvas[data-testid='qrcode']")),
                )
            )
        except TimeoutException:
            return {
                "success": False,
                "code": "whatsapp_unreachable",
                "reason": "WhatsApp Web no respondió a tiempo. Intentá nuevamente en unos minutos.",
                "session_expired": False,
            }

        if driver.find_elements(By.CSS_SELECTOR, "canvas[data-testid='qrcode']"):
            return {
                "success": False,
                "code": "session_expired",
                "reason": "La sesión de WhatsApp caducó. Volvé a escanear el código QR.",
                "session_expired": True,
            }

        driver.get(f"https://web.whatsapp.com/send?phone={digits}")

        try:
            wait.until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "div[contenteditable='true'][data-testid='conversation-compose-box-input']")
                )
            )
        except TimeoutException:
            message_text = _collect_selenium_alert_text(driver) or (
                "No se pudo abrir la conversación. Confirmá que el número tenga WhatsApp."
            )
            return {
                "success": False,
                "code": "chat_unavailable",
                "reason": message_text,
                "session_expired": False,
            }

        try:
            input_box = driver.find_element(
                By.CSS_SELECTOR,
                "div[contenteditable='true'][data-testid='conversation-compose-box-input']",
            )
        except Exception:  # noqa: BLE001
            return {
                "success": False,
                "code": "input_missing",
                "reason": "No se encontró el cuadro de mensaje en WhatsApp Web.",
                "session_expired": False,
            }

        try:
            input_box.click()
            input_box.send_keys(Keys.CONTROL, "a")
            input_box.send_keys(Keys.DELETE)
        except Exception:  # noqa: BLE001
            try:
                input_box.click()
                input_box.send_keys(Keys.COMMAND, "a")
                input_box.send_keys(Keys.DELETE)
            except Exception:  # noqa: BLE001
                pass

        typed_message = message or ""
        if typed_message.strip():
            for index, line in enumerate(typed_message.splitlines() or [""]):
                if index:
                    input_box.send_keys(Keys.SHIFT, Keys.ENTER)
                if line:
                    input_box.send_keys(line)
                else:
                    input_box.send_keys(" ")
        else:
            input_box.send_keys(" ")

        snapshot = (input_box.text or "").strip()
        if not snapshot:
            return {
                "success": False,
                "code": "empty_message",
                "reason": "El mensaje quedó vacío y no se envió a WhatsApp.",
                "session_expired": False,
            }

        try:
            send_button = wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button[data-testid='compose-btn-send']"))
            )
        except TimeoutException:
            return {
                "success": False,
                "code": "send_disabled",
                "reason": "WhatsApp no habilitó el botón de envío para este contacto.",
                "session_expired": False,
            }

        before_count = len(driver.find_elements(By.CSS_SELECTOR, "div[data-testid='msg-container']"))
        send_button.click()

        try:
            wait.until(
                lambda drv: len(drv.find_elements(By.CSS_SELECTOR, "div[data-testid='msg-container']"))
                > before_count
            )
        except TimeoutException:
            return {
                "success": False,
                "code": "send_unconfirmed",
                "reason": "WhatsApp no confirmó el mensaje en la conversación.",
                "session_expired": False,
            }

        bubbles = driver.find_elements(By.CSS_SELECTOR, "div[data-testid='msg-container']")
        if not bubbles:
            return {
                "success": False,
                "code": "bubble_missing",
                "reason": "No se detectó el mensaje dentro de la conversación.",
                "session_expired": False,
            }

        last_bubble = bubbles[-1]
        bubble_text = _extract_selenium_bubble_text(last_bubble)
        normalized_message = "\n".join(line.strip() for line in typed_message.splitlines()).strip()
        normalized_bubble = "\n".join(line.strip() for line in bubble_text.splitlines()).strip()
        if normalized_message and (
            normalized_message not in normalized_bubble
            and normalized_bubble not in normalized_message
        ):
            return {
                "success": False,
                "code": "text_mismatch",
                "reason": "WhatsApp no mostró el contenido del mensaje enviado.",
                "session_expired": False,
            }

        confirmation = "enviado"
        try:
            if last_bubble.find_elements(By.CSS_SELECTOR, "svg[data-testid='msg-dblcheck-read']"):
                confirmation = "leido"
            elif last_bubble.find_elements(By.CSS_SELECTOR, "svg[data-testid='msg-dblcheck']"):
                confirmation = "entregado"
            elif last_bubble.find_elements(By.CSS_SELECTOR, "svg[data-testid='msg-check']"):
                confirmation = "enviado"
        except Exception:  # noqa: BLE001
            confirmation = "enviado"

        note = "Mensaje confirmado en WhatsApp Web mediante Selenium."
        if driver_label:
            note = f"Mensaje confirmado en WhatsApp Web mediante Selenium ({driver_label})."

        return {
            "success": True,
            "confirmation": confirmation,
            "note": note,
            "delivered_at": _now_iso(),
            "session_expired": False,
        }
    except TimeoutException:
        return {
            "success": False,
            "code": "timeout",
            "reason": "WhatsApp Web tardó demasiado en responder al enviar el mensaje.",
            "session_expired": False,
        }
    except WebDriverException:
        return {
            "success": False,
            "code": "webdriver_error",
            "reason": "Selenium reportó un error inesperado durante el envío.",
            "session_expired": False,
        }
    finally:
        try:
            driver.quit()
        except Exception:  # noqa: BLE001
            pass


def _deliver_event(store: WhatsAppDataStore, run: dict[str, Any], event: dict[str, Any]) -> bool:
    if (event.get("status") or "") != "pendiente":
        return False
    delivered_at = _now_iso()
    contact_list = store.find_list(run.get("list_alias", ""))
    if not contact_list:
        event["status"] = "omitido"
        event["delivered_at"] = delivered_at
        event["confirmation"] = "no_enviado"
        event["error_code"] = "list_missing"
        event["notes"] = "La lista vinculada fue eliminada."
        _append_run_log(
            run,
            f"La lista '{run.get('list_alias')}' ya no existe. Se omitió el envío a {event.get('contact')}.",
        )
    else:
        contact = _locate_contact(contact_list, event.get("contact"))
        if not contact:
            event["status"] = "omitido"
            event["delivered_at"] = delivered_at
            event["confirmation"] = "no_enviado"
            event["error_code"] = "contact_missing"
            event["notes"] = "El contacto ya no está disponible en la lista."
            _append_run_log(
                run,
                f"No se encontró el contacto {event.get('contact')} dentro de la lista.",
            )
        else:
            validation = _update_contact_validation(contact)
            event["validation_status"] = validation.get("status")
            sender = store.find_number(run.get("number_id", ""))
            failure_reason: str | None = None
            failure_code: str | None = None
            delivery_result: dict[str, Any] | None = None
            if validation["status"] == "invalid":
                failure_reason = validation.get("message") or "Número inválido."
                failure_code = "invalid_number"
            elif not sender:
                failure_reason = "El número de envío ya no está registrado."
                failure_code = "sender_missing"
            elif not sender.get("connected"):
                failure_reason = "La sesión de WhatsApp seleccionada no está activa."
                failure_code = "session_inactiva"
            elif (sender.get("connection_state") or "").lower() == "fallido":
                failure_reason = "La vinculación del número presentó un error reciente."
                failure_code = "session_error"
            else:
                delivery_result = _send_message_via_backend(sender, contact, event)
                if not delivery_result.get("success"):
                    failure_reason = (
                        delivery_result.get("reason")
                        or "WhatsApp no confirmó el envío del mensaje."
                    )
                    failure_code = delivery_result.get("code") or "send_failed"
                    if delivery_result.get("session_expired"):
                        sender["connected"] = False
                        sender["connection_state"] = "fallido"
                        sender["last_connected_at"] = None
                        sender.setdefault("session_notes", []).append(
                            {
                                "created_at": _now_iso(),
                                "text": "La sesión caducó durante un envío automático. Repetí la vinculación escaneando el QR.",
                            }
                        )
                        _append_run_log(
                            run,
                            "La sesión de WhatsApp se cerró durante el envío. Es necesario volver a vincular el número.",
                        )
                else:
                    delivered_at = delivery_result.get("delivered_at") or delivered_at

            if failure_reason:
                contact["status"] = "observado"
                contact.setdefault("history", []).append(
                    {
                        "type": "send_failed",
                        "run_id": run.get("id"),
                        "message": event.get("message", ""),
                        "attempted_at": delivered_at,
                        "error": failure_reason,
                    }
                )
                event["status"] = "fallido"
                event["notes"] = failure_reason
                event["error_code"] = failure_code
                event["confirmation"] = "no_enviado"
                event["delivered_at"] = delivered_at
                _append_delivery_log(
                    contact,
                    run,
                    status="fallido",
                    reason=failure_reason,
                    confirmation="no_enviado",
                )
                _append_run_log(
                    run,
                    f"Fallo el envío a {contact.get('name') or contact.get('number')}: {failure_reason}",
                )
            else:
                confirmation_value = "entregado"
                success_note = event.get("notes") or "Mensaje enviado correctamente."
                if delivery_result:
                    confirmation_value = (
                        delivery_result.get("confirmation") or confirmation_value
                    )
                    success_note = delivery_result.get("note") or success_note
                contact["status"] = "mensaje enviado"
                contact["last_message_at"] = event.get("scheduled_at") or delivered_at
                contact.setdefault("history", []).append(
                    {
                        "type": "send",
                        "run_id": run.get("id"),
                        "message": event.get("message", ""),
                        "sent_at": delivered_at,
                        "delay": run.get("delay"),
                        "confirmation": confirmation_value,
                    }
                )
                event["status"] = "enviado"
                event["delivered_at"] = delivered_at
                event["confirmation"] = confirmation_value
                event["error_code"] = None
                event["notes"] = success_note
                _append_delivery_log(
                    contact,
                    run,
                    status="enviado",
                    reason=success_note,
                    confirmation=confirmation_value,
                )
                _append_run_log(
                    run,
                    "WhatsApp confirmó el mensaje para {} con estado {}.".format(
                        contact.get("name") or contact.get("number"),
                        confirmation_value,
                    ),
                )
    run["last_activity_at"] = delivered_at
    _refresh_run_counters(run)
    if (event.get("status") or "") in {"enviado", "fallido"}:
        run["status"] = "en progreso"
    return True


def _locate_contact(contact_list: dict[str, Any], number: str | None) -> dict[str, Any] | None:
    if not number:
        return None
    for contact in contact_list.get("contacts", []):
        if contact.get("number") == number:
            return contact
    return None


def _mark_contact_scheduled(
    contact: dict[str, Any],
    run_id: str,
    message: str,
    scheduled_at: str,
    min_delay: float,
    max_delay: float,
) -> None:
    preview = textwrap.shorten(message, width=80, placeholder="…") if message else ""
    contact.setdefault("history", []).append(
        {
            "type": "scheduled",
            "run_id": run_id,
            "scheduled_at": scheduled_at,
            "message": preview,
            "delay": {"min": min_delay, "max": max_delay},
        }
    )
    current_status = (contact.get("status") or "").lower()
    if not current_status or any(hint in current_status for hint in ("sin", "espera", "program")):
        contact["status"] = "mensaje programado"


def _reset_contact_for_cancellation(
    store: WhatsAppDataStore,
    run: dict[str, Any],
    event: dict[str, Any],
) -> None:
    contact_list = store.find_list(run.get("list_alias", ""))
    if not contact_list:
        event["status"] = "cancelado"
        event["delivered_at"] = _now_iso()
        return
    contact = _locate_contact(contact_list, event.get("contact"))
    event["status"] = "cancelado"
    event["delivered_at"] = _now_iso()
    if not contact:
        return
    history = contact.setdefault("history", [])
    history.append(
        {
            "type": "cancelled",
            "run_id": run.get("id"),
            "scheduled_at": event.get("scheduled_at"),
            "cancelled_at": event.get("delivered_at"),
        }
    )
    current_status = (contact.get("status") or "").lower()
    if "program" in current_status and not contact.get("last_message_at"):
        contact["status"] = "sin mensaje"

def _choose_number(store: WhatsAppDataStore) -> dict[str, Any] | None:
    options = list(store.iter_numbers())
    if not options:
        return None
    print(_line())
    _subtitle("Seleccioná el número de envío")
    for idx, item in enumerate(options, 1):
        if item.get("connected"):
            status = "🟢 verificado"
        elif item.get("connection_state") == "fallido":
            status = "🔴 error"
        else:
            status = "⚪ pendiente"
        print(f"{idx}) {item.get('alias')} ({item.get('phone')}) - {status}")
    idx = ask_int("Número elegido: ", min_value=1)
    if idx > len(options):
        _info("Selección fuera de rango.", color=Fore.YELLOW)
        press_enter()
        return None
    return options[idx - 1]


def _choose_contact_list(store: WhatsAppDataStore) -> dict[str, Any] | None:
    lists = list(store.iter_lists())
    if not lists:
        return None
    print(_line())
    _subtitle("Seleccioná la lista de contactos")
    for idx, (alias, data) in enumerate(lists, 1):
        total = len(data.get("contacts", []))
        print(f"{idx}) {alias} ({total} contactos)")
    idx = ask_int("Lista elegida: ", min_value=1)
    if idx > len(lists):
        _info("Selección fuera de rango.", color=Fore.YELLOW)
        press_enter()
        return None
    alias, data = lists[idx - 1]
    data["alias"] = alias
    return data


def _ask_delay_range() -> tuple[float, float]:
    while True:
        try:
            min_delay = float(ask("Delay mínimo (segundos): ").strip())
            max_delay = float(ask("Delay máximo (segundos): ").strip())
        except ValueError:
            _info("Ingresá números válidos para los delays.", color=Fore.YELLOW)
            continue
        if min_delay <= 0 or max_delay <= 0:
            _info("Los delays deben ser mayores a cero.", color=Fore.YELLOW)
            continue
        if max_delay < min_delay:
            _info("El máximo debe ser mayor o igual al mínimo.", color=Fore.YELLOW)
            continue
        return min_delay, max_delay


def _render_message(template: str, contact: dict[str, Any]) -> str:
    safe_contact = {"nombre": contact.get("name", ""), "numero": contact.get("number", "")}
    try:
        return template.format(**{"nombre": safe_contact["nombre"], "numero": safe_contact["numero"]})
    except KeyError:
        return template


# ----------------------------------------------------------------------
# 4) Automatizar respuestas con IA -------------------------------------

def _configure_ai_responses(store: WhatsAppDataStore) -> None:
    number = _choose_number(store)
    if not number:
        return
    configs = store.state.setdefault("ai_automations", {})
    current = configs.get(number["id"], store._ensure_ai_config({}))
    while True:
        banner()
        title("Automatización de respuestas con IA")
        print(_line())
        _info(f"Número seleccionado: {number.get('alias')} ({number.get('phone')})", bold=True)
        status = "🟢 activo" if current.get("active") else "⚪ en espera"
        print(f"Estado actual: {status}")
        print(f"Delay configurado: {_format_delay(current.get('delay', {'min': 5.0, 'max': 15.0}))}")
        prompt_preview = textwrap.shorten(current.get("prompt", ""), width=90, placeholder="…")
        print(f"Prompt base: {prompt_preview or '(sin definir)'}")
        print(f"Envío de audios: {'sí' if current.get('send_audio') else 'no'}")
        print(_line())
        print("1) Activar o actualizar configuración")
        print("2) Pausar automatización para este número")
        print("3) Volver\n")
        op = ask("Opción: ").strip()
        if op == "1":
            prompt = ask_multiline("Prompt guía para la IA: ").strip() or current.get("prompt", "")
            min_delay, max_delay = _ask_delay_range()
            audio = ask("¿Enviar audios cuando sea posible? (s/n): ").strip().lower().startswith("s")
            current.update(
                {
                    "active": True,
                    "prompt": prompt,
                    "delay": {"min": min_delay, "max": max_delay},
                    "send_audio": audio,
                    "last_updated_at": _now_iso(),
                }
            )
            configs[number["id"]] = current
            store.save()
            ok("Automatización actualizada. Se responderá siguiendo un tono humano y cordial.")
            press_enter()
        elif op == "2":
            current["active"] = False
            current["last_updated_at"] = _now_iso()
            configs[number["id"]] = current
            store.save()
            ok("Automatización pausada para este número.")
            press_enter()
        elif op == "3":
            return
        else:
            _info("Opción inválida.", color=Fore.YELLOW)
            press_enter()


# ----------------------------------------------------------------------
# 5) Captura desde Instagram -------------------------------------------

def _instagram_capture(store: WhatsAppDataStore) -> None:
    config = store.state.setdefault("instagram", store._ensure_instagram_config({}))
    while True:
        banner()
        title("Captura de números desde Instagram")
        print(_line())
        print(f"Estado: {'🟢 activo' if config.get('active') else '⚪ en pausa'}")
        print(f"Mensaje inicial: {textwrap.shorten(config.get('message', ''), width=80, placeholder='…')}")
        print(f"Delay configurado: {_format_delay(config.get('delay', {'min': 5.0, 'max': 12.0}))}")
        print(f"Total de capturas: {len(config.get('captures', []))}")
        print(_line())
        print("1) Configurar mensaje y delays")
        print("2) Registrar número capturado manualmente")
        print("3) Ver seguimiento de conversiones")
        print("4) Volver\n")
        op = ask("Opción: ").strip()
        if op == "1":
            active = ask("¿Activar la escucha automática? (s/n): ").strip().lower().startswith("s")
            message = ask_multiline("Mensaje inicial automático: ").strip() or config.get("message", "")
            min_delay, max_delay = _ask_delay_range()
            config.update(
                {
                    "active": active,
                    "message": message,
                    "delay": {"min": min_delay, "max": max_delay},
                    "last_reviewed_at": _now_iso(),
                }
            )
            store.save()
            ok("Integración actualizada. Los leads de Instagram se contactarán de forma natural.")
            press_enter()
        elif op == "2":
            _register_instagram_capture(store, config)
        elif op == "3":
            _show_instagram_tracking(config)
        elif op == "4":
            return
        else:
            _info("Opción inválida.", color=Fore.YELLOW)
            press_enter()


def _register_instagram_capture(store: WhatsAppDataStore, config: dict[str, Any]) -> None:
    name = ask("Nombre de la persona: ").strip()
    number = ask("Número detectado: ").strip()
    if not number:
        _info("Se requiere un número válido.", color=Fore.YELLOW)
        press_enter()
        return
    source = ask("Origen o nota de la conversación (opcional): ").strip() or "Instagram"
    capture = {
        "id": str(uuid.uuid4()),
        "name": name or number,
        "number": number,
        "source": source,
        "captured_at": _now_iso(),
        "message_sent": False,
        "message_sent_at": None,
        "notes": "",
    }
    delay = config.get("delay", {"min": 5.0, "max": 12.0})
    message = config.get("message", "")
    if message:
        capture["message_sent"] = True
        capture["message_sent_at"] = _now_iso()
        capture["notes"] = (
            f"Mensaje inicial programado con delays humanos {_format_delay(delay)}."
        )
    config.setdefault("captures", []).append(capture)
    _auto_add_to_master_list(store, capture)
    store.save()
    ok("Lead capturado y mensaje inicial configurado correctamente.")
    press_enter()


def _auto_add_to_master_list(store: WhatsAppDataStore, capture: dict[str, Any]) -> None:
    lists = store.state.setdefault("contact_lists", {})
    alias = "instagram_auto"
    contact = {
        "name": capture.get("name", capture.get("number", "")),
        "number": capture.get("number", ""),
        "status": "mensaje enviado" if capture.get("message_sent") else "sin mensaje",
        "last_message_at": capture.get("message_sent_at"),
        "last_response_at": None,
        "last_followup_at": None,
        "last_payment_at": None,
        "access_sent_at": None,
        "notes": capture.get("source", "Instagram"),
        "history": [
            {
                "type": "captured",
                "source": capture.get("source", "Instagram"),
                "timestamp": capture.get("captured_at"),
            }
        ],
    }
    validation = _validate_phone_number(contact.get("number", ""))
    contact["number"] = validation.get("normalized") or contact.get("number", "")
    contact.setdefault("history", []).append(
        {
            "type": "validation",
            "status": validation["status"],
            "checked_at": validation["checked_at"],
            "message": validation["message"],
        }
    )
    contact["validation"] = validation
    contact["delivery_log"] = []
    if capture.get("message_sent"):
        contact["history"].append(
            {
                "type": "send",
                "message": capture.get("notes", ""),
                "timestamp": capture.get("message_sent_at"),
            }
        )
    if alias not in lists:
        lists[alias] = {
            "alias": alias,
            "created_at": _now_iso(),
            "contacts": [contact],
            "notes": "Leads generados automáticamente desde Instagram",
        }
    else:
        lists[alias]["contacts"].append(contact)


def _show_instagram_tracking(config: dict[str, Any]) -> None:
    banner()
    title("Seguimiento de conversiones desde Instagram")
    print(_line())
    captures = config.get("captures", [])
    if not captures:
        _info("Aún no hay capturas registradas.")
        press_enter()
        return
    for item in captures:
        status = "mensaje enviado" if item.get("message_sent") else "pendiente"
        print(
            f"• {item.get('name')} ({item.get('number')}) - {status} | "
            f"Detectado: {item.get('captured_at')} | Origen: {item.get('source')}"
        )
    press_enter()


# ----------------------------------------------------------------------
# 6) Seguimiento a no respondidos --------------------------------------

def _followup_manager(store: WhatsAppDataStore) -> None:
    config = store.state.setdefault("followup", store._ensure_followup_config({}))
    banner()
    title("Seguimiento automático de contactos sin respuesta")
    print(_line())
    wait_minutes = ask_int(
        "¿Cuántos minutos esperar antes de etiquetar como no respondido?: ",
        min_value=10,
        default=config.get("default_wait_minutes", 120),
    )
    config["default_wait_minutes"] = wait_minutes
    threshold = _now() - timedelta(minutes=wait_minutes)
    candidates = _find_followup_candidates(store, threshold)
    if not candidates:
        _info("No hay contactos pendientes de seguimiento en este momento.")
        store.save()
        press_enter()
        return
    _info(f"Se encontraron {len(candidates)} contactos sin respuesta.", bold=True)
    mode = ask(
        "¿Enviar mensaje personalizado (p) o generar con IA (i)? [p/i]: "
    ).strip().lower()
    if mode.startswith("i"):
        prompt = ask_multiline("Prompt base para el seguimiento (opcional): ").strip() or config.get(
            "ai_prompt", ""
        )
        config["ai_prompt"] = prompt
        message_base = (
            "Mensaje generado automáticamente siguiendo un tono humano cercano y cordial."
        )
    else:
        message_base = ask_multiline("Mensaje de seguimiento: ").strip() or config.get(
            "manual_message", ""
        )
        config["manual_message"] = message_base
    min_delay, max_delay = _ask_delay_range()
    for entry in candidates:
        contact = entry["contact"]
        personalized = _render_message(message_base, contact)
        contact["status"] = "seguimiento enviado"
        contact["last_followup_at"] = _now_iso()
        contact.setdefault("history", []).append(
            {
                "type": "followup",
                "message": personalized,
                "delay": {"min": min_delay, "max": max_delay},
                "sent_at": _now_iso(),
            }
        )
    config.setdefault("history", []).append(
        {
            "executed_at": _now_iso(),
            "count": len(candidates),
            "delay": {"min": min_delay, "max": max_delay},
            "mode": "ia" if mode.startswith("i") else "manual",
        }
    )
    store.save()
    ok("Seguimiento configurado y mensajes programados con comportamiento humano natural.")
    press_enter()


def _find_followup_candidates(store: WhatsAppDataStore, threshold: datetime) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for alias, data in store.iter_lists():
        for contact in data.get("contacts", []):
            last_message = contact.get("last_message_at")
            responded = contact.get("last_response_at")
            if not last_message:
                continue
            if responded and responded >= last_message:
                continue
            if contact.get("status") in {"pagó", "acceso enviado"}:
                continue
            try:
                sent_dt = datetime.fromisoformat(last_message.replace("Z", ""))
            except Exception:
                continue
            if sent_dt <= threshold:
                results.append({"list": alias, "contact": contact})
    return results


# ----------------------------------------------------------------------
# 7) Gestión de pagos ---------------------------------------------------

def _payments_menu(store: WhatsAppDataStore) -> None:
    payments = store.state.setdefault("payments", store._ensure_payments_config({}))
    while True:
        banner()
        title("Gestión de pagos y entrega de accesos")
        print(_line())
        print(f"Administrador notificaciones: {payments.get('admin_number') or '(sin definir)'}")
        print(f"Pagos pendientes: {len(payments.get('pending', []))}")
        print(f"Pagos completados: {len(payments.get('history', []))}")
        print(_line())
        print("1) Procesar nueva captura de pago")
        print("2) Revisar pendientes y enviar accesos")
        print("3) Configurar mensajes y datos del administrador")
        print("4) Volver\n")
        op = ask("Opción: ").strip()
        if op == "1":
            _process_payment_capture(store, payments)
        elif op == "2":
            _review_pending_payments(store, payments)
        elif op == "3":
            _configure_payment_settings(store, payments)
        elif op == "4":
            return
        else:
            _info("Opción inválida.", color=Fore.YELLOW)
            press_enter()


def _process_payment_capture(store: WhatsAppDataStore, payments: dict[str, Any]) -> None:
    name = ask("Nombre del contacto: ").strip()
    number = ask("Número de WhatsApp: ").strip()
    evidence = ask("Ruta de la captura o palabras clave detectadas: ").strip()
    detected_keywords = _detect_keywords(evidence)
    status = "validado" if _is_payment_valid(detected_keywords) else "pendiente"
    entry = {
        "id": str(uuid.uuid4()),
        "name": name or number,
        "number": number,
        "evidence": evidence,
        "keywords": detected_keywords,
        "status": status,
        "created_at": _now_iso(),
        "validated_at": _now_iso() if status == "validado" else None,
        "welcome_sent_at": None,
        "alert_sent_at": None,
        "notes": "",
    }
    payments.setdefault("pending", []).append(entry)
    if status != "validado":
        entry["alert_sent_at"] = _now_iso()
        _notify_admin(payments, entry)
    else:
        _finalize_payment(store, payments, entry, auto=True)
    store.save()
    ok("Pago registrado. El flujo de validación continúa en segundo plano.")
    press_enter()


def _detect_keywords(evidence: str) -> list[str]:
    lowered = evidence.lower()
    keywords = []
    for hint in ("aprob", "pago", "$", "transfer", "ok", "exitoso"):
        if hint in lowered:
            keywords.append(hint)
    return keywords


def _is_payment_valid(keywords: list[str]) -> bool:
    return any(hint in keywords for hint in ("aprob", "pago", "$", "exitoso"))


def _notify_admin(payments: dict[str, Any], entry: dict[str, Any]) -> None:
    admin = payments.get("admin_number")
    if not admin:
        return
    entry["notes"] = (
        f"Alerta enviada al administrador {admin} para validar el pago de {entry.get('name')}"
    )


def _review_pending_payments(store: WhatsAppDataStore, payments: dict[str, Any]) -> None:
    pending = payments.get("pending", [])
    if not pending:
        _info("No hay pagos pendientes.")
        press_enter()
        return
    for entry in pending:
        print(_line())
        print(f"Contacto: {entry.get('name')} ({entry.get('number')})")
        print(f"Palabras clave detectadas: {', '.join(entry.get('keywords', [])) or 'ninguna'}")
        print(f"Estado actual: {entry.get('status')}")
        decision = ask("¿Marcar como confirmado (c), rechazar (r) o saltar (s)? ").strip().lower()
        if decision.startswith("c"):
            entry["status"] = "validado"
            entry["validated_at"] = _now_iso()
            _finalize_payment(store, payments, entry)
        elif decision.startswith("r"):
            entry["status"] = "rechazado"
            entry["notes"] = "El pago requiere nueva evidencia."
            _send_custom_message(store, entry, "Pago observado. Por favor compartinos una captura clara.")
    payments["pending"] = [
        item for item in pending if item.get("status") not in {"finalizado", "rechazado"}
    ]
    store.save()
    press_enter()


def _finalize_payment(
    store: WhatsAppDataStore,
    payments: dict[str, Any],
    entry: dict[str, Any],
    *,
    auto: bool = False,
) -> None:
    message = payments.get("welcome_message", "")
    link = payments.get("access_link", "")
    composed = message
    if link:
        composed = f"{message}\n{link}" if message else link
    _send_custom_message(store, entry, composed)
    entry["status"] = "finalizado"
    entry["welcome_sent_at"] = _now_iso()
    payments.setdefault("history", []).append(
        {
            "id": entry.get("id"),
            "name": entry.get("name"),
            "number": entry.get("number"),
            "status": "completado",
            "completed_at": _now_iso(),
            "notes": "Procesado automáticamente" if auto else entry.get("notes", ""),
        }
    )
    _update_contact_payment_status(store, entry)


def _send_custom_message(store: WhatsAppDataStore, entry: dict[str, Any], message: str) -> None:
    if not message:
        return
    contact = _locate_contact_by_number(store, entry.get("number", ""))
    if contact:
        contact.setdefault("history", []).append(
            {
                "type": "payment",
                "message": message,
                "sent_at": _now_iso(),
            }
        )
        contact["status"] = "acceso enviado"
        contact["access_sent_at"] = _now_iso()
        contact["last_payment_at"] = _now_iso()
    entry["notes"] = message


def _update_contact_payment_status(store: WhatsAppDataStore, entry: dict[str, Any]) -> None:
    contact = _locate_contact_by_number(store, entry.get("number", ""))
    if not contact:
        return
    contact["status"] = "pagó"
    contact["last_payment_at"] = _now_iso()
    contact.setdefault("history", []).append(
        {
            "type": "payment_confirmed",
            "timestamp": _now_iso(),
            "details": entry.get("notes", ""),
        }
    )


def _locate_contact_by_number(store: WhatsAppDataStore, number: str) -> dict[str, Any] | None:
    for _, data in store.iter_lists():
        for contact in data.get("contacts", []):
            if contact.get("number") == number:
                return contact
    return None


def _configure_payment_settings(store: WhatsAppDataStore, payments: dict[str, Any]) -> None:
    admin = ask("Número del administrador para alertas: ").strip()
    welcome = ask_multiline("Mensaje de bienvenida tras confirmar pago: ").strip() or payments.get(
        "welcome_message", ""
    )
    link = ask("Link de acceso (opcional): ").strip() or payments.get("access_link", "")
    payments.update(
        {
            "admin_number": admin,
            "welcome_message": welcome,
            "access_link": link,
        }
    )
    store.save()
    ok("Datos actualizados. Los pagos se gestionarán con notificaciones limpias.")
    press_enter()


# ----------------------------------------------------------------------
# 8) Estado de contactos y actividad -----------------------------------

def _contacts_state(store: WhatsAppDataStore) -> None:
    banner()
    title("Estado general de contactos y actividad")
    print(_line())
    lists = list(store.iter_lists())
    if not lists:
        _info("Todavía no se cargaron listas de contactos.")
        press_enter()
        return
    totals = []
    for alias, data in lists:
        contacts = data.get("contacts", [])
        summary = _summarize_contacts(contacts)
        totals.append(summary)
        print(f"Lista: {alias}")
        for key, value in summary.items():
            print(f"   - {key}: {value}")
        print()
    if ask("¿Deseás exportar un CSV con el detalle? (s/n): ").strip().lower().startswith("s"):
        path = _export_contacts_csv(store)
        ok(f"Resumen exportado en {path}")
    press_enter()


def _summarize_contacts(contacts: Iterable[dict[str, Any]]) -> dict[str, int]:
    summary = {
        "Total": 0,
        "Mensaje enviado": 0,
        "En espera": 0,
        "Respondió": 0,
        "Pagó": 0,
        "Acceso enviado": 0,
    }
    for contact in contacts:
        summary["Total"] += 1
        status = (contact.get("status") or "").lower()
        if "seguimiento" in status or "sin" in status:
            summary["En espera"] += 1
        if "mensaje" in status:
            summary["Mensaje enviado"] += 1
        if "respond" in status:
            summary["Respondió"] += 1
        if "pag" in status:
            summary["Pagó"] += 1
        if "acceso" in status:
            summary["Acceso enviado"] += 1
    return summary


def _export_contacts_csv(store: WhatsAppDataStore) -> Path:
    now = _now().strftime("%Y%m%d-%H%M%S")
    path = EXPORTS_DIR / f"whatsapp_estado_{now}.csv"
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "lista",
                "nombre",
                "numero",
                "status",
                "ultimo_mensaje",
                "ultima_respuesta",
                "ultimo_seguimiento",
                "ultimo_pago",
                "acceso_enviado",
            ]
        )
        for alias, data in store.iter_lists():
            for contact in data.get("contacts", []):
                writer.writerow(
                    [
                        alias,
                        contact.get("name"),
                        contact.get("number"),
                        contact.get("status"),
                        contact.get("last_message_at"),
                        contact.get("last_response_at"),
                        contact.get("last_followup_at"),
                        contact.get("last_payment_at"),
                        contact.get("access_sent_at"),
                    ]
                )
    return path


__all__ = ["menu_whatsapp"]
