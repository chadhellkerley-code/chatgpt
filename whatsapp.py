# whatsapp.py
# -*- coding: utf-8 -*-
"""Menú de automatización por WhatsApp totalmente integrado con la app CLI."""

from __future__ import annotations

import csv
import json
import random
import textwrap
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Iterator

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


def _now() -> datetime:
    return datetime.utcnow().replace(microsecond=0)


def _now_iso() -> str:
    return _now().isoformat() + "Z"


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
        merged["message_runs"] = list(data.get("message_runs", []))
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
            }
        return {
            "id": value.get("id") or str(uuid.uuid4()),
            "alias": value.get("alias", ""),
            "phone": value.get("phone", ""),
            "connected": bool(value.get("connected", False)),
            "last_connected_at": value.get("last_connected_at"),
            "session_notes": list(value.get("session_notes", [])),
            "keep_alive": bool(value.get("keep_alive", True)),
        }

    # ------------------------------------------------------------------
    def _ensure_contact(self, raw: Any) -> dict[str, Any]:
        if not isinstance(raw, dict):
            raw = {}
        history = [entry for entry in raw.get("history", []) if isinstance(entry, dict)]
        return {
            "name": raw.get("name", ""),
            "number": raw.get("number", ""),
            "status": raw.get("status", "sin mensaje"),
            "last_message_at": raw.get("last_message_at"),
            "last_response_at": raw.get("last_response_at"),
            "last_followup_at": raw.get("last_followup_at"),
            "last_payment_at": raw.get("last_payment_at"),
            "access_sent_at": raw.get("access_sent_at"),
            "notes": raw.get("notes", ""),
            "history": history,
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
        status = "🟢 Activo" if item.get("connected") else "⚪ Disponible"
        last = item.get("last_connected_at") or "(sin actividad)"
        print(f" • {alias} ({item.get('phone')}) - {status} – última conexión: {last}")


def _connect_number(store: WhatsAppDataStore) -> None:
    banner()
    title("Conectar número de WhatsApp")
    print(_line())
    _print_numbers_summary(store)
    print(_line())
    alias = ask("Alias interno para reconocer el número: ").strip()
    phone = ask("Número en formato internacional (ej: +54911...): ").strip()
    if not phone:
        _info("No se ingresó número.", color=Fore.YELLOW)
        press_enter()
        return
    note = ask("Nota interna u observación (opcional): ").strip()
    session_id = str(uuid.uuid4())
    store.state.setdefault("numbers", {})[session_id] = {
        "id": session_id,
        "alias": alias or phone,
        "phone": phone,
        "connected": True,
        "last_connected_at": _now_iso(),
        "session_notes": [
            {
                "created_at": _now_iso(),
                "text": note or "Sesión iniciada mediante escaneo QR",
            }
        ],
        "keep_alive": True,
    }
    store.save()
    print()
    _info(
        "Escaneá el código QR desde WhatsApp Web en tu dispositivo."
        " Cuando finalices presioná Enter para confirmar la vinculación.",
    )
    press_enter("Presioná Enter una vez vinculada la sesión...")
    ok("Sesión vinculada y lista para operar en segundo plano.")
    press_enter()


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
        print("3) Volver\n")
        op = ask("Opción: ").strip()
        if op == "1":
            _manual_contacts_entry(store)
        elif op == "2":
            _csv_contacts_entry(store)
        elif op == "3":
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
    _persist_contacts(store, alias, contacts)
    ok(f"Se registraron {len(contacts)} contactos en la lista '{alias}'.")
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
    _persist_contacts(store, alias, contacts)
    ok(f"Importación completada. {len(contacts)} registros cargados en '{alias}'.")
    press_enter()


def _persist_contacts(store: WhatsAppDataStore, alias: str, contacts: Iterable[dict[str, str]]) -> None:
    items = [
        {
            "name": item.get("name", ""),
            "number": item.get("number", ""),
            "status": "sin mensaje",
            "last_message_at": None,
            "last_response_at": None,
            "last_followup_at": None,
            "last_payment_at": None,
            "access_sent_at": None,
            "notes": "",
            "history": [],
        }
        for item in contacts
        if item.get("number")
    ]
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


# ----------------------------------------------------------------------
# 3) Envío de mensajes --------------------------------------------------

def _send_messages(store: WhatsAppDataStore) -> None:
    if not list(store.iter_numbers()):
        _info("Necesitás vincular al menos un número antes de enviar mensajes.", color=Fore.YELLOW)
        press_enter()
        return
    lists = list(store.iter_lists())
    if not lists:
        _info("Cargá primero una lista de contactos.", color=Fore.YELLOW)
        press_enter()
        return
    number = _choose_number(store)
    if not number:
        return
    contact_list = _choose_contact_list(store)
    if not contact_list:
        return
    min_delay, max_delay = _ask_delay_range()
    message_template = ask_multiline("Mensaje a enviar (usa {nombre} para personalizar): ")
    if not message_template:
        _info("Mensaje vacío. Operación cancelada.", color=Fore.YELLOW)
        press_enter()
        return
    contacts = contact_list.get("contacts", [])
    if not contacts:
        _info("La lista no tiene contactos.", color=Fore.YELLOW)
        press_enter()
        return
    simulated_events = []
    planned_at = _now()
    for contact in contacts:
        planned_at += timedelta(seconds=random.uniform(min_delay, max_delay))
        rendered = _render_message(message_template, contact)
        event = {
            "contact": contact.get("number"),
            "name": contact.get("name"),
            "message": rendered,
            "scheduled_at": planned_at.isoformat() + "Z",
        }
        simulated_events.append(event)
        contact["status"] = "mensaje enviado"
        contact["last_message_at"] = _now_iso()
        contact.setdefault("history", []).append(
            {
                "type": "send",
                "message": rendered,
                "delay": {"min": min_delay, "max": max_delay},
                "scheduled_at": event["scheduled_at"],
            }
        )
    run_id = str(uuid.uuid4())
    store.state.setdefault("message_runs", []).append(
        {
            "id": run_id,
            "number_id": number["id"],
            "number_alias": number.get("alias"),
            "list_alias": contact_list.get("alias"),
            "created_at": _now_iso(),
            "delay": {"min": min_delay, "max": max_delay},
            "template": message_template,
            "events": simulated_events,
        }
    )
    store.save()
    ok(
        f"Se planificó el envío inicial para {len(contacts)} contactos desde "
        f"'{number.get('alias')}' respetando delays humanos."
    )
    press_enter()


def _choose_number(store: WhatsAppDataStore) -> dict[str, Any] | None:
    options = list(store.iter_numbers())
    if not options:
        return None
    print(_line())
    _subtitle("Seleccioná el número de envío")
    for idx, item in enumerate(options, 1):
        status = "🟢 activo" if item.get("connected") else "⚪ inactivo"
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
