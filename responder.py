import json
import logging
import re
import subprocess
import sys
import time
import unicodedata
import uuid
import warnings
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, time as dt_time, timezone
from pathlib import Path
from typing import Dict, List, Optional

from accounts import (
    auto_login_with_saved_password,
    get_account,
    list_all,
    mark_connected,
    prompt_login,
)
from config import (
    SETTINGS,
    read_app_config,
    read_env_local,
    refresh_settings,
    update_app_config,
    update_env_local,
)
from proxy_manager import apply_proxy_to_client, record_proxy_failure, should_retry_proxy
from paths import runtime_base
from runtime import (
    STOP_EVENT,
    ensure_logging,
    request_stop,
    reset_stop_event,
    sleep_with_stop,
    start_q_listener,
)
from session_store import has_session, load_into
from storage import get_auto_state, log_conversation_status, save_auto_state
from ui import Fore, full_line, style_text
from utils import ask, ask_int, banner, ok, press_enter, warn
from zoneinfo import ZoneInfo

try:  # pragma: no cover - depende de dependencia opcional
    from dateutil import parser as date_parser
except Exception:  # pragma: no cover - fallback si falta dependencia
    date_parser = None  # type: ignore[assignment]

try:  # pragma: no cover - depende de dependencia opcional
    import requests
    from requests import RequestException
except Exception:  # pragma: no cover - fallback si requests no está
    requests = None  # type: ignore
    RequestException = Exception  # type: ignore

try:  # pragma: no cover - depende de dependencias opcionales
    from google.oauth2.credentials import Credentials  # type: ignore
    from googleapiclient.discovery import build  # type: ignore
    from google.auth.transport.requests import Request as GoogleAuthRequest  # type: ignore
except Exception:  # pragma: no cover - si faltan dependencias opcionales
    Credentials = None  # type: ignore
    build = None  # type: ignore
    GoogleAuthRequest = None  # type: ignore

try:  # pragma: no cover - depende de dependencias opcionales
    from google_auth_oauthlib.flow import InstalledAppFlow  # type: ignore
except Exception:  # pragma: no cover - si falta dependencia opcional
    InstalledAppFlow = None  # type: ignore

logger = logging.getLogger(__name__)

DEFAULT_PROMPT = "Respondé cordial, breve y como humano."
PROMPT_KEY = "autoresponder_system_prompt"
ACTIVE_ALIAS: str | None = None
MAX_SYSTEM_PROMPT_CHARS = 50000


def _safe_parse_datetime(*args, **kwargs) -> Optional[datetime]:
    """Parsea una fecha utilizando dateutil si está disponible."""
    if date_parser is None:
        return None
    try:
        return date_parser.parse(*args, **kwargs)
    except Exception:
        return None

_GOHIGHLEVEL_FILE = runtime_base(Path(__file__).resolve().parent) / "storage" / "gohighlevel.json"
_GOHIGHLEVEL_BASE = "https://rest.gohighlevel.com/v1"
_PHONE_PATTERN = re.compile(r"(?<!\d)(?:\+?\d[\d\s().-]{7,}\d)")
_EMAIL_PATTERN = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)
_GOHIGHLEVEL_STATE: Dict[str, dict] | None = None
_DEFAULT_GOHIGHLEVEL_PROMPT = (
    "Sos un asistente que evalúa conversaciones de Instagram y determina si un lead está "
    "calificado para enviarse automáticamente al CRM GoHighLevel. Respondé únicamente "
    "con 'SI' cuando corresponda enviarlo y 'NO' cuando no cumpla con los criterios. "
    "Considerá el contexto, el interés real del lead y si el equipo comercial debería "
    "contactarlo."
)

_PROMPT_STORAGE_DIR = runtime_base(Path(__file__).resolve().parent) / "data" / "autoresponder"
_PROMPT_DEFAULT_ALIAS = "default"

_GOHIGHLEVEL_FILE = runtime_base(Path(__file__).resolve().parent) / "storage" / "gohighlevel.json"
_GOHIGHLEVEL_BASE = "https://rest.gohighlevel.com/v1"
_PHONE_PATTERN = re.compile(r"(?<!\d)(?:\+?\d[\d\s().-]{7,}\d)")
_EMAIL_PATTERN = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)
_GOHIGHLEVEL_STATE: Dict[str, dict] | None = None

_PROMPT_STORAGE_DIR = runtime_base(Path(__file__).resolve().parent) / "data" / "autoresponder"
_PROMPT_DEFAULT_ALIAS = "default"

_GOHIGHLEVEL_FILE = runtime_base(Path(__file__).resolve().parent) / "storage" / "gohighlevel.json"
_GOHIGHLEVEL_BASE = "https://rest.gohighlevel.com/v1"
_PHONE_PATTERN = re.compile(r"(?<!\d)(?:\+?\d[\d\s().-]{7,}\d)")
_EMAIL_PATTERN = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)
_GOHIGHLEVEL_STATE: Dict[str, dict] | None = None

_PROMPT_STORAGE_DIR = runtime_base(Path(__file__).resolve().parent) / "data" / "autoresponder"
_PROMPT_DEFAULT_ALIAS = "default"

_GOHIGHLEVEL_FILE = runtime_base(Path(__file__).resolve().parent) / "storage" / "gohighlevel.json"
_GOHIGHLEVEL_BASE = "https://rest.gohighlevel.com/v1"
_PHONE_PATTERN = re.compile(r"(?<!\d)(?:\+?\d[\d\s().-]{7,}\d)")
_EMAIL_PATTERN = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)
_GOHIGHLEVEL_STATE: Dict[str, dict] | None = None

_PROMPT_STORAGE_DIR = runtime_base(Path(__file__).resolve().parent) / "data" / "autoresponder"
_PROMPT_DEFAULT_ALIAS = "default"

_GOHIGHLEVEL_FILE = runtime_base(Path(__file__).resolve().parent) / "storage" / "gohighlevel.json"
_GOHIGHLEVEL_BASE = "https://rest.gohighlevel.com/v1"
_PHONE_PATTERN = re.compile(r"(?<!\d)(?:\+?\d[\d\s().-]{7,}\d)")
_EMAIL_PATTERN = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)
_GOHIGHLEVEL_STATE: Dict[str, dict] | None = None

_PROMPT_STORAGE_DIR = runtime_base(Path(__file__).resolve().parent) / "data" / "autoresponder"
_PROMPT_DEFAULT_ALIAS = "default"

_GOHIGHLEVEL_FILE = runtime_base(Path(__file__).resolve().parent) / "storage" / "gohighlevel.json"
_GOHIGHLEVEL_BASE = "https://rest.gohighlevel.com/v1"
_PHONE_PATTERN = re.compile(r"(?<!\d)(?:\+?\d[\d\s().-]{7,}\d)")
_EMAIL_PATTERN = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)
_GOHIGHLEVEL_STATE: Dict[str, dict] | None = None

_PROMPT_STORAGE_DIR = runtime_base(Path(__file__).resolve().parent) / "data" / "autoresponder"
_PROMPT_DEFAULT_ALIAS = "default"

_GOHIGHLEVEL_FILE = runtime_base(Path(__file__).resolve().parent) / "storage" / "gohighlevel.json"
_GOHIGHLEVEL_BASE = "https://rest.gohighlevel.com/v1"
_PHONE_PATTERN = re.compile(r"(?<!\d)(?:\+?\d[\d\s().-]{7,}\d)")
_EMAIL_PATTERN = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)
_GOHIGHLEVEL_STATE: Dict[str, dict] | None = None

_GOOGLE_CALENDAR_FILE = (
    runtime_base(Path(__file__).resolve().parent) / "storage" / "google_calendar.json"
)
_GOOGLE_DEVICE_CODE_URL = "https://oauth2.googleapis.com/device/code"
_GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
_GOOGLE_REVOKE_URL = "https://oauth2.googleapis.com/revoke"
_GOOGLE_CALENDAR_BASE = "https://www.googleapis.com/calendar/v3"
_GOOGLE_SCOPE = "https://www.googleapis.com/auth/calendar"
_DEFAULT_GOOGLE_CALENDAR_PROMPT = ""
_GOOGLE_REDIRECT_URI = "http://localhost"
_GOOGLE_STATE: Dict[str, dict] | None = None
_MEETING_TIME_PATTERN = re.compile(
    r"(?P<hour>\b[01]?\d|2[0-3])(?:(?:[:h\.])(?P<minute>[0-5]\d))?\s*(?P<ampm>am|pm)?\s*(?P<label>hs|hrs|horas)?",
    re.IGNORECASE,
)
_MEETING_DATE_PATTERN = re.compile(
    r"\b(?:\d{4}-\d{2}-\d{2}|\d{1,2}[/-]\d{1,2}(?:[/-]\d{2,4})?)\b",
    re.IGNORECASE,
)
_RELATIVE_DATE_KEYWORDS = (
    ("hoy", 0),
    ("manana", 1),
    ("pasado manana", 2),
)
_WEEKDAY_KEYWORDS = {
    "lunes": 0,
    "martes": 1,
    "miercoles": 2,
    "miércoles": 2,
    "jueves": 3,
    "viernes": 4,
    "sabado": 5,
    "sábado": 5,
    "domingo": 6,
}

_PROMPT_STORAGE_DIR = runtime_base(Path(__file__).resolve().parent) / "data" / "autoresponder"
_PROMPT_DEFAULT_ALIAS = "default"

_GOHIGHLEVEL_FILE = runtime_base(Path(__file__).resolve().parent) / "storage" / "gohighlevel.json"
_GOHIGHLEVEL_BASE = "https://rest.gohighlevel.com/v1"
_PHONE_PATTERN = re.compile(r"(?<!\d)(?:\+?\d[\d\s().-]{7,}\d)")
_EMAIL_PATTERN = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)
_GOHIGHLEVEL_STATE: Dict[str, dict] | None = None

_GOOGLE_CALENDAR_FILE = (
    runtime_base(Path(__file__).resolve().parent) / "storage" / "google_calendar.json"
)
_GOOGLE_DEVICE_CODE_URL = "https://oauth2.googleapis.com/device/code"
_GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
_GOOGLE_REVOKE_URL = "https://oauth2.googleapis.com/revoke"
_GOOGLE_CALENDAR_BASE = "https://www.googleapis.com/calendar/v3"
_GOOGLE_SCOPE = "https://www.googleapis.com/auth/calendar"
_GOOGLE_STATE: Dict[str, dict] | None = None
_MEETING_TIME_PATTERN = re.compile(
    r"(?P<hour>\b[01]?\d|2[0-3])(?:(?:[:h\.])(?P<minute>[0-5]\d))?\s*(?P<ampm>am|pm)?\s*(?P<label>hs|hrs|horas)?",
    re.IGNORECASE,
)
_MEETING_DATE_PATTERN = re.compile(
    r"\b(?:\d{4}-\d{2}-\d{2}|\d{1,2}[/-]\d{1,2}(?:[/-]\d{2,4})?)\b",
    re.IGNORECASE,
)
_RELATIVE_DATE_KEYWORDS = (
    ("hoy", 0),
    ("manana", 1),
    ("pasado manana", 2),
)
_WEEKDAY_KEYWORDS = {
    "lunes": 0,
    "martes": 1,
    "miercoles": 2,
    "miércoles": 2,
    "jueves": 3,
    "viernes": 4,
    "sabado": 5,
    "sábado": 5,
    "domingo": 6,
}

_POSITIVE_KEYWORDS = (
    "si",
    "quiero saber mas",
    "me interesa",
    "interesado",
)
_NEGATIVE_KEYWORDS = (
    "no",
    "ya tengo",
    "no me interesa",
    "no gracias",
)
_INFO_KEYWORDS = (
    "info",
    "informacion",
    "información",
    "detalle",
    "detalles",
    "precio",
    "costo",
    "mas info",
    "más info",
)
_CALL_KEYWORDS = (
    "agenda",
    "agendar",
    "llamar",
    "llamada",
    "cita",
    "call",
    "reunion",
    "reunión",
)
_DEFAULT_LEAD_TAG = "Lead sin clasificar"


def _format_handle(value: str | None) -> str:
    if not value:
        return "@-"
    value = value.strip()
    if value.startswith("@"):
        return value
    return f"@{value}"


def _default_timezone_label() -> str:
    try:
        tz = datetime.now().astimezone().tzinfo
        if tz is None:
            return "UTC"
        key = getattr(tz, "key", None)
        if key:
            return str(key)
        zone = getattr(tz, "zone", None)
        if zone:
            return str(zone)
    except Exception:
        pass
    return "UTC"


def _safe_timezone(label: str) -> ZoneInfo:
    try:
        return ZoneInfo(label)
    except Exception:
        try:
            return ZoneInfo(_default_timezone_label())
        except Exception:
            return ZoneInfo("UTC")


def _print_response_summary(index: int, sender: str, recipient: str, success: bool) -> None:
    icon = "✔️" if success else "❌"
    status = "OK" if success else "ERROR"
    print(
        f"[{icon}] Respuesta {index} | Emisor: {_format_handle(sender)} | "
        f"Receptor: {_format_handle(recipient)} | Estado: {status}"
    )


@contextmanager
def _suppress_console_noise() -> None:
    root = logging.getLogger()
    stream_handlers: list[logging.Handler] = [
        handler
        for handler in root.handlers
        if isinstance(handler, logging.StreamHandler)
    ]
    original_levels = [handler.level for handler in stream_handlers]
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        try:
            for handler in stream_handlers:
                handler.setLevel(logging.CRITICAL + 1)
            yield
        finally:
            for handler, level in zip(stream_handlers, original_levels):
                handler.setLevel(level)


def _normalize_text_for_match(value: str) -> str:
    normalized = unicodedata.normalize("NFD", value.lower())
    return "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")


def _contains_token(text: str, token: str) -> bool:
    token = token.strip()
    if not token:
        return False
    if " " in token:
        return token in text
    return (
        text == token
        or text.startswith(token + " ")
        or text.endswith(" " + token)
        or f" {token} " in text
    )


def _classify_response(message: str) -> str | None:
    norm = _normalize_text_for_match(message)
    if not norm:
        return None
    for keyword in _POSITIVE_KEYWORDS:
        if _contains_token(norm, keyword):
            return "Interesado"
    for keyword in _NEGATIVE_KEYWORDS:
        if _contains_token(norm, keyword):
            return "No interesado"
    return None


def _resolve_username(client, thread, target_user_id: int) -> str:
    try:
        for participant in getattr(thread, "users", []) or []:
            pk = getattr(participant, "pk", None) or getattr(participant, "id", None)
            if pk == target_user_id:
                username = getattr(participant, "username", None)
                if username:
                    return username
    except Exception:
        pass
    try:
        info = client.user_info(target_user_id)
        username = getattr(info, "username", None)
        if username:
            return username
    except Exception:
        pass
    return str(target_user_id)


@dataclass
class BotStats:
    alias: str
    responded: int = 0
    errors: int = 0
    responses: int = 0
    accounts: set[str] = field(default_factory=set)

    def _bump_responses(self, account: str) -> int:
        self.responses += 1
        self.accounts.add(account)
        return self.responses

    def record_success(self, account: str) -> int:
        index = self._bump_responses(account)
        self.responded += 1
        return index

    def record_response_error(self, account: str) -> int:
        index = self._bump_responses(account)
        self.errors += 1
        return index

    def record_error(self, account: str) -> None:
        self.errors += 1
        self.accounts.add(account)


def _client_for(username: str):
    from instagrapi import Client

    account = get_account(username)
    cl = Client()
    binding = None
    try:
        binding = apply_proxy_to_client(cl, username, account, reason="autoresponder")
    except Exception as exc:
        if account and account.get("proxy_url"):
            record_proxy_failure(username, exc)
            raise RuntimeError(f"El proxy de @{username} no respondió: {exc}") from exc
        logger.warning("Proxy no disponible para @%s: %s", username, exc, exc_info=False)

    try:
        load_into(cl, username)
    except FileNotFoundError as exc:
        mark_connected(username, False)
        raise RuntimeError(f"No hay sesión para {username}.") from exc
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
        raise RuntimeError(
            f"La sesión guardada para {username} no es válida. Iniciá sesión nuevamente."
        ) from exc
    return cl


def _ensure_session(username: str) -> bool:
    try:
        _client_for(username)
        return True
    except Exception:
        return False


def _gen_response(api_key: str, system_prompt: str, convo_text: str) -> str:
    try:
        from openai import OpenAI

        client = OpenAI(api_key=api_key)
        msg = client.responses.create(
            model="gpt-4o-mini",
            input=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": convo_text},
            ],
            temperature=0.6,
            max_output_tokens=180,
        )
        return (msg.output_text or "").strip() or "Gracias por tu mensaje 🙌 ¿Cómo te puedo ayudar?"
    except Exception as e:  # pragma: no cover - depende de red externa
        logger.warning("Fallo al generar respuesta con OpenAI: %s", e, exc_info=False)
        return "Gracias por tu mensaje 🙌 ¿Cómo te puedo ayudar?"


def _choose_targets(alias: str) -> list[str]:
    accounts_data = list_all()
    alias_key = alias.lstrip("@")
    alias_lower = alias_key.lower()

    if alias.upper() == "ALL":
        candidates = [a["username"] for a in accounts_data if a.get("active")]
    else:
        alias_matches = [
            a for a in accounts_data if a.get("alias", "").lower() == alias_lower and a.get("active")
        ]
        if alias_matches:
            candidates = [a["username"] for a in alias_matches]
        else:
            username_matches = [
                a for a in accounts_data if a.get("username", "").lower() == alias_lower and a.get("active")
            ]
            if username_matches:
                candidates = [username_matches[0]["username"]]
            else:
                candidates = [alias_key]

    seen = set()
    deduped: list[str] = []
    for user in candidates:
        norm = user.lstrip("@")
        if norm not in seen:
            seen.add(norm)
            deduped.append(norm)
    return deduped


def _filter_valid_sessions(targets: list[str]) -> list[str]:
    verified: list[str] = []
    needing_login: list[tuple[str, str]] = []
    for user in targets:
        if not has_session(user):
            needing_login.append((user, "sin sesión guardada"))
            continue
        if not _ensure_session(user):
            needing_login.append((user, "sesión expirada"))
            continue
        verified.append(user)

    if needing_login:
        remaining: list[tuple[str, str]] = []
        for user, reason in needing_login:
            if auto_login_with_saved_password(user) and _ensure_session(user):
                if user not in verified:
                    verified.append(user)
            else:
                remaining.append((user, reason))

        if remaining:
            print("\nLas siguientes cuentas necesitan volver a iniciar sesión:")
            for user, reason in remaining:
                print(f" - @{user}: {reason}")
            if ask("¿Iniciar sesión ahora? (s/N): ").strip().lower() == "s":
                for user, _ in remaining:
                    if auto_login_with_saved_password(user) and _ensure_session(user):
                        if user not in verified:
                            verified.append(user)
                        continue
                    if prompt_login(user, interactive=False) and _ensure_session(user):
                        if user not in verified:
                            verified.append(user)
            else:
                warn("Se omitieron las cuentas sin sesión válida.")
    return verified


def _mask_key(value: str) -> str:
    value = value.strip()
    if not value:
        return ""
    if len(value) <= 6:
        return value[:2] + "…"
    return f"{value[:4]}…{value[-2:]}"


def _system_prompt_file(alias: str | None = None) -> Path:
    alias_key = (alias or _PROMPT_DEFAULT_ALIAS).strip() or _PROMPT_DEFAULT_ALIAS
    safe_alias = re.sub(r"[^a-z0-9_.-]", "_", alias_key.lower())
    return _PROMPT_STORAGE_DIR / safe_alias / "system_prompt.txt"


def _normalize_system_prompt_text(value: str) -> str:
    if not value:
        return ""
    return value.replace("\r\n", "\n").replace("\r", "\n")


def _read_system_prompt_from_file(alias: str | None = None) -> str | None:
    path = _system_prompt_file(alias)
    if not path.exists():
        return None
    try:
        return _normalize_system_prompt_text(path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("No se pudo leer %s: %s", path, exc, exc_info=False)
        return None


def _persist_system_prompt(prompt: str, alias: str | None = None) -> str:
    normalized = _normalize_system_prompt_text(prompt)
    path = _system_prompt_file(alias)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(normalized, encoding="utf-8")
    except Exception as exc:
        logger.warning("No se pudo escribir %s: %s", path, exc, exc_info=False)
    try:
        update_app_config({PROMPT_KEY: normalized})
    except Exception as exc:
        logger.warning("No se pudo actualizar el system prompt en config: %s", exc, exc_info=False)
    return normalized


def _load_preferences() -> tuple[str, str]:
    env_values = read_env_local()
    api_key = env_values.get("OPENAI_API_KEY") or SETTINGS.openai_api_key or ""
    config_values = read_app_config()
    prompt = _read_system_prompt_from_file() or config_values.get(PROMPT_KEY, "") or ""
    prompt = _normalize_system_prompt_text(prompt) or DEFAULT_PROMPT
    return api_key, prompt


def _read_gohighlevel_state(refresh: bool = False) -> Dict[str, dict]:
    global _GOHIGHLEVEL_STATE
    if refresh or _GOHIGHLEVEL_STATE is None:
        data: Dict[str, dict] = {"aliases": {}}
        if _GOHIGHLEVEL_FILE.exists():
            try:
                loaded = json.loads(_GOHIGHLEVEL_FILE.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    data.update(loaded)
            except Exception:
                data = {"aliases": {}}
        if "aliases" not in data or not isinstance(data["aliases"], dict):
            data["aliases"] = {}
        _GOHIGHLEVEL_STATE = data
    return _GOHIGHLEVEL_STATE


def _write_gohighlevel_state(state: Dict[str, dict]) -> None:
    state.setdefault("aliases", {})
    _GOHIGHLEVEL_FILE.parent.mkdir(parents=True, exist_ok=True)
    _GOHIGHLEVEL_FILE.write_text(
        json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    _read_gohighlevel_state(refresh=True)


def _read_google_calendar_state(refresh: bool = False) -> Dict[str, dict]:
    global _GOOGLE_STATE
    if refresh or _GOOGLE_STATE is None:
        data: Dict[str, dict] = {"aliases": {}}
        if _GOOGLE_CALENDAR_FILE.exists():
            try:
                loaded = json.loads(_GOOGLE_CALENDAR_FILE.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    data.update(loaded)
            except Exception:
                data = {"aliases": {}}
        if "aliases" not in data or not isinstance(data["aliases"], dict):
            data["aliases"] = {}
        _GOOGLE_STATE = data
    return _GOOGLE_STATE


def _write_google_calendar_state(state: Dict[str, dict]) -> None:
    state.setdefault("aliases", {})
    _GOOGLE_CALENDAR_FILE.parent.mkdir(parents=True, exist_ok=True)
    _GOOGLE_CALENDAR_FILE.write_text(
        json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    _read_google_calendar_state(refresh=True)


def _normalize_alias_key(alias: str) -> str:
    return alias.strip().lower()


def _normalize_lead_id(lead: str) -> str:
    return lead.strip().lower()


def _sanitize_location_ids(raw: object) -> List[str]:
    if raw is None:
        return []
    tokens: List[str] = []
    if isinstance(raw, str):
        parts = re.split(r"[\s,;]+", raw)
        tokens = [part.strip() for part in parts if part.strip()]
    elif isinstance(raw, (list, tuple, set)):
        for item in raw:
            if isinstance(item, str):
                value = item.strip()
                if value:
                    tokens.append(value)
    else:
        try:
            iterable = list(raw)  # type: ignore[arg-type]
        except Exception:
            iterable = []
        for item in iterable:
            if isinstance(item, str):
                value = item.strip()
                if value:
                    tokens.append(value)
    seen: set[str] = set()
    cleaned: List[str] = []
    for token in tokens:
        norm = token.strip()
        if not norm:
            continue
        if norm in seen:
            continue
        seen.add(norm)
        cleaned.append(norm)
    return cleaned


def _get_gohighlevel_entry(alias: str) -> Dict[str, dict]:
    state = _read_gohighlevel_state()
    key = _normalize_alias_key(alias)
    aliases: Dict[str, dict] = state.get("aliases", {})
    entry = aliases.get(key)
    if isinstance(entry, dict):
        entry.setdefault("alias", alias.strip())
        entry.setdefault("sent", {})
        entry.setdefault("qualify_prompt", _DEFAULT_GOHIGHLEVEL_PROMPT)
        if "location_ids" in entry:
            entry["location_ids"] = _sanitize_location_ids(entry.get("location_ids"))
        return entry
    return {}


def _set_gohighlevel_entry(alias: str, updates: Dict[str, object]) -> None:
    alias = alias.strip()
    if not alias:
        warn("Alias inválido.")
        return
    state = _read_gohighlevel_state()
    aliases: Dict[str, dict] = state.setdefault("aliases", {})
    key = _normalize_alias_key(alias)
    entry = aliases.get(key, {})
    entry.setdefault("alias", alias)
    entry.setdefault("sent", {})
    normalized_updates: Dict[str, object] = {}
    for key, value in updates.items():
        if value is None:
            continue
        if key == "location_ids":
            normalized_updates[key] = _sanitize_location_ids(value)
        else:
            normalized_updates[key] = value
    entry.update(normalized_updates)
    aliases[key] = entry
    _write_gohighlevel_state(state)


def _get_google_calendar_entry(alias: str) -> Dict[str, object]:
    state = _read_google_calendar_state()
    key = _normalize_alias_key(alias)
    aliases: Dict[str, dict] = state.get("aliases", {})
    entry = aliases.get(key)
    if isinstance(entry, dict):
        entry.setdefault("alias", alias.strip())
        entry.setdefault("scheduled", {})
        entry.setdefault("event_name", "{{username}} - Sistema de adquisición con IA")
        entry.setdefault("duration_minutes", 30)
        entry.setdefault("timezone", _default_timezone_label())
        entry.setdefault("auto_meet", True)
        entry.setdefault("schedule_prompt", _DEFAULT_GOOGLE_CALENDAR_PROMPT)
        return entry
    return {}


def _set_google_calendar_entry(alias: str, updates: Dict[str, object]) -> None:
    alias = alias.strip()
    if not alias:
        warn("Alias inválido.")
        return
    state = _read_google_calendar_state()
    aliases: Dict[str, dict] = state.setdefault("aliases", {})
    key = _normalize_alias_key(alias)
    entry = aliases.get(key, {})
    entry.setdefault("alias", alias)
    entry.setdefault("scheduled", {})
    entry.setdefault("event_name", "{{username}} - Sistema de adquisición con IA")
    entry.setdefault("duration_minutes", 30)
    entry.setdefault("timezone", _default_timezone_label())
    entry.setdefault("auto_meet", True)
    entry.setdefault("schedule_prompt", _DEFAULT_GOOGLE_CALENDAR_PROMPT)
    normalized_updates: Dict[str, object] = {}
    for key_name, value in updates.items():
        if value is None:
            continue
        if key_name == "duration_minutes":
            try:
                normalized_updates[key_name] = max(5, int(value))
            except Exception:
                continue
        elif key_name == "timezone":
            try:
                tz_value = str(value).strip() or _default_timezone_label()
                _ = _safe_timezone(tz_value)
                normalized_updates[key_name] = tz_value
            except Exception:
                warn("Zona horaria inválida; se mantiene el valor previo.")
                continue
        elif key_name == "schedule_prompt":
            normalized_updates[key_name] = str(value)
        else:
            normalized_updates[key_name] = value
    entry.update(normalized_updates)
    aliases[key] = entry
    _write_google_calendar_state(state)


def _mask_google_calendar_status(entry: Dict[str, object]) -> str:
    connected = bool(entry.get("connected"))
    enabled = bool(entry.get("enabled"))
    status = "🟢 Activo" if connected and enabled else "🟡 Conectado" if connected else "⚪ Inactivo"
    summary = entry.get("event_name") or "(sin nombre)"
    tz_label = entry.get("timezone") or "UTC"
    return f"{status} • Evento: {summary} • TZ: {tz_label}"


def _google_calendar_status_lines() -> List[str]:
    state = _read_google_calendar_state()
    aliases: Dict[str, dict] = state.get("aliases", {})
    if not aliases:
        return ["(sin configuraciones)"]
    rows: List[str] = []
    for key in sorted(aliases.keys()):
        entry = aliases[key]
        label = str(entry.get("alias") or key)
        rows.append(f" - {label}: {_mask_google_calendar_status(entry)}")
    return rows


def _google_calendar_summary_line() -> str:
    state = _read_google_calendar_state()
    aliases: Dict[str, dict] = state.get("aliases", {})
    enabled_aliases = [
        entry
        for entry in aliases.values()
        if isinstance(entry, dict) and entry.get("connected") and entry.get("enabled")
    ]
    if not enabled_aliases:
        configured_aliases = [
            entry
            for entry in aliases.values()
            if isinstance(entry, dict) and entry.get("connected")
        ]
        if configured_aliases:
            labels = sorted(str(entry.get("alias") or "?") for entry in configured_aliases)
            return f"Google Calendar: conectado para {', '.join(labels)} (inactivo)"
        return "Google Calendar: (sin configurar)"
    labels = sorted(str(entry.get("alias") or "?") for entry in enabled_aliases)
    return f"Google Calendar: activo para {', '.join(labels)}"


def _google_calendar_mark_scheduled(
    alias: str, lead: str, phone: str, event_id: str, link: str | None
) -> None:
    lead_key = f"{_normalize_lead_id(lead)}|{_normalize_phone(phone)}"
    entry = _get_google_calendar_entry(alias)
    scheduled = entry.setdefault("scheduled", {})
    scheduled[lead_key] = {
        "event_id": event_id,
        "link": link or "",
        "ts": int(time.time()),
    }
    _set_google_calendar_entry(alias, {"scheduled": scheduled})


def _google_calendar_already_scheduled(alias: str, lead: str, phone: str) -> bool:
    entry = _get_google_calendar_entry(alias)
    scheduled = entry.get("scheduled") or {}
    lead_key = f"{_normalize_lead_id(lead)}|{_normalize_phone(phone)}"
    return lead_key in scheduled


def _google_calendar_token_is_valid(entry: Dict[str, object]) -> bool:
    expires_at = entry.get("token_expires_at")
    try:
        expires_float = float(expires_at)
    except Exception:
        return False
    return expires_float - time.time() > 60


def _google_calendar_store_tokens(
    alias: str, entry: Dict[str, object], token_data: Dict[str, object]
) -> Dict[str, object]:
    access_token = token_data.get("access_token") or entry.get("access_token")
    refresh_token = token_data.get("refresh_token") or entry.get("refresh_token")
    token_type = token_data.get("token_type") or entry.get("token_type")
    expires_in = token_data.get("expires_in")
    if isinstance(expires_in, str) and expires_in.isdigit():
        expires_in = int(expires_in)
    if not isinstance(expires_in, (int, float)):
        expires_in = 3600
    expires_at = time.time() + float(expires_in)
    updated = {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": token_type,
        "token_expires_at": expires_at,
        "connected": bool(access_token and refresh_token),
    }
    _set_google_calendar_entry(alias, updated)
    entry.update(updated)
    return entry


def _google_calendar_refresh_access_token(
    alias: str, entry: Dict[str, object]
) -> Optional[str]:
    if requests is None and (Credentials is None or build is None):
        return None
    refresh_token = entry.get("refresh_token")
    client_id = entry.get("client_id")
    client_secret = entry.get("client_secret")
    if not refresh_token or not client_id:
        return None
    data = {
        "client_id": client_id,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
    }
    if client_secret:
        data["client_secret"] = client_secret
    try:
        response = requests.post(_GOOGLE_TOKEN_URL, data=data, timeout=15)
    except RequestException as exc:  # pragma: no cover - depende de red externa
        logger.warning("No se pudo refrescar el token de Google Calendar: %s", exc, exc_info=False)
        return None
    if response.status_code != 200:
        logger.warning(
            "Respuesta inesperada al refrescar token de Google Calendar: %s", response.text
        )
        return None
    token_data = response.json()
    entry = _google_calendar_store_tokens(alias, entry, token_data)
    return entry.get("access_token")


def _google_calendar_update_tokens_from_credentials(
    alias: str, entry: Dict[str, object], creds: object
) -> None:
    token = getattr(creds, "token", None)
    if not token:
        return
    refresh_token = getattr(creds, "refresh_token", None) or entry.get("refresh_token")
    expiry = getattr(creds, "expiry", None)
    expires_in = 3600
    if expiry is not None:
        try:
            expiry_dt = expiry
            if isinstance(expiry_dt, str):
                parsed = _safe_parse_datetime(expiry_dt)
                if parsed is not None:
                    expiry_dt = parsed
            if isinstance(expiry_dt, datetime):
                if expiry_dt.tzinfo is None:
                    expiry_dt = expiry_dt.replace(tzinfo=timezone.utc)
                delta = expiry_dt - datetime.now(timezone.utc)
                expires_in = max(60, int(delta.total_seconds()))
        except Exception:
            expires_in = 3600
    token_payload = {
        "access_token": token,
        "refresh_token": refresh_token,
        "token_type": "Bearer",
        "expires_in": expires_in,
    }
    _google_calendar_store_tokens(alias, entry, token_payload)


def _google_calendar_credentials_from_entry(
    alias: str, entry: Dict[str, object]
) -> Optional[object]:
    if Credentials is None:
        return None
    access_token = entry.get("access_token")
    refresh_token = entry.get("refresh_token")
    client_id = entry.get("client_id")
    if not access_token or not refresh_token or not client_id:
        return None
    try:
        creds = Credentials(
            token=str(access_token),
            refresh_token=str(refresh_token),
            token_uri=_GOOGLE_TOKEN_URL,
            client_id=str(client_id),
            client_secret=str(entry.get("client_secret") or "") or None,
            scopes=[_GOOGLE_SCOPE],
        )
    except Exception as exc:  # pragma: no cover - depende de librerías externas
        logger.warning(
            "No se pudieron preparar credenciales de Google Calendar: %s",
            exc,
            exc_info=False,
        )
        return None
    if not getattr(creds, "valid", False) and getattr(creds, "refresh_token", None):
        if GoogleAuthRequest is None:
            return creds
        try:
            creds.refresh(GoogleAuthRequest())  # type: ignore[misc]
            _google_calendar_update_tokens_from_credentials(alias, entry, creds)
        except Exception as exc:  # pragma: no cover - depende de red/creds
            logger.warning(
                "No se pudo refrescar credenciales de Google Calendar via google-auth: %s",
                exc,
                exc_info=False,
            )
            return None
    return creds


def _google_calendar_create_event_via_service(
    alias: str,
    entry: Dict[str, object],
    payload: Dict[str, object],
) -> Optional[Dict[str, object]]:
    if build is None or Credentials is None:
        return None
    creds = _google_calendar_credentials_from_entry(alias, entry)
    if not creds:
        return None
    try:
        service = build("calendar", "v3", credentials=creds, cache_discovery=False)
    except Exception as exc:  # pragma: no cover - depende de librería externa
        logger.warning(
            "No se pudo inicializar el cliente de Google Calendar: %s",
            exc,
            exc_info=False,
        )
        return None
    kwargs: Dict[str, object] = {}
    if "conferenceData" in payload:
        kwargs["conferenceDataVersion"] = 1
    try:
        event = (
            service.events()  # type: ignore[call-arg]
            .insert(calendarId="primary", body=payload, **kwargs)
            .execute()
        )
    except Exception as exc:  # pragma: no cover - depende de librería externa
        logger.warning(
            "Error al crear evento de Google Calendar mediante googleapiclient: %s",
            exc,
            exc_info=False,
        )
        return None
    _google_calendar_update_tokens_from_credentials(alias, entry, creds)
    if isinstance(event, dict):
        return event
    return None


def _google_calendar_create_event_via_requests(
    alias: str,
    entry: Dict[str, object],
    payload: Dict[str, object],
    params: Dict[str, object],
    access_token: Optional[str],
) -> Optional[Dict[str, object]]:
    if requests is None and (Credentials is None or build is None):
        return None
    token_value = access_token or entry.get("access_token")
    if not token_value:
        return None
    headers = {
        "Authorization": f"Bearer {token_value}",
        "Content-Type": "application/json",
    }
    url = f"{_GOOGLE_CALENDAR_BASE}/calendars/primary/events"
    try:
        response = requests.post(  # type: ignore[call-arg]
            url,
            headers=headers,
            json=payload,
            params=params or None,
            timeout=20,
        )
    except RequestException as exc:  # pragma: no cover - depende de red externa
        logger.warning("No se pudo crear el evento en Google Calendar: %s", exc, exc_info=False)
        return None
    if response.status_code == 401:
        new_token = _google_calendar_refresh_access_token(alias, entry)
        if not new_token:
            return None
        headers["Authorization"] = f"Bearer {new_token}"
        try:
            response = requests.post(  # type: ignore[call-arg]
                url,
                headers=headers,
                json=payload,
                params=params or None,
                timeout=20,
            )
        except RequestException as exc:  # pragma: no cover - depende de red externa
            logger.warning(
                "No se pudo crear el evento en Google Calendar tras refrescar token: %s",
                exc,
                exc_info=False,
            )
            return None
    if response.status_code not in {200, 201}:
        logger.warning(
            "Respuesta inesperada al crear evento de Google Calendar (%s): %s",
            response.status_code,
            response.text,
        )
        return None
    try:
        data = response.json()
    except Exception:
        data = {}
    if isinstance(data, dict):
        return data
    return None


def _google_calendar_create_event(
    alias: str,
    entry: Dict[str, object],
    payload: Dict[str, object],
    params: Dict[str, object],
    access_token: Optional[str],
) -> Optional[Dict[str, object]]:
    event = _google_calendar_create_event_via_service(alias, entry, payload)
    if event:
        return event
    return _google_calendar_create_event_via_requests(alias, entry, payload, params, access_token)


def _google_calendar_ensure_token(alias: str, entry: Dict[str, object]) -> Optional[str]:
    access_token = entry.get("access_token")
    if access_token and _google_calendar_token_is_valid(entry):
        return str(access_token)
    return _google_calendar_refresh_access_token(alias, entry)


def _google_calendar_enabled_entry_for(username: str) -> tuple[Optional[str], Dict[str, object]]:
    alias_candidates: List[str] = []
    if ACTIVE_ALIAS:
        alias_candidates.append(ACTIVE_ALIAS)
    account = get_account(username) or {}
    account_alias = str(account.get("alias") or "").strip()
    if account_alias:
        alias_candidates.append(account_alias)
    alias_candidates.append(username)
    alias_candidates.append("ALL")

    seen: set[str] = set()
    for alias in alias_candidates:
        norm = _normalize_alias_key(alias)
        if not norm or norm in seen:
            continue
        seen.add(norm)
        entry = _get_google_calendar_entry(alias)
        if entry.get("connected") and entry.get("enabled"):
            access_token = entry.get("access_token")
            refresh_token = entry.get("refresh_token")
            if access_token and refresh_token:
                return alias, entry
    return None, {}


def _google_calendar_lead_qualifies(
    entry: Dict[str, object],
    conversation: str,
    status: Optional[str],
    phone_numbers: List[str],
    meeting_dt: datetime,
    api_key: Optional[str],
) -> bool:
    prompt_text = str(entry.get("schedule_prompt") or "").strip()
    if not prompt_text:
        return True
    if not api_key:
        return True
    try:  # pragma: no cover - depende de dependencia externa
        from openai import OpenAI
    except Exception as exc:  # pragma: no cover - entorno sin openai
        logger.warning(
            "No se pudo importar OpenAI para evaluar Google Calendar: %s",
            exc,
            exc_info=False,
        )
        return True
    try:  # pragma: no cover - depende de credenciales externas
        client = OpenAI(api_key=api_key)
    except Exception as exc:
        logger.warning(
            "No se pudo inicializar OpenAI para evaluar Google Calendar: %s",
            exc,
            exc_info=False,
        )
        return True

    system_prompt = (
        prompt_text
        + "\n\nResponde únicamente con 'SI' o 'NO' indicando si se debe crear un evento en Google Calendar."
    )
    context_lines = [
        f"Estado detectado: {status or 'desconocido'}",
        "Teléfonos detectados: "
        + (", ".join(phone_numbers) if phone_numbers else "(sin teléfono)"),
        f"Fecha/hora detectada: {meeting_dt.isoformat()}",
        "Conversación completa:",
        conversation,
    ]
    user_content = "\n".join(context_lines)
    try:  # pragma: no cover - depende de red externa
        response = client.responses.create(
            model="gpt-4o-mini",
            input=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_content},
            ],
            temperature=0,
            max_output_tokens=20,
        )
        decision = (response.output_text or "").strip().lower()
    except Exception as exc:  # pragma: no cover - depende de red externa
        logger.warning(
            "No se pudo evaluar el criterio de Google Calendar con OpenAI: %s",
            exc,
            exc_info=False,
        )
        return True

    normalized = _normalize_text_for_match(decision)
    return normalized.startswith("s")


def _mask_gohighlevel_status(entry: Dict[str, object]) -> str:
    api_key = str(entry.get("api_key") or "")
    enabled = bool(entry.get("enabled"))
    status = "🟢 Activo" if enabled else "⚪ Inactivo"
    location_ids = _sanitize_location_ids(entry.get("location_ids"))
    locations_text = (
        f"{len(location_ids)} Location ID(s)"
        if location_ids
        else "Location IDs: (sin definir)"
    )
    return (
        f"{status} • API Key: {_mask_key(api_key) or '(sin definir)'}"
        f" • {locations_text}"
    )


def _gohighlevel_status_lines() -> List[str]:
    state = _read_gohighlevel_state()
    aliases: Dict[str, dict] = state.get("aliases", {})
    if not aliases:
        return ["(sin configuraciones)"]
    rows: List[str] = []
    for key in sorted(aliases.keys()):
        entry = aliases[key]
        label = str(entry.get("alias") or key)
        rows.append(f" - {label}: {_mask_gohighlevel_status(entry)}")
    return rows


def _gohighlevel_summary_line() -> str:
    state = _read_gohighlevel_state()
    aliases: Dict[str, dict] = state.get("aliases", {})
    if not aliases:
        return "GoHighLevel: (sin configurar)"
    active = sum(1 for entry in aliases.values() if entry.get("enabled"))
    configured = sum(1 for entry in aliases.values() if entry.get("api_key"))
    return f"GoHighLevel: {active} activos / {configured} configurados"


def _gohighlevel_mark_sent(alias: str, lead: str, phone: str) -> None:
    state = _read_gohighlevel_state()
    aliases: Dict[str, dict] = state.setdefault("aliases", {})
    key = _normalize_alias_key(alias)
    entry = aliases.setdefault(key, {"alias": alias.strip(), "sent": {}})
    entry.setdefault("sent", {})
    entry["sent"][_normalize_lead_id(lead)] = {"phone": phone, "ts": int(time.time())}
    aliases[key] = entry
    _write_gohighlevel_state(state)


def _gohighlevel_already_sent(alias: str, lead: str, phone: str) -> bool:
    entry = _get_gohighlevel_entry(alias)
    sent: Dict[str, dict] = entry.get("sent", {})  # type: ignore[assignment]
    record = sent.get(_normalize_lead_id(lead))
    if not isinstance(record, dict):
        return False
    stored_phone = str(record.get("phone") or "")
    return bool(stored_phone) and stored_phone == phone


def _gohighlevel_enabled_entry_for(username: str) -> tuple[Optional[str], Dict[str, object]]:
    alias_candidates: List[str] = []
    if ACTIVE_ALIAS:
        alias_candidates.append(ACTIVE_ALIAS)
    account = get_account(username) or {}
    account_alias = str(account.get("alias") or "").strip()
    if account_alias:
        alias_candidates.append(account_alias)
    alias_candidates.append(username)
    alias_candidates.append("ALL")

    seen: set[str] = set()
    for alias in alias_candidates:
        norm = _normalize_alias_key(alias)
        if not norm or norm in seen:
            continue
        seen.add(norm)
        entry = _get_gohighlevel_entry(alias)
        api_key = str(entry.get("api_key") or "")
        if api_key and entry.get("enabled"):
            return alias, entry
    return None, {}


def _gohighlevel_lead_qualifies(
    entry: Dict[str, object],
    conversation: str,
    status: Optional[str],
    phone_numbers: List[str],
    api_key: Optional[str],
) -> bool:
    prompt_text = str(entry.get("qualify_prompt") or "").strip()
    if not prompt_text:
        return True
    if not api_key:
        return True
    try:  # pragma: no cover - depende de dependencia externa
        from openai import OpenAI
    except Exception as exc:  # pragma: no cover - entorno sin openai
        logger.warning(
            "No se pudo importar OpenAI para evaluar GoHighLevel: %s", exc, exc_info=False
        )
        return True
    try:  # pragma: no cover - depende de credenciales externas
        client = OpenAI(api_key=api_key)
    except Exception as exc:
        logger.warning(
            "No se pudo inicializar OpenAI para evaluar GoHighLevel: %s",
            exc,
            exc_info=False,
        )
        return True

    system_prompt = (
        prompt_text
        + "\n\nResponde únicamente con 'SI' o 'NO' indicando si se debe enviar el lead a GoHighLevel."
    )
    context_lines = [
        f"Estado detectado: {status or 'desconocido'}",
        "Teléfonos detectados: "
        + (", ".join(phone_numbers) if phone_numbers else "(sin teléfono)"),
        "Conversación completa:",
        conversation,
    ]
    user_content = "\n".join(context_lines)
    try:  # pragma: no cover - depende de red externa
        response = client.responses.create(
            model="gpt-4o-mini",
            input=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_content},
            ],
            temperature=0,
            max_output_tokens=20,
        )
        decision = (response.output_text or "").strip().lower()
    except Exception as exc:  # pragma: no cover - depende de red externa
        logger.warning(
            "No se pudo evaluar el criterio de GoHighLevel con OpenAI: %s",
            exc,
            exc_info=False,
        )
        return True

    normalized = _normalize_text_for_match(decision)
    return normalized.startswith("s")


def _require_requests() -> bool:
    if requests is None:  # pragma: no cover - entorno sin dependencia
        warn("La librería 'requests' no está disponible. Instalála para usar GoHighLevel.")
        press_enter()
        return False
    return True


def _gohighlevel_select_alias() -> Optional[str]:
    alias = _prompt_alias_selection()
    if not alias:
        warn("Alias inválido.")
        press_enter()
        return None
    return alias


def _gohighlevel_configure_key() -> None:
    banner()
    print(style_text("GoHighLevel • Configurar API Key", color=Fore.CYAN, bold=True))
    print(full_line(color=Fore.BLUE))
    for line in _gohighlevel_status_lines():
        print(line)
    print(full_line(color=Fore.BLUE))
    alias = _gohighlevel_select_alias()
    if not alias:
        return
    current = _get_gohighlevel_entry(alias)
    print(f"Actual: {_mask_key(str(current.get('api_key') or '')) or '(sin definir)'}")
    new_key = ask("Ingresá la API Key de GoHighLevel (vacío para cancelar): ").strip()
    if not new_key:
        warn("No se modificó la API Key de GoHighLevel.")
        press_enter()
        return
    _set_gohighlevel_entry(alias, {"api_key": new_key})
    ok(f"API Key guardada para {alias}.")
    press_enter()


def _gohighlevel_configure_locations() -> None:
    banner()
    print(style_text("GoHighLevel • Configurar Location IDs", color=Fore.CYAN, bold=True))
    print(full_line(color=Fore.BLUE))
    for line in _gohighlevel_status_lines():
        print(line)
    print(full_line(color=Fore.BLUE))
    alias = _gohighlevel_select_alias()
    if not alias:
        return
    entry = _get_gohighlevel_entry(alias)
    current_ids = _sanitize_location_ids(entry.get("location_ids"))
    if current_ids:
        print("Actual:")
        for idx, value in enumerate(current_ids, start=1):
            print(f" {idx}) {value}")
    else:
        print("Actual: (sin definir)")
    print()
    prompt = (
        "Ingresá uno o más Location IDs (separados por coma o espacio).\n"
        "Escribí 'eliminar N' para borrar uno específico (usa el número de la lista),\n"
        "'limpiar' para eliminar todos o dejá vacío para cancelar: "
    )
    raw = ask(prompt).strip()
    if not raw:
        warn("No se modificaron los Location IDs.")
        press_enter()
        return
    if raw.lower().startswith("eliminar"):
        if not current_ids:
            warn("No hay Location IDs para eliminar.")
            press_enter()
            return
        indexes = [token for token in re.split(r"[^0-9]+", raw) if token.isdigit()]
        if not indexes:
            warn("Indicá el número del Location ID a eliminar.")
            press_enter()
            return
        to_remove: set[int] = set()
        for token in indexes:
            try:
                idx = int(token)
            except ValueError:
                continue
            if 1 <= idx <= len(current_ids):
                to_remove.add(idx - 1)
        if not to_remove:
            warn("Los números indicados no coinciden con Location IDs existentes.")
            press_enter()
            return
        remaining = [value for idx, value in enumerate(current_ids) if idx not in to_remove]
        _set_gohighlevel_entry(alias, {"location_ids": remaining})
        ok(
            "Se eliminaron los Location IDs seleccionados. Total restante: "
            f"{len(remaining)}"
        )
        press_enter()
        return
    if raw.lower() in {"limpiar", "clear", "ninguno", "eliminar", "borrar"}:
        _set_gohighlevel_entry(alias, {"location_ids": []})
        ok(f"Se eliminaron los Location IDs para {alias}.")
        press_enter()
        return
    location_ids = _sanitize_location_ids(raw)
    if not location_ids:
        warn("No se detectaron Location IDs válidos.")
        press_enter()
        return
    _set_gohighlevel_entry(alias, {"location_ids": location_ids})
    ok(f"Location IDs guardados para {alias}. Total: {len(location_ids)}")
    press_enter()


def _gohighlevel_configure_prompt() -> None:
    banner()
    print(style_text("GoHighLevel • Criterios de envío", color=Fore.CYAN, bold=True))
    print(full_line(color=Fore.BLUE))
    for line in _gohighlevel_status_lines():
        print(line)
    print(full_line(color=Fore.BLUE))
    alias = _gohighlevel_select_alias()
    if not alias:
        return
    entry = _get_gohighlevel_entry(alias)
    current_prompt = str(entry.get("qualify_prompt") or _DEFAULT_GOHIGHLEVEL_PROMPT)
    print(style_text("Prompt actual:", color=Fore.BLUE))
    print(current_prompt or "(sin definir)")
    print(full_line(color=Fore.BLUE))
    print("Elegí una opción:")
    print("  E) Editar prompt")
    print("  D) Restaurar prompt predeterminado")
    print("  Enter) Cancelar")
    action = ask("Acción: ").strip().lower()
    if not action:
        warn("No se modificó el prompt de calificación.")
        press_enter()
        return
    if action in {"d", "default", "predeterminado"}:
        _set_gohighlevel_entry(alias, {"qualify_prompt": _DEFAULT_GOHIGHLEVEL_PROMPT})
        ok("Se restauró el prompt predeterminado para GoHighLevel.")
        press_enter()
        return
    if action not in {"e", "editar"}:
        warn("Opción inválida. No se modificó el prompt de calificación.")
        press_enter()
        return
    print(
        style_text(
            "Pegá el nuevo prompt y finalizá con una línea que diga <<<END>>>."
            " Dejá vacío para cancelar.",
            color=Fore.CYAN,
        )
    )
    lines: List[str] = []
    while True:
        line = ask("› ")
        if line.strip() == "<<<END>>>":
            break
        lines.append(line.replace("\r", ""))
    new_prompt = "\n".join(lines).strip()
    if not new_prompt:
        warn("No se modificó el prompt de calificación.")
        press_enter()
        return
    _set_gohighlevel_entry(alias, {"qualify_prompt": new_prompt})
    ok(f"Prompt actualizado. Longitud: {len(new_prompt)} caracteres.")
    press_enter()


def _gohighlevel_activate() -> None:
    if not _require_requests():
        return
    banner()
    print(style_text("GoHighLevel • Activar envío automático", color=Fore.CYAN, bold=True))
    print(full_line(color=Fore.BLUE))
    for line in _gohighlevel_status_lines():
        print(line)
    print(full_line(color=Fore.BLUE))
    alias = _gohighlevel_select_alias()
    if not alias:
        return
    entry = _get_gohighlevel_entry(alias)
    api_key = str(entry.get("api_key") or "")
    if not api_key:
        warn("Configurá la API Key antes de activar la conexión.")
        press_enter()
        return
    _set_gohighlevel_entry(alias, {"enabled": True})
    ok(f"Conexión GoHighLevel activada para {alias}.")
    press_enter()


def _gohighlevel_deactivate() -> None:
    banner()
    print(style_text("GoHighLevel • Desactivar conexión", color=Fore.CYAN, bold=True))
    print(full_line(color=Fore.BLUE))
    for line in _gohighlevel_status_lines():
        print(line)
    print(full_line(color=Fore.BLUE))
    alias = _gohighlevel_select_alias()
    if not alias:
        return
    _set_gohighlevel_entry(alias, {"enabled": False})
    ok(f"Conexión GoHighLevel desactivada para {alias}.")
    press_enter()


def _gohighlevel_menu() -> None:
    while True:
        banner()
        print(style_text("Conectar con GoHighLevel", color=Fore.CYAN, bold=True))
        print(full_line(color=Fore.BLUE))
        for line in _gohighlevel_status_lines():
            print(line)
        print(full_line(color=Fore.BLUE))
        print("1) Ingresar API Key de GoHighLevel")
        print("2) Configurar Location IDs de GoHighLevel")
        print("3) Activar el envío automático de leads calificados al CRM de GoHighLevel")
        print("4) Desactivar conexión")
        print("5) Configurar criterios de calificación")
        print("6) Volver al submenú anterior")
        print(full_line(color=Fore.BLUE))
        choice = ask("Opción: ").strip()
        if choice == "1":
            _gohighlevel_configure_key()
        elif choice == "2":
            _gohighlevel_configure_locations()
        elif choice == "3":
            _gohighlevel_activate()
        elif choice == "4":
            _gohighlevel_deactivate()
        elif choice == "5":
            _gohighlevel_configure_prompt()
        elif choice == "6":
            break
        else:
            warn("Opción inválida.")
            press_enter()


def _google_calendar_select_alias() -> Optional[str]:
    alias = _prompt_alias_selection()
    if not alias:
        warn("Alias inválido.")
        press_enter()
        return None
    return alias


def _google_calendar_perform_device_flow(
    client_id: str, client_secret: str | None
) -> Optional[Dict[str, object]]:
    if requests is None:
        return None
    try:
        response = requests.post(
            _GOOGLE_DEVICE_CODE_URL,
            data={"client_id": client_id, "scope": _GOOGLE_SCOPE},
            timeout=15,
        )
    except RequestException as exc:  # pragma: no cover - depende de red externa
        warn(f"No se pudo iniciar la autorización de Google: {exc}")
        press_enter()
        return None
    if response.status_code != 200:
        warn(f"Respuesta inesperada de Google: {response.text}")
        press_enter()
        return None
    payload = response.json()
    device_code = payload.get("device_code")
    if not device_code:
        warn("Google no devolvió device_code válido.")
        press_enter()
        return None
    verification_url = payload.get("verification_url") or payload.get("verification_uri")
    user_code = payload.get("user_code")
    print(style_text("Para continuar:", color=Fore.CYAN, bold=True))
    if verification_url and user_code:
        print(f"1. Visitá {verification_url}")
        print(f"2. Ingresá el código: {user_code}")
    elif user_code:
        print(f"Ingresá el código: {user_code}")
    else:
        print("Abrí la URL indicada por Google y autorizá el acceso.")
    print("Esperando confirmación...")
    interval = int(payload.get("interval", 5))
    expires_at = time.time() + int(payload.get("expires_in", 1800))
    data = {
        "client_id": client_id,
        "device_code": device_code,
        "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
    }
    if client_secret:
        data["client_secret"] = client_secret
    while time.time() < expires_at:
        time.sleep(interval)
        try:
            token_response = requests.post(_GOOGLE_TOKEN_URL, data=data, timeout=15)
        except RequestException as exc:  # pragma: no cover - depende de red externa
            warn(f"Error al consultar token de Google: {exc}")
            press_enter()
            return None
        if token_response.status_code == 200:
            return token_response.json()
        try:
            error_payload = token_response.json()
        except Exception:
            error_payload = {}
        error_code = (error_payload or {}).get("error")
        if error_code in {"authorization_pending"}:
            continue
        if error_code == "slow_down":
            interval = min(interval + 2, 15)
            continue
        if error_code in {"expired_token", "access_denied"}:
            warn("La autorización no fue completada.")
            press_enter()
            return None
        warn(f"Error al obtener token de Google: {token_response.text}")
        press_enter()
        return None
    warn("El código de autorización expiró. Intentá nuevamente.")
    press_enter()
    return None


def _google_calendar_validate_client_payload(
    payload: Dict[str, object]
) -> tuple[Optional[Dict[str, object]], Optional[str]]:
    if not isinstance(payload, dict):
        return None, "El archivo JSON no contiene una estructura válida."
    installed = payload.get("installed")
    if not isinstance(installed, dict):
        return (
            None,
            "El archivo JSON debe corresponder a una 'Aplicación de escritorio' generada en Google Cloud Console.",
        )
    redirect_uris = installed.get("redirect_uris")
    normalized_uris: set[str] = set()
    if isinstance(redirect_uris, (list, tuple)):
        normalized_uris = {
            str(uri).strip().rstrip("/")
            for uri in redirect_uris
            if isinstance(uri, str) and uri.strip()
        }
    if _GOOGLE_REDIRECT_URI.rstrip("/") not in normalized_uris:
        return (
            None,
            "El JSON debe incluir http://localhost como redirect URI autorizado en la consola de Google.",
        )
    client_id = installed.get("client_id")
    if not client_id:
        return None, "El archivo JSON no contiene un Client ID válido."
    return installed, None


def _google_calendar_extract_client_credentials(
    payload: Dict[str, object]
) -> tuple[Optional[str], Optional[str]]:
    config, _ = _google_calendar_validate_client_payload(payload)
    if not config:
        return None, None
    client_id = config.get("client_id")
    client_secret = config.get("client_secret")
    return (
        (str(client_id) if client_id else None),
        (str(client_secret) if client_secret else None),
    )


def _google_calendar_report_oauth_error(exc: Exception) -> None:
    base_message = (
        "Error de autenticación. Verificá que el JSON cargado sea válido, "
        "que estés autorizado como tester y que el proyecto esté correctamente configurado."
    )
    details = str(exc).strip()
    if details:
        warn(f"{base_message} Detalle: {details}")
    else:
        warn(base_message)


def _ensure_google_auth_oauthlib() -> bool:
    global InstalledAppFlow
    if InstalledAppFlow is not None:
        return True
    flow_cls = None
    try:
        from google_auth_oauthlib.flow import InstalledAppFlow as flow_cls  # type: ignore
    except Exception:
        warn("Esta opción requiere la librería google-auth-oauthlib.")
        confirm = (
            ask("¿Deseás que la instalemos automáticamente ahora? (s/n): ")
            .strip()
            .lower()
        )
        if confirm not in {"s", "si", "sí", "y", "yes"}:
            warn("Instalación cancelada. Instalá google-auth-oauthlib para continuar.")
            press_enter()
            return False
        python_bin = sys.executable or "python3"
        print(
            style_text(
                "Instalando google-auth-oauthlib, por favor esperá...",
                color=Fore.YELLOW,
            )
        )
        try:
            subprocess.check_call(
                [python_bin, "-m", "pip", "install", "google-auth-oauthlib"]
            )
        except Exception as exc:
            warn(f"No se pudo instalar google-auth-oauthlib automáticamente: {exc}")
            press_enter()
            return False
        try:
            from google_auth_oauthlib.flow import InstalledAppFlow as flow_cls  # type: ignore
        except Exception as exc:
            warn(f"La librería google-auth-oauthlib no pudo cargarse: {exc}")
            press_enter()
            return False
        ok("La librería google-auth-oauthlib se instaló correctamente.")
    if flow_cls is None:
        warn("No se pudo cargar la librería google-auth-oauthlib.")
        press_enter()
        return False
    InstalledAppFlow = flow_cls
    return True


def _google_calendar_load_credentials_json() -> None:
    if not _ensure_google_auth_oauthlib():
        return
    banner()
    print(
        style_text(
            "Google Calendar • Cargar credenciales JSON",
            color=Fore.CYAN,
            bold=True,
        )
    )
    print(full_line(color=Fore.BLUE))
    for line in _google_calendar_status_lines():
        print(line)
    print(full_line(color=Fore.BLUE))
    alias = _google_calendar_select_alias()
    if not alias:
        return
    path_input = ask(
        "Ruta del archivo JSON de Google (vacío para cancelar): "
    ).strip()
    if not path_input:
        warn("No se cargó ningún archivo de credenciales.")
        press_enter()
        return
    file_path = Path(path_input).expanduser()
    if not file_path.exists():
        warn("El archivo especificado no existe.")
        press_enter()
        return
    try:
        json_payload = json.loads(file_path.read_text(encoding="utf-8"))
    except Exception as exc:
        warn(f"No se pudo leer el archivo JSON: {exc}")
        press_enter()
        return
    config, error = _google_calendar_validate_client_payload(json_payload)
    if error:
        warn(error)
        press_enter()
        return
    client_id = str(config.get("client_id") or "")
    client_secret_value = config.get("client_secret")
    client_secret = str(client_secret_value) if client_secret_value else None
    if not client_id:
        warn("El archivo JSON no contiene un Client ID válido.")
        press_enter()
        return
    _set_google_calendar_entry(
        alias,
        {"client_id": client_id, "client_secret": client_secret},
    )
    try:
        flow = InstalledAppFlow.from_client_secrets_file(
            str(file_path), scopes=[_GOOGLE_SCOPE]
        )
        try:
            flow.redirect_uri = _GOOGLE_REDIRECT_URI
        except Exception:
            # Algunos objetos Flow no exponen redirect_uri hasta ejecutar run_*.
            pass
    except Exception as exc:  # pragma: no cover - depende de librería externa
        warn(f"No se pudo inicializar el flujo OAuth: {exc}")
        press_enter()
        return
    try:
        credentials = flow.run_local_server(port=0)
    except Exception as exc_local:  # pragma: no cover - depende de librería externa
        logger.debug(
            "Fallo run_local_server para Google OAuth, se intenta modo consola",
            exc_info=exc_local,
        )
        try:
            credentials = flow.run_console()
        except Exception as exc_console:  # pragma: no cover - depende de librería externa
            _google_calendar_report_oauth_error(exc_console)
            press_enter()
            return
    entry = _get_google_calendar_entry(alias)
    _google_calendar_update_tokens_from_credentials(alias, entry, credentials)
    entry = _get_google_calendar_entry(alias)
    if entry.get("connected"):
        ok(f"Google Calendar conectado para {alias}.")
    else:
        warn("No se pudo completar la conexión con Google Calendar.")
    press_enter()


def _google_calendar_connect() -> None:
    if not _require_requests():
        return
    banner()
    print(style_text("Google Calendar • Conectar cuenta", color=Fore.CYAN, bold=True))
    print(full_line(color=Fore.BLUE))
    for line in _google_calendar_status_lines():
        print(line)
    print(full_line(color=Fore.BLUE))
    alias = _google_calendar_select_alias()
    if not alias:
        return
    entry = _get_google_calendar_entry(alias)
    current_client_id = str(entry.get("client_id") or "")
    current_client_secret = str(entry.get("client_secret") or "")
    print(f"Client ID actual: {current_client_id or '(sin definir)'}")
    client_id = ask("Ingresá el Client ID de OAuth (vacío mantiene actual): ").strip()
    if not client_id:
        client_id = current_client_id
    if not client_id:
        warn("Se requiere un Client ID válido para continuar.")
        press_enter()
        return
    client_secret = ask(
        "Ingresá el Client Secret (vacío mantiene actual o se omite si no aplica): "
    ).strip()
    if not client_secret:
        client_secret = current_client_secret
    _set_google_calendar_entry(alias, {"client_id": client_id, "client_secret": client_secret})
    token_data = _google_calendar_perform_device_flow(client_id, client_secret or None)
    if not token_data:
        return
    entry = _get_google_calendar_entry(alias)
    entry = _google_calendar_store_tokens(alias, entry, token_data)
    if entry.get("connected"):
        ok(f"Google Calendar conectado para {alias}.")
    else:
        warn("No se pudo completar la conexión con Google Calendar.")
    press_enter()


def _google_calendar_configure_event() -> None:
    banner()
    print(style_text("Google Calendar • Configuración de eventos", color=Fore.CYAN, bold=True))
    print(full_line(color=Fore.BLUE))
    for line in _google_calendar_status_lines():
        print(line)
    print(full_line(color=Fore.BLUE))
    alias = _google_calendar_select_alias()
    if not alias:
        return
    entry = _get_google_calendar_entry(alias)
    current_name = str(entry.get("event_name") or "{{username}} - Sistema de adquisición con IA")
    current_duration = int(entry.get("duration_minutes") or 30)
    current_timezone = str(entry.get("timezone") or _default_timezone_label())
    current_auto_meet = bool(entry.get("auto_meet", True))
    print(f"Nombre actual del evento: {current_name}")
    new_name = ask("Nuevo nombre (usa {{username}} para el lead, Enter mantiene): ").strip()
    updates: Dict[str, object] = {}
    if new_name:
        updates["event_name"] = new_name
    duration_input = ask(
        f"Duración en minutos (actual {current_duration}, Enter mantiene): "
    ).strip()
    if duration_input:
        try:
            updates["duration_minutes"] = max(5, int(duration_input))
        except Exception:
            warn("Duración inválida; se mantiene el valor actual.")
    tz_input = ask(
        f"Zona horaria (actual {current_timezone}, Enter mantiene): "
    ).strip()
    if tz_input:
        updates["timezone"] = tz_input
    auto_meet_input = ask(
        f"Generar enlace de Google Meet automáticamente? (S/N, actual {'S' if current_auto_meet else 'N'}): "
    ).strip().lower()
    if auto_meet_input in {"s", "si", "sí"}:
        updates["auto_meet"] = True
    elif auto_meet_input in {"n", "no"}:
        updates["auto_meet"] = False
    if updates:
        _set_google_calendar_entry(alias, updates)
        ok("Configuración de eventos actualizada.")
    else:
        warn("No se realizaron cambios.")
    press_enter()


def _google_calendar_configure_prompt() -> None:
    banner()
    print(
        style_text(
            "Google Calendar • Criterio para creación de eventos",
            color=Fore.CYAN,
            bold=True,
        )
    )
    print(full_line(color=Fore.BLUE))
    for line in _google_calendar_status_lines():
        print(line)
    print(full_line(color=Fore.BLUE))
    alias = _google_calendar_select_alias()
    if not alias:
        return
    entry = _get_google_calendar_entry(alias)
    current_prompt = str(entry.get("schedule_prompt") or _DEFAULT_GOOGLE_CALENDAR_PROMPT)
    print(style_text("Prompt actual:", color=Fore.BLUE))
    print(current_prompt.strip() or "(sin definir)")
    print(full_line(color=Fore.BLUE))
    print("Elegí una opción:")
    print("  E) Editar prompt")
    print("  D) Restaurar valor predeterminado")
    print("  Enter) Cancelar")
    action = ask("Acción: ").strip().lower()
    if not action:
        warn("No se modificó el criterio de calendario.")
        press_enter()
        return
    if action in {"d", "default", "predeterminado"}:
        _set_google_calendar_entry(
            alias,
            {"schedule_prompt": _DEFAULT_GOOGLE_CALENDAR_PROMPT},
        )
        ok("Se restauró el criterio predeterminado de Google Calendar.")
        press_enter()
        return
    if action not in {"e", "editar"}:
        warn("Opción inválida. No se modificó el criterio de calendario.")
        press_enter()
        return
    print(
        style_text(
            (
                "Pegá el nuevo criterio y finalizá con una línea que diga <<<END>>>."
                " Dejá vacío para cancelar."
            ),
            color=Fore.CYAN,
        )
    )
    lines: List[str] = []
    while True:
        line = ask("› ")
        if line.strip() == "<<<END>>>":
            break
        lines.append(line.replace("\r", ""))
    new_prompt = "\n".join(lines).strip()
    _set_google_calendar_entry(alias, {"schedule_prompt": new_prompt})
    if new_prompt:
        ok(f"Criterio actualizado. Longitud: {len(new_prompt)} caracteres.")
    else:
        ok("Se eliminó el criterio personalizado. Se usará la lógica automática predeterminada.")
    press_enter()


def _google_calendar_activate() -> None:
    banner()
    print(style_text("Google Calendar • Activar creación automática", color=Fore.CYAN, bold=True))
    print(full_line(color=Fore.BLUE))
    for line in _google_calendar_status_lines():
        print(line)
    print(full_line(color=Fore.BLUE))
    alias = _google_calendar_select_alias()
    if not alias:
        return
    entry = _get_google_calendar_entry(alias)
    if not entry.get("connected"):
        warn("Conectá Google Calendar antes de activar la lógica automática.")
        press_enter()
        return
    _set_google_calendar_entry(alias, {"enabled": True})
    ok(f"Lógica automática activada para {alias}.")
    press_enter()


def _google_calendar_deactivate() -> None:
    banner()
    print(style_text("Google Calendar • Desactivar creación automática", color=Fore.CYAN, bold=True))
    print(full_line(color=Fore.BLUE))
    for line in _google_calendar_status_lines():
        print(line)
    print(full_line(color=Fore.BLUE))
    alias = _google_calendar_select_alias()
    if not alias:
        return
    _set_google_calendar_entry(alias, {"enabled": False})
    ok(f"Lógica automática desactivada para {alias}.")
    press_enter()


def _google_calendar_revoke() -> None:
    banner()
    print(style_text("Google Calendar • Revocar conexión", color=Fore.CYAN, bold=True))
    print(full_line(color=Fore.BLUE))
    for line in _google_calendar_status_lines():
        print(line)
    print(full_line(color=Fore.BLUE))
    alias = _google_calendar_select_alias()
    if not alias:
        return
    entry = _get_google_calendar_entry(alias)
    token = entry.get("access_token") or entry.get("refresh_token")
    if token and _require_requests():
        try:
            requests.post(_GOOGLE_REVOKE_URL, params={"token": token}, timeout=15)
        except RequestException:
            logger.warning("No se pudo notificar la revocación a Google.", exc_info=False)
    _set_google_calendar_entry(
        alias,
        {
            "access_token": "",
            "refresh_token": "",
            "token_type": "",
            "token_expires_at": 0,
            "connected": False,
            "enabled": False,
        },
    )
    ok(f"Conexión revocada para {alias}.")
    press_enter()


def _google_calendar_menu() -> None:
    while True:
        banner()
        print(style_text("Conectar con Google Calendar", color=Fore.CYAN, bold=True))
        print(full_line(color=Fore.BLUE))
        for line in _google_calendar_status_lines():
            print(line)
        print(full_line(color=Fore.BLUE))
        print("1) Conectar cuenta mediante OAuth")
        print("2) Configurar parámetros del evento")
        print("3) Configurar criterio para creación de evento")
        print("4) Activar creación automática de eventos")
        print("5) Desactivar creación automática de eventos")
        print("6) Revocar conexión")
        print("7) Cargar credenciales JSON (Google OAuth 2.0)")
        print("8) Volver al submenú anterior")
        print(full_line(color=Fore.BLUE))
        choice = ask("Opción: ").strip()
        if choice == "1":
            _google_calendar_connect()
        elif choice == "2":
            _google_calendar_configure_event()
        elif choice == "3":
            _google_calendar_configure_prompt()
        elif choice == "4":
            _google_calendar_activate()
        elif choice == "5":
            _google_calendar_deactivate()
        elif choice == "6":
            _google_calendar_revoke()
        elif choice == "7":
            _google_calendar_load_credentials_json()
        elif choice == "8":
            break
        elif choice == "7":
            _google_calendar_load_credentials_json()
        else:
            warn("Opción inválida.")
            press_enter()


def _configure_api_key() -> None:
    banner()
    current_key, _ = _load_preferences()
    print(style_text("Configurar OPENAI_API_KEY", color=Fore.CYAN, bold=True))
    print(f"Actual: {(_mask_key(current_key) or '(sin definir)')}")
    print()
    new_key = ask("Nueva API Key (vacío para cancelar): ").strip()
    if not new_key:
        warn("Se mantuvo la API Key actual.")
        press_enter()
        return
    update_env_local({"OPENAI_API_KEY": new_key})
    refresh_settings()
    ok("OPENAI_API_KEY guardada en .env.local")
    press_enter()


def _configure_prompt() -> None:
    while True:
        banner()
        _, current_prompt = _load_preferences()
        print(style_text("Configurar System Prompt", color=Fore.CYAN, bold=True))
        print(style_text("Actual:", color=Fore.BLUE))
        print(current_prompt or "(sin definir)")
        print()
        print(f"Longitud actual: {len(current_prompt or '')} caracteres.")
        print(full_line(color=Fore.BLUE))
        print("1) Editar/pegar en consola (delimitador <<<END>>>)")
        print("2) Cargar desde archivo .txt")
        print("3) Ver primeros 400 caracteres")
        print("4) Volver")
        print(full_line(color=Fore.BLUE))
        choice = ask("Opción: ").strip()

        if choice == "1":
            print(style_text(
                "Pegá tu System Prompt y cerrá con una línea que diga <<<END>>>.",
                color=Fore.CYAN,
            ))
            lines: list[str] = []
            while True:
                line = ask("› ")
                if line.strip() == "<<<END>>>":
                    break
                lines.append(line.replace("\r", ""))
            new_prompt = "\n".join(lines)
            if not _normalize_system_prompt_text(new_prompt):
                warn("No se modificó el prompt.")
                press_enter()
                continue
            saved_prompt = _persist_system_prompt(new_prompt)
            ok(f"System Prompt guardado. Longitud: {len(saved_prompt)} caracteres.")
            press_enter()
        elif choice == "2":
            path_input = ask("Ruta del archivo .txt (vacío para cancelar): ").strip()
            if not path_input:
                warn("No se modificó el prompt.")
                press_enter()
                continue
            file_path = Path(path_input).expanduser()
            if not file_path.exists():
                warn("El archivo especificado no existe.")
                press_enter()
                continue
            try:
                file_contents = file_path.read_text(encoding="utf-8")
            except Exception as exc:
                warn(f"No se pudo leer el archivo: {exc}")
                press_enter()
                continue
            if not _normalize_system_prompt_text(file_contents):
                warn("No se modificó el prompt.")
                press_enter()
                continue
            saved_prompt = _persist_system_prompt(file_contents)
            ok(f"System Prompt guardado. Longitud: {len(saved_prompt)} caracteres.")
            press_enter()
        elif choice == "3":
            preview = (current_prompt or "")[:400]
            print(style_text("Primeros 400 caracteres:", color=Fore.BLUE))
            if not preview:
                print("(sin definir)")
            else:
                print(preview)
                if len(current_prompt or "") > 400:
                    print(style_text("… (truncado)", color=Fore.YELLOW))
            press_enter()
        elif choice == "4":
            break
        else:
            warn("Opción inválida.")
            press_enter()


def _available_aliases() -> List[str]:
    aliases: set[str] = {"ALL"}
    for account in list_all():
        if account.get("alias"):
            aliases.add(account["alias"].strip())
        if account.get("username"):
            aliases.add(account["username"].strip())
    return sorted(a for a in aliases if a)


def _preview_prompt(prompt: str) -> str:
    if not prompt:
        return "(sin definir)"
    first_line = prompt.splitlines()[0]
    if len(first_line) > 60:
        return first_line[:57] + "…"
    if len(prompt.splitlines()) > 1:
        return first_line + " …"
    return first_line


def autoresponder_menu_options() -> List[str]:
    return [
        "1) Configurar API Key",
        "2) Configurar System Prompt",
        "3) Activar bot (alias/grupo)",
        "4) Conectar con GoHighLevel",
        "5) Conectar con Google Calendar",
        "6) Desactivar bot",
        "7) Volver",
    ]


def autoresponder_prompt_length() -> int:
    _, prompt = _load_preferences()
    return len(prompt or "")


def _print_menu_header() -> None:
    banner()
    api_key, prompt = _load_preferences()
    status = (
        style_text(f"Estado: activo para {ACTIVE_ALIAS}", color=Fore.GREEN, bold=True)
        if ACTIVE_ALIAS
        else style_text("Estado: inactivo", color=Fore.YELLOW, bold=True)
    )
    print(style_text("Auto-responder con OpenAI", color=Fore.CYAN, bold=True))
    print(full_line(color=Fore.BLUE))
    print(f"API Key: {_mask_key(api_key) or '(sin definir)'}")
    print(f"System prompt: {_preview_prompt(prompt)}")
    print(status)
    print(_gohighlevel_summary_line())
    print(_google_calendar_summary_line())
    print(full_line(color=Fore.BLUE))
    for option in autoresponder_menu_options():
        print(option)
    print(full_line(color=Fore.BLUE))


def _prompt_alias_selection() -> str | None:
    options = _available_aliases()
    print("Alias/grupos disponibles:")
    for idx, alias in enumerate(options, start=1):
        print(f" {idx}) {alias}")
    raw = ask("Seleccioná alias (número o texto, Enter=ALL): ").strip()
    if not raw:
        return "ALL"
    if raw.isdigit():
        idx = int(raw)
        if 1 <= idx <= len(options):
            return options[idx - 1]
        warn("Número fuera de rango.")
        return None
    return raw


def _handle_account_issue(user: str, exc: Exception, active: List[str]) -> None:
    message = str(exc).lower()
    if should_retry_proxy(exc):
        label = style_text(f"[WARN][@{user}] proxy falló", color=Fore.YELLOW, bold=True)
        record_proxy_failure(user, exc)
        print(label)
        warn("Revisá la opción 1 para actualizar o quitar el proxy de esta cuenta.")
    elif "login_required" in message:
        label = style_text(f"[ERROR][@{user}] sesión inválida", color=Fore.RED, bold=True)
        print(label)
    elif any(key in message for key in ("challenge", "checkpoint")):
        label = style_text(f"[WARN][@{user}] checkpoint requerido", color=Fore.YELLOW, bold=True)
        print(label)
    elif "feedback_required" in message or "rate" in message:
        label = style_text(f"[WARN][@{user}] rate limit detectado", color=Fore.YELLOW, bold=True)
        print(label)
    else:
        label = style_text(f"[WARN][@{user}] error inesperado", color=Fore.YELLOW, bold=True)
        print(label)
    logger.warning("Incidente con @%s en auto-responder: %s", user, exc, exc_info=False)

    while True:
        choice = ask("¿Continuar sin esta cuenta (C) / Reintentar (R) / Pausar (P)? ").strip().lower()
        if choice in {"c", "r", "p"}:
            break
        warn("Elegí C, R o P.")

    if choice == "c":
        if user in active:
            active.remove(user)
        mark_connected(user, False)
        warn(f"Se excluye @{user} del ciclo actual.")
        return

    if choice == "p":
        request_stop("pausa solicitada desde menú del bot")
        return

    while choice == "r":
        if prompt_login(user, interactive=False) and _ensure_session(user):
            mark_connected(user, True)
            ok(f"Sesión renovada para @{user}")
            return
        warn("La sesión sigue fallando. Intentá nuevamente o elegí otra opción.")
        choice = ask("¿Reintentar (R) / Continuar sin la cuenta (C) / Pausar (P)? ").strip().lower()
        if choice == "c":
            if user in active:
                active.remove(user)
            mark_connected(user, False)
            warn(f"Se excluye @{user} del ciclo actual.")
            return
        if choice == "p":
            request_stop("pausa solicitada desde menú del bot")
            return


def _normalize_phone(raw: str) -> str:
    raw = raw.strip()
    if not raw:
        return ""
    has_plus = raw.startswith("+")
    digits = "".join(ch for ch in raw if ch.isdigit())
    if not digits:
        return ""
    return f"+{digits}" if has_plus else digits


def _extract_phone_numbers(text: str) -> List[str]:
    if not text:
        return []
    matches = _PHONE_PATTERN.findall(text)
    numbers: List[str] = []
    for match in matches:
        normalized = _normalize_phone(match)
        if normalized and len(normalized.replace("+", "")) >= 8:
            if normalized not in numbers:
                numbers.append(normalized)
    return numbers


def _extract_email_from_text(text: str) -> Optional[str]:
    if not text:
        return None
    matches = list(_EMAIL_PATTERN.findall(text))
    if not matches:
        return None
    return matches[-1]


def _infer_lead_tag(
    conversation: str,
    phone_numbers: List[str],
    status: Optional[str] = None,
) -> str:
    if status and status.strip().lower() == "no interesado":
        return "No calificado"
    normalized = _normalize_text_for_match(conversation)
    if any(_contains_token(normalized, keyword) for keyword in _NEGATIVE_KEYWORDS):
        return "No calificado"
    if phone_numbers:
        if any(word in normalized for word in _CALL_KEYWORDS):
            return "Listo para agendar llamada"
        if status and status.strip().lower() == "interesado":
            return "Listo para agendar llamada"
        return "Listo para agendar llamada"
    if any(_contains_token(normalized, keyword) for keyword in _POSITIVE_KEYWORDS):
        return "Interesado sin número"
    if any(keyword in normalized for keyword in _INFO_KEYWORDS) or "?" in conversation:
        return "Solicita más info"
    if normalized.strip():
        return _DEFAULT_LEAD_TAG
    return _DEFAULT_LEAD_TAG


def _build_conversation_note(
    account: str, recipient: str, conversation: str, status: Optional[str] = None
) -> str:
    header = [f"Cuenta IG: @{account}"]
    if recipient:
        header.append(f"Usuario: @{recipient}")
    if status:
        header.append(f"Estado detectado: {status}")
    header.append("Historial completo:")
    return "\n".join(header + [conversation])


def _next_weekday_date(base: datetime, target_weekday: int) -> datetime.date:
    days_ahead = (target_weekday - base.weekday() + 7) % 7
    if days_ahead == 0:
        days_ahead = 7
    return (base + timedelta(days=days_ahead)).date()


def _parse_meeting_datetime_from_text(text: str, tz_label: str) -> Optional[datetime]:
    if not text:
        return None
    if not _MEETING_TIME_PATTERN.search(text):
        return None
    match = _MEETING_TIME_PATTERN.search(text)
    if not match:
        return None
    try:
        hour = int(match.group("hour"))
    except Exception:
        return None
    minute_str = match.group("minute")
    minute = int(minute_str) if minute_str and minute_str.isdigit() else 0
    ampm = match.group("ampm")
    if ampm:
        ampm = ampm.lower()
        if ampm == "pm" and hour < 12:
            hour += 12
        if ampm == "am" and hour == 12:
            hour = 0
    hour %= 24
    tz = _safe_timezone(tz_label)
    base = datetime.now(tz)
    normalized = _normalize_text_for_match(text)
    date_value: Optional[datetime.date] = None
    for keyword, offset in _RELATIVE_DATE_KEYWORDS:
        if keyword in normalized:
            date_value = (base + timedelta(days=offset)).date()
            break
    if date_value is None:
        for keyword, weekday in _WEEKDAY_KEYWORDS.items():
            if _contains_token(normalized, keyword):
                date_value = _next_weekday_date(base, weekday)
                break
    if date_value is None and _MEETING_DATE_PATTERN.search(text):
        parsed = _safe_parse_datetime(text, fuzzy=True, dayfirst=True, default=base)
        if isinstance(parsed, datetime):
            date_value = parsed.date()
    if date_value is None:
        return None
    meeting_dt = datetime.combine(date_value, dt_time(hour=hour, minute=minute), tz)
    if meeting_dt < base:
        if _MEETING_DATE_PATTERN.search(text):
            return None
        meeting_dt += timedelta(days=7)
    return meeting_dt


def _detect_meeting_datetime(conversation: str, tz_label: str) -> Optional[datetime]:
    lines = [line for line in conversation.splitlines() if line.startswith("ELLOS:")]
    for line in reversed(lines):
        _, _, content = line.partition(":")
        meeting_dt = _parse_meeting_datetime_from_text(content.strip(), tz_label)
        if meeting_dt:
            return meeting_dt
    return None


def _render_calendar_summary(template: str, username: str) -> str:
    template = template or "{{username}} - Sistema de adquisición con IA"
    return template.replace("{{username}}", username or "Lead")


def _parse_gohighlevel_contact_id(data: Dict[str, object]) -> Optional[str]:
    if not isinstance(data, dict):
        return None
    contact = data.get("contact") if isinstance(data.get("contact"), dict) else None
    if isinstance(contact, dict):
        for key in ("id", "Id", "contactId"):
            if contact.get(key):
                return str(contact[key])
    for key in ("contactId", "id", "Id"):
        value = data.get(key)
        if value:
            return str(value)
    return None


def _update_gohighlevel_contact(
    api_key: str, contact_id: str, payload: Dict[str, object]
) -> Optional[str]:
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    url = f"{_GOHIGHLEVEL_BASE}/contacts/{contact_id}"
    response = requests.put(url, json=payload, headers=headers, timeout=15)  # type: ignore[call-arg]
    response.raise_for_status()
    data: Dict[str, object] = {}
    if response.content:
        try:
            data = response.json()
        except ValueError:
            data = {}
    return _parse_gohighlevel_contact_id(data) or contact_id


def _create_gohighlevel_contact(
    api_key: str, payload: Dict[str, object]
) -> tuple[Optional[str], bool]:
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    url = f"{_GOHIGHLEVEL_BASE}/contacts/"
    response = requests.post(url, json=payload, headers=headers, timeout=15)  # type: ignore[call-arg]
    data: Dict[str, object] = {}
    if response.status_code == 409:
        try:
            data = response.json()
        except ValueError:
            data = {}
        contact_id = _parse_gohighlevel_contact_id(data)
        if contact_id:
            updated_id = _update_gohighlevel_contact(api_key, str(contact_id), payload)
            return updated_id, False
    response.raise_for_status()
    if response.content:
        try:
            data = response.json()
        except ValueError:
            data = {}
    contact_id = _parse_gohighlevel_contact_id(data)
    if contact_id:
        return contact_id, True
    return None, True


def _attach_gohighlevel_note(api_key: str, contact_id: str, note: str) -> None:
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    url = f"{_GOHIGHLEVEL_BASE}/contacts/{contact_id}/notes/"
    payload = {"body": note}
    response = requests.post(url, json=payload, headers=headers, timeout=15)  # type: ignore[call-arg]
    response.raise_for_status()


def _send_lead_to_gohighlevel(
    account: str,
    recipient: str,
    conversation: str,
    phone_numbers: List[str],
    status: Optional[str],
    openai_api_key: Optional[str] = None,
) -> None:
    if requests is None:
        logger.warning("GoHighLevel no disponible: falta la librería requests.")
        return
    if not phone_numbers:
        return
    alias, entry = _gohighlevel_enabled_entry_for(account)
    if not alias or not entry:
        return
    api_key = str(entry.get("api_key") or "")
    if not api_key:
        return
    location_ids = _sanitize_location_ids(entry.get("location_ids"))
    if not location_ids:
        logger.info(
            "GoHighLevel sin Location IDs configurados | alias=%s | cuenta=%s",
            alias,
            account,
        )
        return
    lead_identifier = recipient or phone_numbers[0]
    normalized_lead = _normalize_lead_id(lead_identifier)
    main_phone = phone_numbers[0]
    if _gohighlevel_already_sent(alias, normalized_lead, main_phone):
        return

    if not _gohighlevel_lead_qualifies(
        entry,
        conversation,
        status,
        phone_numbers,
        openai_api_key,
    ):
        return

    contact_payload: Dict[str, object] = {
        "name": recipient or "Lead Instagram",
        "phone": main_phone,
    }
    email = _extract_email_from_text(conversation)
    if email:
        contact_payload["email"] = email
    note_text = _build_conversation_note(account, recipient, conversation, status)
    lead_tag = _infer_lead_tag(conversation, phone_numbers, status)
    successes: List[str] = []
    for location_id in location_ids:
        payload = dict(contact_payload)
        payload["locationId"] = location_id
        if lead_tag:
            payload["tags"] = [lead_tag]
        try:
            contact_id, created = _create_gohighlevel_contact(api_key, payload)
            if not contact_id:
                message = (
                    "No se obtuvo contactId al crear contacto en GoHighLevel para %s (location %s)."
                )
                logger.warning(
                    message,
                    recipient or "(sin usuario)",
                    location_id,
                )
                print(
                    f"❌ Falló el envío a GHL (Location {location_id}): no se recibió identificador del contacto"
                )
                continue
            _attach_gohighlevel_note(api_key, contact_id, note_text)
            successes.append(location_id)
            action = "creado" if created else "actualizado"
            print(
                f"✅ Lead enviado a GHL (Location {location_id}) — contacto {action} (ID {contact_id})"
            )
        except RequestException as exc:  # pragma: no cover - depende de red externa
            logger.warning(
                "Error enviando lead a GoHighLevel (location %s): %s",
                location_id,
                exc,
                exc_info=False,
            )
            print(f"❌ Falló el envío a GHL (Location {location_id}): {exc}")
        except Exception as exc:  # pragma: no cover - manejo defensivo
            logger.warning(
                "Fallo inesperado con GoHighLevel (location %s): %s",
                location_id,
                exc,
                exc_info=False,
            )
            print(f"❌ Falló el envío a GHL (Location {location_id}): {exc}")
    if not successes:
        return

    _gohighlevel_mark_sent(alias, normalized_lead, main_phone)
    logger.info(
        "Lead enviado a GoHighLevel | alias=%s | cuenta=%s | contacto=%s | locations=%s | tag=%s",
        alias,
        account,
        recipient or "(sin usuario)",
        ",".join(successes),
        lead_tag,
    )


def _maybe_schedule_google_calendar_event(
    account: str,
    recipient: str,
    conversation: str,
    phone_numbers: List[str],
    status: Optional[str],
    openai_api_key: Optional[str] = None,
) -> Optional[str]:
    if ACTIVE_ALIAS is None:
        return None
    if status and status.strip().lower() == "no interesado":
        return None
    alias, entry = _google_calendar_enabled_entry_for(account)
    if not alias or not entry:
        return None
    if requests is None and (Credentials is None or build is None):
        return None
    tz_label = str(entry.get("timezone") or _default_timezone_label())
    meeting_dt = _detect_meeting_datetime(conversation, tz_label)
    if not meeting_dt:
        return None
    normalized_convo = _normalize_text_for_match(conversation)
    prompt_text = str(entry.get("schedule_prompt") or "").strip()
    if not prompt_text and not any(
        keyword in normalized_convo for keyword in _CALL_KEYWORDS
    ):
        return None
    if not _google_calendar_lead_qualifies(
        entry,
        conversation,
        status,
        phone_numbers,
        meeting_dt,
        openai_api_key,
    ):
        return None
    main_phone = _normalize_phone(phone_numbers[0]) if phone_numbers else ""
    normalized_lead = recipient or main_phone or f"{account}-lead"
    if _google_calendar_already_scheduled(alias, normalized_lead, main_phone):
        return None
    access_token = _google_calendar_ensure_token(alias, entry)
    if not access_token and requests is None and (Credentials is None or build is None):
        return None
    summary_template = str(entry.get("event_name") or "{{username}} - Sistema de adquisición con IA")
    summary = _render_calendar_summary(summary_template, recipient or "Lead")
    try:
        duration = int(entry.get("duration_minutes") or 30)
    except Exception:
        duration = 30
    duration = max(5, duration)
    tz = _safe_timezone(tz_label)
    start_dt = meeting_dt.astimezone(tz)
    end_dt = start_dt + timedelta(minutes=duration)
    email = _extract_email_from_text(conversation)
    description_lines = [
        "Evento generado automáticamente desde el bot de Instagram.",
        f"Cuenta IG: @{account}",
    ]
    if recipient:
        description_lines.append(f"Usuario IG: @{recipient}")
    if main_phone:
        description_lines.append(f"Teléfono: {main_phone}")
    else:
        description_lines.append("Teléfono: (sin proporcionar)")
    if email:
        description_lines.append(f"Email: {email}")
    if status:
        description_lines.append(f"Estado detectado: {status}")
    description_lines.append("")
    description_lines.append("Historial de la conversación:")
    description_lines.append(conversation)
    description = "\n".join(description_lines)
    payload: Dict[str, object] = {
        "summary": summary,
        "description": description,
        "start": {"dateTime": start_dt.isoformat(), "timeZone": tz_label},
        "end": {"dateTime": end_dt.isoformat(), "timeZone": tz_label},
    }
    attendees: List[Dict[str, str]] = []
    if email:
        attendees.append({"email": email})
    if attendees:
        payload["attendees"] = attendees
    params: Dict[str, object] = {}
    if entry.get("auto_meet", True):
        payload["conferenceData"] = {
            "createRequest": {
                "requestId": uuid.uuid4().hex,
                "conferenceSolutionKey": {"type": "hangoutsMeet"},
            }
        }
        params["conferenceDataVersion"] = 1
    event = _google_calendar_create_event(alias, entry, payload, params, access_token)
    if not event:
        return None
    event_id = event.get("id") if isinstance(event, dict) else None
    if not event_id:
        return None
    event_link = ""
    backup_link = ""
    if isinstance(event, dict):
        event_link = str(event.get("htmlLink") or "")
        backup_link = str(event.get("hangoutLink") or "")
        if not backup_link:
            conference_data = event.get("conferenceData") if isinstance(event.get("conferenceData"), dict) else {}
            if isinstance(conference_data, dict):
                entry_points = conference_data.get("entryPoints")
                if isinstance(entry_points, list):
                    for item in entry_points:
                        if isinstance(item, dict) and item.get("uri"):
                            backup_link = str(item["uri"])
                            break
    stored_link = event_link or backup_link
    _google_calendar_mark_scheduled(alias, normalized_lead, main_phone, event_id, stored_link)
    logger.info(
        "Evento programado en Google Calendar | alias=%s | cuenta=%s | lead=%s | inicio=%s",
        alias,
        account,
        recipient or "(sin usuario)",
        start_dt.isoformat(),
    )
    formatted_dt = start_dt.strftime("%d/%m/%Y %H:%M")
    message_lines = [
        f"Listo, acabo de agendar nuestra llamada para {formatted_dt} ({tz_label}).",
    ]
    if event_link:
        message_lines.append(
            f"Te paso el link del evento para que confirmes la asistencia: {event_link}"
        )
    elif stored_link:
        message_lines.append(
            f"Te compartí los detalles de la reunión en nuestro calendario: {stored_link}"
        )
    else:
        message_lines.append("Te compartí los detalles de la reunión en nuestro calendario.")
    return "\n".join(message_lines)


def _process_inbox(
    client,
    user: str,
    state: Dict[str, Dict[str, str]],
    api_key: str,
    system_prompt: str,
    stats: BotStats,
) -> None:
    inbox = client.direct_threads(selected_filter="unread", amount=10)
    if not inbox:
        return
    state.setdefault(user, {})
    for thread in inbox:
        if STOP_EVENT.is_set():
            break
        thread_id = thread.id
        messages = client.direct_messages(thread_id, amount=10)
        if not messages:
            continue
        last = messages[0]
        if last.user_id == client.user_id:
            continue
        last_seen = state[user].get(thread_id)
        if last_seen == last.id:
            continue
        convo = "\n".join(
            [
                f"{'YO' if msg.user_id == client.user_id else 'ELLOS'}: {msg.text or ''}"
                for msg in reversed(messages)
            ]
        )
        recipient_username = _resolve_username(client, thread, last.user_id) or str(last.user_id)
        status = _classify_response(last.text or "")
        if status and recipient_username:
            msg_ts = getattr(last, "timestamp", None)
            ts_value = None
            if isinstance(msg_ts, datetime):
                ts_value = int(msg_ts.timestamp())
            log_conversation_status(user, recipient_username, status, timestamp=ts_value)
        phone_numbers = _extract_phone_numbers(last.text or "")
        if not phone_numbers:
            phone_numbers = _extract_phone_numbers(convo)
        calendar_message: Optional[str] = None
        if status != "No interesado":
            if phone_numbers:
                _send_lead_to_gohighlevel(
                    user,
                    recipient_username,
                    convo,
                    phone_numbers,
                    status,
                    api_key,
                )
            calendar_message = _maybe_schedule_google_calendar_event(
                user,
                recipient_username,
                convo,
                phone_numbers,
                status,
                api_key,
            )
        try:
            reply = _gen_response(api_key, system_prompt, convo)
            client.direct_send(reply, [last.user_id])
            if calendar_message:
                client.direct_send(calendar_message, [last.user_id])
        except Exception as exc:
            setattr(exc, "_autoresponder_sender", user)
            setattr(exc, "_autoresponder_recipient", recipient_username)
            setattr(exc, "_autoresponder_message_attempt", True)
            raise
        state[user][thread_id] = last.id
        save_auto_state(state)
        index = stats.record_success(user)
        logger.info("Respuesta enviada por @%s en hilo %s", user, thread_id)
        _print_response_summary(index, user, recipient_username, True)


def _print_bot_summary(stats: BotStats) -> None:
    print(full_line(color=Fore.MAGENTA))
    print(style_text("=== BOT DETENIDO ===", color=Fore.YELLOW, bold=True))
    print(style_text(f"Alias: {stats.alias}", color=Fore.WHITE, bold=True))
    print(style_text(f"Mensajes respondidos: {stats.responded}", color=Fore.GREEN, bold=True))
    print(style_text(f"Cuentas activas: {len(stats.accounts)}", color=Fore.CYAN, bold=True))
    print(style_text(f"Errores: {stats.errors}", color=Fore.RED if stats.errors else Fore.GREEN, bold=True))
    print(full_line(color=Fore.MAGENTA))
    press_enter()


def _activate_bot() -> None:
    global ACTIVE_ALIAS
    api_key, system_prompt = _load_preferences()
    if not api_key:
        warn("Configurá OPENAI_API_KEY antes de activar el bot.")
        press_enter()
        return

    alias = _prompt_alias_selection()
    if not alias:
        warn("Alias inválido.")
        press_enter()
        return

    targets = _choose_targets(alias)
    if not targets:
        warn("No se encontraron cuentas activas para ese alias.")
        press_enter()
        return

    active_accounts = _filter_valid_sessions(targets)
    if not active_accounts:
        warn("Ninguna cuenta tiene sesión válida.")
        press_enter()
        return

    settings = refresh_settings()
    delay_default = max(1, settings.autoresponder_delay)
    delay = ask_int(
        f"Delay entre chequeos (segundos) [{delay_default}]: ",
        1,
        default=delay_default,
    )

    ensure_logging(quiet=settings.quiet, log_dir=settings.log_dir, log_file=settings.log_file)
    reset_stop_event()
    state = get_auto_state()
    stats = BotStats(alias=alias)
    ACTIVE_ALIAS = alias
    listener = start_q_listener("Presioná Q para detener el auto-responder.", logger)
    print(style_text(f"Bot activo para {alias} ({len(active_accounts)} cuentas)", color=Fore.GREEN, bold=True))
    logger.info(
        "Auto-responder activo para %d cuentas (alias %s). Delay: %ss",
        len(active_accounts),
        alias,
        delay,
    )

    try:
        with _suppress_console_noise():
            while not STOP_EVENT.is_set() and active_accounts:
                for user in list(active_accounts):
                    if STOP_EVENT.is_set():
                        break
                    try:
                        client = _client_for(user)
                    except Exception as exc:
                        stats.record_error(user)
                        _handle_account_issue(user, exc, active_accounts)
                        continue

                    try:
                        _process_inbox(client, user, state, api_key, system_prompt, stats)
                    except KeyboardInterrupt:
                        raise
                    except Exception as exc:  # pragma: no cover - depende de SDK/insta
                        if getattr(exc, "_autoresponder_message_attempt", False):
                            index = stats.record_response_error(user)
                            sender = getattr(exc, "_autoresponder_sender", user)
                            recipient = getattr(exc, "_autoresponder_recipient", "-")
                            _print_response_summary(index, sender, recipient, False)
                        else:
                            stats.record_error(user)
                        logger.warning(
                            "Error en auto-responder para @%s: %s",
                            user,
                            exc,
                            exc_info=not settings.quiet,
                        )
                        _handle_account_issue(user, exc, active_accounts)

                if active_accounts and not STOP_EVENT.is_set():
                    sleep_with_stop(delay)

        if not active_accounts:
            warn("No quedan cuentas activas; el bot se detiene.")
            request_stop("sin cuentas activas para responder")

    except KeyboardInterrupt:
        request_stop("interrupción con Ctrl+C")
    finally:
        request_stop("auto-responder detenido")
        if listener:
            listener.join(timeout=0.1)
        ACTIVE_ALIAS = None
        _print_bot_summary(stats)


def _manual_stop() -> None:
    if STOP_EVENT.is_set():
        warn("El bot ya está detenido.")
    else:
        request_stop("detención solicitada desde el menú")
        warn("Si el bot está activo, finalizará al terminar el ciclo en curso.")
    press_enter()


def menu_autoresponder():
    while True:
        _print_menu_header()
        choice = ask("Opción: ").strip()
        if choice == "1":
            _configure_api_key()
        elif choice == "2":
            _configure_prompt()
        elif choice == "3":
            _activate_bot()
        elif choice == "4":
            _gohighlevel_menu()
        elif choice == "5":
            _google_calendar_menu()
        elif choice == "6":
            _manual_stop()
        elif choice == "7":
            break
        else:
            warn("Opción inválida.")
            press_enter()
