import json
import logging
import re
import time
import unicodedata
import warnings
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from accounts import get_account, list_all, mark_connected, prompt_login
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

try:  # pragma: no cover - depende de dependencia opcional
    import requests
    from requests import RequestException
except Exception:  # pragma: no cover - fallback si requests no está
    requests = None  # type: ignore
    RequestException = Exception  # type: ignore

logger = logging.getLogger(__name__)

DEFAULT_PROMPT = "Respondé cordial, breve y como humano."
PROMPT_KEY = "autoresponder_system_prompt"
ACTIVE_ALIAS: str | None = None

_PROMPT_STORAGE_DIR = runtime_base(Path(__file__).resolve().parent) / "data" / "autoresponder"
_PROMPT_DEFAULT_ALIAS = "default"

_GOHIGHLEVEL_FILE = runtime_base(Path(__file__).resolve().parent) / "storage" / "gohighlevel.json"
_GOHIGHLEVEL_BASE = "https://rest.gohighlevel.com/v1"
_PHONE_PATTERN = re.compile(r"(?<!\d)(?:\+?\d[\d\s().-]{7,}\d)")
_EMAIL_PATTERN = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)
_GOHIGHLEVEL_STATE: Dict[str, dict] | None = None

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


def _format_handle(value: str | None) -> str:
    if not value:
        return "@-"
    value = value.strip()
    if value.startswith("@"):
        return value
    return f"@{value}"


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
        print("\nLas siguientes cuentas necesitan volver a iniciar sesión:")
        for user, reason in needing_login:
            print(f" - @{user}: {reason}")
        if ask("¿Iniciar sesión ahora? (s/N): ").strip().lower() == "s":
            for user, _ in needing_login:
                if prompt_login(user) and _ensure_session(user):
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


def _normalize_alias_key(alias: str) -> str:
    return alias.strip().lower()


def _normalize_lead_id(lead: str) -> str:
    return lead.strip().lower()


def _get_gohighlevel_entry(alias: str) -> Dict[str, dict]:
    state = _read_gohighlevel_state()
    key = _normalize_alias_key(alias)
    aliases: Dict[str, dict] = state.get("aliases", {})
    entry = aliases.get(key)
    if isinstance(entry, dict):
        entry.setdefault("alias", alias.strip())
        entry.setdefault("sent", {})
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
    entry.update({k: v for k, v in updates.items() if v is not None})
    aliases[key] = entry
    _write_gohighlevel_state(state)


def _mask_gohighlevel_status(entry: Dict[str, object]) -> str:
    api_key = str(entry.get("api_key") or "")
    enabled = bool(entry.get("enabled"))
    status = "🟢 Activo" if enabled else "⚪ Inactivo"
    return f"{status} • API Key: {_mask_key(api_key) or '(sin definir)'}"


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
        print("2) Activar el envío automático de leads calificados al CRM de GoHighLevel")
        print("3) Desactivar conexión")
        print("4) Volver al submenú anterior")
        print(full_line(color=Fore.BLUE))
        choice = ask("Opción: ").strip()
        if choice == "1":
            _gohighlevel_configure_key()
        elif choice == "2":
            _gohighlevel_activate()
        elif choice == "3":
            _gohighlevel_deactivate()
        elif choice == "4":
            break
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
        "5) Desactivar bot",
        "6) Volver",
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
        if prompt_login(user) and _ensure_session(user):
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


def _build_conversation_note(account: str, recipient: str, conversation: str) -> str:
    header = [f"Cuenta IG: @{account}"]
    if recipient:
        header.append(f"Usuario: @{recipient}")
    header.append("Historial completo:")
    return "\n".join(header + [conversation])


def _create_gohighlevel_contact(api_key: str, payload: Dict[str, object]) -> Optional[str]:
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
        contact = data.get("contact") if isinstance(data.get("contact"), dict) else None
        contact_id = data.get("contactId") or (contact.get("id") if contact else None)
        if contact_id:
            return str(contact_id)
    response.raise_for_status()
    if response.content:
        try:
            data = response.json()
        except ValueError:
            data = {}
    contact_data = data.get("contact") if isinstance(data.get("contact"), dict) else {}
    if isinstance(contact_data, dict):
        for key in ("id", "Id", "contactId"):
            if contact_data.get(key):
                return str(contact_data[key])
    for key in ("contactId", "id", "Id"):
        value = data.get(key)
        if value:
            return str(value)
    return None


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
    lead_identifier = recipient or phone_numbers[0]
    normalized_lead = _normalize_lead_id(lead_identifier)
    main_phone = phone_numbers[0]
    if _gohighlevel_already_sent(alias, normalized_lead, main_phone):
        return

    contact_payload: Dict[str, object] = {
        "name": recipient or "Lead Instagram",
        "phone": main_phone,
    }
    email = _extract_email_from_text(conversation)
    if email:
        contact_payload["email"] = email
    try:
        contact_id = _create_gohighlevel_contact(api_key, contact_payload)
        if not contact_id:
            logger.warning(
                "No se obtuvo contactId al crear contacto en GoHighLevel para %s.",
                recipient or "(sin usuario)",
            )
            return
        note_text = _build_conversation_note(account, recipient, conversation)
        _attach_gohighlevel_note(api_key, contact_id, note_text)
    except RequestException as exc:  # pragma: no cover - depende de red externa
        logger.warning("Error enviando lead a GoHighLevel: %s", exc, exc_info=False)
        return
    except Exception as exc:  # pragma: no cover - manejo defensivo
        logger.warning("Fallo inesperado con GoHighLevel: %s", exc, exc_info=False)
        return

    _gohighlevel_mark_sent(alias, normalized_lead, main_phone)
    logger.info(
        "Lead enviado a GoHighLevel | alias=%s | cuenta=%s | contacto=%s",
        alias,
        account,
        recipient or "(sin usuario)",
    )


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
        if phone_numbers and status != "No interesado":
            _send_lead_to_gohighlevel(user, recipient_username, convo, phone_numbers)
        try:
            reply = _gen_response(api_key, system_prompt, convo)
            client.direct_send(reply, [last.user_id])
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
            _manual_stop()
        elif choice == "6":
            break
        else:
            warn("Opción inválida.")
            press_enter()
