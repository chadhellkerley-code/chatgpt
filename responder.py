import logging
import time
import unicodedata
import warnings
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List

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
from utils import ask, ask_int, ask_multiline, banner, ok, press_enter, warn

logger = logging.getLogger(__name__)

DEFAULT_PROMPT = "Respondé cordial, breve y como humano."
PROMPT_KEY = "autoresponder_system_prompt"
ACTIVE_ALIAS: str | None = None

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
                {"role": "user", "content": convo_text[:6000]},
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


def _load_preferences() -> tuple[str, str]:
    env_values = read_env_local()
    api_key = env_values.get("OPENAI_API_KEY") or SETTINGS.openai_api_key or ""
    config_values = read_app_config()
    prompt = config_values.get(PROMPT_KEY, "") or ""
    prompt = prompt.strip() or DEFAULT_PROMPT
    return api_key, prompt


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
    banner()
    _, current_prompt = _load_preferences()
    print(style_text("Configurar System Prompt", color=Fore.CYAN, bold=True))
    print(style_text("Actual:", color=Fore.BLUE))
    print(current_prompt or "(sin definir)")
    print()
    new_prompt = ask_multiline("Ingresá el nuevo System Prompt")
    if not new_prompt:
        warn("No se modificó el prompt.")
        press_enter()
        return
    update_app_config({PROMPT_KEY: new_prompt})
    ok("System prompt guardado en storage/config.json")
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
    print(full_line(color=Fore.BLUE))
    print("1) Configurar API Key")
    print("2) Configurar System Prompt")
    print("3) Activar bot (alias/grupo)")
    print("4) Desactivar bot")
    print("5) Volver")
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
            _manual_stop()
        elif choice == "5":
            break
        else:
            warn("Opción inválida.")
            press_enter()
