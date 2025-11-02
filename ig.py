# ig.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
import queue
import random
import math
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta, time as dt_time, timezone
from typing import Callable, Dict, Optional

try:  # pragma: no cover - depende de la versión de Python
    from zoneinfo import ZoneInfo as _BuiltinZoneInfo
except Exception:  # pragma: no cover - fallback si falta la stdlib
    _BuiltinZoneInfo = None  # type: ignore[assignment]

try:  # pragma: no cover - depende de dependencia opcional
    from backports.zoneinfo import ZoneInfo as _BackportZoneInfo  # type: ignore
except Exception:  # pragma: no cover - fallback si falta el backport
    _BackportZoneInfo = None  # type: ignore[assignment]

try:  # pragma: no cover - depende de dependencia opcional
    from dateutil import tz as _dateutil_tz  # type: ignore
except Exception:  # pragma: no cover - si falta dateutil
    _dateutil_tz = None  # type: ignore[assignment]

from accounts import (
    auto_login_with_saved_password,
    get_account,
    has_valid_session_settings,
    list_all,
    mark_connected,
    prompt_login,
)
from config import SETTINGS
from leads import load_list
from proxy_manager import apply_proxy_to_client, record_proxy_failure, should_retry_proxy
from runtime import (
    STOP_EVENT,
    ensure_logging,
    jitter_delay,
    request_stop,
    reset_stop_event,
    sleep_with_stop,
    start_q_listener,
)
from session_store import has_session, load_into
from storage import already_contacted, log_sent, sent_totals
from ui import Fore, LiveTable, banner, full_line, highlight, style_text
from utils import ask, ask_int, press_enter, warn

logger = logging.getLogger(__name__)


@dataclass
class SendEvent:
    username: str
    lead: str
    success: bool
    detail: str
    attention: str | None = None
    reason_code: str | None = None
    reason_label: str | None = None
    suggestion: str | None = None
    scope: str | None = None


_LIVE_COUNTS = {"base_ok": 0, "base_fail": 0, "run_ok": 0, "run_fail": 0}
_LIVE_LOCK = threading.Lock()


def _load_timezone(label: str):
    for provider in (_BuiltinZoneInfo, _BackportZoneInfo):
        if provider is None:
            continue
        try:
            return provider(label)
        except Exception:
            continue
    if _dateutil_tz is not None:
        tzinfo = _dateutil_tz.gettz(label)
        if tzinfo is not None:
            return tzinfo
    return timezone.utc


AR_TZ = _load_timezone("America/Argentina/Cordoba")


def today_ar():
    return datetime.now(AR_TZ).date()


def next_midnight_ar(now=None):
    now = now or datetime.now(AR_TZ)
    tomorrow = now.date() + timedelta(days=1)
    return datetime.combine(tomorrow, dt_time(0, 0), tzinfo=AR_TZ)


def create_daily_send_state() -> Dict[str, object]:
    """Return a fresh counter state for the send screen.

    Keeping this in a helper lets both owner and client launchers
    initialize the exact same midnight reset behaviour.
    """

    return {
        "date": today_ar(),
        "sent": 0,
        "errors": 0,
        "next_reset_at": next_midnight_ar(),
    }


def _refresh_daily_state(send_state: Dict[str, object]) -> None:
    try:
        now = datetime.now(AR_TZ)
        stored_date = send_state.get("date")
        next_reset = send_state.get("next_reset_at")
        if (
            stored_date is None
            or next_reset is None
            or now >= next_reset
            or today_ar() != stored_date
        ):
            send_state["date"] = today_ar()
            send_state["sent"] = 0
            send_state["errors"] = 0
            send_state["next_reset_at"] = next_midnight_ar(now)
    except Exception:
        try:
            send_state["date"] = today_ar()
            send_state.setdefault("sent", 0)
            send_state.setdefault("errors", 0)
            send_state["next_reset_at"] = next_midnight_ar()
        except Exception:
            pass


def _reset_live_counters(reset_run: bool = True) -> None:
    base_ok, base_fail = sent_totals()
    with _LIVE_LOCK:
        _LIVE_COUNTS["base_ok"] = base_ok
        _LIVE_COUNTS["base_fail"] = base_fail
        if reset_run:
            _LIVE_COUNTS["run_ok"] = 0
            _LIVE_COUNTS["run_fail"] = 0


def get_message_totals() -> tuple[int, int]:
    with _LIVE_LOCK:
        ok_total = _LIVE_COUNTS["base_ok"] + _LIVE_COUNTS["run_ok"]
        error_total = _LIVE_COUNTS["base_fail"] + _LIVE_COUNTS["run_fail"]
    return ok_total, error_total


def _client_for(username: str):
    from instagrapi import Client

    account = get_account(username)
    cl = Client()
    binding = None
    try:
        binding = apply_proxy_to_client(cl, username, account, reason="envio")
    except Exception as exc:
        if account and account.get("proxy_url"):
            record_proxy_failure(username, exc)
            raise RuntimeError(
                f"El proxy configurado para @{username} no respondió: {exc}"
            ) from exc
        logger.warning("Proxy no disponible para @%s: %s", username, exc, exc_info=False)

    try:
        load_into(cl, username)
    except FileNotFoundError as exc:
        mark_connected(username, False)
        raise RuntimeError(f"No hay sesión guardada para {username}. Usá opción 1.") from exc
    except Exception as exc:
        if binding and should_retry_proxy(exc):
            record_proxy_failure(username, exc)
        mark_connected(username, False)
        raise

    if not has_valid_session_settings(cl):
        mark_connected(username, False)
        raise RuntimeError(
            f"La sesión guardada para {username} no contiene credenciales activas. Iniciá sesión nuevamente."
        )

    mark_connected(username, True)
    return cl


def _ensure_session(username: str) -> bool:
    try:
        _client_for(username)
        return True
    except Exception:
        return False


def _send_dm(cl, to_username: str, message: str) -> bool:
    try:
        uid = cl.user_id_from_username(to_username)
        cl.direct_send(message, [uid])
        return True
    except Exception as exc:
        if should_retry_proxy(exc):
            raise
        logger.debug("Error enviando DM a @%s: %s", to_username, exc, exc_info=False)
        return False


def _diagnose_exception(exc: Exception) -> str | None:
    text = str(exc).lower()
    mapping = {
        "login_required": "Instagram solicitó un nuevo login.",
        "challenge_required": "Se requiere resolver un challenge en la app.",
        "feedback_required": "Instagram bloqueó temporalmente acciones de esta cuenta.",
        "rate_limit": "Se alcanzó un rate limit. Conviene pausar unos minutos.",
        "checkpoint": "Instagram requiere verificación adicional (checkpoint).",
        "consent_required": "La sesión requiere aprobación en la app oficial.",
    }
    for key, message in mapping.items():
        if key in text:
            return message
    return None


_ERROR_SIGNATURES = [
    {
        "keywords": ("login_required",),
        "code": "login_required",
        "detail": "Sesión vencida (Instagram pidió login)",
        "attention": "Instagram solicitó un nuevo login.",
        "label": "Login requerido",
        "suggestion": "Reiniciá la sesión desde la opción 1 antes de continuar.",
        "scope": "account",
    },
    {
        "keywords": ("challenge_required",),
        "code": "challenge_required",
        "detail": "Instagram pidió resolver un challenge",
        "attention": "Se requiere resolver un challenge en la app.",
        "label": "Challenge requerido",
        "suggestion": "Ingresá a la app oficial y resolvé el challenge pendiente.",
        "scope": "account",
    },
    {
        "keywords": (
            "feedback_required",
            "please wait a few minutes",
            "try again later",
            "we restrict certain activity",
        ),
        "code": "temporary_block",
        "detail": "Instagram bloqueó temporalmente las acciones de esta cuenta",
        "attention": "Instagram bloqueó temporalmente acciones de esta cuenta.",
        "label": "Bloqueo temporal",
        "suggestion": "Pausá la campaña unos minutos y revisá el calentamiento de la cuenta.",
        "scope": "account",
    },
    {
        "keywords": ("rate_limit", "too many requests", "throttled", "429"),
        "code": "rate_limit",
        "detail": "Se alcanzó un límite de envíos (rate limit)",
        "attention": "Se alcanzó un rate limit. Conviene pausar unos minutos.",
        "label": "Rate limit",
        "suggestion": "Aumentá los delays o reducí la concurrencia para esta campaña.",
        "scope": "account",
    },
    {
        "keywords": ("checkpoint",),
        "code": "checkpoint",
        "detail": "Instagram requiere un checkpoint de seguridad",
        "attention": "Instagram requiere verificación adicional (checkpoint).",
        "label": "Checkpoint requerido",
        "suggestion": "Ingresá a la app oficial o al sitio para completar el checkpoint.",
        "scope": "account",
    },
    {
        "keywords": ("consent_required",),
        "code": "consent_required",
        "detail": "La sesión necesita aprobar el acceso desde la app oficial",
        "attention": "La sesión requiere aprobación en la app oficial.",
        "label": "Consentimiento pendiente",
        "suggestion": "Aceptá el inicio de sesión desde la app oficial y reintentá.",
        "scope": "account",
    },
    {
        "keywords": (
            "privacy",
            "private account",
            "not authorized to view",
            "user can't receive your message",
            "recipient can't receive your message",
            "recipients have opted out",
        ),
        "code": "recipient_restricted",
        "detail": "El destinatario tiene restricciones de privacidad para recibir mensajes",
        "label": "Privacidad del destinatario",
        "suggestion": "Saltá este lead: la cuenta objetivo no acepta mensajes.",
        "scope": "recipient",
    },
    {
        "keywords": (
            "inactive user",
            "user not found",
            "username does not exist",
            "unknown user",
        ),
        "code": "user_not_found",
        "detail": "El usuario objetivo no existe o no está disponible",
        "label": "Usuario no disponible",
        "suggestion": "Verificá el usuario en la lista de leads.",
        "scope": "recipient",
    },
    {
        "keywords": ("spam", "suspicious activity"),
        "code": "spam_block",
        "detail": "Instagram detectó actividad sospechosa y bloqueó el envío",
        "attention": "Instagram marcó la acción como sospechosa.",
        "label": "Actividad sospechosa",
        "suggestion": "Reducí el ritmo de envíos y revisá el warm-up de la cuenta.",
        "scope": "account",
    },
    {
        "keywords": (
            "socket",
            "timed out",
            "connection aborted",
            "connection reset",
            "connection error",
            "temporarily unavailable",
        ),
        "code": "network",
        "detail": "Error de red al contactar Instagram",
        "attention": "Se detectó un error de red. Revisá la conexión o el proxy.",
        "label": "Error de red",
        "suggestion": "Verificá la conexión o cambiá de proxy antes de reanudar.",
        "scope": "network",
    },
]


def _classify_exception(exc: Exception) -> tuple[str, str | None, str | None, str | None, str | None, str | None]:
    text = str(exc).strip()
    lowered = text.lower()
    name = exc.__class__.__name__.lower()
    for signature in _ERROR_SIGNATURES:
        if any(key in lowered or key in name for key in signature["keywords"]):
            detail = signature["detail"]
            if text and text.lower() not in detail.lower():
                detail = f"{detail} ({text})"
            attention = signature.get("attention") or _diagnose_exception(exc)
            label = signature.get("label")
            suggestion = signature.get("suggestion")
            scope = signature.get("scope")
            return detail, attention, signature.get("code"), label, suggestion, scope
    fallback_detail = text or "envío falló"
    return fallback_detail, _diagnose_exception(exc), None, text or "Error desconocido", None, None


def _render_progress(
    alias: str,
    leads_left: int,
    success_totals: Dict[str, int],
    failed_totals: Dict[str, int],
    live_table: LiveTable,
    send_state: Dict[str, object],
) -> None:
    banner()
    _refresh_daily_state(send_state)
    print(full_line())
    print(style_text(f"Alias: {alias}", color=Fore.CYAN, bold=True))
    print(style_text(f"Leads pendientes: {leads_left}", bold=True))
    print(full_line())
    print(style_text("Totales por cuenta (ésta campaña)", color=Fore.CYAN, bold=True))
    for username in sorted(set(success_totals) | set(failed_totals)):
        ok_run = success_totals.get(username, 0)
        fail_run = failed_totals.get(username, 0)
        print(f" @{username}: {ok_run} OK / {fail_run} errores")
    print(full_line())
    print(style_text("Envíos en vuelo", color=Fore.CYAN, bold=True))
    print(live_table.render())
    print(full_line())
    sent_today = int(send_state.get("sent", 0) or 0)
    err_today = int(send_state.get("errors", 0) or 0)
    ok_line = style_text(
        f"Mensajes enviados: {sent_today}", color=Fore.GREEN, bold=True
    )
    err_line = style_text(
        f"Mensajes con error: {err_today}", color=Fore.RED, bold=True
    )
    print(ok_line)
    print(err_line)
    print(full_line())


def _handle_event(
    event: SendEvent,
    success: Dict[str, int],
    failed: Dict[str, int],
    live_table: LiveTable,
    remaining: Dict[str, int],
    bump: Callable[[str, int], None],
    error_tracker: Dict[str, Dict[str, object]],
) -> Optional[str]:
    username = event.username
    if event.success:
        success[username] += 1
        detail = ""
        live_table.complete(username, True, detail)
        log_sent(username, event.lead, True, detail)
        with _LIVE_LOCK:
            _LIVE_COUNTS["run_ok"] += 1
        bump("sent", 1)
        summary = style_text(
            f"✅ @{username} → @{event.lead}", color=Fore.GREEN, bold=True
        )
        print(summary)
    else:
        failed[username] += 1
        detail = event.detail or "envío falló"
        live_table.complete(username, False, detail)
        log_sent(username, event.lead, False, detail)
        with _LIVE_LOCK:
            _LIVE_COUNTS["run_fail"] += 1
        bump("errors", 1)
        summary = style_text(
            f"❌ @{username} → @{event.lead} ({detail})", color=Fore.RED, bold=True
        )
        print(summary)
        reason_key = (event.reason_code or "").strip().lower()
        if not reason_key:
            reason_key = (event.reason_label or detail).strip().lower()
        label = event.reason_label or detail or "Error desconocido"
        tracker = None
        if reason_key:
            tracker = error_tracker.setdefault(
                reason_key,
                {
                    "count": 0,
                    "alerted": False,
                    "label": label,
                    "suggestion": event.suggestion,
                },
            )
            tracker["count"] = int(tracker.get("count", 0)) + 1
            if event.reason_label and tracker.get("label") != event.reason_label:
                tracker["label"] = event.reason_label
            if event.suggestion and not tracker.get("suggestion"):
                tracker["suggestion"] = event.suggestion
            if not tracker.get("alerted") and tracker["count"] >= 3:
                print(full_line(char="!", color=Fore.YELLOW, bold=True))
                print(
                    style_text(
                        f"Patrón detectado: {tracker['count']} errores con motivo '{tracker['label']}'.",
                        color=Fore.YELLOW,
                        bold=True,
                    )
                )
                suggestion = tracker.get("suggestion")
                if suggestion:
                    print(style_text(f"Sugerencia: {suggestion}", color=Fore.YELLOW))
                else:
                    print(
                        style_text(
                            "Sugerencia: pausá la campaña o ajustá delays/concurrencia antes de continuar.",
                            color=Fore.YELLOW,
                        )
                    )
                print(full_line(char="!", color=Fore.YELLOW, bold=True))
                tracker["alerted"] = True
    if event.attention:
        print(full_line(char="=", color=Fore.RED, bold=True))
        print(highlight(f"Atención en @{username}", color=Fore.RED))
        print(event.attention)
        print(full_line(char="=", color=Fore.RED, bold=True))
        print("[1] Continuar sin esta cuenta")
        print("[2] Pausar todo")
        choice = ask("Opción: ").strip() or "1"
        if choice == "1":
            remaining[username] = 0
            warn(f"Se omitirá @{username} en esta campaña.")
            return "continue"
        else:
            request_stop(f"usuario decidió pausar tras incidente con @{username}")
            return "stop"
    return None


def _build_accounts_for_alias(alias: str) -> list[Dict]:
    all_acc = [a for a in list_all() if a.get("alias") == alias and a.get("active")]
    if not all_acc:
        warn("No hay cuentas activas en ese alias.")
        press_enter()
        return []

    verified: list[Dict] = []
    needing_login: list[tuple[Dict, str]] = []
    for account in all_acc:
        username = account["username"]
        if not has_session(username):
            needing_login.append((account, "sin sesión guardada"))
            continue
        if not _ensure_session(username):
            needing_login.append((account, "sesión expirada"))
            continue
        verified.append(account)

    if needing_login:
        remaining: list[tuple[Dict, str]] = []
        for account, reason in needing_login:
            username = account["username"]
            if auto_login_with_saved_password(username, account=account) and _ensure_session(username):
                refreshed = get_account(username) or account
                if refreshed not in verified:
                    verified.append(refreshed)
            else:
                remaining.append((account, reason))

        if remaining:
            print("\nLas siguientes cuentas necesitan volver a iniciar sesión:")
            for account, reason in remaining:
                print(f" - @{account['username']}: {reason}")
            if ask("¿Iniciar sesión ahora? (s/N): ").strip().lower() == "s":
                for account, _ in remaining:
                    username = account["username"]
                    if auto_login_with_saved_password(username, account=account) and _ensure_session(username):
                        refreshed = get_account(username) or account
                        if refreshed not in verified:
                            verified.append(refreshed)
                        continue
                    if prompt_login(username, interactive=False) and _ensure_session(username):
                        refreshed = get_account(username) or account
                        if refreshed not in verified:
                            verified.append(refreshed)
            else:
                warn("Se omitieron las cuentas sin sesión válida.")

    if not verified:
        warn("No hay cuentas con sesión válida para enviar mensajes.")
        press_enter()
        return []

    verified.sort(key=lambda acct: (acct.get("low_profile", False), acct.get("username", "")))
    low_profile_accounts = [acct for acct in verified if acct.get("low_profile")]
    if low_profile_accounts:
        warn(
            "Se detectaron cuentas en modo bajo perfil. Se aplicarán límites conservadores automáticamente."
        )
        for acct in low_profile_accounts:
            reason = acct.get("low_profile_reason") or "motivo no especificado"
            print(f" - @{acct['username']}: {reason}")
        print()

    return verified


def _schedule_inputs(
    settings, concurrency_override: Optional[int]
) -> Optional[tuple[int, int, int, int, list[str]]]:
    alias = ask("Alias/grupo: ").strip() or "default"
    listname = ask("Nombre de la lista (text/leads/<nombre>.txt): ").strip()

    per_acc_default = max(1, settings.max_per_account)
    per_acc_input = ask_int(
        f"¿Cuántos mensajes por cuenta? [{per_acc_default}]: ",
        1,
        default=per_acc_default,
    )
    if per_acc_input < 1:
        warn("La cantidad mínima por cuenta es 1. Se ajusta a 1.")
    per_acc = max(1, per_acc_input)

    if concurrency_override is not None:
        concurr_input = max(1, concurrency_override)
        print(f"Concurrencia forzada: {concurr_input}")
    else:
        concurr_input = ask_int(
            f"Cuentas en simultáneo? [{settings.max_concurrency}]: ",
            1,
            default=settings.max_concurrency,
        )
    if concurr_input < 1:
        warn("La concurrencia mínima es 1. Se ajusta a 1.")
    concurr = max(1, concurr_input)

    dmin_default = max(10, settings.delay_min)
    dmin_input = ask_int(
        f"Delay mínimo (seg) [{dmin_default}]: ",
        1,
        default=dmin_default,
    )
    if dmin_input < 10:
        warn("El delay mínimo recomendado es 10s. Se ajusta automáticamente.")
    delay_min = max(10, dmin_input)

    dmax_default = max(delay_min, settings.delay_max)
    dmax_input = ask_int(
        f"Delay máximo (seg) [>= {delay_min}, por defecto {dmax_default}]: ",
        delay_min,
        default=dmax_default,
    )
    if dmax_input < delay_min:
        warn("Delay máximo ajustado al mínimo indicado.")
    delay_max = max(delay_min, dmax_input)

    print("Escribí plantillas (una por línea). Línea vacía para terminar:")
    templates: list[str] = []
    while True:
        s = ask("")
        if not s:
            break
        templates.append(s)
    if not templates:
        templates = ["hola!"]

    return alias, listname, per_acc, concurr, delay_min, delay_max, templates


def menu_send_rotating(concurrency_override: Optional[int] = None) -> None:
    ensure_logging(
        quiet=SETTINGS.quiet,
        log_dir=SETTINGS.log_dir,
        log_file=SETTINGS.log_file,
    )
    reset_stop_event()
    banner()
    _reset_live_counters()
    settings = SETTINGS

    send_state: Dict[str, object] = create_daily_send_state()

    def bump(kind: str, delta: int = 1) -> None:
        if kind not in {"sent", "errors"}:
            return
        try:
            _refresh_daily_state(send_state)
            current = int(send_state.get(kind, 0) or 0)
            send_state[kind] = current + delta
        except Exception:
            try:
                send_state[kind] = int(send_state.get(kind, 0) or 0) + delta
            except Exception:
                pass

    inputs = _schedule_inputs(settings, concurrency_override)
    if inputs is None:
        return
    (
        alias,
        listname,
        per_acc,
        concurr,
        delay_min,
        delay_max,
        templates,
    ) = inputs

    accounts = _build_accounts_for_alias(alias)
    if not accounts:
        return

    def _account_cap(record: Dict) -> int:
        limit = per_acc
        if record.get("low_profile"):
            limit = min(limit, SETTINGS.low_profile_daily_cap or limit)
        return max(1, limit)

    def _account_delay_range(record: Dict) -> tuple[int, int]:
        if not record.get("low_profile"):
            return delay_min, delay_max
        factor = max(100, getattr(SETTINGS, "low_profile_delay_factor", 150))
        multiplier = max(1.0, factor / 100.0)
        scaled_min = max(delay_min, int(math.ceil(delay_min * multiplier)))
        scaled_max = max(scaled_min, int(math.ceil(delay_max * multiplier)))
        return scaled_min, scaled_max

    account_caps = {a["username"]: _account_cap(a) for a in accounts}
    account_delays = {a["username"]: _account_delay_range(a) for a in accounts}

    if any(a.get("low_profile") for a in accounts):
        delay_multiplier = max(1.0, getattr(SETTINGS, "low_profile_delay_factor", 150) / 100.0)
        logger.info(
            "Modo bajo perfil aplicado: límite %d mensajes/cuenta y delay x%.2f.",
            SETTINGS.low_profile_daily_cap,
            delay_multiplier,
        )

    users = deque([u for u in load_list(listname) if not already_contacted(u)])
    if not users:
        warn("No hay leads (o todos ya fueron contactados).")
        press_enter()
        return

    remaining = {a["username"]: account_caps[a["username"]] for a in accounts}
    success = defaultdict(int)
    failed = defaultdict(int)
    semaphore = threading.Semaphore(concurr)
    account_locks = {a["username"]: threading.Lock() for a in accounts}
    result_queue: queue.Queue[SendEvent] = queue.Queue()
    live_table = LiveTable(max_entries=concurr)
    error_tracker: Dict[str, Dict[str, object]] = {}

    listener = start_q_listener("Presioná Q para detener la campaña.", logger)
    threads: list[threading.Thread] = []

    logger.info(
        "Iniciando campaña con %d cuentas activas y %d leads pendientes. Límite/cuenta: %d, concurrencia: %d, delay: %s-%ss",
        len(accounts),
        len(users),
        per_acc,
        concurr,
        delay_min,
        delay_max,
    )

    def _attempt_send(account: Dict, lead: str, message: str) -> tuple[SendEvent, int]:
        username = account["username"]
        attention_message: str | None = None
        detail = ""
        success_flag = False
        reason_code: str | None = None
        reason_label: str | None = None
        suggestion: str | None = None
        scope: str | None = None
        max_retries = 3
        retries = 0
        while not STOP_EVENT.is_set():
            try:
                cl = _client_for(username)
                success_flag = _send_dm(cl, lead, message)
                if not success_flag:
                    detail = "Instagram no confirmó el envío"
                    reason_code = reason_code or "send_failed"
                    reason_label = reason_label or "Envío no confirmado"
                    suggestion = suggestion or "Revisá si la cuenta puede enviar mensajes manualmente."
                    scope = scope or "account"
                break
            except Exception as exc:  # pragma: no cover - external SDK
                if should_retry_proxy(exc):
                    retries += 1
                    record_proxy_failure(username, exc)
                    wait_retry = min(30, 5 * retries)
                    logger.warning(
                        "Proxy error con @%s → @%s (intento %d/%d): %s",
                        username,
                        lead,
                        retries,
                        max_retries,
                        exc,
                        exc_info=False,
                    )
                    if retries >= max_retries:
                        detail = "proxy sin respuesta"
                        attention_message = (
                            "El proxy configurado para @"
                            f"{username} falló repetidamente. Revisá la opción 1 para actualizarlo o quitarlo."
                        )
                        reason_code = "proxy_unavailable"
                        reason_label = "Proxy sin respuesta"
                        suggestion = (
                            "Actualizá o quitá el proxy configurado antes de continuar con esta cuenta."
                        )
                        scope = "account"
                        break
                    sleep_with_stop(wait_retry)
                    continue
                detail, diag_attention, code, label, suggestion_hint, scope_hint = _classify_exception(exc)
                if code and not reason_code:
                    reason_code = code
                if label and not reason_label:
                    reason_label = label
                if suggestion_hint and not suggestion:
                    suggestion = suggestion_hint
                if diag_attention:
                    attention_message = diag_attention
                scope = scope_hint or scope
                logger.warning(
                    "Fallo inesperado con @%s → @%s: %s",
                    username,
                    lead,
                    exc,
                    exc_info=False,
                )
                break

        if STOP_EVENT.is_set() and not success_flag and not detail:
            detail = "envío cancelado"
        if not success_flag and not detail:
            detail = "envío falló"
        if reason_label is None and reason_code:
            reason_label = reason_code.replace("_", " ").capitalize()
        event = SendEvent(
            username=username,
            lead=lead,
            success=success_flag,
            detail=detail,
            attention=attention_message,
            reason_code=reason_code,
            reason_label=reason_label,
            suggestion=suggestion,
            scope=scope,
        )
        wait_min, wait_max = account_delays.get(username, (delay_min, delay_max))
        wait_time = jitter_delay(wait_min, wait_max)
        if account.get("low_profile"):
            logger.debug(
                "Modo bajo perfil para @%s: espera %ss (rango %s-%ss)",
                username,
                wait_time,
                wait_min,
                wait_max,
            )
        else:
            logger.debug(
                "Esperando %ss antes del próximo envío de @%s",
                wait_time,
                username,
            )
        return event, wait_time

    def _worker(account: Dict, lead: str, message: str, account_lock: threading.Lock) -> None:
        wait_time = 0
        try:
            if STOP_EVENT.is_set():
                return
            event, wait_time = _attempt_send(account, lead, message)
            result_queue.put(event)
        finally:
            if wait_time > 0:
                sleep_with_stop(wait_time)
            account_lock.release()
            semaphore.release()

    if not STOP_EVENT.is_set():
        print(
            style_text(
                "Ejecutando prueba previa de envío por cuenta...",
                color=Fore.CYAN,
                bold=True,
            )
        )
        tested_accounts = 0
        for account in accounts:
            if STOP_EVENT.is_set():
                break
            username = account["username"]
            if remaining[username] <= 0:
                continue
            if not users:
                warn("No quedan leads suficientes para completar la prueba previa.")
                break
            lead = users.popleft()
            message = random.choice(templates)
            remaining[username] -= 1
            live_table.begin(username, lead)
            event, wait_time = _attempt_send(account, lead, message)
            if wait_time > 0:
                sleep_with_stop(wait_time)
            action = _handle_event(
                event,
                success,
                failed,
                live_table,
                remaining,
                bump,
                error_tracker,
            )
            _render_progress(
                alias,
                len(users),
                success,
                failed,
                live_table,
                send_state,
            )
            tested_accounts += 1
            if not event.success:
                if event.scope == "account":
                    remaining[username] = 0
                    warn(
                        f"@{username} se omitirá tras fallar la prueba previa ({event.detail})."
                    )
                    logger.warning(
                        "Cuenta omitida tras prueba previa: @%s (%s)",
                        username,
                        event.detail,
                    )
                else:
                    remaining[username] += 1
            if action == "stop" or STOP_EVENT.is_set():
                break
        if tested_accounts:
            logger.info("Prueba previa completada para %d cuentas.", tested_accounts)

    try:
        last_render = 0.0
        while users and any(v > 0 for v in remaining.values()) and not STOP_EVENT.is_set():
            _refresh_daily_state(send_state)
            need_render = False
            # procesar resultados pendientes
            try:
                while True:
                    event = result_queue.get_nowait()
                    action = _handle_event(
                        event,
                        success,
                        failed,
                        live_table,
                        remaining,
                        bump,
                        error_tracker,
                    )
                    need_render = True
                    if action == "stop":
                        break
            except queue.Empty:
                pass

            if STOP_EVENT.is_set():
                break

            for account in accounts:
                if STOP_EVENT.is_set():
                    break
                username = account["username"]
                if remaining[username] <= 0:
                    continue
                if not users:
                    break
                account_lock = account_locks[username]
                if not account_lock.acquire(blocking=False):
                    continue

                acquired = semaphore.acquire(timeout=0.1)
                if not acquired:
                    account_lock.release()
                    continue

                lead = users.popleft()
                message = random.choice(templates)
                remaining[username] -= 1
                live_table.begin(username, lead)
                thread = threading.Thread(
                    target=_worker,
                    args=(account, lead, message, account_lock),
                    daemon=True,
                )
                thread.start()
                threads.append(thread)

                if STOP_EVENT.is_set():
                    break

            now = time.time()
            if need_render or now - last_render > 0.5:
                _render_progress(
                    alias,
                    len(users),
                    success,
                    failed,
                    live_table,
                    send_state,
                )
                last_render = now
            time.sleep(0.1)

        # drenar eventos restantes
        while True:
            try:
                event = result_queue.get(timeout=0.5)
                _handle_event(
                    event,
                    success,
                    failed,
                    live_table,
                    remaining,
                    bump,
                    error_tracker,
                )
                _render_progress(
                    alias,
                    len(users),
                    success,
                    failed,
                    live_table,
                    send_state,
                )
            except queue.Empty:
                break

    except KeyboardInterrupt:
        request_stop("interrupción con Ctrl+C")
    finally:
        if not users:
            request_stop("no quedan leads por procesar")
        elif not any(v > 0 for v in remaining.values()):
            request_stop("se alcanzó el límite de envíos por cuenta")

        for t in threads:
            t.join()
        if listener:
            listener.join(timeout=0.1)

        _reset_live_counters()
        _render_progress(
            alias,
            len(users),
            success,
            failed,
            live_table,
            send_state,
        )

    print("\n== Resumen ==")
    total_ok = sum(success.values())
    print(f"OK: {total_ok}")
    for account in accounts:
        user = account["username"]
        print(f" - {user}: {success[user]} enviados, {failed[user]} errores")
    if STOP_EVENT.is_set():
        logger.info("Proceso detenido (%s).", "stop_event activo")
    press_enter()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Enviar mensajes rotando cuentas")
    parser.add_argument(
        "--concurrency",
        type=int,
        help="Cantidad de cuentas enviando en simultáneo",
    )
    args = parser.parse_args()
    menu_send_rotating(concurrency_override=args.concurrency)
