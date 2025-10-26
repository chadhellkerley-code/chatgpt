# ig.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
import queue
import random
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Dict, Optional

from accounts import get_account, list_all, mark_connected, prompt_login
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


_LIVE_COUNTS = {"base_ok": 0, "base_fail": 0, "run_ok": 0, "run_fail": 0}
_LIVE_LOCK = threading.Lock()


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


def _render_progress(
    alias: str,
    leads_left: int,
    success_totals: Dict[str, int],
    failed_totals: Dict[str, int],
    live_table: LiveTable,
) -> None:
    banner()
    ok_total, err_total = get_message_totals()
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
    ok_line = style_text(f"Mensajes enviados: {ok_total}", color=Fore.GREEN, bold=True)
    err_line = style_text(f"Mensajes con error: {err_total}", color=Fore.RED, bold=True)
    print(ok_line)
    print(err_line)
    print(full_line())


def _handle_event(
    event: SendEvent,
    success: Dict[str, int],
    failed: Dict[str, int],
    live_table: LiveTable,
    remaining: Dict[str, int],
) -> Optional[str]:
    username = event.username
    if event.success:
        success[username] += 1
        detail = ""
        live_table.complete(username, True, detail)
        log_sent(username, event.lead, True, detail)
        with _LIVE_LOCK:
            _LIVE_COUNTS["run_ok"] += 1
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
        summary = style_text(
            f"❌ @{username} → @{event.lead} ({detail})", color=Fore.RED, bold=True
        )
        print(summary)
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
        print("\nLas siguientes cuentas necesitan volver a iniciar sesión:")
        for account, reason in needing_login:
            print(f" - @{account['username']}: {reason}")
        if ask("¿Iniciar sesión ahora? (s/N): ").strip().lower() == "s":
            for account, _ in needing_login:
                if prompt_login(account["username"]) and _ensure_session(account["username"]):
                    if account not in verified:
                        verified.append(account)
        else:
            warn("Se omitieron las cuentas sin sesión válida.")

    if not verified:
        warn("No hay cuentas con sesión válida para enviar mensajes.")
        press_enter()
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

    users = deque([u for u in load_list(listname) if not already_contacted(u)])
    if not users:
        warn("No hay leads (o todos ya fueron contactados).")
        press_enter()
        return

    remaining = {a["username"]: per_acc for a in accounts}
    success = defaultdict(int)
    failed = defaultdict(int)
    semaphore = threading.Semaphore(concurr)
    account_locks = {a["username"]: threading.Lock() for a in accounts}
    result_queue: queue.Queue[SendEvent] = queue.Queue()
    live_table = LiveTable(max_entries=concurr)

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

    def _worker(account: Dict, lead: str, message: str, account_lock: threading.Lock) -> None:
        username = account["username"]
        attention_message: str | None = None
        detail = ""
        success_flag = False
        max_retries = 3
        retries = 0
        try:
            if STOP_EVENT.is_set():
                return
            while not STOP_EVENT.is_set():
                try:
                    cl = _client_for(username)
                    success_flag = _send_dm(cl, lead, message)
                    if not success_flag:
                        detail = "envío falló"
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
                            break
                        sleep_with_stop(wait_retry)
                        continue
                    detail = str(exc)
                    attention_message = _diagnose_exception(exc)
                    logger.warning(
                        "Fallo inesperado con @%s → @%s: %s",
                        username,
                        lead,
                        exc,
                        exc_info=False,
                    )
                    break
            if not success_flag and not detail:
                detail = "envío falló"
        finally:
            result_queue.put(
                SendEvent(
                    username=username,
                    lead=lead,
                    success=success_flag,
                    detail=detail,
                    attention=attention_message,
                )
            )
            wait_time = jitter_delay(delay_min, delay_max)
            logger.debug(
                "Esperando %ss antes del próximo envío de @%s", wait_time, username
            )
            if wait_time > 0:
                sleep_with_stop(wait_time)
            account_lock.release()
            semaphore.release()

    try:
        last_render = 0.0
        while users and any(v > 0 for v in remaining.values()) and not STOP_EVENT.is_set():
            need_render = False
            # procesar resultados pendientes
            try:
                while True:
                    event = result_queue.get_nowait()
                    action = _handle_event(event, success, failed, live_table, remaining)
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
                _render_progress(alias, len(users), success, failed, live_table)
                last_render = now
            time.sleep(0.1)

        # drenar eventos restantes
        while True:
            try:
                event = result_queue.get(timeout=0.5)
                _handle_event(event, success, failed, live_table, remaining)
                _render_progress(alias, len(users), success, failed, live_table)
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
        _render_progress(alias, len(users), success, failed, live_table)

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
