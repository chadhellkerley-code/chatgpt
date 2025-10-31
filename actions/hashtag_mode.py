"""Exploración automática por hashtag."""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable

from accounts import (
    auto_login_with_saved_password,
    get_account,
    has_valid_session_settings,
    mark_connected,
    prompt_login,
)
from config import SETTINGS
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
from ui import Fore, banner, full_line, style_text
from utils import ask, ask_int, ok, press_enter, warn

logger = logging.getLogger(__name__)


@dataclass
class AccountSummary:
    username: str
    likes: int = 0
    follows: int = 0
    errors: int = 0


def _client_for(username: str):
    from instagrapi import Client

    account = get_account(username)
    cl = Client()
    binding = None
    try:
        binding = apply_proxy_to_client(cl, username, account, reason="hashtag")
    except Exception as exc:
        if account and account.get("proxy_url"):
            record_proxy_failure(username, exc)
            raise RuntimeError(f"El proxy configurado para @{username} falló: {exc}") from exc
        logger.warning("Proxy no disponible para @%s: %s", username, exc, exc_info=False)

    try:
        load_into(cl, username)
    except FileNotFoundError as exc:
        mark_connected(username, False)
        raise RuntimeError(f"No hay sesión guardada para @{username}. Usá la opción 1.") from exc
    except Exception as exc:
        if binding and should_retry_proxy(exc):
            record_proxy_failure(username, exc)
        mark_connected(username, False)
        raise

    if not has_valid_session_settings(cl):
        mark_connected(username, False)
        raise RuntimeError(
            f"La sesión guardada para @{username} no contiene credenciales activas. Iniciá sesión nuevamente."
        )

    mark_connected(username, True)
    return cl


def _ensure_account_ready(username: str) -> bool:
    if not has_session(username):
        warn(f"@{username} no tiene sesión guardada.")
        if auto_login_with_saved_password(username) and has_session(username):
            return _ensure_account_ready(username)
        if ask("¿Iniciar sesión ahora? (s/N): ").strip().lower() == "s":
            if auto_login_with_saved_password(username) and has_session(username):
                return _ensure_account_ready(username)
            if prompt_login(username, interactive=False):
                return _ensure_account_ready(username)
        return False
    try:
        _client_for(username)
        return True
    except Exception as exc:
        warn(str(exc))
        if auto_login_with_saved_password(username) and has_session(username):
            return _ensure_account_ready(username)
        if ask("¿Reintentar login ahora? (s/N): ").strip().lower() == "s":
            if auto_login_with_saved_password(username) and has_session(username):
                return _ensure_account_ready(username)
            if prompt_login(username, interactive=False):
                return _ensure_account_ready(username)
        return False


def _prompt_parameters() -> tuple[str, int, int, int]:
    banner()
    print(style_text("🧭 Exploración automática por hashtag", color=Fore.CYAN, bold=True))
    print(full_line())
    hashtag = ask("Hashtag (sin #): ").strip()
    hashtag = hashtag.lstrip("#").strip()
    if not hashtag:
        warn("Debés indicar un hashtag.")
        press_enter()
        return "", 0, 0, 0
    likes = ask_int("Cantidad de likes por sesión: ", min_value=0, default=0)
    follows = ask_int("Cantidad de follows por sesión: ", min_value=0, default=0)
    delay = ask_int("Delay entre acciones (segundos, mínimo 10): ", min_value=10, default=SETTINGS.delay_min)
    if likes <= 0 and follows <= 0:
        warn("Definí al menos una acción (likes o follows).")
        press_enter()
        return "", 0, 0, 0
    return hashtag, likes, follows, delay


def _iter_medias(client, hashtag: str, amount: int):
    try:
        medias = client.hashtag_medias_recent(hashtag, amount=amount)
    except Exception as exc:
        logger.warning("No se pudo obtener el hashtag #%s: %s", hashtag, exc, exc_info=False)
        return []
    return medias or []


def _run_actions_for_account(
    username: str,
    hashtag: str,
    like_limit: int,
    follow_limit: int,
    delay: int,
) -> AccountSummary:
    summary = AccountSummary(username=username)
    try:
        client = _client_for(username)
    except Exception as exc:
        summary.errors += 1
        logger.error("No se pudo preparar @%s: %s", username, exc, exc_info=False)
        return summary

    targets = _iter_medias(client, hashtag, amount=max(like_limit, follow_limit) * 4 or 20)
    seen_users: set[int] = set()

    for media in targets:
        if STOP_EVENT.is_set():
            break
        try:
            user = media.user
            user_id = int(user.pk if hasattr(user, "pk") else user.id)
            username_to_follow = getattr(user, "username", "")
        except Exception:
            user_id = 0
            username_to_follow = ""

        if like_limit > 0 and summary.likes < like_limit:
            try:
                client.media_like(media.id)
                summary.likes += 1
                logger.info("@%s → like en %s", username, media.id)
                print(style_text(
                    f"@{username} ❤️ like {summary.likes}/{like_limit}",
                    color=Fore.GREEN,
                    bold=True,
                ))
            except Exception as exc:
                summary.errors += 1
                logger.warning("Like falló para @%s: %s", username, exc, exc_info=False)
            sleep_with_stop(delay)
            if STOP_EVENT.is_set():
                break

        if follow_limit > 0 and summary.follows < follow_limit and user_id and user_id not in seen_users:
            try:
                client.user_follow(user_id)
                summary.follows += 1
                seen_users.add(user_id)
                logger.info("@%s → follow a %s", username, username_to_follow)
                print(style_text(
                    f"@{username} ➕ follow {summary.follows}/{follow_limit}",
                    color=Fore.CYAN,
                    bold=True,
                ))
            except Exception as exc:
                summary.errors += 1
                logger.warning("Follow falló para @%s: %s", username, exc, exc_info=False)
            sleep_with_stop(delay)
        if summary.likes >= like_limit and summary.follows >= follow_limit:
            break

    return summary


def run_from_menu(usernames: Iterable[str]) -> None:
    selected = [u.lstrip("@") for u in usernames]
    ready_accounts = [username for username in selected if _ensure_account_ready(username)]
    if not ready_accounts:
        warn("Ninguna cuenta tiene sesión válida para ejecutar el modo hashtag.")
        press_enter()
        return

    hashtag, likes, follows, delay = _prompt_parameters()
    if not hashtag:
        return

    ensure_logging(
        quiet=SETTINGS.quiet,
        log_dir=SETTINGS.log_dir,
        log_file=SETTINGS.log_file,
    )
    reset_stop_event()

    listener = start_q_listener("Presioná Q para detener la exploración por hashtag.", logger)
    start_time = time.perf_counter()
    totals = defaultdict(int)

    try:
        for username in ready_accounts:
            if STOP_EVENT.is_set():
                break
            summary = _run_actions_for_account(username, hashtag, likes, follows, delay)
            totals["likes"] += summary.likes
            totals["follows"] += summary.follows
            totals["errors"] += summary.errors
    finally:
        request_stop("exploración finalizada")
        listener.join(timeout=0.2)

    elapsed = time.perf_counter() - start_time
    print(full_line(color=Fore.MAGENTA))
    print(style_text("=== EXPLORACIÓN FINALIZADA ===", color=Fore.YELLOW, bold=True))
    print(style_text(f"Cuentas usadas: {len(ready_accounts)}", color=Fore.CYAN, bold=True))
    print(style_text(f"Likes realizados: {totals['likes']}", color=Fore.GREEN, bold=True))
    print(style_text(f"Follows realizados: {totals['follows']}", color=Fore.GREEN, bold=True))
    print(style_text(f"Errores registrados: {totals['errors']}", color=Fore.RED if totals["errors"] else Fore.GREEN, bold=True))
    print(
        style_text(
            f"Tiempo total: {int(elapsed // 60):02d}:{int(elapsed % 60):02d}",
            color=Fore.WHITE,
            bold=True,
        )
    )
    print(full_line(color=Fore.MAGENTA))
    ok("Exploración completada.")
    press_enter()
