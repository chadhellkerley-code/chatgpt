"""Interacciones automatizadas: comentarios y vistas/likes de reels."""

from __future__ import annotations

import csv
import logging
import random
import time
from dataclasses import dataclass, field
from pathlib import Path
from queue import Empty, Queue
from typing import List, Sequence

from accounts import get_account, list_all, mark_connected, prompt_login
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
from utils import ask, ask_int, ask_multiline, ok, press_enter, warn

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "storage" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
INTERACTIONS_LOG = DATA_DIR / "interactions_log.csv"


@dataclass
class InteractionSummary:
    username: str
    performed: int = 0
    errors: int = 0
    messages: List[str] = field(default_factory=list)


def _client_for(username: str):
    from instagrapi import Client

    account = get_account(username)
    cl = Client()
    binding = None
    try:
        binding = apply_proxy_to_client(cl, username, account, reason="interactions")
    except Exception as exc:
        if account and account.get("proxy_url"):
            record_proxy_failure(username, exc)
            raise RuntimeError(f"El proxy configurado para @{username} falló: {exc}") from exc
        logger.warning("Proxy no disponible para @%s: %s", username, exc, exc_info=False)

    try:
        load_into(cl, username)
    except FileNotFoundError as exc:
        mark_connected(username, False)
        raise RuntimeError(
            f"No hay sesión guardada para {username}. Iniciá sesión desde el menú."  # noqa: B950
        ) from exc
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


def _ensure_account_ready(username: str) -> bool:
    if not has_session(username):
        warn(f"@{username} no tiene sesión guardada.")
        if ask("¿Iniciar sesión ahora? (s/N): ").strip().lower() == "s":
            if prompt_login(username):
                return _ensure_account_ready(username)
        return False
    try:
        _client_for(username)
        return True
    except Exception as exc:
        warn(str(exc))
        if ask("¿Reintentar login ahora? (s/N): ").strip().lower() == "s":
            if prompt_login(username):
                return _ensure_account_ready(username)
        return False


def _select_accounts(alias: str) -> List[str]:
    accounts = [acct for acct in list_all() if acct.get("alias") == alias]
    active_accounts = [acct for acct in accounts if acct.get("active")]
    if not active_accounts:
        warn("No hay cuentas activas en este alias.")
        press_enter()
        return []

    print("Seleccioná cuentas activas (coma separada, * para todas):")
    for idx, acct in enumerate(active_accounts, start=1):
        sess = "[sesión]" if has_session(acct["username"]) else "[sin sesión]"
        proxy_flag = " [proxy]" if acct.get("proxy_url") else ""
        print(f" {idx}) @{acct['username']} {sess}{proxy_flag}")

    raw = ask("Selección: ").strip() or "*"
    if raw == "*":
        chosen = [acct["username"] for acct in active_accounts]
    else:
        selected: set[str] = set()
        for part in raw.split(","):
            part = part.strip()
            if not part:
                continue
            if part.isdigit():
                idx = int(part)
                if 1 <= idx <= len(active_accounts):
                    selected.add(active_accounts[idx - 1]["username"])
            else:
                selected.add(part.lstrip("@"))
        chosen = [acct["username"] for acct in active_accounts if acct["username"] in selected]

    if not chosen:
        warn("No se encontraron cuentas con esos datos.")
        press_enter()
    return chosen


def _append_interaction_log(
    alias: str,
    username: str,
    action: str,
    target: str,
    success: bool,
    detail: str,
) -> None:
    new_file = not INTERACTIONS_LOG.exists()
    with INTERACTIONS_LOG.open("a", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        if new_file:
            writer.writerow(
                ["fecha_hora", "alias", "cuenta", "accion", "objetivo", "resultado", "detalle"]
            )
        writer.writerow(
            [
                time.strftime("%Y-%m-%d %H:%M:%S"),
                alias,
                username,
                action,
                target,
                "ok" if success else "error",
                detail,
            ]
        )


def _expand_spintax(text: str) -> str:
    result = text
    while "{" in result and "}" in result:
        start = result.find("{")
        end = result.find("}", start)
        if end == -1:
            break
        block = result[start + 1 : end]
        options = [opt.strip() for opt in block.split("|") if opt.strip()]
        if not options:
            break
        chosen = random.choice(options)
        result = result[:start] + chosen + result[end + 1 :]
    return result


def _prepare_comments() -> List[str]:
    templates = ask_multiline(
        "Texto del comentario (una plantilla por línea, spintax opcional)."
    )
    lines = [line.strip() for line in templates.splitlines() if line.strip()]
    if not lines:
        warn("Debés ingresar al menos un comentario.")
    return lines


def _targets_from_hashtag(client, hashtag: str, limit: int, kind: str):
    hashtag = hashtag.lstrip("#").strip()
    if not hashtag:
        return []
    try:
        if kind == "story":
            stories = client.hashtag_stories(hashtag) or []
            return list(stories)[:limit]
        medias = client.hashtag_medias_recent(hashtag, amount=max(limit * 2, 10)) or []
        if kind == "reel":
            filtered = []
            for media in medias:
                product_type = getattr(media, "product_type", "").lower()
                if "clip" in product_type or product_type == "igtv":
                    filtered.append(media)
            medias = filtered
        return medias[:limit]
    except Exception as exc:
        logger.warning("No se pudo obtener el hashtag #%s: %s", hashtag, exc, exc_info=False)
        return []


def _targets_from_urls(client, entries: Sequence[str], kind: str):
    results = []
    for entry in entries:
        value = entry.strip()
        if not value:
            continue
        try:
            if value.lower().endswith(".csv") and Path(value).expanduser().exists():
                with Path(value).expanduser().open("r", encoding="utf-8") as fh:
                    for line in fh:
                        val = line.strip()
                        if val:
                            results.extend(_targets_from_urls(client, [val], kind))
                continue
            if value.startswith("http"):
                if kind == "story":
                    pk = client.story_pk_from_url(value)
                    if pk:
                        results.append(pk)
                else:
                    pk = client.media_pk_from_url(value)
                    if pk:
                        results.append(pk)
            else:
                user = value.lstrip("@")
                user_id = client.user_id_from_username(user)
                if kind == "story":
                    stories = client.user_stories(user_id) or []
                    results.extend(stories)
                else:
                    medias = client.user_medias(user_id, amount=12) or []
                    results.extend(medias)
        except Exception as exc:
            logger.warning("No se pudo resolver %s: %s", value, exc, exc_info=False)
    return results


def _comment_on_targets(alias: str, username: str, client, targets, templates, delay_range, limit, kind: str) -> InteractionSummary:
    summary = InteractionSummary(username=username)
    for idx, target in enumerate(targets, start=1):
        if STOP_EVENT.is_set():
            break
        if limit and summary.performed >= limit:
            break
        template = random.choice(templates)
        comment = _expand_spintax(template)
        try:
            if kind == "story":
                client.story_comment(target.pk if hasattr(target, "pk") else target, comment)
                target_display = getattr(target, "pk", str(target))
            else:
                pk = target.pk if hasattr(target, "pk") else target
                client.media_comment(pk, comment)
                target_display = getattr(target, "code", pk)
            summary.performed += 1
            _append_interaction_log(alias, username, f"comentario_{kind}", target_display, True, comment[:80])
            logger.info("@%s comentó %s", username, target_display)
        except Exception as exc:
            summary.errors += 1
            detail = f"Error comentando: {exc}"
            summary.messages.append(detail)
            _append_interaction_log(alias, username, f"comentario_{kind}", str(target), False, detail)
            if should_retry_proxy(exc):
                record_proxy_failure(username, exc)
        delay = random.randint(delay_range[0], delay_range[1]) if delay_range[1] else delay_range[0]
        if delay:
            sleep_with_stop(delay)
        if STOP_EVENT.is_set():
            break
    return summary


def _fetch_reels(client, source: str, hashtag: str, amount: int):
    try:
        if source == "hashtag":
            reels = _targets_from_hashtag(client, hashtag, amount, "reel")
        else:
            reels = client.discover_reels() or []
        return list(reels)[:amount]
    except Exception as exc:
        logger.warning("No se pudieron obtener reels (%s): %s", source, exc, exc_info=False)
        return []


def _view_like_reels(alias: str, username: str, client, reels, like: bool, delay_range, view_range) -> InteractionSummary:
    summary = InteractionSummary(username=username)
    for reel in reels:
        if STOP_EVENT.is_set():
            break
        pk = reel.pk if hasattr(reel, "pk") else reel
        try:
            view_time = random.randint(view_range[0], view_range[1]) if view_range[1] else view_range[0]
            if view_time:
                sleep_with_stop(view_time)
            if like:
                client.media_like(pk)
            summary.performed += 1
            action = "ver_like_reel" if like else "ver_reel"
            _append_interaction_log(alias, username, action, getattr(reel, "code", pk), True, f"view={view_time}")
        except Exception as exc:
            summary.errors += 1
            detail = f"Error en reel {pk}: {exc}"
            summary.messages.append(detail)
            _append_interaction_log(alias, username, "ver_reel", str(pk), False, detail)
            if should_retry_proxy(exc):
                record_proxy_failure(username, exc)
        delay = random.randint(delay_range[0], delay_range[1]) if delay_range[1] else delay_range[0]
        if delay:
            sleep_with_stop(delay)
        if STOP_EVENT.is_set():
            break
    return summary


def _summaries_from_queue(queue: Queue) -> List[InteractionSummary]:
    summaries: List[InteractionSummary] = []
    while True:
        try:
            summaries.append(queue.get_nowait())
        except Empty:
            break
    return summaries


def _print_summary(title: str, summaries: List[InteractionSummary], start: float) -> None:
    elapsed = time.perf_counter() - start
    total_ok = sum(s.performed for s in summaries)
    total_err = sum(s.errors for s in summaries)

    print(full_line(color=Fore.MAGENTA))
    print(style_text(title, color=Fore.CYAN, bold=True))
    print(style_text(f"Acciones exitosas: {total_ok}", color=Fore.GREEN, bold=True))
    print(style_text(f"Errores: {total_err}", color=Fore.RED if total_err else Fore.GREEN, bold=True))
    print(style_text(
        f"Tiempo total: {int(elapsed // 60):02d}:{int(elapsed % 60):02d}",
        color=Fore.WHITE,
        bold=True,
    ))
    print(full_line(color=Fore.MAGENTA))
    for summary in summaries:
        color = Fore.GREEN if summary.errors == 0 else Fore.YELLOW
        print(style_text(f"@{summary.username}: {summary.performed} acciones / {summary.errors} errores", color=color, bold=True))
        for message in summary.messages:
            print(f"  - {message}")
    print(full_line(color=Fore.MAGENTA))


def _run_comment_flow(alias: str) -> None:
    banner()
    print(style_text("🎯 Interacciones - Comentarios", color=Fore.CYAN, bold=True))
    print(full_line())
    usernames = _select_accounts(alias)
    if not usernames:
        return
    ready = [user for user in usernames if _ensure_account_ready(user)]
    if not ready:
        warn("Ninguna cuenta tiene sesión válida.")
        press_enter()
        return

    print("Destino del comentario:")
    print("1) Historias")
    print("2) Posts")
    print("3) Reels")
    destination = {"1": "story", "2": "post", "3": "reel"}.get(ask("Opción: ").strip())
    if not destination:
        warn("Opción inválida.")
        press_enter()
        return

    source = ask("Origen (hashtag=h, urls=u) [h/u]: ").strip().lower() or "h"

    limit = ask_int("Cantidad máxima por cuenta: ", min_value=1, default=10)
    delay_min = ask_int("Delay mínimo entre acciones (seg): ", min_value=0, default=SETTINGS.delay_min)
    delay_max = ask_int(
        "Delay máximo entre acciones (seg): ", min_value=delay_min, default=max(delay_min, SETTINGS.delay_max)
    )

    templates = _prepare_comments()
    if not templates:
        press_enter()
        return

    entries: List[str] = []
    hashtag = ""
    if source == "u":
        multiline = ask_multiline(
            "Pegá URLs de historias/posts/reels o usernames (una por línea):"
        )
        entries = [line.strip() for line in multiline.splitlines() if line.strip()]
        if not entries:
            warn("Debés ingresar al menos un destino.")
            press_enter()
            return
    else:
        hashtag = ask("Hashtag (sin #): ").strip()
        if not hashtag:
            warn("Debés indicar un hashtag.")
            press_enter()
            return

    targets_by_account: dict[str, tuple[object, Sequence]] = {}
    for username in ready:
        try:
            client = _client_for(username)
        except Exception as exc:
            warn(str(exc))
            continue
        if source == "u":
            targets = _targets_from_urls(client, entries, destination)
        else:
            targets = _targets_from_hashtag(client, hashtag, limit * 2, destination)
        if not targets:
            warn(f"No se encontraron objetivos para @{username}.")
            continue
        targets_by_account[username] = (client, targets)

    if not targets_by_account:
        warn("No hay objetivos para comentar.")
        press_enter()
        return

    ensure_logging(quiet=SETTINGS.quiet, log_dir=SETTINGS.log_dir, log_file=SETTINGS.log_file)
    reset_stop_event()
    listener = start_q_listener("Presioná Q para detener las interacciones.", logger)
    start_time = time.perf_counter()
    queue: Queue = Queue()

    try:
        for username, payload in targets_by_account.items():
            if STOP_EVENT.is_set():
                break
            client, targets = payload
            summary = _comment_on_targets(
                alias,
                username,
                client,
                targets,
                templates,
                (delay_min, delay_max),
                limit,
                destination,
            )
            queue.put(summary)
    finally:
        request_stop("interacciones detenidas")
        listener.join(timeout=0.2)

    summaries = _summaries_from_queue(queue)
    _print_summary("=== INTERACCIONES COMPLETADAS ===", summaries, start_time)
    ok("Interacciones finalizadas.")
    press_enter()


def _run_reel_flow(alias: str) -> None:
    banner()
    print(style_text("🎯 Interacciones - Ver & Like Reels", color=Fore.CYAN, bold=True))
    print(full_line())
    usernames = _select_accounts(alias)
    if not usernames:
        return
    ready = [user for user in usernames if _ensure_account_ready(user)]
    if not ready:
        warn("Ninguna cuenta tiene sesión válida.")
        press_enter()
        return

    source_choice = ask("Origen (1=Hashtag, 2=Explorar): ").strip()
    source = "hashtag" if source_choice != "2" else "explore"
    hashtag = ""
    if source == "hashtag":
        hashtag = ask("Hashtag (sin #): ").strip()
    amount = ask_int("Cantidad de reels por cuenta: ", min_value=1, default=5)
    like = ask("¿Dar like? (s/N): ").strip().lower() == "s"
    delay_min = ask_int("Delay mínimo entre reels (seg): ", min_value=0, default=5)
    delay_max = ask_int("Delay máximo entre reels (seg): ", min_value=delay_min, default=max(delay_min, 10))
    view_min = ask_int("Tiempo mínimo de visualización (seg): ", min_value=0, default=5)
    view_max = ask_int("Tiempo máximo de visualización (seg): ", min_value=view_min, default=max(view_min, 12))

    reels_by_account: dict[str, tuple[object, Sequence]] = {}
    for username in ready:
        try:
            client = _client_for(username)
        except Exception as exc:
            warn(str(exc))
            continue
        reels = _fetch_reels(client, source, hashtag, amount)
        if not reels:
            warn(f"No hay reels disponibles para @{username}.")
            continue
        reels_by_account[username] = (client, reels)

    if not reels_by_account:
        warn("No se encontraron reels para procesar.")
        press_enter()
        return

    ensure_logging(quiet=SETTINGS.quiet, log_dir=SETTINGS.log_dir, log_file=SETTINGS.log_file)
    reset_stop_event()
    listener = start_q_listener("Presioná Q para detener la acción.", logger)
    start_time = time.perf_counter()
    queue: Queue = Queue()

    try:
        for username, payload in reels_by_account.items():
            if STOP_EVENT.is_set():
                break
            client, reels = payload
            summary = _view_like_reels(
                alias,
                username,
                client,
                reels,
                like,
                (delay_min, delay_max),
                (view_min, view_max),
            )
            queue.put(summary)
    finally:
        request_stop("reels finalizados")
        listener.join(timeout=0.2)

    summaries = _summaries_from_queue(queue)
    _print_summary("=== REELS COMPLETADOS ===", summaries, start_time)
    ok("Proceso finalizado.")
    press_enter()


def run_from_menu(alias: str) -> None:
    while True:
        banner()
        print(style_text("🎯 Interacciones (Comentar / Ver & Like Reels)", color=Fore.CYAN, bold=True))
        print(full_line())
        print("1) Comentar (historias / posts / reels)")
        print("2) Ver & Like Reels")
        print("3) ↩️ Volver")
        option = ask("Opción: ").strip()
        if option == "1":
            _run_comment_flow(alias)
        elif option == "2":
            _run_reel_flow(alias)
        else:
            break
