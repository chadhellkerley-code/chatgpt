# leads.py
# -*- coding: utf-8 -*-
import csv
import logging
import os
import random
import shutil
import sys
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from accounts import (
    auto_login_with_saved_password,
    get_account,
    has_valid_session_settings,
    list_all,
    mark_connected,
    prompt_login,
)
from paths import runtime_base
from proxy_manager import apply_proxy_to_client, record_proxy_failure, should_retry_proxy
from session_store import has_session, load_into
from utils import (
    ask,
    ask_int,
    ask_multiline,
    banner,
    ok,
    press_enter,
    title,
    warn,
)

BASE = runtime_base(Path(__file__).resolve().parent)
BASE.mkdir(parents=True, exist_ok=True)
TEXT = BASE / "text" / "leads"
TEXT.mkdir(parents=True, exist_ok=True)

def list_files()->List[str]:
    return sorted([p.stem for p in TEXT.glob("*.txt")])

def load_list(name:str)->List[str]:
    p=TEXT/f"{name}.txt"
    if not p.exists(): return []
    return [line.strip().lstrip("@") for line in p.read_text(encoding="utf-8").splitlines() if line.strip()]

def append_list(name:str, usernames:List[str]):
    p=TEXT/f"{name}.txt"
    with p.open("a", encoding="utf-8") as f:
        for u in usernames:
            f.write(u.strip().lstrip("@")+"\n")


def save_list(name: str, usernames: List[str]) -> None:
    p = TEXT / f"{name}.txt"
    with p.open("w", encoding="utf-8") as f:
        for u in usernames:
            f.write(u.strip().lstrip("@") + "\n")

def import_csv(path:str, name:str):
    path=Path(path)
    if not path.exists():
        warn("CSV no encontrado."); return
    users=[]
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.reader(f):
            if not row: continue
            users.append(row[0].strip().lstrip("@"))
    append_list(name, users)
    ok(f"Importados {len(users)} a {name}.")

def show_list(name:str):
    users=load_list(name)
    print(f"{name}: {len(users)} usuarios")
    for i,u in enumerate(users[:50],1):
        print(f"{i:02d}. @{u}")
    if len(users)>50: print(f"... (+{len(users)-50})")

def delete_list(name:str):
    p=TEXT/f"{name}.txt"
    if p.exists(): p.unlink(); ok("Eliminada.")
    else: warn("No existe.")

def menu_leads():
    while True:
        banner()
        title("Listas de leads")
        files=list_files()
        if files: print("Disponibles:", ", ".join(files))
        else: print("(aún no hay listas)")
        print("\n1) Crear lista y agregar manual")
        print("2) Importar CSV a una lista")
        print("3) Ver lista")
        print("4) Eliminar lista")
        print("5) Scraping automático de perfiles")
        print("6) Volver\n")
        op=ask("Opción: ").strip()
        if op=="1":
            name=ask("Nombre de la lista: ").strip() or "default"
            print("Pegá usernames (uno por línea). Línea vacía para terminar:")
            lines=[]
            while True:
                s=ask("")
                if not s: break
                lines.append(s)
            append_list(name, lines); ok("Guardado."); press_enter()
        elif op=="2":
            path=ask("Ruta del CSV: ")
            name=ask("Importar a la lista (nombre): ").strip() or "default"
            import_csv(path, name); press_enter()
        elif op=="3":
            name=ask("Nombre de la lista: ").strip()
            show_list(name); press_enter()
        elif op=="4":
            name=ask("Nombre de la lista: ").strip()
            delete_list(name); press_enter()
        elif op=="5":
            _scrape_menu()
        elif op=="6":
            break
        else:
            warn("Opción inválida."); press_enter()


@dataclass
class ScrapeFilters:
    min_followers: int
    max_followers: int
    min_posts: int
    max_posts: int
    privacy: str
    max_results: int
    delay: float


@dataclass
class ScrapedUser:
    username: str
    biography: str
    full_name: str
    follower_count: int
    media_count: int
    is_private: bool


class DelayController:
    def __init__(self, delay: float) -> None:
        self._delay = max(0.0, float(delay))
        self._last_recorded: Optional[float] = None

    def pause(self) -> None:
        if self._delay <= 0:
            self._last_recorded = time.monotonic()
            return
        now = time.monotonic()
        if self._last_recorded is None:
            self._last_recorded = now
            return
        jitter = min(2.0, self._delay * 0.3 + 0.5)
        lower = max(0.5, self._delay - jitter)
        upper = self._delay + jitter
        target = random.uniform(lower, upper)
        elapsed = now - self._last_recorded
        remaining = max(0.0, target - elapsed)
        if remaining > 0:
            time.sleep(remaining)
            now = time.monotonic()
        self._last_recorded = now


def _scrape_menu() -> None:
    while True:
        banner()
        title("Scraping automático de perfiles")
        print("1) Scrapear por hashtag")
        print("2) Scrapear desde perfiles base")
        print("3) Volver\n")
        choice = ask("Opción: ").strip() or "3"
        if choice == "1":
            _scrape_from_hashtag_flow()
        elif choice == "2":
            _scrape_from_profiles_flow()
        elif choice == "3":
            break
        else:
            warn("Opción inválida."); press_enter()


def _scrape_from_hashtag_flow() -> None:
    username = _choose_scrape_account()
    if not username:
        press_enter()
        return
    if not _ensure_account_ready(username):
        press_enter()
        return
    hashtag = ask("Hashtag (sin #): ").strip().lstrip("#")
    if not hashtag:
        warn("Debés indicar un hashtag.")
        press_enter()
        return
    filters = _prompt_filters()
    if not filters:
        return
    try:
        client = _client_for_scraping(username)
    except Exception as exc:
        warn(str(exc))
        press_enter()
        return
    print(f"Buscando perfiles que usaron #{hashtag}...")
    results = _run_scrape(
        lambda cl, progress: _scrape_hashtag(cl, username, hashtag, filters, progress),
        client,
        username,
    )
    _handle_scrape_results(results)


def _scrape_from_profiles_flow() -> None:
    username = _choose_scrape_account()
    if not username:
        press_enter()
        return
    if not _ensure_account_ready(username):
        press_enter()
        return
    raw = ask_multiline(
        "Pegá la lista de perfiles base (uno por línea, sin @)."
    )
    base_profiles = [chunk.strip().lstrip("@") for chunk in raw.splitlines() if chunk.strip()]
    if not base_profiles:
        warn("No se ingresaron perfiles base.")
        press_enter()
        return
    print("\n¿Qué querés extraer de esos perfiles?")
    print("1) Seguidores")
    print("2) Seguidos (following)")
    mode_choice = ask("Opción (1/2): ").strip() or "1"
    mode = "followers" if mode_choice == "1" else "following"
    filters = _prompt_filters()
    if not filters:
        return
    try:
        client = _client_for_scraping(username)
    except Exception as exc:
        warn(str(exc))
        press_enter()
        return
    label = "seguidores" if mode == "followers" else "seguidos"
    print(f"Buscando {label} que cumplan los filtros...")
    results = _run_scrape(
        lambda cl, progress: _scrape_from_profiles(cl, username, base_profiles, mode, filters, progress),
        client,
        username,
    )
    _handle_scrape_results(results)


def _choose_scrape_account() -> Optional[str]:
    try:
        records = list_all()
    except Exception as exc:
        warn(f"No se pudieron cargar las cuentas: {exc}")
        return None
    available: List[Tuple[str, Dict]] = []
    for acct in records:
        username = (acct.get("username") or "").strip()
        if not username:
            continue
        available.append((username, acct))
    if not available:
        warn("No hay cuentas configuradas.")
        return None
    print("Seleccioná la cuenta que se usará para scrapear:")
    for idx, (username, acct) in enumerate(available, start=1):
        alias = acct.get("alias") or ""
        alias_part = f" (alias: {alias})" if alias else ""
        session_flag = "[sesión]" if has_session(username) else "[sin sesión]"
        print(f" {idx}) @{username}{alias_part} {session_flag}")
    print(" (Enter para cancelar)")
    while True:
        raw = ask("Cuenta: ").strip()
        if not raw:
            warn("Operación cancelada.")
            return None
        if raw.isdigit():
            idx = int(raw)
            if 1 <= idx <= len(available):
                return available[idx - 1][0]
        normalized = raw.lstrip("@").lower()
        for username, _ in available:
            if username.lower() == normalized:
                return username
        warn("Selección inválida. Probá nuevamente.")


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
        _client_for_scraping(username)
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


def _client_for_scraping(username: str):
    from instagrapi import Client
    from instagrapi.exceptions import LoginRequired

    account = get_account(username)
    cl = Client()
    try:
        cl.logger.setLevel(logging.WARNING)
        for handler in list(cl.logger.handlers):
            handler.setLevel(logging.WARNING)
    except Exception:
        pass
    binding = None
    try:
        binding = apply_proxy_to_client(cl, username, account, reason="lead-scraper")
    except Exception as exc:
        if account and account.get("proxy_url"):
            record_proxy_failure(username, exc)
            raise RuntimeError(
                f"El proxy configurado para @{username} no respondió: {exc}"
            ) from exc
        warn(f"Proxy no disponible para @{username}: {exc}")
    try:
        load_into(cl, username)
    except FileNotFoundError as exc:
        mark_connected(username, False)
        raise RuntimeError(
            f"No hay sesión guardada para @{username}. Usá la opción de login primero."
        ) from exc
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

    try:
        cl.account_info()
    except LoginRequired as exc:
        mark_connected(username, False)
        raise RuntimeError(
            f"La sesión guardada para @{username} no está activa en Instagram. Iniciá sesión nuevamente."
        ) from exc
    except Exception as exc:
        warn(f"No se pudo verificar la sesión de @{username}: {exc}")

    mark_connected(username, True)
    return cl


def _prompt_filters() -> Optional[ScrapeFilters]:
    print("\nConfigurá los filtros para la extracción:")
    min_followers = ask_int("Mínimo de seguidores (0 sin mínimo): ", min_value=0, default=0)
    max_followers = ask_int("Máximo de seguidores (0 sin máximo): ", min_value=0, default=0)
    if max_followers and max_followers < min_followers:
        warn("El máximo de seguidores era menor al mínimo. Se invirtieron los valores.")
        min_followers, max_followers = max_followers, min_followers

    min_posts = ask_int("Mínimo de posteos (0 sin mínimo): ", min_value=0, default=0)
    max_posts = ask_int("Máximo de posteos (0 sin máximo): ", min_value=0, default=0)
    if max_posts and max_posts < min_posts:
        warn("El máximo de posteos era menor al mínimo. Se invirtieron los valores.")
        min_posts, max_posts = max_posts, min_posts

    print("\nPrivacidad de cuentas a incluir:")
    print("1) Solo públicas")
    print("2) Solo privadas")
    print("3) Ambas")
    privacy_choice = ask("Opción (3 por defecto): ").strip() or "3"
    if privacy_choice == "1":
        privacy = "public"
    elif privacy_choice == "2":
        privacy = "private"
    else:
        privacy = "any"

    max_results = ask_int("Cantidad máxima de usuarios a scrapear: ", min_value=1, default=50)
    delay_seconds = ask_int(
        "Delay entre extracciones (segundos, mínimo 5): ", min_value=5, default=8
    )

    return ScrapeFilters(
        min_followers=min_followers,
        max_followers=max_followers,
        min_posts=min_posts,
        max_posts=max_posts,
        privacy=privacy,
        max_results=max_results,
        delay=float(delay_seconds),
    )


def _run_scrape(worker, client, username: str) -> List[ScrapedUser]:
    from instagrapi.exceptions import LoginRequired

    working_client = client
    while True:
        progress = ScrapeProgress()
        results: List[ScrapedUser] = []
        try:
            with progress:
                results = worker(working_client, progress)
        except LoginRequired:
            if not _refresh_session(username):
                warn(
                    "Instagram solicitó validar la sesión y no se pudo renovar automáticamente. "
                    "Iniciá sesión nuevamente desde el menú de cuentas."
                )
                return []
            try:
                working_client = _client_for_scraping(username)
            except Exception as exc:
                warn(str(exc))
                return []
            continue
        except KeyboardInterrupt:
            progress.stop("ctrl_c")
            progress.record_issue("Scraping interrumpido manualmente con Ctrl+C.")
            results = []
        progress.summarize()
        return results


def _refresh_session(username: str) -> bool:
    refreshed = False
    if auto_login_with_saved_password(username) and has_session(username):
        refreshed = True
    elif prompt_login(username, interactive=False) and has_session(username):
        refreshed = True
    if refreshed:
        ok(f"Sesión de @{username} renovada correctamente.")
    return refreshed


class ScrapeProgress:
    def __init__(self) -> None:
        self.count = 0
        size = shutil.get_terminal_size((80, 24))
        self._max_rows = max(size.lines - 5, 5)
        self._recent = deque(maxlen=self._max_rows)
        self._issues: List[str] = []
        self._is_tty = sys.stdout.isatty()
        self._monitor = _KeyPressMonitor()
        self.stopped = False
        self.stop_reason: Optional[str] = None
        self._active = False

    def __enter__(self) -> "ScrapeProgress":
        self._monitor.__enter__()
        self._active = True
        self._redraw()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self._monitor.__exit__(exc_type, exc, tb)
        self._active = False

    def update(self, username: str) -> None:
        self.count += 1
        self._recent.append(username)
        self._redraw()

    def should_stop(self) -> bool:
        if self.stopped:
            return True
        if self._monitor.poll():
            self.stop("q")
        return self.stopped

    def stop(self, reason: str = "manual") -> None:
        self.stopped = True
        if not self.stop_reason:
            self.stop_reason = reason

    def record_issue(self, message: str) -> None:
        message = (message or "").strip()
        if not message:
            return
        if message not in self._issues:
            self._issues.append(message)

    def summarize(self) -> None:
        if self._is_tty:
            self._clear_screen()
        print(f"Total encontrados: {self.count}")
        if self.stopped:
            if self.stop_reason == "q":
                print("Proceso detenido manualmente (Q).")
            elif self.stop_reason == "ctrl_c":
                print("Proceso interrumpido con Ctrl+C.")
            else:
                print("Proceso detenido manualmente.")
        else:
            print("Proceso de scraping finalizado.")
        if self._issues:
            print("\nAvisos durante el scraping:")
            for issue in self._issues[:5]:
                print(f" - {issue}")
            if len(self._issues) > 5:
                print(f" - ... {len(self._issues) - 5} eventos adicionales omitidos.")
        print("")

    def _redraw(self) -> None:
        if not self._active:
            return
        if self._is_tty:
            self._clear_screen()
            header = [
                "Scraping en curso... Presioná Q para detener.",
                f"Total encontrados: {self.count}",
                "",
            ]
            print("\n".join(header))
            for name in self._recent:
                print(f"Perfil encontrado: @{name}")
            sys.stdout.flush()
        else:
            if self._recent:
                print(f"Total encontrados: {self.count} → @{self._recent[-1]}")
            else:
                print(f"Total encontrados: {self.count}")

    def _clear_screen(self) -> None:
        if not self._is_tty:
            return
        try:
            if os.name == "nt":
                os.system("cls")
            else:
                print("\033c", end="", flush=True)
        except Exception:
            print("\033[2J\033[H", end="", flush=True)


class _KeyPressMonitor:
    def __init__(self) -> None:
        self._using_windows = os.name == "nt"
        self._isatty = sys.stdin.isatty()
        self._fd = None
        self._old_settings = None
        self._msvcrt = None

    def __enter__(self) -> "_KeyPressMonitor":
        if self._using_windows:
            try:
                import msvcrt  # type: ignore

                self._msvcrt = msvcrt
            except ImportError:
                self._using_windows = False
        if not self._using_windows and self._isatty:
            import termios
            import tty

            self._fd = sys.stdin.fileno()
            self._old_settings = termios.tcgetattr(self._fd)
            tty.setcbreak(self._fd)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._using_windows:
            return
        if self._fd is not None and self._old_settings is not None:
            import termios

            termios.tcsetattr(self._fd, termios.TCSADRAIN, self._old_settings)

    def poll(self) -> bool:
        if self._using_windows and self._msvcrt is not None:
            try:
                while self._msvcrt.kbhit():
                    key = self._msvcrt.getch()
                    if key in (b"q", b"Q"):
                        return True
            except Exception:
                return False
            return False
        if not self._isatty or self._fd is None:
            return False
        import select

        ready, _, _ = select.select([sys.stdin], [], [], 0)
        if ready:
            try:
                key = sys.stdin.read(1)
            except Exception:
                return False
            return key.lower() == "q"
        return False


def _scrape_hashtag(
    client,
    username: str,
    hashtag: str,
    filters: ScrapeFilters,
    progress: "ScrapeProgress",
) -> List[ScrapedUser]:
    amount = min(max(filters.max_results * 12, filters.max_results + 80), 6000)
    medias = _collect_hashtag_medias(client, username, hashtag, amount, progress)
    if not medias:
        warn(f"No se encontraron publicaciones recientes con #{hashtag}.")
        return []
    seen: set[int] = set()
    cache: Dict[int, object] = {}
    collected: List[ScrapedUser] = []
    delay = DelayController(filters.delay)
    try:
        for media in medias:
            user_id, candidate = _resolve_media_user(media)
            if not user_id or user_id in seen:
                continue
            seen.add(user_id)
            if progress.should_stop():
                break
            info = _fetch_user_info(client, user_id, cache, progress, candidate)
            if not info or not getattr(info, "username", None):
                continue
            if _passes_filters(info, filters):
                collected.append(_build_scraped_user(info))
                progress.update(info.username)
                if len(collected) >= filters.max_results:
                    break
                delay.pause()
    except KeyboardInterrupt:
        progress.stop("ctrl_c")
    return _dedupe_scraped(collected)


def _collect_hashtag_medias(client, username: str, hashtag: str, amount: int, progress: "ScrapeProgress"):
    from instagrapi.exceptions import LoginRequired

    medias: List[object] = []
    seen_media: set[int] = set()
    fetchers = [
        ("recientes", getattr(client, "hashtag_medias_recent", None)),
        ("populares", getattr(client, "hashtag_medias_top", None)),
        ("v1", getattr(client, "hashtag_medias_v1", None)),
    ]
    for label, func in fetchers:
        if progress.should_stop():
            break
        if not callable(func):
            continue
        remaining = max(amount - len(medias), 0)
        if remaining <= 0:
            break
        try:
            chunk = func(hashtag, amount=remaining)
        except LoginRequired:
            mark_connected(username, False)
            raise
        except Exception as exc:
            progress.record_issue(f"No se pudo obtener datos ({label}) de #{hashtag}: {exc}")
            continue
        for media in chunk or []:
            key = getattr(media, "pk", None) or getattr(media, "id", None)
            if key is None:
                continue
            try:
                key_int = int(key)
            except Exception:
                continue
            if key_int in seen_media:
                continue
            seen_media.add(key_int)
            medias.append(media)
            if len(medias) >= amount:
                break
    return medias


def _scrape_from_profiles(
    client,
    username: str,
    base_profiles: Iterable[str],
    mode: str,
    filters: ScrapeFilters,
    progress: "ScrapeProgress",
) -> List[ScrapedUser]:
    from instagrapi.exceptions import LoginRequired

    collected: List[ScrapedUser] = []
    seen: set[int] = set()
    cache: Dict[int, object] = {}
    delay = DelayController(filters.delay)
    try:
        for base in base_profiles:
            if len(collected) >= filters.max_results:
                break
            try:
                base_id = client.user_id_from_username(base)
            except LoginRequired:
                mark_connected(username, False)
                raise
            except Exception as exc:
                progress.record_issue(f"No se pudo resolver @{base}: {exc}")
                continue
            fetch_amount = min(max(filters.max_results * 4, filters.max_results + 20), 1200)
            try:
                if mode == "followers":
                    candidates = client.user_followers(base_id, amount=fetch_amount)
                else:
                    candidates = client.user_following(base_id, amount=fetch_amount)
            except LoginRequired:
                mark_connected(username, False)
                raise
            except Exception as exc:
                progress.record_issue(f"Error obteniendo datos de @{base}: {exc}")
                continue
            items: Iterable[Tuple[int, object]]
            if isinstance(candidates, dict):
                items = candidates.items()
            else:
                temp_list: List[Tuple[int, object]] = []
                for cand in candidates or []:
                    cand_id = getattr(cand, "pk", None)
                    if cand_id is None:
                        continue
                    try:
                        cand_id_int = int(cand_id)
                    except Exception:
                        continue
                    temp_list.append((cand_id_int, cand))
                items = temp_list
            for cand_id, cand in items:
                if len(collected) >= filters.max_results:
                    break
                try:
                    user_id = int(cand_id)
                except Exception:
                    continue
                if user_id in seen:
                    continue
                seen.add(user_id)
                if progress.should_stop():
                    break
                info = _fetch_user_info(client, user_id, cache, progress, cand)
                if not info or not getattr(info, "username", None):
                    continue
                if _passes_filters(info, filters):
                    collected.append(_build_scraped_user(info))
                    progress.update(info.username)
                    if len(collected) >= filters.max_results:
                        break
                    delay.pause()
            if progress.should_stop() or len(collected) >= filters.max_results:
                break
    except KeyboardInterrupt:
        progress.stop("ctrl_c")
    return _dedupe_scraped(collected)


def _fetch_user_info(
    client,
    user_id: int,
    cache: Dict[int, object],
    progress: Optional["ScrapeProgress"] = None,
    candidate: Optional[object] = None,
):
    from instagrapi.exceptions import LoginRequired

    if user_id in cache:
        return cache[user_id]

    username_hint = None
    if candidate is not None:
        username_hint = getattr(candidate, "username", None) or ""
        if not username_hint and isinstance(candidate, dict):
            username_hint = candidate.get("username")
        if username_hint:
            username_hint = str(username_hint).strip().lstrip("@")

    attempts = []

    def _add_attempt(label: str, func) -> None:
        if not callable(func):
            return
        attempts.append((label, func))

    _add_attempt("user_info", lambda: client.user_info(user_id))
    if hasattr(client, "user_info_gql"):
        _add_attempt("user_info_gql", lambda: client.user_info_gql(str(user_id)))
    if username_hint:
        by_username = getattr(client, "user_info_by_username", None)
        if callable(by_username):
            _add_attempt("user_info_by_username", lambda: by_username(username_hint))
        by_username_v1 = getattr(client, "user_info_by_username_v1", None)
        if callable(by_username_v1):
            _add_attempt("user_info_by_username_v1", lambda: by_username_v1(username_hint))

    errors: List[str] = []
    for label, func in attempts:
        try:
            info = func()
        except LoginRequired:
            raise
        except Exception as exc:
            errors.append(f"{label}: {exc}")
            continue
        if info:
            cache[user_id] = info
            return info

    if progress and errors:
        progress.record_issue(
            "No se pudo obtener info del usuario "
            f"{user_id}: "
            + "; ".join(errors[:2])
            + ("; ..." if len(errors) > 2 else "")
        )
    elif errors:
        warn(
            "No se pudo obtener info del usuario "
            f"{user_id}: "
            + "; ".join(errors[:2])
            + ("; ..." if len(errors) > 2 else "")
        )
    return None


def _passes_filters(user_info, filters: ScrapeFilters) -> bool:
    is_private = bool(getattr(user_info, "is_private", False))
    if filters.privacy == "public" and is_private:
        return False
    if filters.privacy == "private" and not is_private:
        return False

    follower_count = int(getattr(user_info, "follower_count", 0) or 0)
    if filters.min_followers and follower_count < filters.min_followers:
        return False
    if filters.max_followers and follower_count > filters.max_followers:
        return False

    media_count = int(getattr(user_info, "media_count", 0) or 0)
    if filters.min_posts and media_count < filters.min_posts:
        return False
    if filters.max_posts and media_count > filters.max_posts:
        return False

    return True


def _handle_scrape_results(users: List[ScrapedUser]) -> None:
    users = [u for u in users if u and getattr(u, "username", None)]
    users = _dedupe_scraped(users)
    if not users:
        warn("No se encontraron usuarios que cumplan los filtros.")
        press_enter()
        return
    current = users
    while True:
        if not current:
            warn("No quedan usuarios en la lista actual.")
            break
        print("\nUsuarios encontrados:")
        for idx, user in enumerate(current[:20], start=1):
            resume = (user.biography or user.full_name or "").strip()
            extra = f" — {resume[:70]}" if resume else ""
            print(f" {idx:02d}. @{user.username}{extra}")
        if len(current) > 20:
            print(f" ... (+{len(current) - 20} más)")

        print("\n¿Qué deseás hacer con la lista?")
        print("1) Agregar a una lista existente")
        print("2) Crear una lista nueva")
        print("3) Aplicar limpieza avanzada")
        print("4) Cancelar y descartar")
        choice = ask("Opción: ").strip() or "4"
        usernames = [u.username.lstrip("@") for u in current]
        if choice == "1":
            files = list_files()
            if not files:
                warn("No hay listas existentes. Creá una nueva.")
                continue
            print("Listas disponibles: " + ", ".join(files))
            name = ask("Nombre de la lista destino: ").strip()
            if not name:
                warn("Debés indicar un nombre.")
                continue
            existing = load_list(name)
            existing_lower = {u.lower() for u in existing}
            new_entries = [u for u in usernames if u.lower() not in existing_lower]
            if not new_entries:
                warn("Todos los usuarios ya estaban presentes en esa lista.")
                continue
            append_list(name, new_entries)
            ok(f"Se agregaron {len(new_entries)} usuarios a {name}.")
            break
        elif choice == "2":
            name = ask("Nombre de la nueva lista: ").strip() or "scrape"
            save_list(name, usernames)
            ok(f"Lista {name} creada con {len(usernames)} usuarios.")
            break
        elif choice == "3":
            filtered = _apply_advanced_filter(current)
            if filtered is current:
                continue
            current = _dedupe_scraped(filtered)
        elif choice == "4":
            warn("Lista descartada.")
            break
        else:
            warn("Opción inválida.")
    press_enter()


def _dedupe_preserve_order(usernames: Iterable[str]) -> List[str]:
    seen: set[str] = set()
    ordered: List[str] = []
    for username in usernames:
        key = username.strip().lstrip("@").lower()
        if not key or key in seen:
            continue
        seen.add(key)
        ordered.append(username.strip().lstrip("@"))
    return ordered


def _dedupe_scraped(users: Iterable[ScrapedUser]) -> List[ScrapedUser]:
    seen: set[str] = set()
    ordered: List[ScrapedUser] = []
    for user in users:
        username = getattr(user, "username", "")
        key = username.strip().lstrip("@").lower()
        if not key or key in seen:
            continue
        seen.add(key)
        ordered.append(user)
    return ordered


def _resolve_media_user(media) -> Tuple[Optional[int], Optional[object]]:
    if media is None:
        return None, None
    user = getattr(media, "user", None)
    user_id = _extract_user_id(user) if user is not None else None
    if user_id:
        return user_id, user
    owner = getattr(media, "owner", None)
    owner_id = _extract_user_id(owner) if owner is not None else None
    if owner_id:
        return owner_id, owner
    user_id_attr = getattr(media, "user_id", None)
    if user_id_attr is not None:
        try:
            return int(user_id_attr), None
        except Exception:
            pass
    if isinstance(media, dict):
        for key in ("user", "owner"):
            candidate = media.get(key)
            if candidate:
                candidate_id = _extract_user_id(candidate)
                if candidate_id:
                    return candidate_id, candidate
        user_id_attr = media.get("user_id")
        if user_id_attr is not None:
            try:
                return int(user_id_attr), None
            except Exception:
                pass
    return None, None


def _apply_advanced_filter(users: List[ScrapedUser]) -> List[ScrapedUser]:
    if not users:
        warn("No hay usuarios para filtrar.")
        return users
    print(
        "\nIngresá palabras o frases clave a buscar en la bio, nombre o usuario. "
        "Separalas con comas o saltos de línea."
    )
    print("Podés anteponer '-' para excluir términos específicos.")
    raw = ask_multiline("Condiciones: ").strip()
    if not raw:
        warn("No se ingresaron filtros. Se mantiene la lista actual.")
        return users
    tokens = [chunk.strip() for chunk in raw.replace("\n", ",").split(",")]
    includes = [t.lstrip("+").lower() for t in tokens if t and not t.startswith("-")]
    excludes = [t[1:].lower() for t in tokens if t.startswith("-") and len(t) > 1]
    includes = [t for t in includes if t]
    excludes = [t for t in excludes if t]
    if not includes and not excludes:
        warn("No se ingresaron filtros válidos. Se mantiene la lista actual.")
        return users
    mode = (
        ask(
            "¿Las palabras obligatorias deben aparecer todas (T) o al menos una (A)? (A/T): "
        )
        .strip()
        .lower()
    )
    require_all = mode == "t"
    filtered: List[ScrapedUser] = []
    for user in users:
        haystack = " ".join(
            filter(
                None,
                [
                    getattr(user, "username", "") or "",
                    getattr(user, "full_name", "") or "",
                    getattr(user, "biography", "") or "",
                ],
            )
        ).lower()
        if includes:
            if require_all:
                if not all(term in haystack for term in includes):
                    continue
            else:
                if not any(term in haystack for term in includes):
                    continue
        if excludes and any(term in haystack for term in excludes):
            continue
        filtered.append(user)
    if not filtered:
        warn(
            "Ningún perfil coincidió con los filtros avanzados. Se mantiene la lista actual."
        )
        return users
    print(f"\nPerfiles tras el filtrado avanzado: {len(filtered)} (de {len(users)}).")
    preview = filtered[:10]
    if preview:
        print("Ejemplos filtrados:")
        for idx, user in enumerate(preview, start=1):
            snippet = (user.biography or user.full_name or "").strip()
            extra = f" — {snippet[:60]}" if snippet else ""
            print(f" {idx:02d}. @{user.username}{extra}")
    confirm = ask("¿Aplicar este filtrado a la lista actual? (s/N): ").strip().lower()
    if confirm != "s":
        warn("Se mantiene la lista sin cambios.")
        return users
    return filtered


def _extract_user_id(user) -> Optional[int]:
    for attr in ("pk", "id"):
        value = getattr(user, attr, None)
        if value is None:
            continue
        try:
            return int(value)
        except Exception:
            continue
    return None


def _format_user(user_info, position: int, limit: int) -> str:
    username = getattr(user_info, "username", "?")
    follower_count = int(getattr(user_info, "follower_count", 0) or 0)
    media_count = int(getattr(user_info, "media_count", 0) or 0)
    privacy = "privada" if getattr(user_info, "is_private", False) else "pública"
    return (
        f" {position:02d}/{limit:02d} → @{username} | "
        f"seguidores: {follower_count:,} | posteos: {media_count} | {privacy}"
    )


def _build_scraped_user(info) -> ScrapedUser:
    biography = (getattr(info, "biography", "") or "").strip()
    full_name = (getattr(info, "full_name", "") or "").strip()
    follower_count = int(getattr(info, "follower_count", 0) or 0)
    media_count = int(getattr(info, "media_count", 0) or 0)
    is_private = bool(getattr(info, "is_private", False))
    username = getattr(info, "username", "").strip()
    return ScrapedUser(
        username=username.lstrip("@"),
        biography=biography,
        full_name=full_name,
        follower_count=follower_count,
        media_count=media_count,
        is_private=is_private,
    )
