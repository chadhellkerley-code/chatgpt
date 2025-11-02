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


def _run_scrape(worker, client, username: str) -> List[str]:
    from instagrapi.exceptions import LoginRequired

    working_client = client
    while True:
        progress = ScrapeProgress()
        results: List[str] = []
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
        print(f"Scrapeados: {self.count}")
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
                f"Scrapeados: {self.count}",
                "",
            ]
            print("\n".join(header))
            for name in self._recent:
                print(f" @{name}")
            sys.stdout.flush()
        else:
            if self._recent:
                print(f"Scrapeados: {self.count} → @{self._recent[-1]}")
            else:
                print(f"Scrapeados: {self.count}")

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
) -> List[str]:
    from instagrapi.exceptions import LoginRequired

    amount = min(max(filters.max_results * 6, filters.max_results + 50), 3000)
    try:
        medias = client.hashtag_medias_recent(hashtag, amount=amount)
    except LoginRequired:
        mark_connected(username, False)
        raise
    except Exception as exc:
        progress.record_issue(f"No se pudo obtener el hashtag #{hashtag}: {exc}")
        return []
    if not medias:
        warn(f"No se encontraron publicaciones recientes con #{hashtag}.")
        return []
    seen: set[int] = set()
    cache: Dict[int, object] = {}
    collected: List[str] = []
    try:
        for media in medias:
            user = getattr(media, "user", None)
            if not user:
                continue
            user_id = _extract_user_id(user)
            if not user_id or user_id in seen:
                continue
            seen.add(user_id)
            if progress.should_stop():
                break
            info = _fetch_user_info(client, user_id, cache, progress)
            if not info:
                continue
            username = getattr(info, "username", None)
            if not username:
                continue
            if _passes_filters(info, filters):
                collected.append(username)
                progress.update(username)
                _apply_delay(filters.delay)
                if len(collected) >= filters.max_results:
                    break
    except KeyboardInterrupt:
        progress.stop("ctrl_c")
    return _dedupe_preserve_order(collected)


def _scrape_from_profiles(
    client,
    username: str,
    base_profiles: Iterable[str],
    mode: str,
    filters: ScrapeFilters,
    progress: "ScrapeProgress",
) -> List[str]:
    from instagrapi.exceptions import LoginRequired

    collected: List[str] = []
    seen: set[int] = set()
    cache: Dict[int, object] = {}
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
                info = _fetch_user_info(client, user_id, cache, progress)
                if not info or not getattr(info, "username", None):
                    continue
                if _passes_filters(info, filters):
                    collected.append(info.username)
                    progress.update(info.username)
                    _apply_delay(filters.delay)
            if progress.should_stop():
                break
    except KeyboardInterrupt:
        progress.stop("ctrl_c")
    return _dedupe_preserve_order(collected)


def _fetch_user_info(
    client,
    user_id: int,
    cache: Dict[int, object],
    progress: Optional["ScrapeProgress"] = None,
):
    if user_id in cache:
        return cache[user_id]
    try:
        info = client.user_info(user_id)
    except Exception as exc:
        if progress:
            progress.record_issue(f"No se pudo obtener info del usuario {user_id}: {exc}")
        else:
            warn(f"No se pudo obtener info del usuario {user_id}: {exc}")
        return None
    cache[user_id] = info
    return info


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


def _handle_scrape_results(usernames: List[str]) -> None:
    usernames = _dedupe_preserve_order([u.lstrip("@") for u in usernames if u])
    if not usernames:
        warn("No se encontraron usuarios que cumplan los filtros.")
        press_enter()
        return
    print("\nUsuarios encontrados:")
    for idx, username in enumerate(usernames[:20], start=1):
        print(f" {idx:02d}. @{username}")
    if len(usernames) > 20:
        print(f" ... (+{len(usernames) - 20} más)")

    while True:
        print("\n¿Qué deseás hacer con la lista?")
        print("1) Agregar a una lista existente")
        print("2) Crear una lista nueva")
        print("3) Cancelar")
        choice = ask("Opción: ").strip() or "3"
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


def _apply_delay(delay: float) -> None:
    base = max(0.0, delay)
    if base <= 0:
        return
    jitter = min(2.0, base * 0.3 + 0.5)
    lower = max(0.5, base - jitter)
    upper = base + jitter
    time.sleep(random.uniform(lower, upper))
