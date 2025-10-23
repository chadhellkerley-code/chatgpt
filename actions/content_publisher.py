"""Publicación de historias, posts y reels desde la consola."""

from __future__ import annotations

import csv
import hashlib
import logging
import os
import random
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from queue import Empty, Queue
from typing import List, Optional, Sequence

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
PUBLISH_LOG = DATA_DIR / "publish_log.csv"
PROCESSED_MEDIA_DIR = DATA_DIR / "processed_media"
PROCESSED_MEDIA_DIR.mkdir(parents=True, exist_ok=True)

_IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp"}
_VIDEO_EXTS = {".mp4", ".mov", ".m4v"}

try:  # pragma: no cover - depende del entorno del operador
    from PIL import Image  # type: ignore
except Exception:  # noqa: S110 - si Pillow no está instalado se gestiona más adelante
    Image = None  # type: ignore

_MOVIEPY_MODULE = None
_MOVIEPY_FAILED = False


@dataclass
class PublishJob:
    kind: str  # "story", "post", "reel"
    media_paths: List[Path]
    caption: str = ""
    first_comment: str = ""
    tags: Sequence[str] = ()
    sticker_link: str = ""
    overlay_text: str = ""
    share_to_feed: bool = False
    cover_path: Optional[Path] = None
    delay_mode: str = "simultaneous"  # simultaneous | staggered
    delay_min: int = 0
    delay_max: int = 0


@dataclass
class PublishSummary:
    username: str
    uploaded: int = 0
    errors: int = 0
    media_ids: List[str] = field(default_factory=list)
    messages: List[str] = field(default_factory=list)


def _client_for(username: str):
    from instagrapi import Client

    account = get_account(username)
    cl = Client()
    binding = None
    try:
        binding = apply_proxy_to_client(cl, username, account, reason="publisher")
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


def _prompt_media_paths(kind: str) -> List[Path]:
    print("Ingresá rutas de archivo (una por línea, Enter vacío para finalizar):")
    paths: List[Path] = []
    while True:
        value = ask("› ").strip()
        if not value:
            break
        path = Path(value).expanduser()
        if not path.exists():
            warn(f"No existe el archivo: {path}")
            continue
        suffix = path.suffix.lower()
        if suffix not in _IMAGE_EXTS | _VIDEO_EXTS:
            warn("Formato no soportado (usa jpg/png/mp4/mov/webp).")
            continue
        paths.append(path)

    if not paths:
        warn("Debés indicar al menos un archivo.")
    if kind == "reel" and len(paths) > 1:
        warn("Para reels se usará sólo el primer archivo indicado.")
        paths = paths[:1]
    return paths


def _hash_for_path(path: Path) -> str:
    stat = path.stat()
    payload = f"{path.resolve()}::{stat.st_size}::{stat.st_mtime}".encode("utf-8")
    return hashlib.md5(payload).hexdigest()[:10]


def _ensure_moviepy():
    global _MOVIEPY_MODULE, _MOVIEPY_FAILED
    if _MOVIEPY_MODULE is not None:
        return _MOVIEPY_MODULE
    if _MOVIEPY_FAILED:
        raise RuntimeError(
            "moviepy==1.0.3 no está disponible. Instalalo manualmente para publicar videos."
        )
    try:
        import moviepy.editor as mpe  # type: ignore

        _MOVIEPY_MODULE = mpe
        return mpe
    except Exception:
        try:
            subprocess.check_call(
                [sys.executable, "-m", "pip", "install", "moviepy==1.0.3"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            import moviepy.editor as mpe  # type: ignore

            _MOVIEPY_MODULE = mpe
            return mpe
        except Exception as exc:  # pragma: no cover - depende del sistema
            _MOVIEPY_FAILED = True
            raise RuntimeError(
                "Error al instalar moviepy==1.0.3. Instalalo manualmente para subir videos."
            ) from exc


def _prepare_video(path: Path) -> Path:
    module = _ensure_moviepy()
    if path.suffix.lower() == ".mp4":
        return path
    output = PROCESSED_MEDIA_DIR / f"{path.stem}_{_hash_for_path(path)}.mp4"
    if output.exists() and output.stat().st_mtime >= path.stat().st_mtime:
        return output
    output.parent.mkdir(parents=True, exist_ok=True)
    clip = None
    target = None
    temp_audio = output.with_suffix(".temp-audio.m4a")
    try:
        clip = module.VideoFileClip(str(path))
        target = clip
        if getattr(clip, "h", 0) and clip.h > 1080:
            target = clip.resize(height=1080)
        target.write_videofile(
            str(output),
            codec="libx264",
            audio_codec="aac",
            temp_audiofile=str(temp_audio),
            remove_temp=True,
            bitrate="3500k",
            preset="medium",
            threads=max(os.cpu_count() or 2, 2),
            logger=None,
        )
    except Exception as exc:
        raise RuntimeError(f"No se pudo convertir el video {path.name}: {exc}") from exc
    finally:
        if target is not None:
            try:
                target.close()
            except Exception:
                pass
        if clip is not None and target is not clip:
            try:
                clip.close()
            except Exception:
                pass
        if temp_audio.exists():
            try:
                temp_audio.unlink()
            except Exception:
                pass
    return output


def _prepare_image(path: Path) -> Path:
    if path.suffix.lower() != ".webp":
        return path
    if Image is None:
        raise RuntimeError(
            "Se necesita Pillow>=8.1.1 para convertir imágenes .webp antes de publicarlas."
        )
    output = PROCESSED_MEDIA_DIR / f"{path.stem}_{_hash_for_path(path)}.jpg"
    if output.exists() and output.stat().st_mtime >= path.stat().st_mtime:
        return output
    output.parent.mkdir(parents=True, exist_ok=True)
    try:
        with Image.open(path) as img:
            img = img.convert("RGB")
            img.save(output, "JPEG", quality=92)
    except Exception as exc:
        raise RuntimeError(f"No se pudo convertir la imagen {path.name}: {exc}") from exc
    return output


def _prepare_media(path: Path) -> Path:
    suffix = path.suffix.lower()
    if suffix in _VIDEO_EXTS:
        return _prepare_video(path)
    if suffix == ".webp":
        return _prepare_image(path)
    return path


def _format_upload_error(kind: str, exc: Exception) -> str:
    return (
        f"Error al subir contenido ({kind}). Verificá el formato del archivo o la conexión de la cuenta seleccionada. "
        f"Detalle: {exc}"
    )


def _prompt_publish_job(kind: str) -> Optional[PublishJob]:
    media_paths = _prompt_media_paths(kind)
    if not media_paths:
        return None

    caption = ""
    first_comment = ""
    tags: tuple[str, ...] = ()
    sticker_link = ""
    overlay_text = ""
    share_to_feed = False
    cover_path: Optional[Path] = None

    if kind == "story":
        sticker_link = ask("Sticker link (opcional, URL): ").strip()
        overlay_text = ask_multiline("Texto overlay (opcional): ")
    elif kind == "post":
        caption = ask_multiline("Caption (multilínea, Enter vacío para terminar): ")
        first_comment = ask_multiline("Primer comentario (opcional): ")
        tags_raw = ask("Etiquetar usuarios (usernames separados por coma, opcional): ").strip()
        if tags_raw:
            tags = tuple(u.strip().lstrip("@") for u in tags_raw.split(",") if u.strip())
    else:  # reel
        caption = ask_multiline("Caption del reel: ")
        cover = ask("Cover opcional (ruta a imagen, Enter para omitir): ").strip()
        if cover:
            cover_path = Path(cover).expanduser()
            if not cover_path.exists():
                warn("La portada indicada no existe. Se omitirá.")
                cover_path = None
        share_to_feed = ask("¿Compartir también al feed? (s/N): ").strip().lower() == "s"

    mode = ask("Modo de publicación (1=Simultáneo, 2=Escalonado): ").strip()
    delay_mode = "simultaneous" if mode != "2" else "staggered"
    delay_min = delay_max = 0
    if delay_mode == "staggered":
        delay_min = ask_int(
            "Delay mínimo entre cuentas (segundos): ", min_value=0, default=SETTINGS.delay_min
        )
        delay_max = ask_int(
            "Delay máximo entre cuentas (segundos): ",
            min_value=delay_min,
            default=max(delay_min, SETTINGS.delay_max),
        )

    return PublishJob(
        kind=kind,
        media_paths=media_paths,
        caption=caption,
        first_comment=first_comment,
        tags=tags,
        sticker_link=sticker_link,
        overlay_text=overlay_text,
        share_to_feed=share_to_feed,
        cover_path=cover_path,
        delay_mode=delay_mode,
        delay_min=delay_min,
        delay_max=delay_max,
    )


def _append_publish_log(alias: str, username: str, job: PublishJob, success: bool, detail: str) -> None:
    hashed = hashlib.sha256((job.caption or job.overlay_text or "").encode("utf-8")).hexdigest()[:12]
    new_file = not PUBLISH_LOG.exists()
    with PUBLISH_LOG.open("a", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        if new_file:
            writer.writerow(
                ["fecha_hora", "alias", "cuenta", "tipo", "archivos", "caption_hash", "resultado", "detalle"]
            )
        writer.writerow(
            [
                time.strftime("%Y-%m-%d %H:%M:%S"),
                alias,
                username,
                job.kind,
                ";".join(path.name for path in job.media_paths),
                hashed,
                "ok" if success else "error",
                detail,
            ]
        )


def _validate_caption(kind: str, caption: str) -> str:
    if not caption:
        return ""
    limits = {"story": 200, "post": 2200, "reel": 2200}
    limit = limits.get(kind, 2200)
    if len(caption) <= limit:
        return caption
    warn(f"El caption supera {limit} caracteres y será truncado.")
    return caption[:limit]


def _resolve_usertags(client, usernames: Sequence[str]):
    if not usernames:
        return None
    try:
        from instagrapi.types import Usertag, UserShort
    except Exception as exc:  # pragma: no cover - instagrapi estructura
        logger.warning("No se pudieron cargar usertags: %s", exc, exc_info=False)
        return None

    tags = []
    for username in usernames:
        try:
            user_id = client.user_id_from_username(username)
        except Exception as exc:
            logger.warning("No se pudo resolver @%s para etiquetar: %s", username, exc)
            continue
        # posiciones aleatorias dentro de la imagen
        tags.append(
            Usertag(user=UserShort(pk=user_id, username=username), x=random.random(), y=random.random())
        )
    return tags or None


def _publish_story(alias: str, client, username: str, job: PublishJob, summary: PublishSummary) -> None:
    caption = _validate_caption("story", job.overlay_text)
    links = []
    if job.sticker_link:
        try:
            from instagrapi.types import StoryLink

            links = [StoryLink(webUri=job.sticker_link)]
        except Exception as exc:  # pragma: no cover - dependencia opcional
            logger.warning("No se pudo aplicar el sticker link: %s", exc)
    for media_path in job.media_paths:
        if STOP_EVENT.is_set():
            break
        try:
            if media_path.suffix.lower() in _VIDEO_EXTS:
                result = client.video_upload_to_story(str(media_path), caption=caption, links=links or None)
            else:
                result = client.photo_upload_to_story(str(media_path), caption=caption, links=links or None)
            summary.uploaded += 1
            summary.media_ids.append(getattr(result, "pk", ""))
            logger.info("@%s publicó historia %s", username, media_path.name)
            _append_publish_log(alias, username, job, True, media_path.name)
        except Exception as exc:
            summary.errors += 1
            message = _format_upload_error("historia", exc)
            summary.messages.append(message)
            logger.warning("%s", message)
            if should_retry_proxy(exc):
                record_proxy_failure(username, exc)
            _append_publish_log(alias, username, job, False, message)
        finally:
            sleep_with_stop(SETTINGS.delay_min)
            if STOP_EVENT.is_set():
                break


def _publish_post(alias: str, client, username: str, job: PublishJob, summary: PublishSummary) -> None:
    caption = _validate_caption("post", job.caption)
    usertags = _resolve_usertags(client, job.tags)
    try:
        if len(job.media_paths) > 1:
            result = client.album_upload([str(p) for p in job.media_paths], caption=caption, usertags=usertags)
        else:
            media = job.media_paths[0]
            if media.suffix.lower() in _VIDEO_EXTS:
                result = client.video_upload(str(media), caption=caption, usertags=usertags)
            else:
                result = client.photo_upload(str(media), caption=caption, usertags=usertags)
        summary.uploaded += 1
        media_pk = getattr(result, "pk", "")
        summary.media_ids.append(media_pk)
        if job.first_comment:
            try:
                client.media_comment(media_pk, job.first_comment)
            except Exception as exc:
                logger.warning("No se pudo publicar el primer comentario: %s", exc)
        logger.info("@%s publicó post con %s archivos", username, len(job.media_paths))
        _append_publish_log(alias, username, job, True, f"post:{len(job.media_paths)}")
    except Exception as exc:
        summary.errors += 1
        message = _format_upload_error("post", exc)
        summary.messages.append(message)
        logger.warning("%s", message)
        if should_retry_proxy(exc):
            record_proxy_failure(username, exc)
        _append_publish_log(alias, username, job, False, message)


def _publish_reel(alias: str, client, username: str, job: PublishJob, summary: PublishSummary) -> None:
    caption = _validate_caption("reel", job.caption)
    media = job.media_paths[0]
    cover = str(job.cover_path) if job.cover_path else None
    try:
        result = client.clip_upload(
            str(media),
            caption=caption,
            thumbnail=cover,
            share_to_feed=job.share_to_feed,
        )
        summary.uploaded += 1
        summary.media_ids.append(getattr(result, "pk", ""))
        logger.info("@%s publicó reel %s", username, media.name)
        _append_publish_log(alias, username, job, True, media.name)
    except Exception as exc:
        summary.errors += 1
        message = _format_upload_error("reel", exc)
        summary.messages.append(message)
        logger.warning("%s", message)
        if should_retry_proxy(exc):
            record_proxy_failure(username, exc)
        _append_publish_log(alias, username, job, False, message)


def _run_job_for_account(alias: str, username: str, job: PublishJob, queue: Queue) -> None:
    summary = PublishSummary(username=username)
    try:
        client = _client_for(username)
    except Exception as exc:
        summary.errors += 1
        summary.messages.append(str(exc))
        logger.error("No se pudo preparar @%s: %s", username, exc, exc_info=False)
        queue.put(summary)
        return

    try:
        if job.kind == "story":
            _publish_story(alias, client, username, job, summary)
        elif job.kind == "post":
            _publish_post(alias, client, username, job, summary)
        else:
            _publish_reel(alias, client, username, job, summary)
    finally:
        queue.put(summary)


def _summaries_from_queue(queue: Queue) -> List[PublishSummary]:
    summaries: List[PublishSummary] = []
    while True:
        try:
            summaries.append(queue.get_nowait())
        except Empty:
            break
    return summaries


def _print_summary(job: PublishJob, summaries: List[PublishSummary], start_time: float) -> None:
    elapsed = time.perf_counter() - start_time
    total_ok = sum(s.uploaded for s in summaries)
    total_err = sum(s.errors for s in summaries)

    print(full_line(color=Fore.MAGENTA))
    print(style_text("=== PUBLICACIÓN FINALIZADA ===", color=Fore.YELLOW, bold=True))
    print(style_text(f"Tipo: {job.kind}", color=Fore.CYAN, bold=True))
    print(style_text(f"Total exitosos: {total_ok}", color=Fore.GREEN, bold=True))
    print(style_text(f"Errores: {total_err}", color=Fore.RED if total_err else Fore.GREEN, bold=True))
    print(style_text(
        f"Tiempo total: {int(elapsed // 60):02d}:{int(elapsed % 60):02d}",
        color=Fore.WHITE,
        bold=True,
    ))
    print(full_line(color=Fore.MAGENTA))
    for summary in summaries:
        color = Fore.GREEN if summary.errors == 0 else Fore.YELLOW
        print(style_text(f"@{summary.username}: {summary.uploaded} OK / {summary.errors} errores", color=color, bold=True))
        for message in summary.messages:
            print(f"  - {message}")
    print(full_line(color=Fore.MAGENTA))


def run_from_menu(alias: str) -> None:
    banner()
    print(style_text("📤 Subir contenidos (Historias / Post / Reels)", color=Fore.CYAN, bold=True))
    print(full_line())
    usernames = _select_accounts(alias)
    if not usernames:
        return

    ready = [user for user in usernames if _ensure_account_ready(user)]
    if not ready:
        warn("Ninguna cuenta tiene sesión válida.")
        press_enter()
        return

    print("Tipo de contenido:")
    print("1) Historia")
    print("2) Post (feed)")
    print("3) Reel")
    choice = ask("Opción: ").strip()
    kind_map = {"1": "story", "2": "post", "3": "reel"}
    kind = kind_map.get(choice)
    if not kind:
        warn("Opción inválida.")
        press_enter()
        return

    job = _prompt_publish_job(kind)
    if not job:
        press_enter()
        return

    original_names = [path.name for path in job.media_paths]
    try:
        job.media_paths = [_prepare_media(path) for path in job.media_paths]
        if job.cover_path:
            job.cover_path = _prepare_image(job.cover_path)
    except RuntimeError as exc:
        warn(str(exc))
        press_enter()
        return

    print(full_line())
    print(style_text("Resumen de publicación", color=Fore.CYAN, bold=True))
    print(f"Cuentas seleccionadas: {', '.join('@'+u for u in ready)}")
    print(f"Archivos: {', '.join(original_names)}")
    if job.caption:
        print(f"Caption: {job.caption[:80]}{'…' if len(job.caption) > 80 else ''}")
    if job.overlay_text:
        print(f"Texto overlay: {job.overlay_text[:80]}{'…' if len(job.overlay_text) > 80 else ''}")
    confirm = ask("¿Confirmar publicación? (s/N): ").strip().lower()
    if confirm != "s":
        warn("Se canceló la publicación.")
        press_enter()
        return

    ensure_logging(quiet=SETTINGS.quiet, log_dir=SETTINGS.log_dir, log_file=SETTINGS.log_file)
    reset_stop_event()
    listener = start_q_listener("Presioná Q para cancelar la publicación.", logger)
    start_time = time.perf_counter()

    queue: Queue = Queue()
    threads: List[threading.Thread] = []

    try:
        for idx, username in enumerate(ready):
            if STOP_EVENT.is_set():
                break
            thread = threading.Thread(
                target=_run_job_for_account,
                args=(alias, username, job, queue),
                daemon=True,
                name=f"publisher-{username}",
            )
            thread.start()
            threads.append(thread)
            if job.delay_mode == "staggered" and idx < len(ready) - 1:
                delay = random.randint(job.delay_min, job.delay_max) if job.delay_max else job.delay_min
                if delay:
                    sleep_with_stop(delay)
                    if STOP_EVENT.is_set():
                        break
    finally:
        for thread in threads:
            thread.join()
        request_stop("publicación finalizada")
        listener.join(timeout=0.2)

    summaries = _summaries_from_queue(queue)
    _print_summary(job, summaries, start_time)
    ok("Publicación finalizada.")
    press_enter()
