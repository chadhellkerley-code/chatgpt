"""Utilidades para normalizar imágenes y videos antes de publicar."""

from __future__ import annotations

import hashlib
import logging
import os
import shutil
import subprocess
from pathlib import Path
from typing import Literal

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent
CACHE_DIR = ROOT / "storage" / "data" / "normalized_media"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

_IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".bmp"}
_EXTRA_IMAGE_EXTS = {".heic", ".heif", ".tif", ".tiff"}
_VIDEO_EXTS = {".mp4", ".mov", ".m4v", ".avi", ".mkv", ".webm"}

try:  # pragma: no cover - depende del entorno del operador
    from PIL import Image  # type: ignore

    try:  # Pillow ≥ 9.1 expone la enumeración Resampling
        _RESAMPLE_METHOD = Image.Resampling.LANCZOS  # type: ignore[attr-defined]
    except AttributeError:  # pragma: no cover - versiones antiguas
        _RESAMPLE_METHOD = getattr(Image, "ANTIALIAS", getattr(Image, "LANCZOS", Image.BICUBIC))
    else:  # pragma: no cover - asegurar compatibilidad retro
        if not hasattr(Image, "ANTIALIAS"):
            # MoviePy <=1.0.3 aún referencia Image.ANTIALIAS
            Image.ANTIALIAS = _RESAMPLE_METHOD  # type: ignore[attr-defined]
except Exception:  # noqa: S110
    Image = None  # type: ignore
    _RESAMPLE_METHOD = None

_MOVIEPY = None
_MOVIEPY_ERROR = None


def prepare_media_for_upload(
    path: str | Path,
    kind: Literal["story", "post", "reel"],
    *,
    output_dir: Path | None = None,
) -> dict:
    """Normaliza una imagen o video y devuelve los datos listos para subir."""

    src = Path(path).expanduser().resolve()
    if not src.exists():
        return {"ok": False, "reason": "file_not_found"}

    output_dir = output_dir or CACHE_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    suffix = src.suffix.lower()
    if suffix in _IMAGE_EXTS or suffix in _EXTRA_IMAGE_EXTS:
        result = normalize_image(src, target=_target_for_image(kind), output_dir=output_dir)
        if result.get("ok"):
            result.setdefault("thumb_path", result.get("media_path"))
        return result

    if suffix in _VIDEO_EXTS:
        return normalize_video(src, target=kind, output_dir=output_dir)

    if Image is not None:
        try:
            with Image.open(src):
                result = normalize_image(src, target=_target_for_image(kind), output_dir=output_dir)
                if result.get("ok"):
                    result.setdefault("thumb_path", result.get("media_path"))
                return result
        except Exception:
            pass

    return {"ok": False, "reason": f"unsupported_extension:{suffix}"}


def normalize_image(
    path: str | Path,
    *,
    target: Literal["story", "post", "reel_cover"],
    output_dir: Path | None = None,
) -> dict:
    """Convierte una imagen al preset adecuado para Instagram."""

    if Image is None:
        return {"ok": False, "reason": "missing_dependency:Pillow"}

    src = Path(path).expanduser().resolve()
    output_dir = output_dir or CACHE_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    suffix = src.suffix.lower()
    if suffix not in _IMAGE_EXTS and suffix not in _EXTRA_IMAGE_EXTS:
        return {"ok": False, "reason": f"unsupported_extension:{suffix}"}

    hash_part = _hash_for(src, target)
    output = output_dir / f"{src.stem}_{hash_part}.jpg"

    try:
        with Image.open(src) as img:
            img = img.convert("RGBA")
            background = Image.new("RGBA", img.size, (0, 0, 0, 255))
            background.paste(img, mask=img if img.mode == "RGBA" else None)
            img = background.convert("RGB")

            target_size = _target_image_size(target)
            img = _letterbox_image(img, target_size)

            img.save(output, format="JPEG", quality=85, optimize=True, progressive=True)
            width, height = img.size
    except Exception as exc:  # pragma: no cover
        return {"ok": False, "reason": f"image_error:{exc}"}

    return {
        "ok": True,
        "kind": "image",
        "media_path": str(output),
        "thumb_path": str(output),
        "meta": {"w": width, "h": height},
    }


def normalize_video(
    path: str | Path,
    *,
    target: Literal["story", "post", "reel"],
    output_dir: Path | None = None,
) -> dict:
    """Convierte un video al preset soportado por Instagram."""

    src = Path(path).expanduser().resolve()
    output_dir = output_dir or CACHE_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    hash_part = _hash_for(src, target)
    output = output_dir / f"{src.stem}_{hash_part}.mp4"

    if output.exists() and output.stat().st_mtime >= src.stat().st_mtime:
        meta = _probe_video(output)
        thumb = _ensure_thumbnail(output, is_video=True)
        if not thumb["ok"]:
            return {"ok": False, "reason": thumb["reason"]}
        return {
            "ok": True,
            "kind": "video",
            "media_path": str(output),
            "thumb_path": thumb.get("thumb_path"),
            "meta": meta or {},
        }

    ffmpeg_bin = shutil.which("ffmpeg")
    success = False
    ffmpeg_reason = None
    notices: list[str] = []
    if not ffmpeg_bin:
        notices.append(
            "⚠️ El archivo no pudo procesarse. Se intentará convertir automáticamente a un formato compatible..."
        )
    if ffmpeg_bin:
        filter_chain = _video_filter_for(target)
        cmd = [
            ffmpeg_bin,
            "-y",
            "-i",
            str(src),
            "-vf",
            filter_chain,
            "-c:v",
            "libx264",
            "-profile:v",
            "high",
            "-level",
            "4.1",
            "-pix_fmt",
            "yuv420p",
            "-r",
            "30",
            "-g",
            "60",
            "-keyint_min",
            "60",
            "-sc_threshold",
            "0",
            "-c:a",
            "aac",
            "-b:a",
            "128k",
            "-ar",
            "44100",
            "-movflags",
            "+faststart",
            str(output),
        ]
        try:
            subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            success = True
        except subprocess.CalledProcessError as exc:
            ffmpeg_reason = f"transcode_failed:{exc.returncode}"
            logger.warning("ffmpeg no pudo convertir %s: %s", src.name, exc)
            notices.append(
                "⚠️ El archivo no pudo procesarse. Se intentará convertir automáticamente a un formato compatible..."
            )

    if not success:
        moviepy = _ensure_moviepy()
        if moviepy is None:
            reason = ffmpeg_reason or "missing_dependency:moviepy==1.0.3"
            return {"ok": False, "reason": reason, "notices": notices}
        try:
            with moviepy.VideoFileClip(str(src)) as clip:
                target_size = _target_video_size(target)
                newsize = _moviepy_resize_size(clip.size, target_size)
                resized = clip.resize(newsize=newsize)
                resized.write_videofile(
                    str(output),
                    codec="libx264",
                    audio_codec="aac",
                    bitrate="3500k",
                    fps=30,
                    preset="medium",
                    threads=max(os.cpu_count() or 2, 2),
                    temp_audiofile=str(output.with_suffix(".temp.m4a")),
                    remove_temp=True,
                    audio=True,
                    verbose=False,
                    logger=None,
                )
            success = True
        except Exception as exc:  # pragma: no cover
            notices.append(
                "⚠️ El archivo no pudo procesarse. Se intentará convertir automáticamente a un formato compatible..."
            )
            return {"ok": False, "reason": f"moviepy_error:{exc}", "notices": notices}
        finally:
            temp_audio = output.with_suffix(".temp.m4a")
            if temp_audio.exists():
                temp_audio.unlink()

    meta = _probe_video(output)
    thumb = _ensure_thumbnail(output, is_video=True)
    if not thumb["ok"]:
        return {"ok": False, "reason": thumb["reason"], "notices": notices}

    return {
        "ok": True,
        "kind": "video",
        "media_path": str(output),
        "thumb_path": thumb.get("thumb_path"),
        "meta": meta or {},
        "notices": notices,
    }


def generate_thumbnail(media_path: str | Path, *, is_video: bool) -> dict:
    media = Path(media_path).resolve()
    if not media.exists():
        return {"ok": False, "reason": "thumb_error:file_not_found"}

    if not is_video:
        return {"ok": True, "thumb_path": str(media)}

    output = media.with_suffix(".thumb.jpg")
    ffmpeg_bin = shutil.which("ffmpeg")
    if ffmpeg_bin:
        cmd = [
            ffmpeg_bin,
            "-y",
            "-ss",
            "1",
            "-i",
            str(media),
            "-frames:v",
            "1",
            "-vf",
            "scale=1080:-2",
            str(output),
        ]
        try:
            subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            return {"ok": True, "thumb_path": str(output)}
        except subprocess.CalledProcessError as exc:
            logger.warning("ffmpeg no pudo generar thumbnail: %s", exc)

    moviepy = _ensure_moviepy()
    if moviepy is None:
        return {"ok": False, "reason": "thumb_error:missing_dependency:moviepy"}

    if Image is None:
        return {"ok": False, "reason": "thumb_error:missing_dependency:Pillow"}

    try:
        with moviepy.VideoFileClip(str(media)) as clip:
            frame = clip.get_frame(min(1.0, clip.duration or 1.0))
            img = Image.fromarray(frame)
            img = img.convert("RGB")
            img.save(output, format="JPEG", quality=85, optimize=True)
            return {"ok": True, "thumb_path": str(output)}
    except Exception as exc:  # pragma: no cover
        return {"ok": False, "reason": f"thumb_error:{exc}"}


def _ensure_thumbnail(path: Path, *, is_video: bool) -> dict:
    return generate_thumbnail(path, is_video=is_video)


def _hash_for(path: Path, target: str) -> str:
    data = f"{path.resolve()}::{path.stat().st_mtime}::{target}".encode()
    return hashlib.md5(data, usedforsecurity=False).hexdigest()[:12]


def _target_image_size(target: str) -> tuple[int, int]:
    if target in {"story", "reel_cover"}:
        return (1080, 1920)
    return (1080, 1350)


def _target_video_size(target: str) -> tuple[int, int]:
    if target in {"story", "reel"}:
        return (1080, 1920)
    return (1080, 1350)


def _letterbox_image(img, target_size: tuple[int, int]):  # type: ignore[no-untyped-def]
    width, height = img.size
    target_w, target_h = target_size
    scale = min(target_w / width, target_h / height)
    scaled_w = int(round(width * scale))
    scaled_h = int(round(height * scale))
    resample = _RESAMPLE_METHOD or getattr(Image, "LANCZOS", getattr(Image, "BICUBIC", Image.NEAREST))
    resized = img.resize((scaled_w, scaled_h), resample=resample)
    background = Image.new("RGB", target_size, (0, 0, 0))
    offset = ((target_w - scaled_w) // 2, (target_h - scaled_h) // 2)
    background.paste(resized, offset)
    return background


def _video_filter_for(target: str) -> str:
    if target in {"story", "reel"}:
        return (
            "scale='min(1080,iw)':'min(1920,ih)':force_original_aspect_ratio=decrease,"
            "pad=1080:1920:(1080-iw*min(1080/iw\,1920/ih))/2:(1920-ih*min(1080/iw\,1920/ih))/2:black"
        )
    return (
        "scale='min(1080,iw)':'min(1350,ih)':force_original_aspect_ratio=decrease,"
        "pad=1080:1350:(1080-iw*min(1080/iw\,1350/ih))/2:(1350-ih*min(1080/iw\,1350/ih))/2:black"
    )


def _probe_video(path: Path) -> dict:
    ffprobe = shutil.which("ffprobe")
    if ffprobe:
        cmd = [
            ffprobe,
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_entries",
            "stream=width,height,avg_frame_rate,duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            str(path),
        ]
        try:
            result = subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            width, height, fps, duration = result.stdout.strip().splitlines()
            fps_val = _parse_fps(fps)
            return {
                "w": int(float(width)),
                "h": int(float(height)),
                "fps": fps_val,
                "dur": float(duration) if duration else None,
            }
        except Exception as exc:  # pragma: no cover
            logger.debug("ffprobe no disponible: %s", exc)
    moviepy = _ensure_moviepy()
    if moviepy is not None:
        try:
            with moviepy.VideoFileClip(str(path)) as clip:
                fps_val = float(clip.fps) if clip.fps else None
                return {
                    "w": int(clip.w),
                    "h": int(clip.h),
                    "fps": fps_val,
                    "dur": float(clip.duration) if clip.duration else None,
                }
        except Exception as exc:  # pragma: no cover
            logger.debug("No se pudo obtener metadata de moviepy: %s", exc)
    return {}


def _parse_fps(value: str) -> float | None:
    if not value:
        return None
    if "/" in value:
        num, denom = value.split("/", 1)
        try:
            return float(num) / float(denom)
        except Exception:
            return None
    try:
        return float(value)
    except Exception:
        return None


def _target_for_image(kind: str) -> Literal["story", "post", "reel_cover"]:
    if kind == "post":
        return "post"
    if kind == "reel":
        return "reel_cover"
    return "story"


def _moviepy_resize_size(size: tuple[int, int], target: tuple[int, int]) -> tuple[int, int]:
    width, height = size
    target_w, target_h = target
    scale = min(target_w / width, target_h / height)
    return int(width * scale), int(height * scale)


def _ensure_moviepy():
    global _MOVIEPY, _MOVIEPY_ERROR
    if _MOVIEPY_ERROR is not None:
        return None
    if _MOVIEPY is not None:
        return _MOVIEPY
    try:  # pragma: no cover
        from moviepy import editor  # type: ignore

        _MOVIEPY = editor
    except Exception as exc:  # noqa: S110
        _MOVIEPY_ERROR = exc
        logger.debug("moviepy no disponible: %s", exc)
        return None
    return _MOVIEPY
