# runtime.py
# -*- coding: utf-8 -*-
"""Coordinación de ejecución compartida (eventos de stop y logging)."""

from __future__ import annotations

import logging
import os
import random
import select
import sys
import threading
import time
from pathlib import Path

STOP_EVENT = threading.Event()


def reset_stop_event() -> None:
    """Limpia el estado del evento global antes de iniciar un flujo."""
    STOP_EVENT.clear()


def request_stop(reason: str) -> None:
    if not STOP_EVENT.is_set():
        logging.getLogger("runtime").info("Deteniendo ejecución: %s", reason)
        STOP_EVENT.set()


def ensure_logging(
    level: int = logging.INFO,
    *,
    quiet: bool = False,
    log_dir: Path | None = None,
    log_file: str = "app.log",
) -> None:
    root = logging.getLogger()
    if root.handlers:
        return

    root.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    handlers: list[logging.Handler] = []
    if log_dir:
        try:
            Path(log_dir).mkdir(parents=True, exist_ok=True)
            file_handler = logging.FileHandler(Path(log_dir) / log_file, encoding="utf-8")
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(formatter)
            handlers.append(file_handler)
        except Exception:
            pass

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(level if not quiet else logging.WARNING)
    stream_handler.setFormatter(formatter)
    handlers.append(stream_handler)

    for handler in handlers:
        root.addHandler(handler)

    logging.getLogger("instagrapi").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def start_q_listener(message: str, logger: logging.Logger) -> threading.Thread:
    def _watch() -> None:
        suffix = "" if os.name == "nt" else " y Enter"
        logger.info("%s%s", message, suffix)
        while not STOP_EVENT.is_set():
            try:
                if os.name == "nt":
                    import msvcrt  # type: ignore

                    if msvcrt.kbhit():
                        ch = msvcrt.getwch()
                        if ch.lower() == "q":
                            request_stop("se presionó Q")
                            break
                else:
                    ready, _, _ = select.select([sys.stdin], [], [], 0.2)
                    if ready:
                        ch = sys.stdin.readline().strip().lower()
                        if ch == "q":
                            request_stop("se presionó Q")
                            break
            except Exception:
                time.sleep(0.3)
            time.sleep(0.1)

    listener = threading.Thread(target=_watch, daemon=True)
    listener.start()
    return listener


def jitter_delay(min_seconds: int, max_seconds: int) -> int:
    if max_seconds <= min_seconds:
        return max(min_seconds, 0)
    return random.randint(min_seconds, max_seconds)


def sleep_with_stop(total_seconds: int, *, step: float = 1.0) -> None:
    slept = 0.0
    while slept < total_seconds and not STOP_EVENT.is_set():
        interval = min(step, total_seconds - slept)
        time.sleep(interval)
        slept += interval
