"""Utility helpers shared across opt-in browser modules."""
from __future__ import annotations

import random
import time
from typing import Iterable, Optional


def human_sleep(min_ms: int, max_ms: int) -> None:
    """Sleep for a human-like random duration between min_ms and max_ms."""
    if max_ms < min_ms:
        max_ms = min_ms
    delay = random.uniform(min_ms / 1000.0, max_ms / 1000.0)
    time.sleep(delay)


def bounded_sleep(seconds: float) -> None:
    """Sleep for the provided seconds if greater than zero."""
    if seconds > 0:
        time.sleep(seconds)


def normalize_selector(selector: str) -> str:
    """Normalize whitespace within selectors for consistency."""
    return " ".join(selector.split())


def cycle_placeholders(text: str, substitutions: Optional[dict[str, str]] = None) -> str:
    """Replace placeholders of the form ${KEY} with substitutions provided."""
    if not substitutions:
        return text
    result = text
    for key, value in substitutions.items():
        result = result.replace(f"${{{key}}}", value)
    return result


def ensure_iterable(value: Optional[Iterable[str]]) -> Iterable[str]:
    """Return an empty tuple when value is None to ease iteration."""
    return value or ()
