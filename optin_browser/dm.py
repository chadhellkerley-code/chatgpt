"""Direct message helpers for the opt-in toolkit."""
from __future__ import annotations

import json
from typing import Optional

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

from . import audit
from .browser_manager import BrowserManager
from .config import cfg
from .utils import human_sleep

_SEARCH_INPUT = "input[placeholder='Search'], input[placeholder='Buscar']"
_MESSAGE_INPUT = "textarea[placeholder='Message...'], textarea[placeholder='Mensaje...']"


class DirectMessageError(RuntimeError):
    """Raised when a direct message cannot be delivered."""


def _type_message(textbox, text: str, typing_like: bool) -> None:
    if typing_like:
        for char in text:
            human_sleep(cfg.typing_min_ms, cfg.typing_max_ms)
            textbox.type(char)
    else:
        textbox.fill(text)


def send_dm(account: str, to_username: str, text: str, *, typing_like: bool = True, max_wait: Optional[int] = None) -> None:
    """Send a direct message to ``to_username`` using the stored session."""
    audit.log_event("dm_attempt", account=account, to=to_username)
    try:
        with BrowserManager(account=account) as manager:
            page = manager.goto_instagram("direct/inbox/")
            search_box = page.wait_for_selector(_SEARCH_INPUT, timeout=20000)
            search_box.fill(to_username)
            page.keyboard.press("Enter")
            page.keyboard.press("Enter")

            message_box = page.wait_for_selector(_MESSAGE_INPUT, timeout=max_wait or 20000)
            _type_message(message_box, text, typing_like)
            page.keyboard.press("Enter")

            snippet = text[-20:] if text else ""
            bubble_selector = f"xpath=//div[contains(@class, '_ab5z') and contains(., {json.dumps(snippet)})]"
            page.wait_for_selector(bubble_selector, timeout=max_wait or 20000)
            audit.log_event("dm_sent", account=account, to=to_username, length=len(text))
    except PlaywrightTimeoutError as exc:
        audit.log_event("dm_failed", account=account, to=to_username, reason="timeout")
        raise DirectMessageError("No se pudo enviar el mensaje a tiempo.") from exc
    except Exception as exc:  # noqa: BLE001
        audit.log_event("dm_failed", account=account, to=to_username, reason=str(exc))
        raise
