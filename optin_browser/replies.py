"""Utilities to reply to unread Instagram DMs."""
from __future__ import annotations

import re
from typing import Optional

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

from . import audit
from .browser_manager import BrowserManager
from .config import cfg
from .dm import _type_message

_UNREAD_THREAD_SELECTOR = "xpath=//a[contains(@aria-label, 'unread') or contains(@aria-label, 'No leído')]"
_MESSAGE_INPUT = "textarea[placeholder='Message...'], textarea[placeholder='Mensaje...']"


def reply_unread(account: str, matcher: Optional[str], reply_template: str) -> int:
    """Reply to unread conversations whose title matches the matcher."""
    audit.log_event("dm_reply_scan", account=account, matcher=matcher)
    pattern = re.compile(matcher, re.IGNORECASE) if matcher else None
    sent = 0

    with BrowserManager(account=account) as manager:
        page = manager.goto_instagram("direct/inbox/")
        try:
            page.wait_for_selector(_UNREAD_THREAD_SELECTOR, timeout=5000)
        except PlaywrightTimeoutError:
            return 0

        threads = page.locator(_UNREAD_THREAD_SELECTOR)
        for index in range(threads.count()):
            thread = threads.nth(index)
            title = thread.inner_text().strip()
            if pattern and not pattern.search(title):
                continue
            thread.click()
            message_box = page.wait_for_selector(_MESSAGE_INPUT, timeout=15000)
            response = reply_template.format(username=title)
            _type_message(message_box, response, typing_like=True)
            page.keyboard.press("Enter")
            sent += 1
            audit.log_event("dm_reply_sent", account=account, to=title, response_length=len(response))
            if cfg.send_cooldown_seconds:
                page.wait_for_timeout(cfg.send_cooldown_seconds * 1000)

    return sent
