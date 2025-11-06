"""Helpers to interact with Instagram direct messages via Playwright."""
from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from typing import Optional

try:  # pragma: no cover - optional dependency guard
    from playwright.async_api import Page
except ImportError:  # pragma: no cover
    class Page:  # type: ignore
        pass

from . import audit, human_engine


@dataclass
class MessengerResult:
    ok: bool
    action_blocked: bool = False
    message: str = ""


async def open_inbox(page: Page) -> MessengerResult:
    await page.goto("https://www.instagram.com/direct/inbox/", wait_until="networkidle")
    await human_engine.wait_for_navigation_idle(page)
    popup = await human_engine.detect_block_popup(page)
    if popup:
        return MessengerResult(False, action_blocked=True, message=popup)
    return MessengerResult(True)


async def open_composer(page: Page) -> MessengerResult:
    result = await human_engine.click(
        page,
        ["[role='button'][href='/direct/new/']", "text=Send message", "svg[aria-label='New message']"],
    )
    if not result.ok:
        return MessengerResult(False, action_blocked=result.action_blocked, message=result.message)
    await human_engine.wait_for_navigation_idle(page)
    return MessengerResult(True)


async def search_user(page: Page, username: str) -> MessengerResult:
    result = await human_engine.fill(page, ["input[name='queryBox']", "input[placeholder='Search...']"], username)
    if not result.ok:
        return MessengerResult(False, message=result.message)
    await human_engine.wait_for_navigation_idle(page)
    select = await human_engine.click(page, [f"text={username}", "[role='button'] span >> nth=0"])
    if not select.ok:
        return MessengerResult(False, message=select.message, action_blocked=select.action_blocked)
    confirm = await human_engine.click(page, ["text=Next", "button[type='button'] >> text=Chat"])
    if not confirm.ok:
        return MessengerResult(False, message=confirm.message, action_blocked=confirm.action_blocked)
    return MessengerResult(True)


async def open_thread(page: Page, username: str) -> MessengerResult:
    result = await human_engine.click(page, [f"text={username}", f"[href='/direct/t/{username}/']"])
    if not result.ok:
        return MessengerResult(False, message=result.message, action_blocked=result.action_blocked)
    await human_engine.wait_for_navigation_idle(page)
    return MessengerResult(True)


async def send_message(page: Page, text: str) -> MessengerResult:
    result = await human_engine.fill(page, ["textarea", "div[aria-label='Message']"], text)
    if not result.ok:
        return MessengerResult(False, message=result.message, action_blocked=result.action_blocked)
    submit = await human_engine.click(page, ["button[type='submit']", "text=Send"])
    if not submit.ok:
        return MessengerResult(False, message=submit.message, action_blocked=submit.action_blocked)
    await human_engine.wait_for_navigation_idle(page)
    popup = await human_engine.detect_block_popup(page)
    if popup:
        return MessengerResult(False, action_blocked=True, message=popup)
    return MessengerResult(True)


async def ensure_rate_limit(account: str, rate_limit_per_hour: Optional[int]) -> None:
    if not rate_limit_per_hour or rate_limit_per_hour <= 0:
        return
    jitter = max(1.0, float(os.getenv("OPTIN_JITTER_FACTOR", "0.2")))
    delay_seconds = 3600 / rate_limit_per_hour
    await asyncio.sleep(delay_seconds * (1 + (jitter * (0.5 - os.urandom(1)[0] / 255))))
