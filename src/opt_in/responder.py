"""Utilities to reply to unread Instagram conversations."""
from __future__ import annotations

import asyncio
from typing import Optional

try:  # pragma: no cover - optional dependency guard
    from playwright.async_api import Page
except ImportError:  # pragma: no cover
    class Page:  # type: ignore
        pass

from . import audit, browser_manager, human_engine, messenger_playwright


async def _iter_unread_threads(page: Page, limit: int):
    count = 0
    while count < limit:
        nodes = await page.query_selector_all("[role='row'] a")
        for node in nodes:
            aria = await node.get_attribute("aria-label")
            if aria and "unread" in aria.lower():
                yield node
                count += 1
                if count >= limit:
                    break
        break


async def respond_unread(account: str, response_text: str, limit: int = 3) -> None:
    browser, context, page = await browser_manager.launch_browser(account)
    try:
        inbox = await messenger_playwright.open_inbox(page)
        if not inbox.ok:
            audit.record_event(account, "responder_inbox", "failed", message=inbox.message)
            return

        async for node in _iter_unread_threads(page, limit):
            username = await node.inner_text()
            await node.click()
            await human_engine.wait_for_navigation_idle(page)
            result = await messenger_playwright.send_message(page, response_text)
            if not result.ok:
                status = "blocked" if result.action_blocked else "failed"
                audit.record_event(account, "responder_reply", status, message=result.message, meta={"thread": username})
                if result.action_blocked:
                    break
            else:
                audit.record_event(account, "responder_reply", "ok", meta={"thread": username})
            await human_engine.human_delay()
    finally:
        await browser_manager.close_browser_tuple(browser, context, page)
