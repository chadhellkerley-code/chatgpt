"""Follow-up helper for Instagram conversations."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta

try:  # pragma: no cover - optional dependency guard
    from playwright.async_api import Page
except ImportError:  # pragma: no cover
    class Page:  # type: ignore
        pass

from . import audit, browser_manager, human_engine, messenger_playwright


async def _conversations_requiring_followup(page: Page, threshold_hours: int):
    candidates = []
    rows = await page.query_selector_all("[role='row']")
    cutoff = datetime.utcnow() - timedelta(hours=threshold_hours)
    for row in rows:
        ts_attr = await row.get_attribute("data-last-message-at")
        if not ts_attr:
            continue
        try:
            ts = datetime.fromisoformat(ts_attr)
        except ValueError:
            continue
        if ts < cutoff:
            username = await row.inner_text()
            candidates.append(username)
    return candidates


async def run_followup(account: str, hours: int, text: str) -> None:
    browser, context, page = await browser_manager.launch_browser(account)
    try:
        inbox = await messenger_playwright.open_inbox(page)
        if not inbox.ok:
            audit.record_event(account, "followup_inbox", "failed", message=inbox.message)
            return

        targets = await _conversations_requiring_followup(page, hours)
        for username in targets:
            open_result = await messenger_playwright.open_thread(page, username)
            if not open_result.ok:
                audit.record_event(account, "followup_open", "failed", message=open_result.message, meta={"thread": username})
                if open_result.action_blocked:
                    break
                continue
            send_result = await messenger_playwright.send_message(page, text)
            status = "ok" if send_result.ok else "blocked" if send_result.action_blocked else "failed"
            audit.record_event(account, "followup_send", status, message=send_result.message, meta={"thread": username})
            if send_result.action_blocked:
                break
            await human_engine.human_delay()
    finally:
        await browser_manager.close_browser_tuple(browser, context, page)
