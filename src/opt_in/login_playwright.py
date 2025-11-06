"""Login helper implemented with Playwright and the human engine."""
from __future__ import annotations

import asyncio
from typing import Optional

import pyotp
try:  # pragma: no cover - optional dependency guard
    from playwright.async_api import Page
except ImportError:  # pragma: no cover
    class Page:  # type: ignore
        pass

from . import audit, human_engine, session_store


async def _handle_two_factor(page: Page, totp_secret: Optional[str]) -> None:
    if totp_secret:
        code = pyotp.TOTP(totp_secret).now()
        await human_engine.fill(page, ["input[name='verificationCode']", "input[name='security_code']"], code)
        await human_engine.click(page, ["button[type='submit']", "text=Confirm"])
        return

    await human_engine.click(page, ["text=Send code", "text=Send security code"])


async def perform_login(page: Page, username: str, password: str, totp_secret: Optional[str] = None) -> bool:
    """Perform a login attempt using Playwright."""

    audit.record_event(username, "login_attempt", "started")

    await page.goto("https://www.instagram.com/accounts/login/", wait_until="networkidle")
    await human_engine.wait_for_navigation_idle(page)

    result_user = await human_engine.fill(page, ["input[name='username']", "input[name='loginUsername']"], username)
    result_pwd = await human_engine.fill(page, ["input[name='password']", "input[name='loginPassword']"], password)
    if not (result_user.ok and result_pwd.ok):
        audit.record_event(username, "login_attempt", "failed", message="credentials_form_not_found")
        return False

    submit_result = await human_engine.click(page, ["button[type='submit']", "text=Log in"])
    if not submit_result.ok:
        audit.record_event(username, "login_attempt", "failed", message=submit_result.message)
        return False

    await human_engine.wait_for_navigation_idle(page, timeout=15000)
    popup = await human_engine.detect_block_popup(page)
    if popup:
        audit.record_event(username, "login_attempt", "failed", message=popup)
        return False

    if await page.query_selector("text=Enter confirmation code"):
        audit.record_event(username, "twofa_challenge", "pending")
        await _handle_two_factor(page, totp_secret)
        await human_engine.wait_for_navigation_idle(page, timeout=20000)

    if await page.query_selector("text=Save Your Login Info"):
        await human_engine.click(page, ["text=Save Info", "text=Save"])

    if await page.query_selector("text=Turn on Notifications"):
        await human_engine.click(page, ["text=Not Now"])

    await session_store.save_storage_state(page.context, username)
    audit.record_event(username, "login_success", "ok")
    return True
