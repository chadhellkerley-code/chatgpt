"""Generic flow runner used by the opt-in Playwright tooling."""
from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Any, Dict, Optional

from playwright.async_api import Page

from . import audit, browser_manager, human_engine, login_playwright, session_store

FLOW_DIR = Path("data/flows")


class FlowError(Exception):
    pass


async def _load_flow(name: str) -> Dict[str, Any]:
    FLOW_DIR.mkdir(parents=True, exist_ok=True)
    path = FLOW_DIR / f"{name}_master.json"
    if not path.exists():
        raise FlowError(f"Flow {name!r} not found at {path}")
    return json.loads(path.read_text(encoding="utf-8"))


async def _execute_step(page: Page, step: Dict[str, Any], variables: Dict[str, Any]) -> None:
    op = step.get("op")
    if not op:
        raise FlowError("step missing op")

    selectors = step.get("selectors") or []

    if op == "goto":
        await page.goto(step["url"], wait_until="networkidle")
        await human_engine.wait_for_navigation_idle(page)
    elif op == "click":
        result = await human_engine.click(page, selectors)
        if not result.ok:
            raise FlowError(f"click failed: {result.message}")
    elif op == "fill":
        text = step.get("text") or variables.get(step.get("var"), "")
        result = await human_engine.fill(page, selectors, text)
        if not result.ok:
            raise FlowError(f"fill failed: {result.message}")
    elif op == "type":
        text = step.get("text") or variables.get(step.get("var"), "")
        await human_engine.type_text(page, text)
    elif op == "wait_for":
        await human_engine.assert_selector(page, selectors, timeout=step.get("timeout", 5000))
    elif op == "assert_selector":
        result = await human_engine.assert_selector(page, selectors, timeout=step.get("timeout", 5000))
        if not result.ok:
            raise FlowError(f"assert_selector failed: {result.message}")
    elif op == "sleep":
        await asyncio.sleep(step.get("seconds", 1))
    elif op == "prompt_otp":
        # Placeholder: in real usage the OTP should be entered manually by the operator.
        raise FlowError("prompt_otp requires manual intervention")
    elif op == "auto_totp":
        secret = variables.get("totp_secret") or os.getenv("OPTIN_IG_TOTP")
        if not secret:
            raise FlowError("auto_totp requested but totp_secret not available")
        from pyotp import TOTP

        code = TOTP(secret).now()
        await human_engine.fill(page, selectors or ["input"], code)
    else:
        raise FlowError(f"Unsupported op: {op}")


async def _run_flow_once(page: Page, flow: Dict[str, Any], variables: Dict[str, Any]) -> None:
    for step in flow.get("steps", []):
        await _execute_step(page, step, variables)


async def run_flow(alias: str, variables: Dict[str, Any], account: str, custom_js_path: Optional[str] = None) -> None:
    """Run the flow identified by ``alias`` for the given ``account``."""

    flow = await _load_flow(alias)
    try:
        login_flow = await _load_flow("login") if alias != "login" else None
    except FlowError:
        login_flow = None

    browser, context, page = await browser_manager.launch_browser(account)
    try:
        try:
            await _run_flow_once(page, flow, variables)
        except FlowError:
            if login_flow is None:
                raise
            await _run_flow_once(page, login_flow, variables)
            await session_store.save_storage_state(context, account)
            await _run_flow_once(page, flow, variables)
    finally:
        await browser_manager.close_browser_tuple(browser, context, page)
