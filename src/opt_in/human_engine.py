"""Human like interaction helpers used by the opt-in flows."""
from __future__ import annotations

import asyncio
import os
import random
from dataclasses import dataclass
from typing import Optional, Sequence

try:  # pragma: no cover - optional dependency guard
    from playwright.async_api import Page, TimeoutError as PlaywrightTimeoutError
except ImportError:  # pragma: no cover
    class Page:  # type: ignore
        pass

    class PlaywrightTimeoutError(Exception):
        pass

DEFAULT_DELAY_MIN = 2.5
DEFAULT_DELAY_MAX = 4.0


@dataclass
class StepResult:
    """Result returned by interaction helpers."""

    ok: bool
    action_blocked: bool = False
    message: str = ""


def _read_delay_pair() -> Sequence[float]:
    try:
        min_v = float(os.getenv("DELAY_MIN_S", DEFAULT_DELAY_MIN))
        max_v = float(os.getenv("DELAY_MAX_S", DEFAULT_DELAY_MAX))
    except ValueError:
        return DEFAULT_DELAY_MIN, DEFAULT_DELAY_MAX
    if min_v > max_v:
        min_v, max_v = max_v, min_v
    return min_v, max_v


async def human_delay(multiplier: float = 1.0) -> None:
    """Sleep for a randomized amount of time between the configured bounds."""

    low, high = _read_delay_pair()
    delay = random.uniform(low, high) * max(multiplier, 0.1)
    await asyncio.sleep(delay)


async def _type_like_human(page: Page, selector: str, text: str) -> None:
    for char in text:
        await page.keyboard.type(char)
        await asyncio.sleep(random.uniform(0.05, 0.2))


async def wait_for_navigation_idle(page: Page, timeout: float = 5000) -> None:
    try:
        await page.wait_for_load_state("networkidle", timeout=timeout)
    except PlaywrightTimeoutError:
        return


async def _resolve_selector(page: Page, selectors: Sequence[str], timeout: float) -> Optional[str]:
    for selector in selectors:
        try:
            await page.wait_for_selector(selector, timeout=timeout)
            return selector
        except PlaywrightTimeoutError:
            continue
    return None


async def click(page: Page, selectors: Sequence[str], timeout: float = 5000) -> StepResult:
    await human_delay(0.2)
    selector = await _resolve_selector(page, selectors, timeout)
    if not selector:
        return StepResult(False, message="selector_not_found")
    try:
        await page.hover(selector)
        await asyncio.sleep(random.uniform(0.1, 0.3))
        await page.click(selector)
        await human_delay(0.3)
        return StepResult(True)
    except PlaywrightTimeoutError:
        return StepResult(False, message="timeout")
    except Exception as exc:  # pragma: no cover - best effort guard
        if "Action Blocked" in str(exc):
            return StepResult(False, action_blocked=True, message="action_blocked")
        return StepResult(False, message=str(exc))


async def fill(page: Page, selectors: Sequence[str], text: str, timeout: float = 5000) -> StepResult:
    selector = await _resolve_selector(page, selectors, timeout)
    if not selector:
        return StepResult(False, message="selector_not_found")
    try:
        await page.click(selector)
        await human_delay(0.2)
        await page.fill(selector, "")
        await _type_like_human(page, selector, text)
        await human_delay(0.2)
        return StepResult(True)
    except PlaywrightTimeoutError:
        return StepResult(False, message="timeout")
    except Exception as exc:  # pragma: no cover
        if "Action Blocked" in str(exc):
            return StepResult(False, action_blocked=True, message="action_blocked")
        return StepResult(False, message=str(exc))


async def type_text(page: Page, text: str) -> None:
    await _type_like_human(page, "", text)


async def detect_block_popup(page: Page) -> Optional[str]:
    patterns = [
        "Action Blocked",
        "Try again later",
        "Daily limit",
    ]
    content = await page.content()
    for pattern in patterns:
        if pattern.lower() in content.lower():
            return pattern
    return None


async def assert_selector(page: Page, selectors: Sequence[str], timeout: float = 5000) -> StepResult:
    selector = await _resolve_selector(page, selectors, timeout)
    if not selector:
        return StepResult(False, message="selector_not_found")
    return StepResult(True)
