"""Execute recorded flows with placeholder substitution."""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Dict, List, Optional

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

from .browser_manager import BrowserManager
from .config import cfg
from .utils import cycle_placeholders, normalize_selector


@dataclass
class FlowAction:
    action: str
    selector: Optional[str]
    value: Optional[str]


class FlowPlaybackError(RuntimeError):
    """Raised when playback cannot complete."""


class FlowPlayer:
    """Load and execute a recorded flow."""

    def __init__(self, alias: str, variables: Optional[Dict[str, str]] = None) -> None:
        self.alias = alias
        self.variables = variables or {}
        self.path = cfg.flows_dir / f"{alias}.json"
        if not self.path.exists():
            raise FlowPlaybackError(f"No existe un flujo guardado con alias '{alias}'.")
        self.actions = self._load_actions()

    def _load_actions(self) -> List[FlowAction]:
        data = json.loads(self.path.read_text(encoding="utf-8"))
        steps = data.get("steps", [])
        actions: List[FlowAction] = []
        for step in steps:
            actions.append(
                FlowAction(
                    action=step.get("action"),
                    selector=step.get("selector"),
                    value=step.get("value"),
                )
            )
        return actions

    def run(self, account: Optional[str] = None) -> None:
        with BrowserManager(account=account) as manager:
            page = manager.page
            if not page:
                raise FlowPlaybackError("No se pudo abrir la página de Playwright.")
            for action in self.actions:
                if action.action == "goto":
                    target = cycle_placeholders(action.value or "", self.variables)
                    page.goto(target, wait_until="load")
                elif action.action == "click":
                    selector = normalize_selector(action.selector or "")
                    page.click(selector)
                elif action.action == "fill":
                    selector = normalize_selector(action.selector or "")
                    value = cycle_placeholders(action.value or "", self.variables)
                    page.fill(selector, value)
                elif action.action == "press":
                    key = cycle_placeholders(action.value or "", self.variables)
                    page.keyboard.press(key)
                elif action.action == "wait_for":
                    selector = normalize_selector(action.selector or "")
                    try:
                        page.wait_for_selector(selector, state="visible", timeout=20000)
                    except PlaywrightTimeoutError:
                        raise FlowPlaybackError(
                            f"El selector '{selector}' no apareció a tiempo."
                        ) from None
                else:
                    raise FlowPlaybackError(f"Acción desconocida: {action.action}")
