"""Minimal recorder to capture Instagram flows for later playback."""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

from .browser_manager import BrowserManager
from .config import cfg
from .utils import normalize_selector


@dataclass
class FlowStep:
    action: str
    selector: Optional[str] = None
    value: Optional[str] = None


@dataclass
class RecordedFlow:
    alias: str
    created_at: str
    steps: List[FlowStep]

    def to_dict(self) -> Dict[str, object]:
        return {
            "alias": self.alias,
            "created_at": self.created_at,
            "steps": [asdict(step) for step in self.steps],
        }


class FlowRecorder:
    """Interactive helper to build flows step by step."""

    def __init__(self, alias: str) -> None:
        self.alias = alias
        self.steps: List[FlowStep] = []

    def record(self) -> Path:
        print("Graba acciones en orden. Comandos: goto, click, fill, press, wait_for, stop")
        with BrowserManager(account=None) as manager:
            page = manager.page
            if not page:
                raise RuntimeError("No se pudo iniciar Playwright.")
            while True:
                command = input("acción> ").strip().lower()
                if command in {"stop", "salir", "fin"}:
                    break
                if command == "goto":
                    url = input("URL destino> ").strip()
                    page.goto(url, wait_until="load")
                    self.steps.append(FlowStep(action="goto", value=url))
                elif command == "click":
                    selector = normalize_selector(input("selector> ").strip())
                    page.click(selector)
                    self.steps.append(FlowStep(action="click", selector=selector))
                elif command == "fill":
                    selector = normalize_selector(input("selector> ").strip())
                    value = input("valor (usa ${PLACEHOLDER} si aplica)> ")
                    page.fill(selector, value)
                    self.steps.append(FlowStep(action="fill", selector=selector, value=value))
                elif command == "press":
                    key = input("tecla (por ej. Enter)> ").strip()
                    page.keyboard.press(key)
                    self.steps.append(FlowStep(action="press", value=key))
                elif command in {"wait", "wait_for"}:
                    selector = normalize_selector(input("selector a esperar> ").strip())
                    try:
                        page.wait_for_selector(selector, state="visible", timeout=15000)
                    except PlaywrightTimeoutError:
                        print("Aviso: no apareció antes de 15s, pero se guardará igual.")
                    self.steps.append(FlowStep(action="wait_for", selector=selector))
                else:
                    print("Comando desconocido. Usa goto/click/fill/press/wait_for/stop")

        flow = RecordedFlow(
            alias=self.alias,
            created_at=datetime.now(timezone.utc).isoformat(),
            steps=self.steps,
        )
        path = cfg.flows_dir / f"{self.alias}.json"
        with path.open("w", encoding="utf-8") as handle:
            json.dump(flow.to_dict(), handle, ensure_ascii=False, indent=2)
        print(f"Flujo guardado en {path}")
        return path


def record_flow(alias: str) -> Path:
    """Convenience function to start a recording session."""
    recorder = FlowRecorder(alias)
    return recorder.record()
