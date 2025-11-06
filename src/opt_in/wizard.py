"""Opt-in wizard helpers."""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

from . import audit

FLOW_TEMPLATES = {
    "login": {"steps": []},
    "send": {"steps": []},
    "reply": {"steps": []},
    "followup": {"steps": []},
}


@dataclass
class WizardSummary:
    flows: Dict[str, str]


def _flows_dir() -> Path:
    path = Path("data/flows")
    path.mkdir(parents=True, exist_ok=True)
    return path


def initialize_flows() -> WizardSummary:
    flows: Dict[str, str] = {}
    for alias, template in FLOW_TEMPLATES.items():
        path = _flows_dir() / f"{alias}_master.json"
        if not path.exists():
            path.write_text(json.dumps(template, indent=2), encoding="utf-8")
        flows[alias] = str(path)
    summary_path = _flows_dir() / "WIZARD_SUMMARY.json"
    summary_path.write_text(json.dumps({"flows": flows}, indent=2), encoding="utf-8")
    audit.record_event("wizard", "initialize", "ok", meta=flows)
    return WizardSummary(flows=flows)
