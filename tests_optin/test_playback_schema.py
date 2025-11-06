from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import optin_browser.playback as playback
from optin_browser.config import cfg


def test_placeholders_are_replaced(tmp_path, monkeypatch):
    flow_path = tmp_path / "demo.json"
    flow_path.write_text(
        json.dumps(
            {
                "alias": "demo",
                "steps": [
                    {"action": "goto", "value": "https://example.com/${USER}"},
                    {"action": "fill", "selector": "input[name=user]", "value": "${USER}"},
                    {"action": "fill", "selector": "input[name=pass]", "value": "${PASSWORD}"},
                    {"action": "press", "value": "Enter"},
                ],
            }
        ),
        encoding="utf-8",
    )

    object.__setattr__(cfg, "flows_dir", tmp_path)

    fake_manager = MagicMock()
    fake_manager.__enter__.return_value = fake_manager
    fake_manager.__exit__.return_value = None
    fake_page = MagicMock()
    fake_manager.page = fake_page
    monkeypatch.setattr(playback, "BrowserManager", lambda account=None: fake_manager)

    player = playback.FlowPlayer("demo", {"USER": "alice", "PASSWORD": "secret"})
    player.run()

    fake_page.goto.assert_called_with("https://example.com/alice", wait_until="load")
    fake_page.fill.assert_any_call("input[name=user]", "alice")
    fake_page.fill.assert_any_call("input[name=pass]", "secret")
    fake_page.keyboard.press.assert_called_with("Enter")
