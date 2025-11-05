from __future__ import annotations

import importlib
import json
import sys
import threading
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import playwright_service


class _Recorder:
    def __init__(self):
        self.loaded = None

    def get_settings(self):
        return {"value": "seed"}

    def load_settings(self, path: str) -> None:
        with open(path, "r", encoding="utf-8") as handle:
            self.loaded = json.load(handle)


def _reload_session_store(monkeypatch, tmp_path: Path, encryption_key: str | None = None):
    monkeypatch.setenv("APP_DATA_ROOT", str(tmp_path))
    if encryption_key is None:
        monkeypatch.delenv("SESSION_ENCRYPTION_KEY", raising=False)
    else:
        monkeypatch.setenv("SESSION_ENCRYPTION_KEY", encryption_key)
    sys.modules.pop("session_store", None)
    import session_store  # type: ignore

    return importlib.reload(session_store)


def test_build_two_factor_payload_prefers_totp(monkeypatch):
    account = {"username": "tester", "password": "secret"}
    session = playwright_service.InstagramPlaywrightSession(
        account,
        headless=True,
        two_factor_code_provider=lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(playwright_service, "generate_totp_code", lambda username: "654321")
    payload = session._build_two_factor_payload()
    assert payload is not None
    assert payload.code == "654321"
    assert payload.label == "TOTP"


def test_manual_two_factor_payload_via_provider(monkeypatch):
    account = {"username": "tester", "password": "secret"}
    captured = []

    def provider(username, method, attempt, timeout):
        captured.append((username, method, attempt, timeout))
        return "123 456"

    session = playwright_service.InstagramPlaywrightSession(
        account,
        headless=True,
        two_factor_code_provider=provider,
    )
    payload = session._manual_two_factor_payload("sms", 1)
    assert payload is not None
    assert payload.code == "123456"
    assert captured[0][0] == "tester"
    assert captured[0][1] == "sms"


def test_session_store_roundtrip(monkeypatch, tmp_path):
    store = _reload_session_store(monkeypatch, tmp_path)
    client = _Recorder()
    saved = store.save_from(client, "tester")
    assert saved.exists()

    loader = _Recorder()
    store.load_into(loader, "tester")
    assert loader.loaded == {"value": "seed"}


def test_session_store_concurrent_writes(monkeypatch, tmp_path):
    store = _reload_session_store(monkeypatch, tmp_path)

    barrier = threading.Barrier(2)

    class SlowClient:
        def __init__(self, value: str):
            self._value = value

        def get_settings(self):
            barrier.wait()
            time.sleep(0.1)
            return {"value": self._value}

    clients = [SlowClient("A"), SlowClient("B")]
    threads = [threading.Thread(target=store.save_from, args=(client, "tester")) for client in clients]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    loader = _Recorder()
    store.load_into(loader, "tester")
    assert loader.loaded["value"] in {"A", "B"}


def test_session_store_encryption(monkeypatch, tmp_path):
    store = _reload_session_store(monkeypatch, tmp_path, encryption_key="s3cret-key")
    client = _Recorder()
    path = store.save_from(client, "tester")
    raw = path.read_bytes()
    assert b"seed" not in raw

    loader = _Recorder()
    store.load_into(loader, "tester")
    assert loader.loaded == {"value": "seed"}
