import asyncio
import os
from pathlib import Path

import pytest
from cryptography.fernet import Fernet

from src.opt_in import session_store


class DummyContext:
    def __init__(self, state):
        self._state = state

    async def storage_state(self):
        return self._state


def test_encryption_roundtrip(tmp_path, monkeypatch):
    key = Fernet.generate_key().decode("utf-8")
    monkeypatch.setenv("SESSION_ENCRYPTION_KEY", key)
    monkeypatch.setattr(session_store, "_STORAGE_DIR", tmp_path)

    state = {"cookies": ["foo"]}
    ctx = DummyContext(state)
    asyncio.run(session_store.save_storage_state(ctx, "account1"))

    raw = next(tmp_path.iterdir()).read_bytes()
    assert raw != b"{}"

    loaded = asyncio.run(session_store.load_storage_state_dict("account1"))
    assert loaded == state
