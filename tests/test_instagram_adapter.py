from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from instagram_adapter import InstagramClientAdapter, TwoFARequired


class FakeClient:
    def __init__(self, settings=None, *, info=None, raise_two_factor=True):
        self.settings = settings or {}
        self.last_json = {"two_factor_info": info or {}}
        self.login_args = None
        self._raise_two_factor = raise_two_factor
        self.private_calls = []
        self.phone_id = "phone"
        self.token = "token"
        self.uuid = "uuid"
        self.android_device_id = "device"
        self.cookie_dict = {}
        self.last_response = SimpleNamespace(headers={})
        self._settings = settings or {}

    def login(self, username, password, verification_code=""):
        self.login_args = (username, password, verification_code)
        if self._raise_two_factor:
            from instagrapi import exceptions as ig_exceptions

            raise ig_exceptions.TwoFactorRequired(message="two_factor_required")
        return True

    def private_request(self, endpoint, payload, login=True):
        self.private_calls.append((endpoint, payload))
        return {"status": "ok"}

    def get_settings(self):
        return self._settings or {"device_settings": {"model": "x"}}

    def set_settings(self, value):
        self._settings = value

    def dump_settings(self, path):  # pragma: no cover - compat hook
        raise NotImplementedError

    def load_settings(self, path):  # pragma: no cover - compat hook
        raise NotImplementedError

    def parse_authorization(self, header):
        return {"sessionid": "abc", "ds_user_id": "1"}

    def login_flow(self):
        return True


def _adapter_with(info, *, raise_two_factor=True):
    def factory(settings=None):
        return FakeClient(settings=settings, info=info, raise_two_factor=raise_two_factor)

    return InstagramClientAdapter(client_factory=factory)


def test_do_login_triggers_whatsapp_two_factor():
    info = {
        "two_factor_identifier": "id-123",
        "whatsapp_two_factor_on": True,
        "sms_two_factor_on": True,
    }
    adapter = _adapter_with(info)

    with pytest.raises(TwoFARequired) as ctx:
        adapter.do_login("tester", "secret")

    assert ctx.value.method == "whatsapp"
    client = adapter._client  # type: ignore[attr-defined]
    assert client.private_calls[0][0] == "accounts/send_two_factor_login_whatsapp/"


def test_do_login_fallbacks_to_sms():
    info = {"two_factor_identifier": "id-456", "sms_two_factor_on": True}
    adapter = _adapter_with(info)

    with pytest.raises(TwoFARequired) as ctx:
        adapter.do_login("tester", "secret")

    assert ctx.value.method == "sms"
    client = adapter._client  # type: ignore[attr-defined]
    assert client.private_calls[0][0] == "accounts/send_two_factor_login_sms/"


def test_do_login_uses_totp(monkeypatch):
    info = {}
    adapter = _adapter_with(info, raise_two_factor=False)

    monkeypatch.setattr("instagram_adapter.generate_totp_code", lambda username: "654321")
    adapter.do_login("tester", "secret")
    client = adapter._client  # type: ignore[attr-defined]
    assert client.login_args[2] == "654321"


def test_session_roundtrip():
    info = {}
    adapter = _adapter_with(info, raise_two_factor=False)
    data = {"device_settings": {"model": "Edge"}}
    adapter.load_session(data)
    dumped = adapter.dump_session()
    assert dumped["device_settings"]["model"] == "Edge"
