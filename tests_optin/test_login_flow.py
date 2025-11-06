import optin_browser.login as login


class FakeLocator:
    def __init__(self) -> None:
        self.clicked = 0

    def is_enabled(self) -> bool:
        return True

    def click(self) -> None:
        self.clicked += 1


class FakePage:
    def __init__(self, locator: FakeLocator) -> None:
        self._locator = locator

    def locator(self, selector: str) -> FakeLocator:
        return self._locator


def test_sms_flow_requests_code_before_prompt(monkeypatch):
    locator = FakeLocator()
    page = FakePage(locator)

    prompts = iter(["", "", "654321"])
    monkeypatch.setattr(login.getpass, "getpass", lambda _: next(prompts))

    times = iter([0, 10, 120, 130])
    monkeypatch.setattr(login.time, "monotonic", lambda: next(times))

    events = []
    monkeypatch.setattr(login.audit, "log_event", lambda event, **payload: events.append((event, payload)))

    code = login._prompt_for_sms_code(page, allow_resend=True)

    assert code == "654321"
    assert locator.clicked == 2
    assert events[0][0] == "twofa_send"
    for _, payload in events:
        assert "654321" not in str(payload)
