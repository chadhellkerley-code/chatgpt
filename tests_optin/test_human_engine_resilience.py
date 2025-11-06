import asyncio

from src.opt_in import human_engine


class DummyLocator:
    def __init__(self, selectors):
        self._selectors = selectors
        self.clicked = []

    async def wait_for_selector(self, selector, timeout=0):
        if selector == self._selectors[0]:
            raise human_engine.PlaywrightTimeoutError("not found")
        return True

    async def hover(self, selector):
        self.clicked.append(("hover", selector))

    async def click(self, selector):
        self.clicked.append(("click", selector))

    async def fill(self, selector, text):
        self.clicked.append(("fill", selector, text))

    async def content(self):
        return ""

    async def wait_for_load_state(self, *args, **kwargs):
        return None

    @property
    def keyboard(self):
        class KB:
            async def type(self, char):
                return None

        return KB()


def test_click_uses_fallback_selector(monkeypatch):
    monkeypatch.setenv("DELAY_MIN_S", "0.01")
    monkeypatch.setenv("DELAY_MAX_S", "0.02")
    page = DummyLocator(["#missing", "#ok"])
    result = asyncio.run(human_engine.click(page, ["#missing", "#ok"]))
    assert result.ok
    assert ("click", "#ok") in page.clicked
