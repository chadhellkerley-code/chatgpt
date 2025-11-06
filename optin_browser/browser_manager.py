"""Browser lifecycle helpers for the opt-in toolkit."""
from __future__ import annotations

from typing import Any, Dict, Optional

from playwright.sync_api import Browser, BrowserContext, Page, sync_playwright

from . import session_store
from .config import cfg


class BrowserManager:
    """Context manager that manages Playwright browser resources."""

    def __init__(self, account: Optional[str] = None) -> None:
        self.account = account
        self._playwright = None
        self._browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None

    def __enter__(self) -> "BrowserManager":
        self._playwright = sync_playwright().start()
        launch_kwargs: Dict[str, Any] = {"headless": cfg.headless}
        if cfg.proxy_url:
            launch_kwargs["proxy"] = {"server": cfg.proxy_url}
        self._browser = self._playwright.chromium.launch(**launch_kwargs)

        context_kwargs: Dict[str, Any] = {}
        if cfg.user_agent:
            context_kwargs["user_agent"] = cfg.user_agent
        if cfg.locale:
            context_kwargs["locale"] = cfg.locale
        if cfg.timezone_id:
            context_kwargs["timezone_id"] = cfg.timezone_id

        if self.account:
            storage_state = session_store.load_state(self.account)
            if storage_state:
                context_kwargs["storage_state"] = storage_state

        self.context = self._browser.new_context(**context_kwargs)
        self.page = self.context.new_page()
        return self

    def goto_instagram(self, path: str = "") -> Page:
        if not self.page:
            raise RuntimeError("La página de Playwright no está inicializada.")
        url = "https://www.instagram.com/" + path.lstrip("/")
        self.page.goto(url, wait_until="load")
        return self.page

    def save_state_on_close(self) -> None:
        if not self.account or not self.context:
            return
        state = self.context.storage_state()
        session_store.save_state(self.account, state)

    def close(self, save_state: bool = True) -> None:
        if save_state:
            self.save_state_on_close()
        if self.page:
            self.page.close()
            self.page = None
        if self.context:
            self.context.close()
            self.context = None
        if self._browser:
            self._browser.close()
            self._browser = None
        if self._playwright:
            self._playwright.stop()
            self._playwright = None

    def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
        self.close(save_state=exc is None)
