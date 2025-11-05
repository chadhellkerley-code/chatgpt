"""Instagram automation helpers backed by Playwright."""
from __future__ import annotations

import contextlib
import logging
import random
import time
from dataclasses import dataclass
from typing import Dict, Optional

from playwright.sync_api import Browser, BrowserContext, Page, TimeoutError as PlaywrightTimeoutError, sync_playwright

from totp_store import generate_code as generate_totp_code

logger = logging.getLogger(__name__)


def _human_delay(min_seconds: float = 0.4, max_seconds: float = 1.1) -> None:
    """Pause execution emulating a human-scale delay."""
    time.sleep(random.uniform(min_seconds, max_seconds))


def _human_type(locator, text: str, *, min_delay: float = 0.05, max_delay: float = 0.18) -> None:
    for char in text:
        locator.type(char)
        time.sleep(random.uniform(min_delay, max_delay))


@dataclass
class _TwoFactorPayload:
    code: str
    label: str


class InstagramPlaywrightSession:
    """Encapsulates a Playwright browser session for Instagram automation."""

    def __init__(self, account: Dict[str, str], *, headless: bool = True) -> None:
        self._account = account
        self._username = (account.get("username") or "").strip()
        self._password = (account.get("password") or "").strip()
        if not self._username or not self._password:
            raise ValueError("La cuenta no tiene credenciales almacenadas para iniciar sesión.")
        self._headless = headless
        self._playwright = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None
        self._page: Optional[Page] = None
        self._logged_in = False

    # Context manager helpers -------------------------------------------------
    def close(self) -> None:
        """Dispose every Playwright resource created for the session."""
        with contextlib.suppress(Exception):
            if self._page is not None:
                self._page.close()
        with contextlib.suppress(Exception):
            if self._context is not None:
                self._context.close()
        with contextlib.suppress(Exception):
            if self._browser is not None:
                self._browser.close()
        if self._playwright is not None:
            with contextlib.suppress(Exception):
                self._playwright.stop()
        self._page = None
        self._context = None
        self._browser = None
        self._playwright = None
        self._logged_in = False

    def __enter__(self) -> "InstagramPlaywrightSession":
        self._ensure_session()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    # Public API --------------------------------------------------------------
    def send_direct_message(self, target_username: str, message: str) -> None:
        """Navigate to a profile and send a DM simulating human behaviour."""
        if not self._logged_in:
            self._ensure_session()
        assert self._page is not None
        page = self._page
        profile_url = f"https://www.instagram.com/{target_username.strip('/')}/"
        logger.debug("Abriendo perfil @%s", target_username)
        page.goto(profile_url, wait_until="networkidle")
        _human_delay(1.2, 2.4)

        self._accept_cookies_if_present()

        message_button = self._locate_message_button()
        message_button.click()
        _human_delay(1.0, 2.0)

        try:
            textarea = page.wait_for_selector("textarea", timeout=20000)
        except PlaywrightTimeoutError as exc:  # pragma: no cover - requiere UI real
            raise RuntimeError("No se encontró el cuadro de mensaje en Instagram.") from exc

        textarea.click()
        _human_delay(0.3, 0.8)
        logger.debug("Escribiendo mensaje para @%s", target_username)
        _human_type(textarea, message)
        _human_delay(0.6, 1.4)
        page.keyboard.press("Enter")
        _human_delay(0.8, 1.6)

    # Internal helpers -------------------------------------------------------
    def _ensure_session(self) -> None:
        if self._logged_in:
            return
        self._start_browser()
        self._login_via_web()
        self._logged_in = True

    def ensure_logged_in(self) -> None:
        """Public wrapper to guarantee that the browser is authenticated."""
        self._ensure_session()

    def _start_browser(self) -> None:
        if self._playwright is not None:
            return
        self._playwright = sync_playwright().start()
        browser_args = {
            "headless": self._headless,
            "args": [
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
            ],
        }
        self._browser = self._playwright.chromium.launch(**browser_args)
        self._context = self._browser.new_context(locale="es-ES", timezone_id="America/Argentina/Buenos_Aires")
        self._page = self._context.new_page()
        self._page.set_default_navigation_timeout(45000)
        self._page.set_default_timeout(30000)

    def _login_via_web(self) -> None:
        assert self._page is not None
        page = self._page
        logger.debug("Iniciando sesión en Instagram para @%s", self._username)
        page.goto("https://www.instagram.com/accounts/login/", wait_until="domcontentloaded")
        self._accept_cookies_if_present()
        _human_delay(1.0, 2.0)

        username_input = page.wait_for_selector("input[name='username']", timeout=20000)
        password_input = page.wait_for_selector("input[name='password']", timeout=20000)

        username_input.click()
        _human_delay(0.2, 0.5)
        _human_type(username_input, self._username)
        _human_delay(0.4, 0.9)

        password_input.click()
        _human_delay(0.2, 0.5)
        _human_type(password_input, self._password)
        _human_delay(0.3, 0.7)

        submit_button = page.locator("button:has-text('Iniciar sesión'), button[type='submit']").first
        submit_button.click()
        _human_delay(1.0, 2.0)

        self._resolve_two_factor_challenge()
        self._dismiss_post_login_modals()

    def _dismiss_post_login_modals(self) -> None:
        assert self._page is not None
        page = self._page
        with contextlib.suppress(PlaywrightTimeoutError):
            save_info = page.wait_for_selector("button:has-text('Ahora no')", timeout=8000)
            save_info.click()
            _human_delay(0.5, 1.0)
        with contextlib.suppress(PlaywrightTimeoutError):
            notifications = page.wait_for_selector("button:has-text('Ahora no')", timeout=8000)
            notifications.click()
            _human_delay(0.5, 1.0)

    def _accept_cookies_if_present(self) -> None:
        assert self._page is not None
        page = self._page
        for selector in (
            "button:has-text('Permitir todas las cookies')",
            "button:has-text('Aceptar')",
            "button:has-text('Allow all cookies')",
        ):
            with contextlib.suppress(PlaywrightTimeoutError):
                button = page.wait_for_selector(selector, timeout=2000)
                button.click()
                _human_delay(0.3, 0.7)
                break

    def _locate_message_button(self):
        assert self._page is not None
        page = self._page
        candidates = [
            "button:has-text('Enviar mensaje')",
            "button:has-text('Message')",
            "div[role='button']:has-text('Enviar mensaje')",
            "div[role='button']:has-text('Message')",
        ]
        for selector in candidates:
            locator = page.locator(selector)
            if locator.count() > 0:
                return locator.first
        raise RuntimeError("No se encontró el botón para enviar mensaje.")

    def _resolve_two_factor_challenge(self) -> None:
        payload = self._build_two_factor_payload()
        if payload is None:
            return
        assert self._page is not None
        page = self._page
        logger.debug("Resolviendo desafío 2FA con %s", payload.label)
        with contextlib.suppress(PlaywrightTimeoutError):
            code_input = page.wait_for_selector("input[name='verificationCode']", timeout=15000)
            code_input.click()
            _human_delay(0.2, 0.4)
            _human_type(code_input, payload.code)
            _human_delay(0.5, 1.0)
            page.keyboard.press("Enter")
            _human_delay(1.0, 2.0)

    def _build_two_factor_payload(self) -> Optional[_TwoFactorPayload]:
        if not self._account:
            return None
        username = self._username
        with contextlib.suppress(Exception):
            totp_code = generate_totp_code(username)
            if totp_code:
                return _TwoFactorPayload(code=totp_code, label="TOTP")
        for key in ("totp_code", "2fa_code", "two_factor_code", "sms_code", "whatsapp_code"):
            value = (self._account.get(key) or "").strip()
            if value:
                return _TwoFactorPayload(code=value, label=key)
        return None


__all__ = ["InstagramPlaywrightSession"]
