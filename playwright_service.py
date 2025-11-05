"""Instagram automation helpers backed by Playwright."""
from __future__ import annotations

import contextlib
import csv
import logging
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence

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


@dataclass
class BulkSendResult:
    """Result of attempting to send a DM from a CSV-provisioned account."""

    username: str
    target: str
    status: str
    error: Optional[str] = None


class InstagramPlaywrightSession:
    """Encapsulates a Playwright browser session for Instagram automation."""

    def __init__(self, account: Dict[str, str], *, headless: bool = True, proxy_override: Optional[Dict[str, str]] = None) -> None:
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
        self._proxy_settings = self._extract_proxy_settings(proxy_override)

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
        if self._proxy_settings:
            browser_args["proxy"] = self._proxy_settings
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

    def _extract_proxy_settings(self, proxy_override: Optional[Dict[str, str]]) -> Optional[Dict[str, str]]:
        """Build the Playwright proxy settings for this session if available."""

        def _pick(mapping: Dict[str, str], *keys: str) -> Optional[str]:
            for key in keys:
                value = mapping.get(key)
                if value:
                    text = str(value).strip()
                    if text:
                        return text
            return None

        candidate: Dict[str, str] = {}
        if proxy_override:
            candidate.update(proxy_override)
        for key in ("proxy_url", "proxy", "proxy server", "server", "url"):
            value = self._account.get(key)
            if value:
                candidate.setdefault(key, value)
        if not candidate:
            return None
        server = _pick(candidate, "proxy_url", "proxy", "proxy server", "server", "url")
        if not server:
            return None
        proxy_settings: Dict[str, str] = {"server": server}
        username = _pick(candidate, "proxy_user", "proxy_username", "proxyuser")
        if username:
            proxy_settings["username"] = username
        password = _pick(candidate, "proxy_pass", "proxy_password", "proxypass")
        if password:
            proxy_settings["password"] = password
        logger.debug("Proxy configurado para @%s", self._username)
        return proxy_settings


def _chunked(sequence: Sequence[Dict[str, str]], size: int) -> Iterable[List[Dict[str, str]]]:
    for index in range(0, len(sequence), size):
        yield list(sequence[index : index + size])


def _load_accounts_from_csv(path: Path) -> List[Dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        rows: List[Dict[str, str]] = []
        for raw_row in reader:
            if not raw_row:
                continue
            normalized = {
                (key or "").strip().lower(): (value or "").strip()
                for key, value in raw_row.items()
                if key is not None
            }
            username = normalized.get("username") or normalized.get("user")
            password = normalized.get("password")
            if not username or not password:
                continue
            rows.append(normalized)
        return rows


def send_messages_from_csv(
    csv_path: str,
    default_target: str,
    default_message: str,
    *,
    batch_size: int = 10,
    headless: bool = True,
) -> List[BulkSendResult]:
    """Process Instagram accounts defined in a CSV concurrently and send a DM."""

    path = Path(csv_path).expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(f"No se encontró el archivo CSV: {path}")

    if batch_size < 1:
        raise ValueError("batch_size debe ser mayor o igual a 1")

    accounts = _load_accounts_from_csv(path)
    if not accounts:
        logger.warning("El CSV %s no contiene cuentas válidas para procesar.", path)
        return []

    normalized_target = (default_target or "").strip().lstrip("@")
    normalized_message = (default_message or "").strip()
    if not normalized_target:
        raise ValueError("Se requiere un usuario objetivo para enviar mensajes.")
    if not normalized_message:
        raise ValueError("Se requiere un mensaje para enviar.")

    results: List[BulkSendResult] = []
    chunk_size = max(10, batch_size)

    def _proxy_payload(data: Dict[str, str]) -> Dict[str, str]:
        payload: Dict[str, str] = {}
        server = (
            data.get("proxy_url")
            or data.get("proxy")
            or data.get("proxy server")
            or data.get("server")
            or data.get("url")
        )
        if server:
            payload["proxy_url"] = server
        user = (
            data.get("proxy_user")
            or data.get("proxy username")
            or data.get("proxy_username")
            or data.get("proxy user")
        )
        if user:
            payload["proxy_user"] = user
        password = (
            data.get("proxy_pass")
            or data.get("proxy password")
            or data.get("proxy_password")
            or data.get("proxy pass")
        )
        if password:
            payload["proxy_pass"] = password
        return payload

    def _send_from_record(record: Dict[str, str]) -> BulkSendResult:
        username = (record.get("username") or record.get("user") or "").lstrip("@")
        password = record.get("password") or ""
        target = (record.get("target") or record.get("lead") or normalized_target).lstrip("@")
        message = record.get("message") or normalized_message

        proxy_payload = _proxy_payload(record)

        account_payload: Dict[str, str] = {
            "username": username,
            "password": password,
            "proxy_url": proxy_payload.get("proxy_url", ""),
            "proxy_user": proxy_payload.get("proxy_user", ""),
            "proxy_pass": proxy_payload.get("proxy_pass", ""),
            "totp_code": record.get("totp")
            or record.get("totp_code")
            or record.get("totp code")
            or record.get("2fa code")
            or "",
            "sms_code": record.get("sms_code") or record.get("sms code") or record.get("sms"),
            "whatsapp_code": record.get("whatsapp_code")
            or record.get("whatsapp code")
            or record.get("whatsapp"),
            "two_factor_code": record.get("two_factor_code")
            or record.get("two factor code")
            or record.get("twofactor"),
        }

        session: Optional[InstagramPlaywrightSession] = None
        try:
            try:
                session = InstagramPlaywrightSession(account_payload, headless=headless)
            except Exception as exc:  # pragma: no cover - requiere entorno Playwright real
                logger.error("No se pudo inicializar sesión para @%s: %s", username, exc, exc_info=False)
                return BulkSendResult(username=username, target=target, status="init_failed", error=str(exc))

            try:
                session.ensure_logged_in()
            except Exception as exc:  # pragma: no cover - requiere entorno Playwright real
                logger.error("Fallo el inicio de sesión para @%s: %s", username, exc, exc_info=False)
                return BulkSendResult(username=username, target=target, status="login_failed", error=str(exc))

            try:
                session.send_direct_message(target, message)
                return BulkSendResult(username=username, target=target, status="sent")
            except Exception as exc:  # pragma: no cover - requiere entorno Playwright real
                logger.error(
                    "No se pudo enviar mensaje desde @%s → @%s: %s", username, target, exc, exc_info=False
                )
                return BulkSendResult(username=username, target=target, status="send_failed", error=str(exc))
        finally:
            if session is not None:
                session.close()

    for batch in _chunked(accounts, chunk_size):
        with ThreadPoolExecutor(max_workers=len(batch)) as executor:
            futures = {executor.submit(_send_from_record, record): record for record in batch}
            for future in as_completed(futures):
                try:
                    result = future.result()
                except Exception as exc:  # pragma: no cover - requiere ejecución real concurrente
                    record = futures[future]
                    username = record.get("username") or record.get("user") or ""
                    target = record.get("target") or record.get("lead") or normalized_target
                    logger.error("Excepción inesperada con @%s: %s", username, exc, exc_info=False)
                    results.append(BulkSendResult(username=username, target=target, status="error", error=str(exc)))
                else:
                    results.append(result)

    return results


__all__ = ["BulkSendResult", "InstagramPlaywrightSession", "send_messages_from_csv"]
