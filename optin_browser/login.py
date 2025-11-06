"""Login flow handling for the opt-in Instagram toolkit."""
from __future__ import annotations

import getpass
import re
import time

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from tenacity import RetryError, retry, stop_after_attempt, wait_exponential

from . import audit
from .browser_manager import BrowserManager
from .config import cfg
from .session_store import save_state

_USERNAME_SELECTOR = "input[name='username']"
_PASSWORD_SELECTOR = "input[name='password']"
_LOGIN_BUTTON = "button[type='submit']"
_CODE_INPUT_SELECTOR = "input[name='verificationCode'], input[aria-label*='code']"
_SEND_CODE_SELECTOR = "button:has-text('Send code'), button:has-text('Enviar código')"


class LoginError(RuntimeError):
    """Raised when the login flow cannot complete."""


def _prompt_for_sms_code(page, allow_resend: bool) -> str:
    """Prompt the operator for the SMS/WhatsApp code after requesting it."""
    try:
        send_button = page.locator(_SEND_CODE_SELECTOR)
        if send_button.is_enabled():
            send_button.click()
            audit.log_event("twofa_send", channel="sms")
    except PlaywrightTimeoutError:
        pass

    deadline = time.monotonic()
    while True:
        code = getpass.getpass("Ingresa el código de 6 dígitos recibido: ").strip()
        if code:
            return code
        print("Código vacío, intenta nuevamente.")
        if not allow_resend:
            continue
        now = time.monotonic()
        elapsed = now - deadline
        if elapsed < cfg.send_cooldown_seconds:
            remaining = int(cfg.send_cooldown_seconds - elapsed)
            print(f"Aún no puedes reenviar, espera {remaining} segundos.")
            continue
        try:
            send_button = page.locator(_SEND_CODE_SELECTOR)
            if send_button.is_enabled():
                send_button.click()
                audit.log_event("twofa_send", channel="sms", reason="resend")
                deadline = time.monotonic()
        except PlaywrightTimeoutError:
            print("No se encontró el botón de reenvío.")


def _fill_code(page, code: str) -> None:
    page.wait_for_selector(_CODE_INPUT_SELECTOR, state="visible", timeout=20000)
    page.fill(_CODE_INPUT_SELECTOR, code)
    page.click(_LOGIN_BUTTON)


def _handle_two_factor(page, allow_resend: bool) -> None:
    try:
        page.wait_for_selector(_CODE_INPUT_SELECTOR, state="visible", timeout=10000)
    except PlaywrightTimeoutError:
        return

    if cfg.ig_totp_secret:
        code = cfg.generate_totp()
        if not code:
            raise LoginError("No se pudo generar el código TOTP.")
        audit.log_event("twofa_send", channel="totp")
    else:
        code = _prompt_for_sms_code(page, allow_resend)
    _fill_code(page, code)


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=20), reraise=True)
def _login_once(account: str, username: str, password: str, allow_resend: bool) -> None:
    with BrowserManager(account=account) as manager:
        page = manager.goto_instagram("accounts/login/")
        page.wait_for_selector(_USERNAME_SELECTOR, timeout=20000)
        page.fill(_USERNAME_SELECTOR, username)
        page.fill(_PASSWORD_SELECTOR, password)
        page.click(_LOGIN_BUTTON)

        _handle_two_factor(page, allow_resend)

        try:
            page.wait_for_url(re.compile(r"instagram.com/"), timeout=60000)
        except PlaywrightTimeoutError as exc:
            raise LoginError("No se pudo confirmar el inicio de sesión.") from exc

        if not manager.context:
            raise LoginError("No se pudo obtener el estado de sesión.")
        state = manager.context.storage_state()
        save_state(account, state)


def login(account: str, username: str, password: str, allow_resend: bool = False) -> None:
    """Perform the Instagram login flow and persist the session state."""
    audit.log_event("login_attempt", account=account, username=username)
    try:
        _login_once(account, username, password, allow_resend)
    except RetryError as exc:
        audit.log_event("login_failed", account=account, username=username)
        raise LoginError("Falló el inicio de sesión tras varios intentos.") from exc
    except Exception:
        audit.log_event("login_failed", account=account, username=username)
        raise
    audit.log_event("login_success", account=account, username=username)
