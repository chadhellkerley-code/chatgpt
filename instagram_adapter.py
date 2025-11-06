"""Adapter para manejar el login y desafíos 2FA de Instagram."""
from __future__ import annotations

import logging
import random
import re
import sys
import time
import uuid
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, List, Optional

from instagrapi import Client
from instagrapi import exceptions as ig_exceptions

from config import SETTINGS
from totp_store import generate_code as generate_totp_code

logger = logging.getLogger(__name__)


_DEFAULT_METHOD_PRIORITY = ("whatsapp", "sms", "email")
_METHOD_CODES = {"sms": "1", "totp": "3", "whatsapp": "5", "email": "2"}


def _sanitize_code(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    digits = re.sub(r"\D", "", raw)
    if 4 <= len(digits) <= 8:
        return digits
    return None


def _human_delay(min_seconds: float = 0.5, max_seconds: float = 1.2) -> None:
    time.sleep(random.uniform(min_seconds, max_seconds))


@dataclass(frozen=True)
class TwoFARequired(Exception):
    """Excepción que señala que Instagram solicitó un código 2FA."""

    method: str
    methods: List[str]
    info: Dict[str, object]

    def __str__(self) -> str:  # pragma: no cover - representación simple
        return f"Se requiere 2FA vía {self.method or 'desconocido'}"


class TwoFactorCodeRejected(RuntimeError):
    """Raised when Instagram keeps the 2FA challenge active after submitting a code."""


class InstagramClientAdapter:
    """Wrapper alrededor de instagrapi.Client con soporte extendido de 2FA."""

    def __init__(
        self,
        *,
        client_factory: Callable[..., Client] | None = None,
    ) -> None:
        factory = client_factory or Client
        self._client: Client = factory(settings={})
        self._username: Optional[str] = None
        self._password: Optional[str] = None
        self._two_factor_identifier: Optional[str] = None
        self._two_factor_methods: List[str] = []
        self._two_factor_info: Dict[str, object] = {}
        self._selected_channel: Optional[str] = None
        self._last_request: Dict[str, float] = {}

    # ------------------------------------------------------------------
    # Propiedades delegadas
    def set_proxy(self, value):
        return self._client.set_proxy(value)

    def get_settings(self) -> Dict:
        return self._client.get_settings()

    def load_settings(self, path: str) -> None:
        self._client.load_settings(path)

    def dump_settings(self, path: str) -> bool:
        return self._client.dump_settings(path)

    # ------------------------------------------------------------------
    def do_login(
        self,
        username: str,
        password: str,
        *,
        verification_code: Optional[str] = None,
    ) -> bool:
        self._username = username
        self._password = password
        if not verification_code:
            verification_code = generate_totp_code(username) or ""
            if verification_code:
                logger.debug("Aplicando código TOTP automático para @%s", username)
        try:
            self._client.login(username, password, verification_code=verification_code or "")
            self._clear_two_factor_state()
            return True
        except ig_exceptions.TwoFactorRequired as exc:
            info = self._extract_two_factor_info()
            methods = self._discover_methods(info)
            preferred = self._select_preferred_method(methods)
            self._two_factor_identifier = str(info.get("two_factor_identifier") or "") or None
            self._two_factor_methods = methods
            self._two_factor_info = info
            logger.info(
                "Instagram solicitó 2FA para @%s (métodos disponibles: %s)",
                username,
                ", ".join(methods) or "ninguno",
            )
            if preferred:
                try:
                    self.request_2fa_code(preferred)
                except Exception as send_exc:
                    logger.warning(
                        "No se pudo iniciar el challenge 2FA vía %s para @%s: %s",
                        preferred,
                        username,
                        send_exc,
                    )
            raise TwoFARequired(method=preferred or "unknown", methods=methods, info=info) from exc

    # ------------------------------------------------------------------
    def request_2fa_code(self, channel: str) -> Dict[str, object]:
        return self._send_two_factor_request(channel, resend=False)

    def resend_2fa_code(self, channel: str) -> Dict[str, object]:
        return self._send_two_factor_request(channel, resend=True)

    def finish_2fa(self, code: str) -> Dict[str, object]:
        sanitized = _sanitize_code(code)
        if not sanitized:
            raise ValueError("El código 2FA proporcionado es inválido")
        if not self._username or not self._password:
            raise RuntimeError("No hay un login pendiente de completar")
        if not self._two_factor_identifier:
            raise RuntimeError("Instagram no solicitó un challenge 2FA")

        method = self._selected_channel or self._select_preferred_method(self._two_factor_methods)
        payload = {
            "verification_code": sanitized,
            "phone_id": self._client.phone_id,
            "_csrftoken": self._client.token,
            "two_factor_identifier": self._two_factor_identifier,
            "username": self._username,
            "trust_this_device": "0",
            "guid": self._client.uuid,
            "device_id": self._client.android_device_id,
            "waterfall_id": str(uuid.uuid4()),
        }
        if method and method in _METHOD_CODES:
            payload["verification_method"] = _METHOD_CODES[method]

        logger.info("Enviando código 2FA para @%s vía %s", self._username, method or "desconocido")
        _human_delay()
        result = self._client.private_request(
            "accounts/two_factor_login/", payload, login=True
        )
        status = str(result.get("status") or "").lower()
        if status != "ok":
            raise TwoFactorCodeRejected(result.get("message") or "Código rechazado")

        authorization = self._client.last_response.headers.get("ig-set-authorization")
        try:
            self._client.authorization_data = self._client.parse_authorization(authorization)
        except Exception:
            pass
        try:
            self._client.login_flow()
        except Exception:
            pass
        self._client.last_login = time.time()
        self._clear_two_factor_state()
        logger.info("Login completado para @%s tras 2FA", self._username)
        return result

    # ------------------------------------------------------------------
    def dump_session(self) -> Dict[str, object]:
        return self._client.get_settings()

    def load_session(self, data: Dict[str, object]) -> None:
        if not isinstance(data, dict):
            raise TypeError("La sesión debe ser un diccionario serializable")
        self._client.set_settings(data)

    def is_logged_in(self) -> bool:
        try:
            return bool(self._client.user_id and self._client.cookie_dict.get("sessionid"))
        except Exception:
            return False

    # ------------------------------------------------------------------
    def _clear_two_factor_state(self) -> None:
        self._two_factor_identifier = None
        self._two_factor_methods = []
        self._two_factor_info = {}
        self._selected_channel = None

    def _extract_two_factor_info(self) -> Dict[str, object]:
        return dict(getattr(self._client, "last_json", {}).get("two_factor_info", {}) or {})

    def _discover_methods(self, info: Dict[str, object]) -> List[str]:
        methods: List[str] = []
        available = info.get("available_two_factor_methods") or info.get("two_factor_methods")
        if isinstance(available, Iterable):
            for method in available:
                name = str(method.get("method" if isinstance(method, dict) else 0) or "").lower()
                if not name and isinstance(method, dict):
                    name = str(method.get("type") or "").lower()
                if name and name not in methods:
                    methods.append(name)
        if info.get("totp_two_factor_on") or info.get("is_totp_two_factor_enabled"):
            methods.append("totp")
        if info.get("whatsapp_two_factor_on") or info.get("should_use_whatsapp_token"):
            methods.append("whatsapp")
        if info.get("sms_two_factor_on") or info.get("is_sms_two_factor_enabled"):
            methods.append("sms")
        if info.get("email_two_factor_on"):
            methods.append("email")
        seen: List[str] = []
        for method in methods:
            if method not in seen:
                seen.append(method)
        return seen

    def _select_preferred_method(self, methods: Iterable[str]) -> Optional[str]:
        normalized = [m.lower() for m in methods if isinstance(m, str)]
        for candidate in _DEFAULT_METHOD_PRIORITY:
            if candidate in normalized:
                self._selected_channel = candidate
                return candidate
        if normalized:
            self._selected_channel = normalized[0]
            return normalized[0]
        return None

    def _send_two_factor_request(self, channel: str, *, resend: bool) -> Dict[str, object]:
        if not self._two_factor_identifier:
            raise RuntimeError("No hay un desafío 2FA pendiente")
        channel = (channel or "").lower()
        if channel not in {"sms", "whatsapp", "email"}:
            raise ValueError(f"Canal 2FA no soportado: {channel}")
        endpoint = self._endpoint_for(channel)
        payload = {
            "two_factor_identifier": self._two_factor_identifier,
            "device_id": self._client.android_device_id,
            "guid": self._client.uuid,
            "phone_id": self._client.phone_id,
            "_csrftoken": self._client.token,
        }
        if channel in _METHOD_CODES:
            payload["verification_method"] = _METHOD_CODES[channel]
        if resend:
            payload["force_resend"] = "1"

        label = f"{channel}{' (reenvío)' if resend else ''}"
        logger.info("Solicitando código 2FA vía %s para @%s", label, self._username)
        _human_delay()
        result = self._client.private_request(endpoint, payload, login=True)
        message = str(result.get("message") or "").strip()
        status = str(result.get("status") or "").lower()
        if status == "ok":
            self._last_request[channel] = time.time()
            self._selected_channel = channel
            logger.info("Instagram aceptó el envío 2FA vía %s para @%s", channel, self._username)
        elif message:
            logger.warning(
                "Instagram devolvió estado %s para el envío 2FA vía %s: %s",
                status or "desconocido",
                channel,
                message,
            )
        return result

    def _endpoint_for(self, channel: str) -> str:
        if channel == "whatsapp":
            return "accounts/send_two_factor_login_whatsapp/"
        if channel == "sms":
            return "accounts/send_two_factor_login_sms/"
        if channel == "email":
            return "accounts/send_two_factor_login_email/"
        raise ValueError(f"Canal 2FA no soportado: {channel}")


def prompt_two_factor_code(username: str, method: str, attempt: int) -> Optional[str]:
    if not SETTINGS.prompt_2fa_sms:
        return None
    label = method.lower()
    prompt = f"Ingrese el código recibido por {label} para {username}: "
    logger.info(
        "Solicitando código 2FA manual para @%s vía %s (intento %d)",
        username,
        label,
        attempt,
    )
    try:
        timeout = getattr(SETTINGS, "prompt_2fa_timeout_seconds", 0)
        effective_timeout = timeout if timeout > 0 else None
        code = _read_input_with_timeout(prompt, effective_timeout)
    except Exception as exc:  # pragma: no cover - interacción depende del entorno
        logger.warning("No se pudo leer el código 2FA manual para @%s: %s", username, exc)
        return None
    sanitized = _sanitize_code(code)
    if not sanitized:
        logger.warning(
            "Código 2FA inválido proporcionado manualmente para @%s vía %s",
            username,
            method,
        )
    return sanitized


def _read_input_with_timeout(prompt: str, timeout: Optional[int]) -> Optional[str]:
    if timeout is None:
        return input(prompt)

    if timeout <= 0:
        return input(prompt)

    deadline = time.time() + timeout
    print(prompt, end="", flush=True)

    if sys.platform.startswith("win"):
        import msvcrt  # type: ignore

        buffer: list[str] = []
        while True:
            remaining = deadline - time.time()
            if remaining <= 0:
                print("\n[Tiempo excedido esperando el código]\n", flush=True)
                return None
            if msvcrt.kbhit():
                ch = msvcrt.getwch()
                if ch in ("\r", "\n"):
                    print()
                    return "".join(buffer)
                if ch == "\b":
                    if buffer:
                        buffer.pop()
                        print("\b \b", end="", flush=True)
                    continue
                buffer.append(ch)
                print("*", end="", flush=True)
            else:
                time.sleep(min(0.2, max(0.0, remaining)))
    else:
        import select

        while True:
            remaining = deadline - time.time()
            if remaining <= 0:
                print("\n[Tiempo excedido esperando el código]\n", flush=True)
                return None
            ready, _, _ = select.select([sys.stdin], [], [], min(1.0, max(0.1, remaining)))
            if ready:
                line = sys.stdin.readline()
                if not line:
                    return None
                return line.rstrip("\n")

    return None

