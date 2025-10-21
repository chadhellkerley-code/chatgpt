from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass
from typing import Dict, Optional
from urllib.parse import urlparse, urlunparse, quote

import requests
from requests import exceptions as req_exc

from config import SETTINGS

logger = logging.getLogger(__name__)

API_CHECK_URL = "https://api.ipify.org"


@dataclass(frozen=True)
class ProxyConfig:
    url: str
    user: Optional[str] = None
    password: Optional[str] = None
    sticky_minutes: int = 10


@dataclass
class ProxyBinding:
    url: str
    proxies: Dict[str, str]
    session_id: str
    expires_at: float
    public_ip: str
    masked_ip: str
    latency: float


_BINDINGS: Dict[str, ProxyBinding] = {}


def default_proxy_settings() -> dict:
    return {
        "url": SETTINGS.proxy_default_url or "",
        "user": SETTINGS.proxy_default_user or "",
        "password": SETTINGS.proxy_default_pass or "",
        "sticky": max(1, SETTINGS.proxy_sticky_minutes),
    }


def _mask_ip(ip: str) -> str:
    if not ip:
        return ""
    if ":" in ip:
        parts = ip.split(":")
        if len(parts) > 2:
            return ":".join(parts[:2]) + ":…"
        return ip
    blocks = ip.split(".")
    if len(blocks) == 4:
        blocks[-1] = "x"
        return ".".join(blocks)
    return ip


def _generate_session_id(username: str) -> str:
    return f"{username}-{uuid.uuid4().hex[:8]}"


def _format_with_auth(url: str, user: Optional[str], password: Optional[str]) -> str:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("El proxy debe comenzar con http:// o https://")
    username = user if user is not None else parsed.username or ""
    password = password if password is not None else parsed.password or ""
    hostname = parsed.hostname or ""
    port = f":{parsed.port}" if parsed.port else ""

    netloc = hostname + port
    if username:
        auth = quote(username, safe="")
        if password:
            auth += ":" + quote(password, safe="")
        netloc = f"{auth}@{netloc}"

    rebuilt = urlunparse(
        (
            parsed.scheme,
            netloc,
            parsed.path,
            parsed.params,
            parsed.query,
            parsed.fragment,
        )
    )
    return rebuilt


def _build_url(config: ProxyConfig, session_id: str) -> str:
    base_url = config.url.strip()
    if not base_url:
        raise ValueError("No se definió URL de proxy")
    formatted = base_url.replace("{session}", session_id)
    user = (config.user or "").replace("{session}", session_id) or None
    password = (config.password or "").replace("{session}", session_id) or None
    formatted = _format_with_auth(formatted, user, password)
    return formatted


def _probe_proxy(url: str) -> tuple[str, float]:
    proxies = {"http": url, "https": url}
    start = time.perf_counter()
    response = requests.get(API_CHECK_URL, proxies=proxies, timeout=10)
    response.raise_for_status()
    latency = time.perf_counter() - start
    ip = response.text.strip()
    return ip, latency


def _create_binding(username: str, config: ProxyConfig) -> ProxyBinding:
    session_id = _generate_session_id(username)
    final_url = _build_url(config, session_id)
    ip, latency = _probe_proxy(final_url)
    masked = _mask_ip(ip)
    binding = ProxyBinding(
        url=final_url,
        proxies={"http": final_url, "https": final_url},
        session_id=session_id,
        expires_at=time.time() + max(1, config.sticky_minutes) * 60,
        public_ip=ip,
        masked_ip=masked,
        latency=latency,
    )
    return binding


def config_from_account(account: Optional[dict]) -> Optional[ProxyConfig]:
    if account is None:
        account = {}
    url = (account.get("proxy_url") or SETTINGS.proxy_default_url or "").strip()
    if not url:
        return None
    sticky = account.get("proxy_sticky_minutes") or SETTINGS.proxy_sticky_minutes or 10
    try:
        sticky_int = int(sticky)
    except Exception:
        sticky_int = SETTINGS.proxy_sticky_minutes or 10
    sticky_int = max(1, sticky_int)
    return ProxyConfig(
        url=url,
        user=(account.get("proxy_user") or SETTINGS.proxy_default_user or "") or None,
        password=(account.get("proxy_pass") or SETTINGS.proxy_default_pass or "") or None,
        sticky_minutes=sticky_int,
    )


def ensure_binding(username: str, account: Optional[dict], *, reason: str = "general") -> Optional[ProxyBinding]:
    config = config_from_account(account)
    if not config:
        return None
    existing = _BINDINGS.get(username)
    now = time.time()
    if existing and existing.expires_at > now:
        return existing
    binding = _create_binding(username, config)
    _BINDINGS[username] = binding
    logger.info(
        "Proxy activado para @%s (%s). IP=%s, latencia=%.2fs, sticky=%s min",
        username,
        reason,
        binding.masked_ip,
        binding.latency,
        config.sticky_minutes,
    )
    return binding


def apply_proxy_to_client(client, username: str, account: Optional[dict], *, reason: str = "general") -> Optional[ProxyBinding]:
    binding = ensure_binding(username, account, reason=reason)
    if not binding:
        return None
    try:
        client.set_proxy(binding.url)
    except Exception:
        client.set_proxy(binding.proxies)
    return binding


def test_proxy_connection(config: ProxyConfig) -> ProxyBinding:
    binding = _create_binding("test", config)
    logger.info(
        "Test de proxy exitoso. IP=%s, latencia=%.2fs", binding.masked_ip, binding.latency
    )
    return binding


def should_retry_proxy(exc: Exception) -> bool:
    if isinstance(
        exc,
        (
            req_exc.ProxyError,
            req_exc.ConnectTimeout,
            req_exc.ReadTimeout,
            req_exc.SSLError,
            req_exc.ConnectionError,
        ),
    ):
        return True
    message = str(exc).lower()
    keywords = ["proxy", "timed out", "timeout", "407", "dns", "tunnel", "connection aborted"]
    return any(key in message for key in keywords)


def record_proxy_failure(username: str, exc: Optional[Exception] = None) -> None:
    if username in _BINDINGS:
        _BINDINGS.pop(username, None)
        logger.warning("Proxy reiniciado para @%s tras error: %s", username, exc)


def clear_proxy(username: str) -> None:
    _BINDINGS.pop(username, None)
