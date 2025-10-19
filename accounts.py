# accounts.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import getpass
import json
from pathlib import Path
from typing import Dict, List, Optional

import logging

from config import SETTINGS
from proxy_manager import (
    ProxyConfig,
    apply_proxy_to_client,
    clear_proxy,
    default_proxy_settings,
    record_proxy_failure,
    should_retry_proxy,
    test_proxy_connection,
)
from session_store import has_session, remove as remove_session, save_from
from totp_store import generate_code as generate_totp_code
from totp_store import has_secret as has_totp_secret
from totp_store import remove_secret as remove_totp_secret
from totp_store import save_secret as save_totp_secret
from utils import ask, banner, em, ok, press_enter, title, warn

BASE = Path(__file__).resolve().parent
DATA = BASE / "data"
DATA.mkdir(exist_ok=True)
FILE = DATA / "accounts.json"

logger = logging.getLogger(__name__)


def _normalize_account(record: Dict) -> Dict:
    result = dict(record)
    result.setdefault("alias", "default")
    result.setdefault("active", True)
    result.setdefault("connected", False)
    result.setdefault("proxy_url", "")
    result.setdefault("proxy_user", "")
    result.setdefault("proxy_pass", "")
    sticky_default = SETTINGS.proxy_sticky_minutes or 10
    try:
        sticky_value = int(result.get("proxy_sticky_minutes", sticky_default))
    except Exception:
        sticky_value = sticky_default
    result["proxy_sticky_minutes"] = max(1, sticky_value)
    username = result.get("username")
    if username:
        result["has_totp"] = has_totp_secret(username)
    else:
        result.setdefault("has_totp", False)
    return result


def _prepare_for_save(record: Dict) -> Dict:
    stored = dict(record)
    if stored.get("proxy_url"):
        try:
            stored["proxy_sticky_minutes"] = int(
                stored.get("proxy_sticky_minutes", SETTINGS.proxy_sticky_minutes)
            )
        except Exception:
            stored["proxy_sticky_minutes"] = SETTINGS.proxy_sticky_minutes
    else:
        stored.pop("proxy_url", None)
        stored.pop("proxy_user", None)
        stored.pop("proxy_pass", None)
        stored.pop("proxy_sticky_minutes", None)
    stored.pop("has_totp", None)
    return stored


def _load() -> List[Dict]:
    if not FILE.exists():
        return []
    try:
        data = json.loads(FILE.read_text(encoding="utf-8"))
    except Exception:
        return []
    normalized: List[Dict] = []
    for item in data:
        if isinstance(item, dict):
            normalized.append(_normalize_account(item))
    return normalized


def _save(items: List[Dict]) -> None:
    cleaned = [_prepare_for_save(_normalize_account(it)) for it in items]
    FILE.write_text(json.dumps(cleaned, ensure_ascii=False, indent=2), encoding="utf-8")


def list_all() -> List[Dict]:
    return _load()


def _find(items: List[Dict], username: str) -> Optional[Dict]:
    username = username.lower()
    for it in items:
        if it.get("username", "").lower() == username:
            return it
    return None


def get_account(username: str) -> Optional[Dict]:
    items = _load()
    account = _find(items, username)
    return _normalize_account(account) if account else None


def update_account(username: str, updates: Dict) -> bool:
    items = _load()
    username_norm = username.lower()
    for idx, item in enumerate(items):
        if item.get("username", "").lower() == username_norm:
            updated = dict(item)
            updated.update(updates)
            items[idx] = _normalize_account(updated)
            _save(items)
            return True
    return False


def _prompt_totp(username: str) -> bool:
    while True:
        raw = ask("TOTP Secret / otpauth URI (opcional): ").strip()
        if not raw:
            return False
        try:
            save_totp_secret(username, raw)
            ok("Se guardó el TOTP cifrado para esta cuenta.")
            return True
        except ValueError as exc:
            warn(f"No se pudo guardar el TOTP: {exc}")
            retry = ask("¿Reintentar ingreso de TOTP? (s/N): ").strip().lower()
            if retry != "s":
                return False


def add_account(username: str, alias: str, proxy: Optional[Dict] = None) -> bool:
    items = _load()
    if _find(items, username):
        warn("Ya existe.")
        return False
    record = {
        "username": username.strip().lstrip("@"),
        "alias": alias,
        "active": True,
        "connected": False,
    }
    if proxy:
        record.update(proxy)
    items.append(_normalize_account(record))
    _save(items)
    ok("Agregada.")
    return True


def remove_account(username: str) -> None:
    items = _load()
    new_items = [it for it in items if it.get("username", "").lower() != username.lower()]
    _save(new_items)
    remove_session(username)
    remove_totp_secret(username)
    clear_proxy(username)
    ok("Eliminada (si existía).")


def set_active(username: str, is_active: bool = True) -> None:
    if update_account(username, {"active": is_active}):
        ok("Actualizada.")
    else:
        warn("No existe.")


def mark_connected(username: str, connected: bool) -> None:
    update_account(username, {"connected": connected})


def _proxy_config_from_inputs(data: Dict) -> ProxyConfig:
    return ProxyConfig(
        url=data.get("proxy_url", ""),
        user=data.get("proxy_user") or None,
        password=data.get("proxy_pass") or None,
        sticky_minutes=int(data.get("proxy_sticky_minutes", SETTINGS.proxy_sticky_minutes)),
    )


def _prompt_proxy_settings(existing: Optional[Dict] = None) -> Dict:
    defaults = default_proxy_settings()
    current = existing or {}
    print("\nConfiguración de proxy (opcional)")
    base_default = current.get("proxy_url") or defaults["url"]
    prompt_default = base_default or "sin proxy"
    raw_url = ask(f"Proxy URL [{prompt_default}]: ").strip()
    if raw_url.lower() in {"-", "none", "sin", "no"}:
        url = ""
    elif not raw_url and base_default:
        url = base_default
    else:
        url = raw_url

    user_default = current.get("proxy_user") or defaults["user"]
    user_prompt = user_default or "(sin definir)"
    proxy_user = ask(f"Usuario (opcional) [{user_prompt}]: ").strip() or user_default

    pass_default = current.get("proxy_pass") or defaults["password"]
    pass_prompt = "***" if pass_default else "(sin definir)"
    proxy_pass = ask(f"Password (opcional) [{pass_prompt}]: ").strip() or pass_default

    sticky_default = current.get("proxy_sticky_minutes") or defaults["sticky"]
    sticky_input = ask(f"Sticky minutes [{sticky_default}]: ").strip()
    try:
        sticky = int(sticky_input) if sticky_input else int(sticky_default)
    except Exception:
        sticky = int(defaults["sticky"] or 10)
    sticky = max(1, sticky)

    proxy_url = url.strip()
    data = {
        "proxy_url": proxy_url,
        "proxy_user": (proxy_user or "").strip(),
        "proxy_pass": (proxy_pass or "").strip(),
        "proxy_sticky_minutes": sticky,
    }

    if not proxy_url:
        return {"proxy_url": "", "proxy_user": "", "proxy_pass": "", "proxy_sticky_minutes": sticky}

    if ask("¿Probar proxy ahora? (s/N): ").strip().lower() == "s":
        try:
            result = test_proxy_connection(_proxy_config_from_inputs(data))
            ok(f"Proxy OK. IP detectada: {result.public_ip} (latencia {result.latency:.2f}s)")
        except Exception as exc:
            warn(f"Proxy falló: {exc}")
            retry = ask("¿Reintentar configuración? (s/N): ").strip().lower()
            if retry == "s":
                return _prompt_proxy_settings(existing)
    return data


def _test_existing_proxy(account: Dict) -> None:
    if not account.get("proxy_url"):
        warn("La cuenta no tiene proxy configurado.")
        return
    try:
        result = test_proxy_connection(_proxy_config_from_inputs(account))
        ok(f"Proxy OK. IP detectada: {result.public_ip} (latencia {result.latency:.2f}s)")
    except Exception as exc:
        warn(f"Error probando proxy: {exc}")


def _login_and_save_session(account: Dict, password: str) -> bool:
    """Login con instagrapi y guarda sesión en storage/sessions."""

    username = account["username"]
    try:
        from instagrapi import Client

        cl = Client()
        binding = apply_proxy_to_client(cl, username, account, reason="login")
        verification_code = ""
        if has_totp_secret(username):
            code = generate_totp_code(username)
            if code:
                verification_code = code
                logger.debug("Aplicando TOTP automático para @%s", username)
            else:
                warn(
                    "No se pudo generar el código 2FA automático. Intentá reconfigurar el TOTP."
                )
        cl.login(username, password, verification_code=verification_code)
        save_from(cl, username)
        mark_connected(username, True)
        ok(f"Sesión guardada para {username}.")
        return True
    except Exception as exc:
        if should_retry_proxy(exc):
            record_proxy_failure(username, exc)
            warn(f"Problema con el proxy de @{username}: {exc}")
        else:
            warn(f"No se pudo iniciar sesión para {username}: {exc}")
        mark_connected(username, False)
        return False


def prompt_login(username: str) -> bool:
    account = get_account(username)
    if not account:
        warn("No existe la cuenta indicada.")
        return False
    pwd = getpass.getpass(f"Password @{account['username']}: ")
    if not pwd:
        warn("Se canceló el inicio de sesión.")
        return False
    return _login_and_save_session(account, pwd)


def _proxy_indicator(account: Dict) -> str:
    return f" {em('🛡️')}" if account.get("proxy_url") else ""


def _totp_indicator(account: Dict) -> str:
    return f" {em('🔐')}" if account.get("has_totp") else ""


def menu_accounts():
    while True:
        banner()
        items = _load()
        aliases = sorted(set([it.get("alias", "default") for it in items]) | {"default"})
        title("Alias disponibles: " + ", ".join(aliases))
        alias = ask("Alias / grupo (ej default, ventas, matias): ").strip() or "default"

        print(f"\nCuentas del alias: {alias}")
        group = [it for it in items if it.get("alias") == alias]
        if not group:
            print("(no hay cuentas aún)")
        else:
            for it in group:
                flag = em("🟢") if it.get("active") else em("⚪")
                conn = "[conectada]" if it.get("connected") else "[no conectada]"
                sess = "[sesión]" if has_session(it["username"]) else "[sin sesión]"
                proxy_flag = _proxy_indicator(it)
                totp_flag = _totp_indicator(it)
                print(
                    f" - @{it['username']} {conn} {sess} {flag} {proxy_flag}{totp_flag}"
                )

        print("\n1) Agregar cuenta")
        print("2) Eliminar cuenta")
        print("3) Activar/Desactivar / Proxy")
        print("4) Iniciar sesión y guardar sesiónid (auto en TODAS del alias)")
        print("5) Iniciar sesión y guardar sesión ID (seleccionar cuenta)")
        print("6) Volver\n")

        op = ask("Opción: ").strip()
        if op == "1":
            u = ask("Username (sin @): ").strip().lstrip("@")
            if not u:
                continue
            if get_account(u):
                warn("Ya existe.")
                press_enter()
                continue
            proxy_data = _prompt_proxy_settings()
            totp_saved = _prompt_totp(u)
            if add_account(u, alias, proxy_data):
                if not totp_saved:
                    remove_totp_secret(u)
                prompt_login(u)
            else:
                if totp_saved:
                    remove_totp_secret(u)
            press_enter()
        elif op == "2":
            u = ask("Username a eliminar: ").strip().lstrip("@")
            remove_account(u)
            press_enter()
        elif op == "3":
            u = ask("Username: ").strip().lstrip("@")
            account = get_account(u)
            if not account:
                warn("No existe la cuenta.")
                press_enter()
                continue
            print("\n1) Activar/Desactivar")
            print("2) Editar proxy")
            print("3) Probar proxy")
            print("4) Configurar/Reemplazar TOTP")
            print("5) Eliminar TOTP")
            print("6) Volver")
            choice = ask("Opción: ").strip() or "6"
            if choice == "1":
                val = ask("1=activar, 0=desactivar: ").strip()
                set_active(u, val == "1")
                press_enter()
            elif choice == "2":
                updates = _prompt_proxy_settings(account)
                update_account(u, updates)
                record_proxy_failure(u)
                ok("Proxy actualizado.")
                press_enter()
            elif choice == "3":
                _test_existing_proxy(account)
                press_enter()
            elif choice == "4":
                configured = _prompt_totp(u)
                if not configured:
                    warn("No se configuró TOTP.")
                press_enter()
                account = get_account(u) or account
            elif choice == "5":
                if has_totp_secret(u):
                    remove_totp_secret(u)
                    ok("Se eliminó el TOTP almacenado.")
                else:
                    warn("La cuenta no tenía TOTP guardado.")
                press_enter()
                account = get_account(u) or account
            else:
                continue
        elif op == "4":
            print("Se pedirá contraseña por cada cuenta...")
            for it in [x for x in _load() if x.get("alias") == alias]:
                prompt_login(it["username"])
            press_enter()
        elif op == "5":
            group = [x for x in _load() if x.get("alias") == alias]
            if not group:
                warn("No hay cuentas para iniciar sesión.")
                press_enter()
                continue
            print("Seleccioná cuentas por número o username (coma separada, * para todas):")
            for idx, acct in enumerate(group, start=1):
                sess = "[sesión]" if has_session(acct["username"]) else "[sin sesión]"
                proxy_flag = _proxy_indicator(acct)
                totp_flag = _totp_indicator(acct)
                print(f" {idx}) @{acct['username']} {sess} {proxy_flag}{totp_flag}")
            raw = ask("Selección: ").strip()
            if not raw:
                warn("Sin selección.")
                press_enter()
                continue
            targets: List[Dict] = []
            if raw == "*":
                targets = group
            else:
                chosen = set()
                for part in raw.split(","):
                    part = part.strip()
                    if not part:
                        continue
                    if part.isdigit():
                        idx = int(part)
                        if 1 <= idx <= len(group):
                            chosen.add(group[idx - 1]["username"])
                    else:
                        chosen.add(part.lstrip("@"))
                targets = [acct for acct in group if acct["username"] in chosen]
            if not targets:
                warn("No se encontraron cuentas con esos datos.")
                press_enter()
                continue
            for acct in targets:
                prompt_login(acct["username"])
            press_enter()
        elif op == "6":
            break
        else:
            warn("Opción inválida.")
            press_enter()


# Mantener compatibilidad con importación dinámica
mark_connected.__doc__ = "Actualiza el flag de conexión en almacenamiento"
