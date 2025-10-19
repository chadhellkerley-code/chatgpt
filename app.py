# app.py
# -*- coding: utf-8 -*-
import importlib
import time

from config import SETTINGS
from storage import sent_totals_today
from ui import Fore, full_line, print_daily_metrics, print_header, style_text
from utils import ask, em, press_enter, warn


def _safe_import(name):
    try:
        return importlib.import_module(name)
    except Exception as e:
        warn(f"Módulo no disponible o con error: {name} ({e})")
        return None


accounts = _safe_import("accounts")
leads = _safe_import("leads")
ig = _safe_import("ig")
storage = _safe_import("storage")
responder = _safe_import("responder")
licensekit = _safe_import("licensekit")


def _counts():
    try:
        items = accounts.list_all()
        total = len(items)
        connected = sum(1 for it in items if it.get("connected"))
        active = sum(1 for it in items if it.get("active"))
        return total, connected, active
    except Exception:
        return 0, 0, 0


def _print_dashboard() -> None:
    print_header()
    total, connected, active = _counts()
    sent_today, err_today, last_reset, tz_label = sent_totals_today()

    line = full_line(color=Fore.BLUE, bold=True)
    section = style_text(em("📊  ESTADO GENERAL"), color=Fore.CYAN, bold=True)
    print(section)
    print(line)
    print(style_text(f"Cuentas totales: {total}", bold=True))
    print(style_text(f"Conectadas: {connected}", color=Fore.GREEN if connected else Fore.WHITE, bold=True))
    print(style_text(f"Activas: {active}", color=Fore.CYAN if active else Fore.WHITE, bold=True))
    print(line)
    print_daily_metrics(
        sent_today,
        err_today,
        tz_label,
        last_reset,
    )
    print()
    options = [
        f"1) {em('🔐')} Gestionar cuentas  ",
        f"2) {em('🗂️')} Gestionar leads (crear / importar CSV)  ",
        f"3) {em('💬')} Enviar mensajes (rotando cuentas activas)  ",
        f"4) {em('📜')} Ver registros de envíos  ",
        f"5) {em('🤖')} Auto-responder con OpenAI  ",
        f"6) {em('📊')} Configurar Supabase  ",
    ]
    if not SETTINGS.client_distribution:
        options.append(f"7) {em('📦')} Entregar a cliente (licencia / EXE)  ")
    options.append(f"8) {em('🚪')} Salir  ")
    for text in options:
        print(style_text(text))
    print()
    print(line)


def menu():
    if licensekit and hasattr(licensekit, "enforce_startup_validation"):
        licensekit.enforce_startup_validation()
    while True:
        _print_dashboard()
        op = ask("Opción: ").strip()
        if op == "1" and accounts:
            accounts.menu_accounts()
        elif op == "2" and leads:
            leads.menu_leads()
        elif op == "3" and ig:
            ig.menu_send_rotating()
        elif op == "4" and storage:
            storage.menu_logs()
        elif op == "5" and responder:
            responder.menu_autoresponder()
        elif op == "6" and storage:
            storage.menu_supabase()
        elif (
            op == "7"
            and licensekit
            and not SETTINGS.client_distribution
        ):
            licensekit.menu_deliver()
        elif op == "8":
            print("Saliendo...")
            time.sleep(0.3)
            break
        else:
            warn("Opción inválida o módulo faltante.")
            press_enter()


if __name__ == "__main__":
    menu()
