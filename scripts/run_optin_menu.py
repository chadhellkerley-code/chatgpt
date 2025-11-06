#!/usr/bin/env python3
"""Command line menu for the Instagram opt-in tooling."""
from __future__ import annotations

import asyncio
import os
from pathlib import Path

from src.opt_in import followup, responder, wizard


def ensure_optin_enabled() -> None:
    if os.getenv("OPTIN_ENABLE") != "1":
        raise SystemExit("Opt-in tooling is disabled. Set OPTIN_ENABLE=1 to continue.")


def _input(prompt: str) -> str:
    try:
        return input(prompt)
    except EOFError:  # pragma: no cover - interactive helper
        return ""


async def _run_single_followup() -> None:
    account = _input("Cuenta alias: ").strip()
    hours = int(_input("Horas desde el último mensaje: ").strip() or "24")
    text = _input("Texto de seguimiento: ")
    await followup.run_followup(account, hours, text)


async def _run_single_responder() -> None:
    account = _input("Cuenta alias: ").strip()
    text = _input("Texto de respuesta: ")
    await responder.respond_unread(account, text)


async def _handle_option(choice: str) -> None:
    if choice == "1":
        wizard.initialize_flows()
    elif choice == "2":
        await _run_single_responder()
    elif choice == "3":
        await _run_single_responder()
    elif choice == "4":
        await _run_single_followup()
    elif choice == "5":
        print("Multi-cuenta envío: ejecutar scripts/run_optin_batch_send.py")
    elif choice == "6":
        print("Multi-cuenta responder: ejecutar scripts/run_optin_batch_reply.py")
    elif choice == "7":
        print("Multi-cuenta seguimiento: ejecutar scripts/run_optin_batch_followup.py")
    elif choice == "8":
        summary = wizard.initialize_flows()
        print("Flujos disponibles:")
        for alias, path in summary.flows.items():
            print(f" - {alias}: {path}")
    elif choice == "9":
        raise SystemExit(0)


async def main() -> None:
    ensure_optin_enabled()
    while True:
        print("Modo Automático (aprendizaje + envío humanizado)")
        print("1) Grabar una sola vez (wizard)")
        print("2) Enviar mensajes (single account)")
        print("3) Responder mensajes (single account)")
        print("4) Seguimiento (single account)")
        print("5) Envío MULTI-CUENTA (paralelo)")
        print("6) Responder MULTI-CUENTA (paralelo)")
        print("7) Seguimiento MULTI-CUENTA (paralelo)")
        print("8) Ver estado (sesiones/flows)")
        print("9) Volver")
        choice = _input("Selecciona una opción: ").strip()
        await _handle_option(choice)


if __name__ == "__main__":
    asyncio.run(main())
