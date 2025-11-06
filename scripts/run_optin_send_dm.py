"""CLI para enviar un DM usando la sesión opt-in."""
from __future__ import annotations

import argparse

from optin_browser.dm import DirectMessageError, send_dm


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Envía un mensaje directo utilizando Playwright.")
    parser.add_argument("--account", required=True, help="Alias local de la cuenta")
    parser.add_argument("--to", required=True, help="Usuario destino")
    parser.add_argument("--text", required=True, help="Contenido del mensaje")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        send_dm(args.account, args.to, args.text)
        print("Mensaje enviado correctamente.")
    except DirectMessageError as exc:
        print(f"Error al enviar: {exc}")


if __name__ == "__main__":
    main()
