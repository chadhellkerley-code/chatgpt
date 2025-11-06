"""CLI para iniciar sesión en Instagram usando el toolkit opt-in."""
from __future__ import annotations

import argparse

from optin_browser.login import login, LoginError


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inicia sesión y guarda la sesión cifrada.")
    parser.add_argument("--account", required=True, help="Alias local de la cuenta")
    parser.add_argument("--user", required=True, help="Usuario de Instagram")
    parser.add_argument("--password", required=True, help="Contraseña de Instagram")
    parser.add_argument("--resend", action="store_true", help="Permitir reenviar el código 2FA si es necesario")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        login(args.account, args.user, args.password, allow_resend=args.resend)
        print("Inicio de sesión completado y sesión guardada.")
    except LoginError as exc:
        print(f"Error: {exc}")


if __name__ == "__main__":
    main()
