"""CLI para responder automáticamente DMs no leídos."""
from __future__ import annotations

import argparse

from optin_browser.replies import reply_unread


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Responde DMs no leídos que coincidan con un filtro.")
    parser.add_argument("--account", required=True, help="Alias local de la cuenta")
    parser.add_argument("--contains", help="Texto o regex a buscar en el hilo")
    parser.add_argument("--reply", required=True, help="Mensaje de respuesta (puede usar {username})")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    total = reply_unread(args.account, args.contains, args.reply)
    print(f"Respuestas enviadas: {total}")


if __name__ == "__main__":
    main()
