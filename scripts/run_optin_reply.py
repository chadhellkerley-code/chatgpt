#!/usr/bin/env python3
"""Reply to unread conversations using the opt-in toolkit."""
from __future__ import annotations

import argparse
import asyncio
import os

from src.opt_in import responder


def ensure_optin_enabled() -> None:
    if os.getenv("OPTIN_ENABLE") != "1":
        raise SystemExit("Opt-in tooling is disabled. Set OPTIN_ENABLE=1 to continue.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reply to Instagram messages")
    parser.add_argument("--account", required=True, help="Account alias")
    parser.add_argument("--text", help="Override reply text")
    parser.add_argument("--template-id", help="Template identifier", dest="template_id")
    return parser.parse_args()


async def main() -> None:
    ensure_optin_enabled()
    args = parse_args()
    text = args.text or f"Template {args.template_id or 'default'}"
    await responder.respond_unread(args.account, text)


if __name__ == "__main__":
    asyncio.run(main())
