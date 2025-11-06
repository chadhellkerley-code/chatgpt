#!/usr/bin/env python3
"""Send a direct message using the opt-in toolkit."""
from __future__ import annotations

import argparse
import asyncio
import os

from src.opt_in import audit, browser_manager, messenger_playwright


def ensure_optin_enabled() -> None:
    if os.getenv("OPTIN_ENABLE") != "1":
        raise SystemExit("Opt-in tooling is disabled. Set OPTIN_ENABLE=1 to continue.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send a DM using the opt-in tooling")
    parser.add_argument("--account", required=True, help="Account alias")
    parser.add_argument("--to", required=True, help="Recipient username")
    parser.add_argument("--text", required=True, help="Text to send")
    return parser.parse_args()


async def main() -> None:
    ensure_optin_enabled()
    args = parse_args()
    browser, context, page = await browser_manager.launch_browser(args.account)
    try:
        inbox = await messenger_playwright.open_inbox(page)
        if not inbox.ok:
            audit.record_event(args.account, "send_open_inbox", "failed", message=inbox.message)
            return
        composer = await messenger_playwright.open_composer(page)
        if not composer.ok:
            audit.record_event(args.account, "send_open_composer", "failed", message=composer.message)
            return
        lookup = await messenger_playwright.search_user(page, args.to)
        if not lookup.ok:
            audit.record_event(args.account, "send_search_user", "failed", message=lookup.message)
            return
        send = await messenger_playwright.send_message(page, args.text)
        status = "ok" if send.ok else "blocked" if send.action_blocked else "failed"
        audit.record_event(args.account, "send_message", status, message=send.message, meta={"to": args.to})
    finally:
        await browser_manager.close_browser_tuple(browser, context, page)


if __name__ == "__main__":
    asyncio.run(main())
