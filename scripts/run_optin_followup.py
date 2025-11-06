#!/usr/bin/env python3
"""Follow-up CLI."""
from __future__ import annotations

import argparse
import asyncio
import os

from src.opt_in import followup


def ensure_optin_enabled() -> None:
    if os.getenv("OPTIN_ENABLE") != "1":
        raise SystemExit("Opt-in tooling is disabled. Set OPTIN_ENABLE=1 to continue.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send follow-up messages")
    parser.add_argument("--account", required=True, help="Account alias")
    parser.add_argument("--hours", type=int, default=24, help="Threshold in hours")
    parser.add_argument("--text", required=True, help="Follow-up message")
    return parser.parse_args()


async def main() -> None:
    ensure_optin_enabled()
    args = parse_args()
    await followup.run_followup(args.account, args.hours, args.text)


if __name__ == "__main__":
    asyncio.run(main())
