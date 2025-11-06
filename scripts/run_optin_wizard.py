#!/usr/bin/env python3
"""CLI wrapper for the opt-in wizard."""
from __future__ import annotations

import argparse
import asyncio
import os

from src.opt_in import wizard


def ensure_optin_enabled() -> None:
    if os.getenv("OPTIN_ENABLE") != "1":
        raise SystemExit("Opt-in tooling is disabled. Set OPTIN_ENABLE=1 to continue.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the opt-in wizard")
    parser.add_argument("--account", required=True, help="Account alias to prepare flows for")
    return parser.parse_args()


async def main() -> None:
    ensure_optin_enabled()
    args = parse_args()
    summary = wizard.initialize_flows()
    print("Flujos creados para", args.account)
    for alias, path in summary.flows.items():
        print(f" - {alias}: {path}")


if __name__ == "__main__":
    asyncio.run(main())
