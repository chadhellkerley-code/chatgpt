#!/usr/bin/env python3
"""Batch reply CLI."""
from __future__ import annotations

import argparse
import asyncio
import os
from pathlib import Path
from typing import Dict

from src.opt_in import account_loader, responder, runner_pool


def ensure_optin_enabled() -> None:
    if os.getenv("OPTIN_ENABLE") != "1":
        raise SystemExit("Opt-in tooling is disabled. Set OPTIN_ENABLE=1 to continue.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batch reply using multiple accounts")
    parser.add_argument("--accounts", required=True, help="Path to accounts CSV/JSON")
    parser.add_argument("--template-id", required=True, help="Template id")
    parser.add_argument("--max-concurrency", type=int, default=2)
    return parser.parse_args()


async def main() -> None:
    ensure_optin_enabled()
    args = parse_args()
    accounts = account_loader.load_accounts(Path(args.accounts))
    accounts_map = {acc.alias: acc for acc in accounts}

    per_account_args: Dict[str, Dict[str, str]] = {
        alias: {"template_id": args.template_id}
        for alias in accounts_map
    }

    async def action(alias: str, params: Dict[str, str]) -> None:
        text = f"Template {params['template_id']}"
        await responder.respond_unread(alias, text)

    await runner_pool.run_many(
        accounts_map.keys(),
        action,
        per_account_args,
        max_concurrency=args.max_concurrency,
        max_consecutive_errors=int(os.getenv("MAX_CONSECUTIVE_ERRORS_PER_ACCOUNT", "3")),
    )


if __name__ == "__main__":
    asyncio.run(main())
