#!/usr/bin/env python3
"""Batch sender supporting multiple accounts concurrently."""
from __future__ import annotations

import argparse
import asyncio
import csv
import os
from collections import defaultdict
from pathlib import Path
from typing import Dict, List

from src.opt_in import account_loader, audit, browser_manager, messenger_playwright, runner_pool


def ensure_optin_enabled() -> None:
    if os.getenv("OPTIN_ENABLE") != "1":
        raise SystemExit("Opt-in tooling is disabled. Set OPTIN_ENABLE=1 to continue.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batch send messages with multiple accounts")
    parser.add_argument("--accounts", required=True, help="Path to accounts CSV/JSON")
    parser.add_argument("--recipients", required=True, help="Path to recipients CSV")
    parser.add_argument("--text", required=True, help="Default text to send")
    parser.add_argument("--max-concurrency", type=int, default=2)
    parser.add_argument("--per-account-delay", default="0,0")
    parser.add_argument("--dm-per-hour-limit", type=int, default=0)
    parser.add_argument("--backoff", default="base=30,max=900")
    return parser.parse_args()


def load_recipients(path: Path) -> Dict[str, List[Dict[str, str]]]:
    mapping: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    with path.open("r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            alias = (row.get("account") or "").strip()
            if not alias:
                continue
            mapping[alias].append(row)
    return mapping


async def send_for_account(account: account_loader.Account, params: Dict[str, str]) -> None:
    recipients: List[Dict[str, str]] = params.get("recipients", [])
    default_text = params.get("default_text", "")
    browser, context, page = await browser_manager.launch_browser(
        account.alias,
        proxy_url=account.proxy_url,
        user_agent=account.user_agent,
    )
    try:
        inbox = await messenger_playwright.open_inbox(page)
        if not inbox.ok:
            audit.record_event(account.alias, "batch_send_open_inbox", "failed", message=inbox.message)
            return
        for row in recipients:
            text = row.get("text") or default_text
            if not text:
                continue
            composer = await messenger_playwright.open_composer(page)
            if not composer.ok:
                audit.record_event(account.alias, "batch_send_open_composer", "failed", message=composer.message)
                break
            lookup = await messenger_playwright.search_user(page, row.get("to_username") or row.get("to"))
            if not lookup.ok:
                audit.record_event(account.alias, "batch_send_search", "failed", message=lookup.message)
                continue
            send = await messenger_playwright.send_message(page, text)
            status = "ok" if send.ok else "blocked" if send.action_blocked else "failed"
            audit.record_event(
                account.alias,
                "batch_send_message",
                status,
                message=send.message,
                meta={"to": row.get("to_username") or row.get("to")},
            )
            if send.action_blocked:
                break
    finally:
        await browser_manager.close_browser_tuple(browser, context, page)


async def main() -> None:
    ensure_optin_enabled()
    args = parse_args()
    accounts = account_loader.load_accounts(Path(args.accounts))
    recipients = load_recipients(Path(args.recipients))
    accounts_map = {acc.alias: acc for acc in accounts}

    per_account_args = {
        alias: {"recipients": recipients.get(alias, []), "default_text": args.text}
        for alias in accounts_map
    }

    rate_limits = None
    if args.dm_per_hour_limit:
        cfg = runner_pool.RateLimitConfig(dm_per_hour_limit=args.dm_per_hour_limit)
        rate_limits = {alias: cfg for alias in accounts_map}

    base, _, maximum = args.backoff.partition(",")
    base_value = float(base.split("=")[-1] or 30)
    max_value = float(maximum.split("=")[-1] or 900)
    backoff_cfg = runner_pool.BackoffConfig(base=base_value, maximum=max_value)

    async def action(alias: str, params: Dict[str, str]) -> None:
        await send_for_account(accounts_map[alias], params)

    await runner_pool.run_many(
        accounts_map.keys(),
        action,
        per_account_args,
        max_concurrency=args.max_concurrency,
        rate_limits=rate_limits,
        backoff=backoff_cfg,
        max_consecutive_errors=int(os.getenv("MAX_CONSECUTIVE_ERRORS_PER_ACCOUNT", "3")),
    )


if __name__ == "__main__":
    asyncio.run(main())
