"""Async runner pool for executing opt-in actions across multiple accounts."""
from __future__ import annotations

import asyncio
import math
import os
import random
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional

from . import audit


@dataclass
class RateLimitConfig:
    dm_per_hour_limit: int
    jitter_factor: float = 0.2


@dataclass
class BackoffConfig:
    base: float = 30
    maximum: float = 900


async def _sleep_with_jitter(base: float) -> None:
    factor = random.uniform(0.8, 1.2)
    await asyncio.sleep(base * factor)


async def run_many(
    accounts: Iterable[str],
    action: Callable[[str, Dict[str, Any]], Awaitable[Any]],
    per_account_args: Dict[str, Dict[str, Any]],
    max_concurrency: int,
    rate_limits: Optional[Dict[str, RateLimitConfig]] = None,
    backoff: Optional[BackoffConfig] = None,
    max_consecutive_errors: int = 3,
) -> None:
    semaphore = asyncio.Semaphore(max_concurrency)
    errors: Dict[str, int] = defaultdict(int)

    async def run_for_account(account: str) -> None:
        async with semaphore:
            args = per_account_args.get(account, {})
            rl = rate_limits.get(account) if rate_limits else None
            backoff_state = backoff or BackoffConfig()
            delay_bounds = [float(os.getenv("DELAY_MIN_S", "1")), float(os.getenv("DELAY_MAX_S", "2"))]
            try:
                await _sleep_with_jitter(delay_bounds[0])
                await action(account, args)
                errors[account] = 0
                if rl and rl.dm_per_hour_limit > 0:
                    base_delay = 3600 / rl.dm_per_hour_limit
                    await _sleep_with_jitter(base_delay)
            except Exception as exc:  # pragma: no cover - orchestrator safeguard
                errors[account] += 1
                audit.record_event(account, "runner_pool_action", "failed", message=str(exc))
                if errors[account] >= max_consecutive_errors:
                    audit.record_event(account, "runner_pool_circuit", "open")
                    return
                cooldown = min(backoff_state.base * (2 ** (errors[account] - 1)), backoff_state.maximum)
                await asyncio.sleep(cooldown)
                await run_for_account(account)

    await asyncio.gather(*(run_for_account(account) for account in accounts))
