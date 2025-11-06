import asyncio

from src.opt_in import audit, runner_pool


def test_runner_pool_respects_circuit_breaker(monkeypatch):
    events = []
    async def noop_sleep(*args, **kwargs):
        return None

    monkeypatch.setattr(runner_pool, "_sleep_with_jitter", lambda base: noop_sleep())
    monkeypatch.setattr(runner_pool.asyncio, "sleep", noop_sleep)
    monkeypatch.setattr(audit, "record_event", lambda *args, **kwargs: events.append((args, kwargs)))

    attempts = {"a": 0}

    async def action(account, params):
        attempts[account] += 1
        raise RuntimeError("boom")

    asyncio.run(runner_pool.run_many(["a"], action, {"a": {}}, max_concurrency=1, max_consecutive_errors=1))

    assert attempts["a"] == 1
