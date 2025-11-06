import asyncio
from src.opt_in import browser_manager, flows


def test_flow_falls_back_to_login(monkeypatch, tmp_path):
    calls = []
    executed = {"main": 0}

    async def fake_launch(account, **kwargs):
        class Dummy:
            async def close(self):
                pass

        page = object()
        return Dummy(), Dummy(), page

    async def fake_close(*args, **kwargs):
        return None

    async def fake_run(page, flow, variables):
        name = flow.get("name")
        calls.append(name)
        if name == "main" and executed["main"] == 0:
            executed["main"] += 1
            raise flows.FlowError("session expired")

    async def fake_load(name):
        return {"name": "login" if name == "login" else "main", "steps": []}

    monkeypatch.setattr(browser_manager, "launch_browser", fake_launch)
    monkeypatch.setattr(browser_manager, "close_browser_tuple", fake_close)
    monkeypatch.setattr(flows, "_load_flow", lambda alias: fake_load(alias))
    monkeypatch.setattr(flows, "_run_flow_once", fake_run)
    monkeypatch.setattr(flows.session_store, "save_storage_state", lambda *a, **k: asyncio.sleep(0))

    asyncio.run(flows.run_flow("main", {}, "account1"))
    assert calls == ["main", "login", "main"]
