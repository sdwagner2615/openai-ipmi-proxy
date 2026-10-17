"""E2E ownership adoption (P5-P7, D12).

A request routed to a service adopts power ownership of its server; a
403-blocked unknown path does not (deliberate deviation from the legacy
proxy, which adopted on every request - recorded in docs/relay/progress.md).
"""

import httpx
import pytest

pytestmark = pytest.mark.e2e

CHAT = "/v1/chat/completions"
BODY = {"model": "mock", "messages": [{"role": "user", "content": "hi"}], "stream": False}


def bmc_actions(env) -> dict:
    return httpx.get(f"http://127.0.0.1:{env.bmc_port}/actions", timeout=3).json()


async def test_blocked_request_does_not_adopt(adopt_env):
    async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{adopt_env.port}", timeout=30) as c:
        d = await adopt_env.monitor()
        assert d["config"]["power_managed"] is False, (
            f"fresh proxy does not own a box it never touched: {d['config']['power_managed']}"
        )
        r = await c.get("/custom/thing")
        assert r.status_code == 403 and r.json()["error"]["code"] == "unknown_api_blocked", (
            f"unknown path blocked: {r.status_code} {r.text[:200]}"
        )
        d = await adopt_env.monitor()
        assert d["config"]["power_managed"] is False, "403-blocked request must not adopt ownership"


async def test_routed_traffic_adopts(adopt_env):
    async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{adopt_env.port}", timeout=30) as c:
        r = await c.post(CHAT, json=BODY, headers={"x-session-id": "adopt-1"})
        assert r.status_code == 200, f"routed request succeeds: {r.status_code} {r.text[:200]}"
        d = await adopt_env.monitor()
        assert d["config"]["power_managed"] is True, (
            f"routed traffic adopted ownership: {d['config']['power_managed']}"
        )
        actions = bmc_actions(adopt_env)
        assert actions["count"] == 0, (
            f"adoption is not a power action (the box was already on): {actions}"
        )
