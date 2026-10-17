"""E2E cold boot: BMC OFF + target down.

The first queued request wakes the box (exactly one power-on: the 30s
cooldown covers the whole test window), no 503 is ever served while the
box "boots", and the request is served once the target comes up.
"""

import asyncio
import time

import httpx
import pytest

pytestmark = pytest.mark.e2e

CHAT = "/v1/chat/completions"
BODY = {"model": "mock", "messages": [{"role": "user", "content": "hi"}], "stream": False}


def bmc_actions(env) -> dict:
    return httpx.get(f"http://127.0.0.1:{env.bmc_port}/actions", timeout=3).json()


async def test_cold_boot_single_power_on_no_503(boot_env):
    async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{boot_env.port}", timeout=30) as c:
        d = await boot_env.monitor()
        assert d["config"]["server_powered_on"] is False, (
            f"box starts powered off: {d['config']['server_powered_on']}"
        )

        t0 = time.monotonic()
        task = asyncio.create_task(c.post(CHAT, json=BODY, headers={"x-session-id": "boot-1"}))
        await asyncio.sleep(3)
        s = boot_env.find_session(await boot_env.monitor(), "boot-1")
        assert s is not None and s["waiting"] == 1 and s["queue_position"] == 1, (
            f"request held in the queue while the box is off: {s}"
        )
        actions = bmc_actions(boot_env)
        assert actions["count"] == 1 and actions["actions"] == ["On"], (
            f"the first waiter triggered exactly one power-on: {actions}"
        )

        # The box "boots": its service comes up on the target port.
        boot_env.spawn_target()
        await boot_env.wait_http(f"http://127.0.0.1:{boot_env.target_port}/health")
        resp = await task
        total = time.monotonic() - t0
        assert resp.status_code == 200 and total > 3, (
            f"no 503; served once the target is healthy: {resp.status_code} after {total:.1f}s"
        )

        d = await boot_env.monitor()
        assert d["config"]["server_powered_on"] is True, (
            f"power state confirmed ON after boot: {d['config']['server_powered_on']}"
        )
        actions = bmc_actions(boot_env)
        assert actions["count"] == 1, (
            f"no re-issued power-on while the boot cycle completes: {actions}"
        )
