"""E2E shutdown engine (P5-P12, D11b, D12).

Drives a real proxy subprocess with a short ``shutdown_tick`` (1s) and short
``idle_timeout`` (3s) so the 60s engine runs in seconds. The cron test uses a
per-minute schedule and therefore waits for the next boundary (~60s).
"""

import asyncio
import time

import httpx
import pytest

pytestmark = pytest.mark.e2e


async def send_request(port: int, session: str = "ses-shutdown") -> int:
    """One routed request: adopts ownership and generates activity."""
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"http://127.0.0.1:{port}/v1/chat/completions",
            json={"model": "mock", "user": session},
            headers={"x-session-id": session},
            timeout=30,
        )
    return response.status_code


async def wait_for(predicate, timeout: float = 30.0, interval: float = 0.5) -> bool:
    """Polls ``await predicate()`` until truthy; True when met, False on timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if await predicate():
            return True
        await asyncio.sleep(interval)
    return False


def _server(d: dict) -> dict:
    return next(s for s in d["servers"] if s["name"] == "testbox")


async def test_shutdown_idle_off(shutdown_env):
    proxy = await shutdown_env.make("idle-off")
    try:
        assert await send_request(proxy.port) == 200  # adopt + activity

        async def off() -> bool:
            return "GracefulShutdown" in await proxy.bmc_actions()

        assert await wait_for(off, timeout=20), "idle off was issued"
        actions = await proxy.bmc_actions()
        assert actions.count("GracefulShutdown") == 1, f"exactly one off: {actions}"
        d = await proxy.monitor()
        assert _server(d)["owned"] is False, "ownership released after a proxy off"
    finally:
        proxy.stop()


async def test_shutdown_manual_on_untouched(shutdown_env):
    proxy = await shutdown_env.make("manual-on")
    try:
        # No traffic: the box is "on" but unowned (the operator's manual-on).
        await asyncio.sleep(8)  # idle_timeout=3, several 1s ticks elapse
        actions = await proxy.bmc_actions()
        assert actions == [], f"unowned server must never be powered off: {actions}"
        d = await proxy.monitor()
        assert _server(d)["owned"] is False
    finally:
        proxy.stop()


async def test_shutdown_per_cycle_toggle_blocks_off(shutdown_env):
    proxy = await shutdown_env.make("toggle")
    try:
        async with httpx.AsyncClient() as client:
            r = await client.post(
                f"http://127.0.0.1:{proxy.port}/monitor/shutdown",
                json={"server": "testbox", "enabled": False},
                timeout=5,
            )
        assert r.status_code == 200 and r.json()["shutdown_enabled"] is False
        d = await proxy.monitor()
        assert _server(d)["shutdown_enabled"] is False
        assert await send_request(proxy.port) == 200  # adopt + activity
        await asyncio.sleep(8)  # idle, but the toggle blocks the off
        actions = await proxy.bmc_actions()
        assert "GracefulShutdown" not in actions, f"auto-off was disabled: {actions}"
    finally:
        proxy.stop()


async def test_shutdown_restart_restores_ownership_and_override(shutdown_env):
    a = await shutdown_env.make("restart-a")
    b = None
    try:
        assert await send_request(a.port) == 200  # adopt ownership
        async with httpx.AsyncClient() as client:
            r = await client.post(
                f"http://127.0.0.1:{a.port}/monitor/shutdown",
                json={"server": "testbox", "enabled": False},
                timeout=5,
            )
        assert r.status_code == 200
        a.stop()  # clean teardown persists owned + override
        b = await shutdown_env.make("restart-b", db=a.db)  # fresh proxy, same DB
        d = await b.monitor()
        assert _server(d)["owned"] is True, "ownership restored across restart"
        assert _server(d)["shutdown_enabled"] is False, "per-cycle override restored"
    finally:
        a.stop()
        if b is not None:
            b.stop()


async def test_shutdown_cron_off(shutdown_env):
    proxy = await shutdown_env.make("cron", idle_timeout=3600, schedule="* * * * *")
    try:
        assert await send_request(proxy.port) == 200  # adopt ownership

        async def off() -> bool:
            return "GracefulShutdown" in await proxy.bmc_actions()

        assert await wait_for(off, timeout=75), "scheduled off was issued"
        d = await proxy.monitor()
        reasons = [e["reason"] for e in d["power_events"]]
        assert "schedule" in reasons, f"scheduled off logged: {reasons}"
    finally:
        proxy.stop()
