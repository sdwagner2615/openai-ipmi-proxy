"""E2E multi-server (Phase 1 task 8).

Two servers on one proxy — alpha (redfish, a real mock BMC) and beta (noop) —
each with its own endpoint (isolated queue) and its own target. Proves:
longest-prefix routing across servers, per-server readiness, isolated queues,
and independent per-server shutdowns (shutting one down never touches the
other).

Alpha's prefix is ``/v1`` so the mock target's slow ``/v1/chat/completions``
route is reachable (used to hold alpha's spot for an in-flight request); beta's
prefix is ``/b``.
"""

import asyncio
import contextlib
import time

import httpx
import pytest

pytestmark = pytest.mark.e2e

ALPHA = "/v1/chat/completions"
ALPHA_BODY = {"model": "mock"}
BETA = "/b/echo"


async def wait_for(predicate, timeout: float = 30.0, interval: float = 0.5) -> bool:
    """Polls ``await predicate()`` until truthy; True when met, False on timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if await predicate():
            return True
        await asyncio.sleep(interval)
    return False


def _endpoints(d: dict) -> dict:
    return {e["name"]: e for e in d["endpoints"]}


def _servers(d: dict) -> dict:
    return {s["name"]: s for s in d["servers"]}


async def test_multi_routing_isolation(multi_env):
    # alpha (redfish) target up; beta (noop) target DOWN.
    env = await multi_env.make("routing", wait_policy="error", target_a_up=True, target_b_up=False)
    try:
        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{env.port}", timeout=10) as c:
            ra = await c.post(ALPHA, json=ALPHA_BODY)
            rb = await c.post(BETA, json={"x": 1})
        assert ra.status_code == 200, (
            f"alpha routed to its (up) target: {ra.status_code} {ra.text[:200]}"
        )
        assert rb.status_code == 503, (
            f"beta routed to its (down) target: {rb.status_code} {rb.text[:200]}"
        )
        assert rb.json()["error"]["code"] == "service_not_ready"
        # Independent readiness on the same proxy: alpha ready, beta not.
        d = await env.monitor()
        eps = _endpoints(d)
        assert eps["alpha-api"]["ready"] is True, f"alpha endpoint ready: {eps['alpha-api']}"
        assert eps["beta-api"]["ready"] is False, f"beta endpoint not ready: {eps['beta-api']}"
    finally:
        env.stop()


async def test_multi_isolated_queues(multi_env):
    # alpha's target is slow (holds its single spot); beta's is fast.
    env = await multi_env.make(
        "queues", target_a_up=True, target_b_up=True, delay_a=6.0, delay_b=0.0
    )
    try:
        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{env.port}", timeout=30) as c:
            slow = asyncio.create_task(c.post(ALPHA, json=ALPHA_BODY))
            try:
                await asyncio.sleep(2.0)  # alpha's request is in-flight, holding its spot
                eps = _endpoints(await env.monitor())
                assert eps["alpha-api"]["free_slots"] == 0, (
                    f"alpha spot held by in-flight request: {eps['alpha-api']}"
                )
                assert eps["beta-api"]["free_slots"] == 1, (
                    f"beta spot free (independent queue): {eps['beta-api']}"
                )
                # beta serves independently while alpha is congested.
                rb = await c.post(BETA, json={"x": 1})
                assert rb.status_code == 200, (
                    f"beta serves while alpha is busy: {rb.status_code} {rb.text[:200]}"
                )
            finally:
                with contextlib.suppress(Exception):
                    await slow  # let the slow alpha request finish (cleanup)
    finally:
        env.stop()


async def test_multi_independent_shutdown(multi_env):
    # alpha idles out in 3s; beta's idle_timeout is an hour (won't fire).
    env = await multi_env.make(
        "shutdown", idle_a=3.0, idle_b=3600.0, target_a_up=True, target_b_up=True
    )
    try:
        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{env.port}", timeout=10) as c:
            assert (await c.post(ALPHA, json=ALPHA_BODY)).status_code == 200  # adopt alpha
            assert (await c.post(BETA, json={"x": 1})).status_code == 200  # adopt beta

        async def alpha_off() -> bool:
            d = await env.monitor()
            return any(
                e["action"] == "power_off" and e["server"] == "alpha" for e in d["power_events"]
            )

        assert await wait_for(alpha_off, timeout=25), "alpha idle-off was issued"
        d = await env.monitor()
        # Only alpha was powered off; beta (long idle_timeout) is untouched.
        offs = [e for e in d["power_events"] if e["action"] == "power_off"]
        assert all(e["server"] == "alpha" for e in offs), f"only alpha powered off: {offs}"
        actions = await env.alpha_actions()
        assert actions.count("GracefulShutdown") == 1, f"exactly one off on alpha's BMC: {actions}"
        servers = _servers(d)
        assert servers["alpha"]["owned"] is False, "alpha released ownership after its off"
        assert servers["beta"]["owned"] is True, "beta still owns its box"
        assert servers["beta"]["power_state"] == "on", f"beta is still on: {servers['beta']}"
    finally:
        env.stop()
