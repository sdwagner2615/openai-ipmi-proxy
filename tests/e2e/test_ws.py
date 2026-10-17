"""E2E WebSocket transport (D18).

Drives a real proxy subprocess against a startable/stoppable mock WS target:
the echo tunnel, the wait-policy hold (keep-alive while the target boots), the
1013 close on the error policy, slot lifetime for a queued endpoint, and
query-param session resolution.
"""

import asyncio
import contextlib

import pytest
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

pytestmark = pytest.mark.e2e

OPEN_TIMEOUT = 15.0
RECV_TIMEOUT = 15.0


def _session(d: dict, sid: str) -> dict | None:
    for row in d["sessions"]:
        if row["session"] == sid:
            return row
    return None


async def test_ws_echo_tunnels(ws_env):
    env = await ws_env.make("ws-echo", target_up=True)
    try:
        url = f"ws://127.0.0.1:{env.port}/ws/echo"
        async with await connect(url, open_timeout=OPEN_TIMEOUT) as ws:
            await ws.send("hello")
            reply = await asyncio.wait_for(ws.recv(), timeout=RECV_TIMEOUT)
            assert reply == "hello"
    finally:
        env.stop()


async def test_ws_keep_alive_while_waiting(ws_env):
    env = await ws_env.make("ws-wait", target_up=False)
    try:
        url = f"ws://127.0.0.1:{env.port}/ws/boot"
        ws = await connect(url, open_timeout=OPEN_TIMEOUT)
        try:
            # Target down: the wait policy holds the (already-accepted)
            # connection open with keep-alives instead of closing it. A data
            # recv() blocks (pings are handled by the client, not surfaced),
            # so a timeout proves the connection was NOT closed.
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(ws.recv(), timeout=3)
            # Bring the target up; the hold ends and the tunnel round-trips.
            await env.start_target()
            await ws.send("hi")
            reply = await asyncio.wait_for(ws.recv(), timeout=RECV_TIMEOUT)
            assert reply == "hi"
        finally:
            with contextlib.suppress(Exception):
                await ws.close()
    finally:
        env.stop()


async def test_ws_error_policy_closes_1013(ws_env):
    env = await ws_env.make("ws-1013", target_up=False)
    try:
        url = f"ws://127.0.0.1:{env.port}/wserr/x"
        async with await connect(url, open_timeout=OPEN_TIMEOUT) as ws:
            with pytest.raises(ConnectionClosed) as exc:
                await ws.recv()
            assert exc.value.rcvd is not None
            assert exc.value.rcvd.code == 1013
    finally:
        env.stop()


async def test_ws_slot_held_for_connection(ws_env):
    env = await ws_env.make("ws-slot", concurrency=1, target_up=True)
    try:
        base = f"ws://127.0.0.1:{env.port}/ws"
        ws1 = await connect(base + "/s1", open_timeout=OPEN_TIMEOUT)
        ws2 = await connect(base + "/s2", open_timeout=OPEN_TIMEOUT)
        try:
            # ws1 takes the single spot and tunnels.
            await ws1.send("one")
            assert await asyncio.wait_for(ws1.recv(), timeout=RECV_TIMEOUT) == "one"
            # ws2 is blocked on the spot: it stays open (keep-alive), no tunnel.
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(ws2.recv(), timeout=3)
            # Releasing ws1's spot lets ws2 acquire it and tunnel.
            await ws1.close()
            await ws2.send("two")
            assert await asyncio.wait_for(ws2.recv(), timeout=RECV_TIMEOUT) == "two"
        finally:
            with contextlib.suppress(Exception):
                await ws1.close()
            with contextlib.suppress(Exception):
                await ws2.close()
    finally:
        env.stop()


async def test_ws_query_param_session_id(ws_env):
    env = await ws_env.make("ws-sid", target_up=True)
    try:
        url = f"ws://127.0.0.1:{env.port}/ws/echo?x-session-id=wsq-1"
        ws = await connect(url, open_timeout=OPEN_TIMEOUT)
        try:
            await ws.send("ping")
            assert await asyncio.wait_for(ws.recv(), timeout=RECV_TIMEOUT) == "ping"
            data = await env.monitor()
            row = _session(data, "wsq-1")
            assert row is not None, f"query-param session not tracked: {data['sessions']}"
            assert row["endpoint"] == "ws"
        finally:
            with contextlib.suppress(Exception):
                await ws.close()
    finally:
        env.stop()
