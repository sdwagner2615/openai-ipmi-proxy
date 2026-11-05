"""E2E: a full disk never stops the proxy (D33, regression for the 2026-11-05
incident where a full disk turned every proxied request into a 500).

Simulates the disk filling up via the store's test hook
(``RELAY_STORE_FAIL_WRITES`` flag file, storage.md "Test hooks"): while the
flag exists, every commit raises the same ``OperationalError`` a full disk
produces. No root, no tmpfs, no real disk pressure — and the flag can be
created/removed at runtime, so the test also proves automatic RECOVERY.

Replay:
  1. baseline: proxying + recording work;
  2. disk "fills up";
  3. a request STILL gets 200 + the target's body (the incident assertion);
  4. the store is visibly degraded (monitor + healthz);
  5. the disk is "drained" -> the store recovers on its own and recording
     resumes (the session row lands).
"""

import asyncio
import sys
import time
from pathlib import Path

import aiosqlite
import httpx
import pytest

from tests.e2e.conftest import CONFIG, _spawn, _stop, free_port, wait_http

pytestmark = pytest.mark.e2e

REPO = Path(__file__).resolve().parents[2]


@pytest.fixture()
def diskfull_env(tmp_path):
    """Mock target + BMC (on) + one proxy wired to the disk-full flag hook."""
    logdir = tmp_path / "logs"
    logdir.mkdir()
    processes = []

    target_port = free_port()
    bmc_port = free_port()
    status_port = free_port()
    port = free_port()

    target = _spawn(
        "diskfull-target",
        [sys.executable, str(REPO / "tests/mocks/mock_target.py")],
        tmp_path,
        logdir,
        {"MOCK_TARGET_PORT": str(target_port), "MOCK_DELAY": "0.5"},
    )
    processes.append(target)
    bmc = _spawn(
        "diskfull-bmc",
        [sys.executable, str(REPO / "tests/mocks/mock_bmc.py")],
        tmp_path,
        logdir,
        {"MOCK_BMC_PORT": str(bmc_port), "MOCK_BMC_INITIAL": "on"},
    )
    processes.append(bmc)
    asyncio.run(wait_http(f"http://127.0.0.1:{target_port}/health"))
    asyncio.run(wait_http(f"http://127.0.0.1:{bmc_port}/redfish/v1/Systems/Self"))

    cfg_dir = tmp_path / "proxy"
    cfg_dir.mkdir()
    flag = tmp_path / "disk-full.flag"
    values = {
        "port": port,
        "policy": "allow",
        "read_timeout": 0,
        "bmc_port": bmc_port,
        "target_port": target_port,
        "status_port": status_port,
        "concurrency": 1,
        "per_session": -1,
        "request_mode": "parallel",
        "queue_timeout": 0,
        "immediate": "true",
        "db": str(cfg_dir / "relay.db"),
    }
    cfg = cfg_dir / "config.yaml"
    cfg.write_text(CONFIG.format(**values))
    proxy = _spawn(
        "diskfull-proxy",
        [sys.executable, "-m", "relay", "--config", str(cfg)],
        cfg_dir,
        logdir,
        {"RELAY_STORE_FAIL_WRITES": str(flag)},
    )
    processes.append(proxy)
    asyncio.run(wait_http(f"http://127.0.0.1:{port}/healthz"))

    yield _Env(
        port=port,
        db=str(cfg_dir / "relay.db"),
        flag=flag,
        logdir=logdir,
    )
    for proc in processes:
        _stop(proc)


class _Env:
    def __init__(self, port, db, flag, logdir):
        self.port = port
        self.db = db
        self.flag = flag
        self.logdir = logdir

    async def chat(self) -> httpx.Response:
        """One queued request through the proxy (the incident's request)."""
        async with httpx.AsyncClient() as client:
            return await client.post(
                f"http://127.0.0.1:{self.port}/v1/chat/completions",
                json={"model": "mock", "user": "diskfull-e2e"},
                headers={"x-session-id": "ses-diskfull"},
                timeout=30,
            )

    async def monitor(self) -> dict:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"http://127.0.0.1:{self.port}/monitor/data", timeout=5)
            response.raise_for_status()
            return response.json()

    async def healthz(self) -> httpx.Response:
        async with httpx.AsyncClient() as client:
            return await client.get(f"http://127.0.0.1:{self.port}/healthz", timeout=5)


async def _store_degraded(env: "_Env", value: bool) -> bool:
    return (await env.monitor())["store"]["degraded"] is value


async def _session_recorded(env: "_Env") -> bool:
    return await _session_request_count(env.db) >= 1


async def _wait_for(predicate, timeout: float = 15.0, interval: float = 0.5) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if await predicate():
            return True
        await asyncio.sleep(interval)
    return False


async def test_disk_full_keeps_proxying_and_recovers(diskfull_env):
    env = diskfull_env

    # 1) Baseline: proxying + recording both work.
    response = await env.chat()
    assert response.status_code == 200
    assert response.json()["choices"][0]["message"]["content"] == "done"
    assert (await env.monitor())["store"]["degraded"] is False

    # 2) The disk "fills up".
    env.flag.touch()

    # 3) THE INCIDENT ASSERTION: the request is still proxied end to end.
    response = await env.chat()
    assert response.status_code == 200
    assert response.json()["choices"][0]["message"]["content"] == "done"
    # (and it keeps working while the disk stays full)
    assert (await env.chat()).status_code == 200

    # 4) The store is visibly degraded — in the monitor and in healthz.
    assert await _wait_for(lambda: _store_degraded(env, True))
    store = (await env.monitor())["store"]
    assert store["dropped_writes"] > 0
    healthz = await env.healthz()
    assert healthz.status_code == 200  # proxying is fine: no 503
    assert healthz.json()["store"] == "degraded"

    # 5) The disk is "drained": the store recovers on its own, and recording
    #    resumes (the session row lands).
    env.flag.unlink()
    assert (await env.chat()).status_code == 200
    assert await _wait_for(lambda: _store_degraded(env, False))
    assert await _wait_for(lambda: _session_recorded(env)), (
        "session history should resume after recovery"
    )
    # healthz returns to normal too.
    assert (await env.healthz()).json()["store"] == "ok"


async def _session_request_count(db_path: str) -> int:
    async with aiosqlite.connect(db_path) as db:
        cursor = await db.execute(
            "SELECT COALESCE(SUM(request_count), 0) FROM sessions WHERE session_id = ?",
            ("ses-diskfull",),
        )
        row = await cursor.fetchone()
    return row[0]
