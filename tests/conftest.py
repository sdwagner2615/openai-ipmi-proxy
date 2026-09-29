"""Shared test fixtures: free ports and wait helpers (D26)."""

import asyncio
import socket
import time

import httpx
import pytest


def free_port() -> int:
    """A free 127.0.0.1 TCP port (bind-then-close; no hardcoded ports)."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def wait_http(url: str, timeout: float = 30.0, expect: int | None = 200) -> None:
    """Polls `url` until it answers `expect` (any <500 when expect is None)."""
    async with httpx.AsyncClient() as client:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                response = await client.get(url, timeout=1.0)
                if expect is None or expect == response.status_code:
                    return
            except Exception:
                pass
            await asyncio.sleep(0.25)
    raise TimeoutError(f"{url} not ready within {timeout}s")


async def wait_until(predicate, timeout: float = 15.0, interval: float = 0.25):
    """Awaits `predicate()` (async or sync) until truthy; returns its value."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if hasattr(value, "__await__"):
            value = await value
        if value:
            return value
        await asyncio.sleep(interval)
    raise TimeoutError(f"condition not met within {timeout}s")


@pytest.fixture
def logdir(tmp_path):
    """A per-test directory for subprocess logs."""
    directory = tmp_path / "logs"
    directory.mkdir()
    return directory


@pytest.fixture(autouse=True)
def isolated_cwd(tmp_path, monkeypatch):
    """Run every test from its tmp dir.

    config.py loads `.env` / `secrets.env` from the CWD and the config dir;
    a deployment `.env` in the repo must never leak into tests (env
    shielding, D26).
    """
    monkeypatch.chdir(tmp_path)
