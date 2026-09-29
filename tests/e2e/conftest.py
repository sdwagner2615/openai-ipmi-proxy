"""Shared e2e environment (D26): mock services + relay proxy variants.

Everything runs on 127.0.0.1 with dynamic ports (no docker, no hardware).
Every proxy gets its own working directory with a fully self-contained
config (no ${ENV} references), so a deployment `.env` can never leak in:
the config loader reads `.env`/`secrets.env` from the CWD and the config
dir, and neither contains one here.

Ported from scripts/test_queue.py (fixed ports became free_port()).
"""

import asyncio
import contextlib
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

REPO = Path(__file__).resolve().parents[2]
MOCK_DELAY = 2.0

# variant name -> overrides over the shared baseline (old BASE_ENV). The keys
# are the CONFIG template placeholder names.
VARIANTS = {
    "base": {},
    "block": {"policy": "block", "queue_timeout": 3},
    "serial": {"per_session": 0},
    "atomic": {"concurrency": 2, "request_mode": "atomic"},
    "parallel": {"concurrency": 2},
    "immediate": {},
    "cooldown": {"immediate": False},
    "readtimeout": {"read_timeout": 1},
}

CONFIG = """\
proxy:
  host: 127.0.0.1
  port: {port}
  unknown_path_policy: {policy}
  target_read_timeout: {read_timeout}
  session_id_headers:
    - x-session-id
  store:
    path: {db}

servers:
  - name: testbox
    type: redfish
    power:
      host: 127.0.0.1
      user: test
      password: test
      base_url: http://127.0.0.1:{bmc_port}
      system_path: /redfish/v1/Systems/Self
      verify_ssl: false
    service_url: http://127.0.0.1:{target_port}
    idle_timeout: 3600
    shutdown_enabled: true
    adopt_on_traffic: true

endpoints:
  - name: openai
    server: testbox
    path_prefix: /v1
    catch_all: true
    readiness:
      path: /health
      interval: 1
      timeout: 1
    wait_policy: wait
    routing: queued
    concurrency: {concurrency}
    queue_timeout: {queue_timeout}
    session_id_body_fields:
      - user
    session:
      per_session_requests: {per_session}
      request_mode: {request_mode}
      busy_window: 2
      expiry: 4
      immediate_idle_release: {immediate}
  - name: anthropic
    server: testbox
    path_prefix: /v1/messages
    readiness:
      path: /health
      interval: 1
      timeout: 1
    wait_policy: wait
    routing: queued
    concurrency: 1
    queue_timeout: {queue_timeout}
    session_id_body_fields:
      - metadata.user_id
    session:
      per_session_requests: -1
      request_mode: parallel
      busy_window: 2
      expiry: 4
      immediate_idle_release: {immediate}

clients:
  - name: opencode
    match:
      ua_prefix: opencode/
      session_headers:
        - x-opencode-session
      gated_session_headers:
        - x-session-affinity
        - x-session-id
    status:
      kind: opencode
      port: {status_port}
      poll_interval: 1
    children:
      kind: parent-chain
      depth: 10
"""


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def wait_http(url: str, timeout: float = 60.0, expect: int = 200) -> None:
    async with httpx.AsyncClient() as client:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                response = await client.get(url, timeout=1.0)
                if response.status_code == expect:
                    return
            except Exception:
                pass
            await asyncio.sleep(0.25)
    raise TimeoutError(f"{url} not ready within {timeout}s")


def _spawn(
    name: str,
    args: list[str],
    cwd: Path,
    logdir: Path,
    extra_env: dict[str, str] | None = None,
) -> subprocess.Popen:
    # Kept open for the subprocess's lifetime (it inherits the handle).
    log = open(logdir / f"{name}.log", "ab")  # noqa: SIM115
    return subprocess.Popen(
        args,
        cwd=str(cwd),
        env={**os.environ, **(extra_env or {})},
        stdout=log,
        stderr=subprocess.STDOUT,
        preexec_fn=os.setsid,
    )


def _stop(proc: subprocess.Popen | None) -> None:
    if proc is None:
        return
    with contextlib.suppress(Exception):
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        with contextlib.suppress(Exception):
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)


def _write_config(
    cfg_dir: Path,
    *,
    port: int,
    bmc_port: int,
    target_port: int,
    status_port: int,
    overrides: dict,
) -> Path:
    cfg_dir.mkdir(parents=True, exist_ok=True)
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
    values.update(overrides)
    if isinstance(values["immediate"], bool):
        values["immediate"] = "true" if values["immediate"] else "false"
    cfg = cfg_dir / "config.yaml"
    cfg.write_text(CONFIG.format(**values))
    return cfg


def find_session(data: dict, sid: str) -> dict | None:
    for s in data["sessions"]:
        if s["session"] == sid:
            return s
    return None


def _mock_set(status_port: int, sid: str, status_type: str) -> None:
    httpx.post(
        f"http://127.0.0.1:{status_port}/set",
        params={"sid": sid, "type": status_type},
        timeout=3,
    )


def _mock_parent(status_port: int, sid: str, parent: str) -> None:
    httpx.post(
        f"http://127.0.0.1:{status_port}/parent",
        params={"sid": sid, "parent": parent},
        timeout=3,
    )


def _mock_permission(status_port: int, sid: str, pending: bool = True) -> None:
    method = httpx.post if pending else httpx.delete
    method(f"http://127.0.0.1:{status_port}/permission", params={"sid": sid}, timeout=3)


def _mock_question(status_port: int, sid: str, pending: bool = True) -> None:
    method = httpx.post if pending else httpx.delete
    method(f"http://127.0.0.1:{status_port}/question", params={"sid": sid}, timeout=3)


@pytest.fixture(scope="session")
def env(tmp_path_factory):
    """The shared environment: mock target + opencode + BMC (on), 8 proxies."""
    root = tmp_path_factory.mktemp("e2e")
    logdir = root / "logs"
    logdir.mkdir()
    processes: list[subprocess.Popen] = []

    target_port = free_port()
    status_port = free_port()
    bmc_port = free_port()

    def spawn_target(port: int = target_port, name: str | None = None) -> subprocess.Popen:
        proc = _spawn(
            name or f"mock-target-{port}",
            [sys.executable, str(REPO / "tests/mocks/mock_target.py")],
            root,
            logdir,
            {"MOCK_TARGET_PORT": str(port), "MOCK_DELAY": str(MOCK_DELAY)},
        )
        processes.append(proc)
        return proc

    target = spawn_target()
    processes.append(
        _spawn(
            "mock-opencode",
            [sys.executable, str(REPO / "tests/mocks/mock_opencode.py")],
            root,
            logdir,
            {"MOCK_STATUS_PORT": str(status_port)},
        )
    )
    processes.append(
        _spawn(
            "mock-bmc",
            [sys.executable, str(REPO / "tests/mocks/mock_bmc.py")],
            root,
            logdir,
            {"MOCK_BMC_PORT": str(bmc_port), "MOCK_BMC_INITIAL": "on"},
        )
    )
    asyncio.run(wait_http(f"http://127.0.0.1:{target_port}/health"))
    asyncio.run(wait_http(f"http://127.0.0.1:{status_port}/session/status"))
    asyncio.run(wait_http(f"http://127.0.0.1:{bmc_port}/redfish/v1/Systems/Self"))

    proxies: dict[str, SimpleNamespace] = {}
    for name, overrides in VARIANTS.items():
        port = free_port()
        cfg_dir = root / name
        cfg = _write_config(
            cfg_dir,
            port=port,
            bmc_port=bmc_port,
            target_port=target_port,
            status_port=status_port,
            overrides=overrides,
        )
        processes.append(
            _spawn(
                f"proxy-{name}",
                [sys.executable, "-m", "relay", "--config", str(cfg)],
                cfg_dir,
                logdir,
            )
        )
        proxies[name] = SimpleNamespace(name=name, port=port, dir=cfg_dir)
        asyncio.run(wait_http(f"http://127.0.0.1:{port}/healthz"))

    async def monitor(name: str) -> dict:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"http://127.0.0.1:{proxies[name].port}/monitor/data", timeout=5
            )
            response.raise_for_status()
            return response.json()

    ns = SimpleNamespace(
        root=root,
        logdir=logdir,
        target=target,
        target_port=target_port,
        status_port=status_port,
        bmc_port=bmc_port,
        proxies=proxies,
        spawn_target=spawn_target,
        monitor=monitor,
        wait_http=wait_http,
        find_session=find_session,
        mock_set=lambda sid, t: _mock_set(status_port, sid, t),
        mock_parent=lambda sid, parent: _mock_parent(status_port, sid, parent),
        mock_permission=lambda sid, pending=True: _mock_permission(status_port, sid, pending),
        mock_question=lambda sid, pending=True: _mock_question(status_port, sid, pending),
    )
    yield ns
    for proc in processes:
        _stop(proc)


@pytest.fixture(scope="session")
def boot_env(tmp_path_factory):
    """Dedicated cold-boot environment: BMC starts OFF, target not running."""
    root = tmp_path_factory.mktemp("e2e-boot")
    logdir = root / "logs"
    logdir.mkdir()
    processes: list[subprocess.Popen] = []

    bmc_port = free_port()
    target_port = free_port()
    port = free_port()
    status_port = free_port()

    processes.append(
        _spawn(
            "mock-bmc-boot",
            [sys.executable, str(REPO / "tests/mocks/mock_bmc.py")],
            root,
            logdir,
            {"MOCK_BMC_PORT": str(bmc_port), "MOCK_BMC_INITIAL": "off"},
        )
    )
    cfg_dir = root / "proxy"
    cfg = _write_config(
        cfg_dir,
        port=port,
        bmc_port=bmc_port,
        target_port=target_port,
        status_port=status_port,
        overrides={},
    )
    processes.append(
        _spawn(
            "proxy-boot",
            [sys.executable, "-m", "relay", "--config", str(cfg)],
            cfg_dir,
            logdir,
        )
    )
    # The target is deliberately NOT started: it "boots" when the test asks.
    target: subprocess.Popen | None = None

    def spawn_target() -> subprocess.Popen:
        nonlocal target
        target = _spawn(
            "mock-target-boot",
            [sys.executable, str(REPO / "tests/mocks/mock_target.py")],
            root,
            logdir,
            {"MOCK_TARGET_PORT": str(target_port), "MOCK_DELAY": str(MOCK_DELAY)},
        )
        processes.append(target)
        return target

    asyncio.run(wait_http(f"http://127.0.0.1:{port}/healthz"))

    async def monitor() -> dict:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"http://127.0.0.1:{port}/monitor/data", timeout=5)
            response.raise_for_status()
            return response.json()

    ns = SimpleNamespace(
        root=root,
        logdir=logdir,
        port=port,
        bmc_port=bmc_port,
        target_port=target_port,
        spawn_target=spawn_target,
        monitor=monitor,
        wait_http=wait_http,
        find_session=find_session,
    )
    yield ns
    for proc in processes:
        _stop(proc)


@pytest.fixture(scope="session")
def adopt_env(tmp_path_factory, env):
    """Fresh proxy + fresh BMC (initially ON) for ownership-adoption tests.

    Reuses the shared running target and opencode status mock from `env`;
    the dedicated BMC lets the tests count power actions exactly.
    """
    root = tmp_path_factory.mktemp("e2e-adopt")
    logdir = root / "logs"
    logdir.mkdir()

    bmc_port = free_port()
    port = free_port()
    bmc = _spawn(
        "mock-bmc-adopt",
        [sys.executable, str(REPO / "tests/mocks/mock_bmc.py")],
        root,
        logdir,
        {"MOCK_BMC_PORT": str(bmc_port), "MOCK_BMC_INITIAL": "on"},
    )
    cfg_dir = root / "proxy"
    cfg = _write_config(
        cfg_dir,
        port=port,
        bmc_port=bmc_port,
        target_port=env.target_port,
        status_port=env.status_port,
        overrides={"policy": "block"},
    )
    proxy = _spawn(
        "proxy-adopt",
        [sys.executable, "-m", "relay", "--config", str(cfg)],
        cfg_dir,
        logdir,
    )
    asyncio.run(wait_http(f"http://127.0.0.1:{port}/healthz"))

    async def monitor() -> dict:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"http://127.0.0.1:{port}/monitor/data", timeout=5)
            response.raise_for_status()
            return response.json()

    ns = SimpleNamespace(
        root=root,
        logdir=logdir,
        port=port,
        bmc_port=bmc_port,
        monitor=monitor,
        wait_http=wait_http,
        find_session=find_session,
    )
    yield ns
    _stop(bmc)
    _stop(proxy)
