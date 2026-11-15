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


# Shutdown-test config: a single redfish server with a short idle timeout and a
# short shutdown tick so the 60s engine can be driven in seconds. ``schedule``
# is null by default (no cron) and injected with quotes when a cron is set.
SHUTDOWN_CONFIG = """\
proxy:
  host: 127.0.0.1
  port: {port}
  unknown_path_policy: allow
  target_read_timeout: 0
  shutdown_tick: {shutdown_tick}
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
    idle_timeout: {idle_timeout}
    shutdown_enabled: {shutdown_enabled}
    adopt_on_traffic: true
    schedule: {schedule}

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
    concurrency: 1
    session_id_body_fields:
      - user
    session:
      per_session_requests: -1
      request_mode: parallel
      busy_window: 1
      expiry: 1
      immediate_idle_release: true
"""


@pytest.fixture(scope="session")
def shutdown_env(tmp_path_factory, env):
    """Fresh BMC + short-cadence proxy per scenario (shutdown / cron / restart).

    Each ``make()`` call spawns an isolated mock BMC (initially on) and a proxy
    wired to it, reusing the shared mock target/status from ``env``. This keeps
    power-action accounting per-test (a fresh BMC counts exactly its own actions).
    """
    root = tmp_path_factory.mktemp("e2e-shutdown")
    logdir = root / "logs"
    logdir.mkdir()
    processes: list[subprocess.Popen] = []
    counter = {"n": 0}

    async def make(
        name: str,
        *,
        idle_timeout: float = 3,
        shutdown_tick: float = 1,
        shutdown_enabled: bool = True,
        schedule: str | None = None,
        db: str | None = None,
    ) -> SimpleNamespace:
        counter["n"] += 1
        n = counter["n"]
        bmc_port = free_port()
        port = free_port()
        bmc = _spawn(
            f"bmc-{n}",
            [sys.executable, str(REPO / "tests/mocks/mock_bmc.py")],
            root,
            logdir,
            {"MOCK_BMC_PORT": str(bmc_port), "MOCK_BMC_INITIAL": "on"},
        )
        processes.append(bmc)
        await wait_http(f"http://127.0.0.1:{bmc_port}/redfish/v1/Systems/Self")
        cfg_dir = root / f"p{n}"
        cfg_dir.mkdir(parents=True, exist_ok=True)
        values = {
            "port": port,
            "bmc_port": bmc_port,
            "target_port": env.target_port,
            "idle_timeout": idle_timeout,
            "shutdown_tick": shutdown_tick,
            "shutdown_enabled": "true" if shutdown_enabled else "false",
            "schedule": f'"{schedule}"' if schedule else "null",
            "db": db or str(cfg_dir / "relay.db"),
        }
        cfg = cfg_dir / "config.yaml"
        cfg.write_text(SHUTDOWN_CONFIG.format(**values))
        proxy = _spawn(
            f"proxy-{n}",
            [sys.executable, "-m", "relay", "--config", str(cfg)],
            cfg_dir,
            logdir,
        )
        processes.append(proxy)
        await wait_http(f"http://127.0.0.1:{port}/healthz")

        async def monitor() -> dict:
            async with httpx.AsyncClient() as client:
                response = await client.get(f"http://127.0.0.1:{port}/monitor/data", timeout=5)
                response.raise_for_status()
                return response.json()

        async def bmc_actions() -> list:
            async with httpx.AsyncClient() as client:
                response = await client.get(f"http://127.0.0.1:{bmc_port}/actions", timeout=5)
                return response.json()["actions"]

        def stop() -> None:
            _stop(proxy)
            _stop(bmc)

        return SimpleNamespace(
            name=name,
            n=n,
            port=port,
            bmc_port=bmc_port,
            dir=cfg_dir,
            db=values["db"],
            monitor=monitor,
            bmc_actions=bmc_actions,
            stop=stop,
        )

    yield SimpleNamespace(root=root, make=make, target_port=env.target_port)
    for proc in processes:
        _stop(proc)


# WS-test config: a redfish server whose service_url is the mock WS target, with
# a queued (wait) endpoint at /ws and an error-policy (concurrent) one at /wserr.
WS_CONFIG = """\
proxy:
  host: 127.0.0.1
  port: {port}
  unknown_path_policy: allow
  target_read_timeout: 0
  store:
    path: {db}

servers:
  - name: wstbox
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
    shutdown_enabled: false
    adopt_on_traffic: true

endpoints:
  - name: ws
    server: wstbox
    path_prefix: /ws
    readiness:
      path: /health
      interval: 1
      timeout: 1
    wait_policy: {wait_policy}
    routing: queued
    concurrency: {concurrency}
    session:
      busy_window: 1
      expiry: 1
      immediate_idle_release: true
  - name: wserr
    server: wstbox
    path_prefix: /wserr
    readiness:
      path: /health
      interval: 1
      timeout: 1
    wait_policy: error
    routing: concurrent
"""


@pytest.fixture(scope="session")
def ws_env(tmp_path_factory):
    """Fresh mock WS target + BMC + proxy per scenario (D18 tunnel tests).

    The WS target is startable/stoppable so tests can drive the readiness gate
    (keep-alive while waiting, 1013 on the error policy).
    """
    root = tmp_path_factory.mktemp("e2e-ws")
    logdir = root / "logs"
    logdir.mkdir()
    processes: list[subprocess.Popen] = []
    counter = {"n": 0}

    async def make(
        name: str, *, wait_policy: str = "wait", concurrency: int = 1, target_up: bool = True
    ) -> SimpleNamespace:
        counter["n"] += 1
        n = counter["n"]
        bmc_port = free_port()
        port = free_port()
        target_port = free_port()
        bmc = _spawn(
            f"bmc-ws-{n}",
            [sys.executable, str(REPO / "tests/mocks/mock_bmc.py")],
            root,
            logdir,
            {"MOCK_BMC_PORT": str(bmc_port), "MOCK_BMC_INITIAL": "on"},
        )
        processes.append(bmc)
        await wait_http(f"http://127.0.0.1:{bmc_port}/redfish/v1/Systems/Self")
        cfg_dir = root / f"ws{n}"
        cfg_dir.mkdir(parents=True, exist_ok=True)
        values = {
            "port": port,
            "bmc_port": bmc_port,
            "target_port": target_port,
            "wait_policy": wait_policy,
            "concurrency": concurrency,
            "db": str(cfg_dir / "relay.db"),
        }
        cfg = cfg_dir / "config.yaml"
        cfg.write_text(WS_CONFIG.format(**values))
        proxy = _spawn(
            f"proxy-ws-{n}",
            [sys.executable, "-m", "relay", "--config", str(cfg)],
            cfg_dir,
            logdir,
        )
        processes.append(proxy)
        await wait_http(f"http://127.0.0.1:{port}/healthz")

        state = {"target": None}

        async def start_target() -> None:
            if state["target"] is not None:
                return
            proc = _spawn(
                f"ws-target-{n}",
                [sys.executable, str(REPO / "tests/mocks/mock_ws_target.py")],
                root,
                logdir,
                {"MOCK_WS_PORT": str(target_port), "MOCK_WS_DELAY": "0"},
            )
            processes.append(proc)
            state["target"] = proc
            await wait_http(f"http://127.0.0.1:{target_port}/health")

        def stop_target() -> None:
            if state["target"] is not None:
                _stop(state["target"])
                state["target"] = None

        async def monitor() -> dict:
            async with httpx.AsyncClient() as client:
                response = await client.get(f"http://127.0.0.1:{port}/monitor/data", timeout=5)
                response.raise_for_status()
                return response.json()

        def stop() -> None:
            _stop(proxy)
            _stop(bmc)
            stop_target()

        if target_up:
            await start_target()

        return SimpleNamespace(
            name=name,
            port=port,
            target_port=target_port,
            dir=cfg_dir,
            monitor=monitor,
            start_target=start_target,
            stop_target=stop_target,
            stop=stop,
        )

    yield SimpleNamespace(root=root, make=make)
    for proc in processes:
        _stop(proc)


# Multi-server config: alpha (redfish, a real mock BMC) + beta (noop, in
# process), each with its own endpoint (isolated queue) and its own target.
# Session windows are short (1s) so a held spot is surrendered quickly and the
# idle-off engine can be driven in seconds.
MULTI_CONFIG = """\
proxy:
  host: 127.0.0.1
  port: {port}
  unknown_path_policy: block
  target_read_timeout: 0
  shutdown_tick: {shutdown_tick}
  store:
    path: {db}

servers:
  - name: alpha
    type: redfish
    power:
      host: 127.0.0.1
      user: test
      password: test
      base_url: http://127.0.0.1:{bmc_port}
      system_path: /redfish/v1/Systems/Self
      verify_ssl: false
    service_url: http://127.0.0.1:{target_a_port}
    idle_timeout: {idle_a}
    shutdown_enabled: true
    adopt_on_traffic: true
  - name: beta
    type: noop
    power:
      initial_state: "on"
    service_url: http://127.0.0.1:{target_b_port}
    idle_timeout: {idle_b}
    shutdown_enabled: true
    adopt_on_traffic: true

endpoints:
  - name: alpha-api
    server: alpha
    path_prefix: /v1
    readiness:
      path: /health
      interval: 1
      timeout: 1
    wait_policy: {wait_policy}
    routing: queued
    concurrency: 1
    session:
      busy_window: 1
      expiry: 1
      immediate_idle_release: true
  - name: beta-api
    server: beta
    path_prefix: /b
    readiness:
      path: /health
      interval: 1
      timeout: 1
    wait_policy: {wait_policy}
    routing: queued
    concurrency: 1
    session:
      busy_window: 1
      expiry: 1
      immediate_idle_release: true
"""


@pytest.fixture(scope="session")
def multi_env(tmp_path_factory):
    """Fresh mock BMC + two startable targets + a two-server proxy per scenario.

    Server alpha is redfish (its mock BMC records power actions); server beta
    is noop (in-process). Each has its own endpoint (isolated queue) and target,
    so routing, per-server readiness, queue isolation, and independent
    shutdowns can be proven on a single proxy.
    """
    root = tmp_path_factory.mktemp("e2e-multi")
    logdir = root / "logs"
    logdir.mkdir()
    processes: list[subprocess.Popen] = []
    counter = {"n": 0}

    async def make(
        name,
        *,
        idle_a: float = 3.0,
        idle_b: float = 3600.0,
        shutdown_tick: float = 1.0,
        wait_policy: str = "wait",
        target_a_up: bool = True,
        target_b_up: bool = True,
        delay_a: float = 0.0,
        delay_b: float = 0.0,
    ) -> SimpleNamespace:
        counter["n"] += 1
        n = counter["n"]
        bmc_port = free_port()
        port = free_port()
        target_a_port = free_port()
        target_b_port = free_port()
        bmc = _spawn(
            f"bmc-multi-{n}",
            [sys.executable, str(REPO / "tests/mocks/mock_bmc.py")],
            root,
            logdir,
            {"MOCK_BMC_PORT": str(bmc_port), "MOCK_BMC_INITIAL": "on"},
        )
        processes.append(bmc)
        await wait_http(f"http://127.0.0.1:{bmc_port}/redfish/v1/Systems/Self")
        cfg_dir = root / f"m{n}"
        cfg_dir.mkdir(parents=True, exist_ok=True)
        values = {
            "port": port,
            "bmc_port": bmc_port,
            "target_a_port": target_a_port,
            "target_b_port": target_b_port,
            "idle_a": idle_a,
            "idle_b": idle_b,
            "shutdown_tick": shutdown_tick,
            "wait_policy": wait_policy,
            "db": str(cfg_dir / "relay.db"),
        }
        cfg = cfg_dir / "config.yaml"
        cfg.write_text(MULTI_CONFIG.format(**values))
        proxy = _spawn(
            f"proxy-multi-{n}",
            [sys.executable, "-m", "relay", "--config", str(cfg)],
            cfg_dir,
            logdir,
        )
        processes.append(proxy)
        await wait_http(f"http://127.0.0.1:{port}/healthz")

        state = {"ta": None, "tb": None}

        async def start_target_a() -> None:
            if state["ta"] is not None:
                return
            proc = _spawn(
                f"target-a-{n}",
                [sys.executable, str(REPO / "tests/mocks/mock_target.py")],
                root,
                logdir,
                {"MOCK_TARGET_PORT": str(target_a_port), "MOCK_DELAY": str(delay_a)},
            )
            processes.append(proc)
            state["ta"] = proc
            await wait_http(f"http://127.0.0.1:{target_a_port}/health")

        async def start_target_b() -> None:
            if state["tb"] is not None:
                return
            proc = _spawn(
                f"target-b-{n}",
                [sys.executable, str(REPO / "tests/mocks/mock_target.py")],
                root,
                logdir,
                {"MOCK_TARGET_PORT": str(target_b_port), "MOCK_DELAY": str(delay_b)},
            )
            processes.append(proc)
            state["tb"] = proc
            await wait_http(f"http://127.0.0.1:{target_b_port}/health")

        def stop_target_a() -> None:
            if state["ta"] is not None:
                _stop(state["ta"])
                state["ta"] = None

        def stop_target_b() -> None:
            if state["tb"] is not None:
                _stop(state["tb"])
                state["tb"] = None

        async def monitor() -> dict:
            async with httpx.AsyncClient() as client:
                response = await client.get(f"http://127.0.0.1:{port}/monitor/data", timeout=5)
                response.raise_for_status()
                return response.json()

        async def alpha_actions() -> list:
            async with httpx.AsyncClient() as client:
                response = await client.get(f"http://127.0.0.1:{bmc_port}/actions", timeout=5)
                return response.json()["actions"]

        def stop() -> None:
            _stop(proxy)
            _stop(bmc)
            stop_target_a()
            stop_target_b()

        if target_a_up:
            await start_target_a()
        if target_b_up:
            await start_target_b()

        return SimpleNamespace(
            name=name,
            port=port,
            bmc_port=bmc_port,
            target_a_port=target_a_port,
            target_b_port=target_b_port,
            dir=cfg_dir,
            db=values["db"],
            monitor=monitor,
            alpha_actions=alpha_actions,
            start_target_a=start_target_a,
            start_target_b=start_target_b,
            stop_target_a=stop_target_a,
            stop_target_b=stop_target_b,
            stop=stop,
        )

    yield SimpleNamespace(root=root, make=make)
    for proc in processes:
        _stop(proc)


# Opencode-parity config (Phase 3, D20/D21): one redfish server (mock BMC,
# on) with a queued (wait) endpoint, and THREE client entries: `opencode`
# (status mock A, parent-chain children), `opencode-b` (status mock B — the
# /offline switch drives the unreachable-grace test), and `mytool`
# (status.kind: none — busy-window inference only).
OC_CONFIG = """\
proxy:
  host: 127.0.0.1
  port: {port}
  unknown_path_policy: allow
  target_read_timeout: 0
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
    shutdown_enabled: false
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
    session:
      per_session_requests: -1
      request_mode: parallel
      busy_window: {busy_window}
      expiry: {expiry}
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
      poll_interval: {poll_interval}
    children:
      kind: parent-chain
      depth: 10
  - name: opencode-b
    match:
      session_headers:
        - x-opencode-b-session
    status:
      kind: opencode
      port: {status_b_port}
      poll_interval: {poll_interval}
  - name: mytool
    match:
      session_headers:
        - x-mytool-session
    status:
      kind: none
"""


@pytest.fixture(scope="session")
def oc_env(tmp_path_factory):
    """Fresh mock target + BMC + two opencode status mocks + proxy per scenario.

    Each ``make()`` call is fully isolated (own target, BMC, and both status
    mocks), so per-mock request counters, the /offline switch, and the
    per-directory maps start clean. The three client entries (opencode,
    opencode-b, mytool/kind:none) are all present in every scenario.
    """
    root = tmp_path_factory.mktemp("e2e-opencode")
    logdir = root / "logs"
    logdir.mkdir()
    processes: list[subprocess.Popen] = []
    counter = {"n": 0}

    async def make(
        name,
        *,
        busy_window: float = 2,
        expiry: float = 4,
        immediate: bool = True,
        poll_interval: float = 1,
        concurrency: int = 1,
        delay: float = 0.0,
    ) -> SimpleNamespace:
        counter["n"] += 1
        n = counter["n"]
        bmc_port = free_port()
        port = free_port()
        target_port = free_port()
        status_port = free_port()
        status_b_port = free_port()
        bmc = _spawn(
            f"bmc-oc-{n}",
            [sys.executable, str(REPO / "tests/mocks/mock_bmc.py")],
            root,
            logdir,
            {"MOCK_BMC_PORT": str(bmc_port), "MOCK_BMC_INITIAL": "on"},
        )
        processes.append(bmc)
        await wait_http(f"http://127.0.0.1:{bmc_port}/redfish/v1/Systems/Self")
        # delay: mock-target response delay, so a request stays in-flight
        # across at least one status-poll tick (the waiting-state window).
        target = _spawn(
            f"target-oc-{n}",
            [sys.executable, str(REPO / "tests/mocks/mock_target.py")],
            root,
            logdir,
            {"MOCK_TARGET_PORT": str(target_port), "MOCK_DELAY": str(delay)},
        )
        processes.append(target)
        await wait_http(f"http://127.0.0.1:{target_port}/health")
        mock = _spawn(
            f"mock-opencode-oc-{n}",
            [sys.executable, str(REPO / "tests/mocks/mock_opencode.py")],
            root,
            logdir,
            {"MOCK_STATUS_PORT": str(status_port)},
        )
        processes.append(mock)
        await wait_http(f"http://127.0.0.1:{status_port}/session/status")
        mock_b = _spawn(
            f"mock-opencode-b-oc-{n}",
            [sys.executable, str(REPO / "tests/mocks/mock_opencode.py")],
            root,
            logdir,
            {"MOCK_STATUS_PORT": str(status_b_port)},
        )
        processes.append(mock_b)
        await wait_http(f"http://127.0.0.1:{status_b_port}/session/status")
        cfg_dir = root / f"oc{n}"
        cfg_dir.mkdir(parents=True, exist_ok=True)
        values = {
            "port": port,
            "bmc_port": bmc_port,
            "target_port": target_port,
            "status_port": status_port,
            "status_b_port": status_b_port,
            "busy_window": busy_window,
            "expiry": expiry,
            "immediate": "true" if immediate else "false",
            "poll_interval": poll_interval,
            "concurrency": concurrency,
            "db": str(cfg_dir / "relay.db"),
        }
        cfg = cfg_dir / "config.yaml"
        cfg.write_text(OC_CONFIG.format(**values))
        proxy = _spawn(
            f"proxy-oc-{n}",
            [sys.executable, "-m", "relay", "--config", str(cfg)],
            cfg_dir,
            logdir,
        )
        processes.append(proxy)
        await wait_http(f"http://127.0.0.1:{port}/healthz")

        async def monitor() -> dict:
            async with httpx.AsyncClient() as client:
                response = await client.get(f"http://127.0.0.1:{port}/monitor/data", timeout=5)
                response.raise_for_status()
                return response.json()

        async def mock_requests(m_port: int) -> dict:
            async with httpx.AsyncClient() as client:
                response = await client.get(f"http://127.0.0.1:{m_port}/requests", timeout=5)
                return response.json()

        async def mock_offline(m_port: int, offline: bool) -> None:
            async with httpx.AsyncClient() as client:
                if offline:
                    await client.post(f"http://127.0.0.1:{m_port}/offline", timeout=5)
                else:
                    await client.delete(f"http://127.0.0.1:{m_port}/offline", timeout=5)

        def stop() -> None:
            _stop(proxy)
            _stop(mock_b)
            _stop(mock)
            _stop(target)
            _stop(bmc)

        return SimpleNamespace(
            name=name,
            n=n,
            port=port,
            bmc_port=bmc_port,
            status_port=status_port,
            status_b_port=status_b_port,
            dir=cfg_dir,
            db=values["db"],
            monitor=monitor,
            mock_requests=mock_requests,
            mock_offline=mock_offline,
            stop=stop,
        )

    yield SimpleNamespace(root=root, make=make)
    for proc in processes:
        _stop(proc)


# EC2-boot-latency config (Phase 2, D10): one aws-ec2 server whose power
# block points a REAL boto3 client at the fake EC2 API (endpoint_url), plus
# one queued (wait) endpoint. The instance id matches the fake's default.
EC2_CONFIG = """\
proxy:
  host: 127.0.0.1
  port: {port}
  unknown_path_policy: allow
  target_read_timeout: 0
  store:
    path: {db}

servers:
  - name: ec2box
    type: aws-ec2
    power:
      region: us-east-1
      instance_id: i-fake
      access_key: test
      secret_key: test
      endpoint_url: http://127.0.0.1:{ec2_port}
      off_action: stop
    service_url: http://127.0.0.1:{target_port}
    idle_timeout: 3600
    shutdown_enabled: true
    adopt_on_traffic: true

endpoints:
  - name: ec2-api
    server: ec2box
    path_prefix: /e
    readiness:
      path: /health
      interval: 1
      timeout: 1
    wait_policy: wait
    routing: queued
    concurrency: 1
    session:
      busy_window: 1
      expiry: 1
      immediate_idle_release: true
"""


@pytest.fixture(scope="session")
def ec2_env(tmp_path_factory):
    """Fake EC2 API (stopped instance) + startable target + one-server proxy.

    The instance starts ``stopped``; the proxy's real boto3 client talks to
    the fake via the power block's ``endpoint_url``. ``start_instances``
    moves it ``pending`` and then ``running`` after the boot delay,
    independently of the (down) target — so the endpoint only becomes ready
    once the test starts the target (the wait-and-poll path with a slow
    boot).
    """
    root = tmp_path_factory.mktemp("e2e-ec2")
    logdir = root / "logs"
    logdir.mkdir()
    processes: list[subprocess.Popen] = []
    counter = {"n": 0}

    async def make(name, *, boot_delay: float = 2.0, target_up: bool = False) -> SimpleNamespace:
        counter["n"] += 1
        n = counter["n"]
        ec2_port = free_port()
        port = free_port()
        target_port = free_port()
        ec2 = _spawn(
            f"fake-ec2-{n}",
            [sys.executable, str(REPO / "tests/mocks/fake_ec2.py")],
            root,
            logdir,
            {
                "FAKE_EC2_PORT": str(ec2_port),
                "FAKE_EC2_INITIAL": "stopped",
                "FAKE_EC2_BOOT_DELAY": str(boot_delay),
            },
        )
        processes.append(ec2)
        await wait_http(f"http://127.0.0.1:{ec2_port}/state")
        cfg_dir = root / f"ec2{n}"
        cfg_dir.mkdir(parents=True, exist_ok=True)
        values = {
            "port": port,
            "ec2_port": ec2_port,
            "target_port": target_port,
            "db": str(cfg_dir / "relay.db"),
        }
        cfg = cfg_dir / "config.yaml"
        cfg.write_text(EC2_CONFIG.format(**values))
        proxy = _spawn(
            f"proxy-ec2-{n}",
            [sys.executable, "-m", "relay", "--config", str(cfg)],
            cfg_dir,
            logdir,
        )
        processes.append(proxy)
        await wait_http(f"http://127.0.0.1:{port}/healthz")

        state = {"target": None}

        async def start_target() -> None:
            if state["target"] is not None:
                return
            proc = _spawn(
                f"ec2-target-{n}",
                [sys.executable, str(REPO / "tests/mocks/mock_target.py")],
                root,
                logdir,
                {"MOCK_TARGET_PORT": str(target_port), "MOCK_DELAY": "0"},
            )
            processes.append(proc)
            state["target"] = proc
            await wait_http(f"http://127.0.0.1:{target_port}/health")

        async def ec2_state() -> str:
            async with httpx.AsyncClient() as client:
                response = await client.get(f"http://127.0.0.1:{ec2_port}/state", timeout=5)
                return response.json()["state"]

        async def ec2_actions() -> list:
            async with httpx.AsyncClient() as client:
                response = await client.get(f"http://127.0.0.1:{ec2_port}/actions", timeout=5)
                return response.json()["actions"]

        async def monitor() -> dict:
            async with httpx.AsyncClient() as client:
                response = await client.get(f"http://127.0.0.1:{port}/monitor/data", timeout=5)
                response.raise_for_status()
                return response.json()

        def stop() -> None:
            if state["target"] is not None:
                _stop(state["target"])
                state["target"] = None
            _stop(proxy)
            _stop(ec2)

        if target_up:
            await start_target()

        return SimpleNamespace(
            name=name,
            n=n,
            port=port,
            ec2_port=ec2_port,
            target_port=target_port,
            dir=cfg_dir,
            db=values["db"],
            monitor=monitor,
            ec2_state=ec2_state,
            ec2_actions=ec2_actions,
            start_target=start_target,
            stop=stop,
        )

    yield SimpleNamespace(root=root, make=make)
    for proc in processes:
        _stop(proc)
