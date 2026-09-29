"""Power-ownership invariant tests (D12, P5-P12) — incl. the operator's
manual-on scenario: a server turned on outside the proxy is never shut down.
"""

import time
from types import SimpleNamespace

import pytest

from relay.models import NoopPowerConfig, ServerConfig
from relay.power.base import PowerState
from relay.power.noop import NoopBackend
from relay.servers import ServerRuntime
from relay.store import Store

pytestmark = pytest.mark.unit


async def make_runtime(tmp_path, *, initial="on", shutdown_enabled=True, idle_timeout=3600):
    store = Store(str(tmp_path / "relay.db"))
    await store.open()
    await store.reconcile_servers(["ws"])
    config = ServerConfig(
        name="ws",
        type="noop",
        power=NoopPowerConfig(initial_state=initial),
        service_url="http://127.0.0.1:8100",
        idle_timeout=idle_timeout,
        shutdown_enabled=shutdown_enabled,
    )
    backend = NoopBackend(config.power)
    runtime = ServerRuntime(config, backend, store)
    return runtime, backend, store


async def test_adopted_on_routed_traffic(tmp_path):
    runtime, _, store = await make_runtime(tmp_path)
    assert runtime.owned is False
    await runtime.on_routed_traffic()
    assert runtime.owned is True
    rows = await store.reconcile_servers(["ws"])
    assert rows["ws"].owned is True
    await store.close()


async def test_not_adopted_when_disabled(tmp_path):
    runtime, _, store = await make_runtime(tmp_path)
    runtime.config.adopt_on_traffic = False
    await runtime.on_routed_traffic()
    assert runtime.owned is False
    await store.close()


async def test_power_on_takes_ownership_and_resets_cycle(tmp_path):
    runtime, backend, store = await make_runtime(tmp_path, initial="off")
    await runtime.set_shutdown_override(False)
    ok = await runtime.power_on()
    assert ok is True
    assert backend.actions == ["power_on"]
    assert runtime.owned is True
    assert runtime.shutdown_override is None  # new cycle: config default
    assert runtime.cycle_id is not None
    await store.close()


async def test_power_off_clears_ownership(tmp_path):
    runtime, backend, store = await make_runtime(tmp_path)
    await runtime.on_routed_traffic()
    ok = await runtime.power_off()
    assert ok is True
    assert backend.actions == ["power_off"]
    assert runtime.owned is False
    await store.close()


async def test_power_on_cooldown_dedupes(tmp_path):
    runtime, backend, store = await make_runtime(tmp_path, initial="off")
    assert await runtime.maybe_power_on() is True
    assert await runtime.maybe_power_on() is False  # within the 30s cooldown
    assert backend.actions == ["power_on"]
    await store.close()


async def test_manually_on_unowned_server_is_never_shut_down(tmp_path):
    # THE operator scenario: the operator powers the box on manually; with no
    # proxy traffic the proxy must never touch it.
    runtime, backend, store = await make_runtime(tmp_path, idle_timeout=10)
    assert runtime.owned is False  # nobody adopted it
    runtime.last_activity = time.monotonic() - 999  # long idle
    await runtime._idle_off_tick()
    assert backend.actions == []  # no power-off issued
    # P12: the clock was reset (re-poll branch) to prevent flapping.
    assert time.monotonic() - runtime.last_activity < 5
    await store.close()


async def test_owned_idle_server_is_shut_down(tmp_path):
    runtime, backend, store = await make_runtime(tmp_path, idle_timeout=10)
    await runtime.on_routed_traffic()
    runtime.last_activity = time.monotonic() - 999
    await runtime._idle_off_tick()
    assert backend.actions == ["power_off"]
    assert runtime.owned is False
    await store.close()


async def test_never_down_while_active(tmp_path):
    runtime, backend, store = await make_runtime(tmp_path, idle_timeout=10)
    await runtime.on_routed_traffic()
    runtime.endpoints.append(SimpleNamespace(has_activity=lambda: True))
    old = time.monotonic() - 999
    runtime.last_activity = old
    await runtime._idle_off_tick()
    assert backend.actions == []
    assert runtime.last_activity > old  # P9: timer restarted
    await store.close()


async def test_unknown_power_state_skips_shutdown(tmp_path):
    runtime, backend, store = await make_runtime(tmp_path, idle_timeout=10)
    await runtime.on_routed_traffic()
    backend.set_state(PowerState.UNKNOWN)  # P10: skip to be safe
    runtime.last_activity = time.monotonic() - 999
    await runtime._idle_off_tick()
    assert backend.actions == []
    await store.close()


async def test_shutdown_disabled_for_the_cycle_blocks_idle_off(tmp_path):
    runtime, backend, store = await make_runtime(tmp_path, idle_timeout=10)
    await runtime.on_routed_traffic()
    await runtime.set_shutdown_override(False)
    runtime.last_activity = time.monotonic() - 999
    await runtime._idle_off_tick()
    assert backend.actions == []
    # The config default (true) comes back after a proxy-initiated power-on.
    runtime.power_state = PowerState.OFF
    await runtime.power_on()
    assert runtime.shutdown_enabled_now is True
    await store.close()


async def test_ownership_and_override_restore_across_restart(tmp_path):
    store = Store(str(tmp_path / "relay.db"))
    await store.open()
    await store.reconcile_servers(["ws"])
    config = ServerConfig(
        name="ws",
        type="noop",
        power=NoopPowerConfig(initial_state="on"),
        service_url="http://127.0.0.1:8100",
    )
    runtime = ServerRuntime(config, NoopBackend(config.power), store)
    await runtime.on_routed_traffic()
    await runtime.set_shutdown_override(False)
    await store.set_server_runtime("ws", power_state="on")

    # "Restart": a brand-new runtime over the same store.
    rows = await store.reconcile_servers(["ws"])
    runtime2 = ServerRuntime(config, NoopBackend(config.power), store)
    await runtime2.restore(rows["ws"])
    assert runtime2.owned is True
    assert runtime2.shutdown_override is False
    assert runtime2.power_state is PowerState.ON
    await store.close()


async def test_sync_state_logs_and_persists(tmp_path):
    runtime, _backend, store = await make_runtime(tmp_path, initial="off")
    await runtime.sync_state()
    assert runtime.power_state is PowerState.OFF
    rows = await store.reconcile_servers(["ws"])
    assert rows["ws"].power_state == "off"
    events = await store.recent_power_events()
    assert events and events[-1]["action"] == "state_sync"
    await store.close()
