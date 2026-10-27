"""Cron off-time (D11b): next-fire, fire-when-idle, deferral while active,
no double-fire, and the ownership / auto-off / unknown-state gating.

The engine's 60s loop is not run here; the (clock-injectable) tick function is
driven directly with controlled wall-clock instants for deterministic coverage.
"""

from datetime import datetime
from types import SimpleNamespace

import pytest

from relay.models import NoopPowerConfig, ServerConfig
from relay.power.base import PowerBackend, PowerState
from relay.scheduler import CronSchedule
from relay.servers import ServerRuntime
from relay.store import Store

pytestmark = pytest.mark.unit


class _FakeBackend(PowerBackend):
    """Scriptable power backend: fixed state + a list of issued actions."""

    def __init__(self, state: PowerState = PowerState.ON):
        self.state = state
        self.actions: list[str] = []

    async def power_on(self) -> bool:
        self.actions.append("power_on")
        self.state = PowerState.ON
        return True

    async def power_off(self) -> bool:
        self.actions.append("power_off")
        self.state = PowerState.OFF
        return True

    async def power_state(self) -> PowerState:
        return self.state

    async def close(self) -> None:
        return None


def dt(y, mo, d, h, mi, s=0) -> datetime:
    return datetime(y, mo, d, h, mi, s)


async def make_runtime(
    tmp_path,
    *,
    schedule: str | None = "* * * * *",
    owned: bool = True,
    state: PowerState = PowerState.ON,
) -> tuple[ServerRuntime, _FakeBackend, Store]:
    store = Store(str(tmp_path / "relay.db"))
    await store.open()
    await store.reconcile_servers(["ws"])
    config = ServerConfig(
        name="ws",
        type="noop",
        power=NoopPowerConfig(initial_state="on"),
        service_url="http://127.0.0.1:8100",
        schedule=schedule,
    )
    backend = _FakeBackend(state)
    runtime = ServerRuntime(config, backend, store)
    runtime.owned = owned
    return runtime, backend, store


def test_next_fire_is_strictly_after():
    cron = CronSchedule("* * * * *")
    base = dt(2026, 1, 1, 10, 0, 30)
    nxt = cron.next_fire(base)
    assert nxt == dt(2026, 1, 1, 10, 1, 0)
    assert nxt > base


def test_last_fire_before_is_the_preceding_boundary():
    cron = CronSchedule("* * * * *")
    assert cron.last_fire_before(dt(2026, 1, 1, 10, 5, 30)) == dt(2026, 1, 1, 10, 5, 0)


def test_invalid_cron_raises():
    with pytest.raises(ValueError):
        CronSchedule("not a cron")


async def test_first_tick_baselines_without_firing(tmp_path):
    runtime, backend, store = await make_runtime(tmp_path)
    await runtime._schedule_off_tick(now=dt(2026, 1, 1, 10, 0, 30))
    assert backend.actions == []
    assert runtime.last_fired_cron == dt(2026, 1, 1, 10, 0, 30)
    await store.close()


async def test_fires_when_idle_at_fire_time(tmp_path):
    runtime, backend, store = await make_runtime(tmp_path)
    await runtime._schedule_off_tick(now=dt(2026, 1, 1, 10, 0, 30))  # baseline
    await runtime._schedule_off_tick(now=dt(2026, 1, 1, 10, 1, 30))  # fire at 10:01:00
    assert backend.actions == ["power_off"]
    assert runtime.owned is False
    await store.close()


async def test_defers_while_active_then_fires_on_idle(tmp_path):
    runtime, backend, store = await make_runtime(tmp_path)
    runtime.endpoints.append(SimpleNamespace(has_activity=lambda: True))
    await runtime._schedule_off_tick(now=dt(2026, 1, 1, 10, 0, 30))  # baseline
    await runtime._schedule_off_tick(now=dt(2026, 1, 1, 10, 1, 30))  # fire while active
    assert backend.actions == []
    assert runtime.schedule_deferred is True
    # Activity clears within the same minute (no new fire) -> deferred off fires.
    runtime.endpoints.clear()
    await runtime._schedule_off_tick(now=dt(2026, 1, 1, 10, 1, 45))
    assert backend.actions == ["power_off"]
    assert runtime.schedule_deferred is False
    await store.close()


async def test_no_double_fire_for_the_same_cron_cycle(tmp_path):
    runtime, backend, store = await make_runtime(tmp_path)
    await runtime._schedule_off_tick(now=dt(2026, 1, 1, 10, 0, 30))
    await runtime._schedule_off_tick(now=dt(2026, 1, 1, 10, 1, 30))
    assert backend.actions == ["power_off"]
    await runtime._schedule_off_tick(now=dt(2026, 1, 1, 10, 1, 45))
    assert backend.actions == ["power_off"]
    await store.close()


async def test_unowned_server_never_fires(tmp_path):
    runtime, backend, store = await make_runtime(tmp_path, owned=False)
    await runtime._schedule_off_tick(now=dt(2026, 1, 1, 10, 0, 30))
    await runtime._schedule_off_tick(now=dt(2026, 1, 1, 10, 1, 30))
    assert backend.actions == []
    await store.close()


async def test_override_off_blocks_the_fire(tmp_path):
    runtime, backend, store = await make_runtime(tmp_path)
    runtime.shutdown_override = False
    await runtime._schedule_off_tick(now=dt(2026, 1, 1, 10, 0, 30))
    await runtime._schedule_off_tick(now=dt(2026, 1, 1, 10, 1, 30))
    assert backend.actions == []
    await store.close()


async def test_unknown_state_skips_the_fire(tmp_path):
    runtime, backend, store = await make_runtime(tmp_path, state=PowerState.UNKNOWN)
    await runtime._schedule_off_tick(now=dt(2026, 1, 1, 10, 0, 30))
    await runtime._schedule_off_tick(now=dt(2026, 1, 1, 10, 1, 30))
    assert backend.actions == []
    await store.close()


async def test_no_schedule_is_a_noop(tmp_path):
    runtime, backend, store = await make_runtime(tmp_path, schedule=None)
    await runtime._schedule_off_tick(now=dt(2026, 1, 1, 10, 1, 30))
    assert backend.actions == []
    assert runtime.last_fired_cron is None
    await store.close()


async def test_next_scheduled_off_returns_the_next_fire(tmp_path):
    runtime, _, store = await make_runtime(tmp_path, schedule="0 2 * * *")
    nxt = runtime.next_scheduled_off()
    assert nxt is not None
    assert (nxt.hour, nxt.minute) == (2, 0)
    await store.close()


async def test_next_scheduled_off_none_when_unscheduled(tmp_path):
    runtime, _, store = await make_runtime(tmp_path, schedule=None)
    assert runtime.next_scheduled_off() is None
    await store.close()
