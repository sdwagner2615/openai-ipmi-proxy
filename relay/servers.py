"""Server runtime: power state machine, ownership, and the idle-off engine.

One ServerRuntime per configured server (D2/D3). Owns:

- the coarse power state (on/off/unknown) plus the transient
  ``powering_on`` / ``powering_off`` display states,
- ownership (D12) and the per-cycle ``shutdown_override`` (D14),
- the single power-on cooldown timer (P2),
- the power-state sync loop (~5s),
- the idle-off shutdown engine (60s tick, P9-P12).

The cron off-time half of the shutdown engine (D11b) arrives with
``scheduler.py`` in Phase 1; the idle-off half runs here from Phase 0.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from typing import TYPE_CHECKING

from relay.models import ServerConfig
from relay.power.base import PowerBackend, PowerState
from relay.store import ServerRuntimeRow, Store

if TYPE_CHECKING:
    from relay.endpoints import EndpointRuntime

logger = logging.getLogger("relay.servers")

#: Power-on dedupe window (P2): re-issues keep a boot attempt alive without
#: stampeding the BMC.
POWER_ON_COOLDOWN = 30.0
#: Power-state poll cadence.
SYNC_INTERVAL = 5.0
#: Shutdown engine tick cadence (P12).
SHUTDOWN_TICK = 60.0

_TRANSIENT_ON = "powering_on"
_TRANSIENT_OFF = "powering_off"


class ServerRuntime:
    """Runtime state + background loops for one configured server."""

    def __init__(
        self,
        config: ServerConfig,
        backend: PowerBackend,
        store: Store,
        *,
        power_on_cooldown: float = POWER_ON_COOLDOWN,
        sync_interval: float = SYNC_INTERVAL,
        shutdown_tick: float = SHUTDOWN_TICK,
    ):
        self.config = config
        self.backend = backend
        self.store = store
        self.power_on_cooldown = power_on_cooldown
        self.sync_interval = sync_interval
        self.shutdown_tick = shutdown_tick
        # Endpoints hosted on this server (wired by the app after construction).
        self.endpoints: list[EndpointRuntime] = []
        # Coarse power state; the transient display state rides along.
        self.power_state = PowerState.UNKNOWN
        self.transient: str | None = None
        self.powered_on_at: float | None = None
        self.powered_off_at: float | None = None
        # Ownership (D12) and the per-cycle auto-off switch (D14).
        self.owned = False
        self.shutdown_override: bool | None = None
        self.cycle_id: str | None = None
        # Idle clock (P11: monotonic; anchored at startup, refreshed by
        # routed traffic and by activity seen in the shutdown tick).
        self.last_activity = time.monotonic()
        # Power-on cooldown bookkeeping (P2).
        self.last_power_on_attempt = 0.0
        # Last successful power-state poll (monitor context).
        self.last_sync_at: float | None = None
        self._sync_task: asyncio.Task | None = None
        self._shutdown_task: asyncio.Task | None = None

    @property
    def name(self) -> str:
        return self.config.name

    @property
    def display_state(self) -> str:
        """The state the monitor shows (transient while an action is in flight)."""
        return self.transient or self.power_state.value

    @property
    def shutdown_enabled_now(self) -> bool:
        """The live per-cycle auto-off switch (D14)."""
        if self.shutdown_override is not None:
            return self.shutdown_override
        return self.config.shutdown_enabled

    # -- startup -----------------------------------------------------------

    async def restore(self, row: ServerRuntimeRow) -> None:
        """Restores persisted runtime state (storage.md startup reconciliation).

        This is what makes restarts safe: a server the proxy brought up
        before a restart is still owned after it; a manually-on server stays
        unowned. Queues and in-flight requests are never restored (D6).
        """
        self.owned = row.owned
        self.shutdown_override = row.shutdown_override
        self.cycle_id = row.cycle_id
        if row.power_state is not None and row.power_state in {p.value for p in PowerState}:
            self.power_state = PowerState(row.power_state)

    async def sync_state(self) -> None:
        """Startup state sync (P4): read the real power state and log it.

        The app probes every endpoint's readiness before calling this; a
        ready service implies the box is on (D8).
        """
        logger.info("Synchronizing current state for server %s...", self.name)
        state = await self.backend.power_state()
        healthy = any(ep.queue.ready for ep in self.endpoints)
        if healthy and state is not PowerState.ON:
            state = PowerState.ON
        self._set_state(state)
        if healthy:
            logger.info("Server %s state: ONLINE and HEALTHY", self.name)
        elif state is PowerState.ON:
            logger.info("Server %s state: POWERED ON but NOT HEALTHY (Booting?)", self.name)
        elif state is PowerState.OFF:
            logger.info("Server %s state: POWERED OFF", self.name)
        else:
            logger.info("Server %s state: UNKNOWN", self.name)
        await self.store.set_server_runtime(self.name, power_state=state.value)
        await self.store.log_power_event(self.name, "state_sync", "startup_sync", "startup", True)

    def _set_state(self, state: PowerState) -> None:
        """Applies a coarse state transition (timestamps + transient clear)."""
        if state is self.power_state:
            return
        self.power_state = state
        self.transient = None
        now = time.monotonic()
        if state is PowerState.ON:
            self.powered_on_at = now
        elif state is PowerState.OFF:
            self.powered_off_at = now

    # -- power actions -------------------------------------------------------

    async def maybe_power_on(self, *, reason: str = "request_wake") -> bool:
        """Cooldown-deduped power-on (P2). True when a power-on was issued."""
        now = time.monotonic()
        if now - self.last_power_on_attempt <= self.power_on_cooldown:
            return False
        self.last_power_on_attempt = now
        return await self.power_on(reason=reason, initiated_by="proxy")

    async def power_on(self, *, reason: str = "manual", initiated_by: str = "monitor-ui") -> bool:
        """Issues a power-on and applies the ownership + cycle rules (P7/P8, D12).

        A success takes ownership and starts a new power cycle: the
        per-cycle auto-off switch resets to the config default and a new
        cycle id is minted. The coarse state is left as-is; the sync loop
        (or a readiness probe) confirms ON once the box is actually up.
        """
        self.transient = _TRANSIENT_ON
        ok = await self.backend.power_on()
        await self.store.log_power_event(self.name, "power_on", reason, initiated_by, ok)
        if ok:
            self.owned = True
            self.shutdown_override = None
            self.cycle_id = str(uuid.uuid4())
            await self.store.set_server_runtime(
                self.name, owned=True, shutdown_override=None, cycle_id=self.cycle_id
            )
            logger.info(
                "Server %s: power-on accepted (reason=%s); ownership taken.",
                self.name,
                reason,
            )
        else:
            self.transient = None
            logger.warning(
                "Server %s: power-on failed (reason=%s); re-trying on cooldown.",
                self.name,
                reason,
            )
        return ok

    async def power_off(self, *, reason: str = "idle_timeout", initiated_by: str = "proxy") -> bool:
        """Issues a graceful power-off; ownership ends when accepted (P7).

        The box keeps reporting ON until the OS actually shuts down; the
        sync loop confirms the transition.
        """
        self.transient = _TRANSIENT_OFF
        ok = await self.backend.power_off()
        await self.store.log_power_event(self.name, "power_off", reason, initiated_by, ok)
        if ok:
            self.owned = False
            await self.store.set_server_runtime(self.name, owned=False)
            logger.info(
                "Server %s: power-off accepted (reason=%s); ownership released.",
                self.name,
                reason,
            )
        else:
            self.transient = None
            logger.warning("Server %s: power-off failed (reason=%s).", self.name, reason)
        return ok

    async def on_routed_traffic(self) -> None:
        """A request was routed to a service on this server (P6, D12/D17).

        Only actually-routed requests (queued / concurrent / passthrough,
        including catch-all passthrough) call this. A 403-blocked request
        neither adopts ownership nor refreshes the idle clock.
        """
        self.last_activity = time.monotonic()
        if self.config.adopt_on_traffic and not self.owned:
            self.owned = True
            if self.cycle_id is None:
                self.cycle_id = str(uuid.uuid4())
            await self.store.set_server_runtime(self.name, owned=True, cycle_id=self.cycle_id)
            await self.store.log_power_event(self.name, "external_change", "adopted", "proxy", True)
            logger.info("Server %s: adopted via routed traffic; ownership taken.", self.name)

    async def note_service_ready(self) -> None:
        """An endpoint readiness probe succeeded: the box is implicitly on (D8)."""
        if self.power_state is PowerState.ON:
            return
        self._set_state(PowerState.ON)
        logger.info("Server %s: service ready; power state implied ON.", self.name)
        await self.store.set_server_runtime(self.name, power_state=PowerState.ON.value)
        await self.store.log_power_event(self.name, "state_sync", "service_ready", "proxy", True)

    async def set_shutdown_override(self, enabled: bool) -> None:
        """Toggles the per-cycle auto-off switch (monitor; P8)."""
        self.shutdown_override = enabled
        await self.store.set_server_runtime(self.name, shutdown_override=enabled)
        logger.info(
            "Server %s: auto power-off for this cycle %s from monitor "
            "(resets to the config default on the next proxy-initiated power-on).",
            self.name,
            "enabled" if enabled else "disabled",
        )

    # -- background loops ------------------------------------------------------

    def start(self) -> None:
        """Starts the per-server background loops (called from the app lifespan)."""
        self._sync_task = asyncio.create_task(
            self.power_sync_loop(), name=f"power-sync-{self.name}"
        )
        self._shutdown_task = asyncio.create_task(
            self.shutdown_loop(), name=f"shutdown-{self.name}"
        )

    async def stop(self) -> None:
        """Cancels the loops and releases the power backend (idempotent)."""
        for task in (self._sync_task, self._shutdown_task):
            if task is not None:
                task.cancel()
        self._sync_task = None
        self._shutdown_task = None
        await self.backend.close()

    async def power_sync_loop(self) -> None:
        """Per-server power-state poll (~5s; architecture.md loop table).

        Detects external on/off changes and confirms our own actions; a
        transient error never kills the loop. The first tick runs
        immediately so /healthz is not stale during the startup window.
        """
        while True:
            try:
                await self._power_sync_tick()
            except Exception:
                logger.exception("Power sync for %s failed; continuing.", self.name)
            await asyncio.sleep(self.sync_interval)

    async def _power_sync_tick(self) -> None:
        state = await self.backend.power_state()
        self.last_sync_at = time.monotonic()
        if state is self.power_state:
            return
        if self.transient is not None:
            # Confirms a power-on/off the proxy itself issued.
            action, initiated_by = "state_sync", "proxy"
        else:
            action, initiated_by = "external_change", "external"
            logger.info(
                "Server %s: power state changed externally: %s -> %s",
                self.name,
                self.power_state.value,
                state.value,
            )
        self._set_state(state)
        await self.store.set_server_runtime(self.name, power_state=state.value)
        await self.store.log_power_event(self.name, action, "poll", initiated_by, True)

    async def shutdown_loop(self) -> None:
        """Per-server idle-off engine (60s tick; P9-P12)."""
        while True:
            try:
                await self._idle_off_tick()
            except Exception:
                logger.exception("Shutdown check for %s failed; continuing.", self.name)
            await asyncio.sleep(self.shutdown_tick)

    def has_activity(self) -> bool:
        """True while any endpoint has queued work, in-flight traffic, or held slots."""
        return any(ep.has_activity() for ep in self.endpoints)

    async def _idle_off_tick(self) -> None:
        now = time.monotonic()
        # P9: never take the server down while it has work; restart the clock.
        if self.has_activity():
            self.last_activity = now
            return
        if not self.shutdown_enabled_now:
            return
        elapsed = now - self.last_activity
        if elapsed <= self.config.idle_timeout:
            return
        # P10: verify the actual power state before attempting a shutdown.
        actual = await self.backend.power_state()
        if actual is PowerState.ON:
            if self.owned:
                logger.info(
                    "Server %s idle for %.0fs. Actual state: ON. Shutting down...",
                    self.name,
                    elapsed,
                )
                await self.power_off(reason="idle_timeout")
            else:
                logger.info(
                    "Server %s idle for %.0fs but was powered on outside the proxy. Leaving it on.",
                    self.name,
                    elapsed,
                )
            # P12: reset the clock after a shutdown attempt (or re-poll) to
            # prevent flapping.
            self.last_activity = now
        elif actual is PowerState.OFF:
            logger.debug("Server %s already off, skipping shutdown.", self.name)
        else:
            logger.warning(
                "Server %s: could not determine power state, skipping shutdown to be safe.",
                self.name,
            )
