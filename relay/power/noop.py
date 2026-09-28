"""No-op power backend for dev/tests (D3): no hardware, scripted state.

The state can be scripted (`set_state`) and every issued action is recorded
(`actions`) so tests can assert on them.
"""

from relay.models import NoopPowerConfig
from relay.power.base import PowerBackend, PowerState


class NoopBackend(PowerBackend):
    """Scriptable power backend that always succeeds."""

    def __init__(self, config: NoopPowerConfig | None = None):
        initial = config.initial_state if config is not None else "on"
        self.state = PowerState.ON if initial == "on" else PowerState.OFF
        # Issued actions, in order: "power_on" / "power_off".
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
        # Nothing to release.
        return None

    def set_state(self, state: PowerState) -> None:
        """Scripts the current state (test/dev control; no action recorded)."""
        self.state = state
