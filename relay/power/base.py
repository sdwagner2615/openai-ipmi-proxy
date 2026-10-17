"""Power backend hierarchy (D9).

PowerBackend (ABC)
  ├── IpmiBackend (abstract middle: shared "talk to a BMC" logic)
  │     └── RedfishBackend
  ├── NoopBackend (dev/tests)
  └── (Phase 2) AwsEc2Backend, directly under PowerBackend (D10)
"""

from abc import ABC, abstractmethod
from enum import StrEnum


class PowerState(StrEnum):
    """Coarse power state of a server (D8: the "is the box on" signal)."""

    ON = "on"
    OFF = "off"
    UNKNOWN = "unknown"


class PowerBackend(ABC):
    """Manages the power state of one server."""

    @abstractmethod
    async def power_on(self) -> bool:
        """Issues a power-on. Returns True when the call was accepted."""

    @abstractmethod
    async def power_off(self) -> bool:
        """Issues a power-off (graceful where the protocol supports it)."""

    @abstractmethod
    async def power_state(self) -> PowerState:
        """Queries the actual power state; UNKNOWN when unreachable."""

    @abstractmethod
    async def close(self) -> None:
        """Releases backend resources (idempotent)."""
