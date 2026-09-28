"""Power backend registry: server type -> backend (D9/D10).

`aws-ec2` registers here in Phase 2 (boto3, extra `relay[aws]`); until then
configuring it fails at startup with a clear error.
"""

from collections.abc import Callable

from relay.config import ConfigError
from relay.models import ServerConfig
from relay.power.base import PowerBackend, PowerState
from relay.power.noop import NoopBackend
from relay.power.redfish import RedfishBackend

__all__ = ["PowerBackend", "PowerState", "create_power_backend"]

_BACKENDS: dict[str, Callable] = {
    "redfish": RedfishBackend,
    "noop": NoopBackend,
    # "aws-ec2": AwsEc2Backend  # Phase 2 (requires the relay[aws] extra)
}


def create_power_backend(server: ServerConfig) -> PowerBackend:
    """Builds the power backend for a configured server."""
    backend_cls = _BACKENDS.get(server.type)
    if backend_cls is None:
        raise ConfigError(
            f"server {server.name!r}: no power backend available for type "
            f"{server.type!r} in this version (aws-ec2 lands in Phase 2)"
        )
    return backend_cls(server.power)
