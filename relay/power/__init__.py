"""Power backend registry: server type -> backend (D9/D10).

`aws-ec2` (D10) is imported lazily because it needs the optional
`relay[aws]` extra (boto3); configuring an aws-ec2 server without the
extra fails at startup with a clear error.
"""

from collections.abc import Callable

from relay.config import ConfigError
from relay.models import AwsEc2PowerConfig, ServerConfig
from relay.power.base import PowerBackend, PowerState
from relay.power.noop import NoopBackend
from relay.power.redfish import RedfishBackend

__all__ = ["PowerBackend", "PowerState", "create_power_backend"]

_BACKENDS: dict[str, Callable] = {
    "redfish": RedfishBackend,
    "noop": NoopBackend,
}


def create_power_backend(server: ServerConfig) -> PowerBackend:
    """Builds the power backend for a configured server."""
    if server.type == "aws-ec2":
        try:
            from relay.power.aws_ec2 import AwsEc2Backend
        except ImportError as e:
            raise ConfigError(
                f"server {server.name!r}: the aws-ec2 backend requires the "
                "optional `relay[aws]` extra (boto3); install it with "
                "`uv pip install 'relay[aws]'`"
            ) from e
        # Config validation guarantees an aws-ec2 server carries an
        # AwsEc2PowerConfig; assert for the type checker.
        assert isinstance(server.power, AwsEc2PowerConfig)
        return AwsEc2Backend(server.power)
    backend_cls = _BACKENDS.get(server.type)
    if backend_cls is None:
        raise ConfigError(
            f"server {server.name!r}: no power backend available for type {server.type!r}"
        )
    return backend_cls(server.power)
