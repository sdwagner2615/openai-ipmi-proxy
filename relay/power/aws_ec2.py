"""AWS EC2 power backend (D10): the first (and only) CSP backend.

boto3 is synchronous, so every call hops to a worker thread
(``asyncio.to_thread``); power operations are rare and the latency is
irrelevant. The client is an optional extra (``relay[aws]``) and is
imported lazily, so the rest of the package works without it.

State map (phases.md, D8): the coarse power state is "is the box on", and
readiness (a separate signal) gates serving — so a booting box is ON:

    pending        -> ON   (booting; the queue manager must not re-issue
                            start_instances while it comes up)
    running        -> ON
    stopping       -> OFF
    stopped        -> OFF
    shutting-down  -> OFF
    terminated     -> OFF  (irreversible; power_on is refused while set)
    anything else  -> UNKNOWN (never act on unknown, D14)
"""

import asyncio
import logging
from typing import Any

from relay.models import AwsEc2PowerConfig
from relay.power.base import PowerBackend, PowerState

logger = logging.getLogger("relay.power.aws_ec2")

_ON_STATES = frozenset({"pending", "running"})
_OFF_STATES = frozenset({"stopping", "stopped", "shutting-down", "terminated"})


def _map_state(ec2_state: str | None) -> PowerState:
    """Maps an EC2 instance state name to the coarse power state."""
    if ec2_state in _ON_STATES:
        return PowerState.ON
    if ec2_state in _OFF_STATES:
        return PowerState.OFF
    return PowerState.UNKNOWN


class AwsEc2Backend(PowerBackend):
    """Power backend for `type: aws-ec2` servers (D10).

    ``client`` is an injected boto3 EC2 client (tests); when omitted a real
    client is built from the config (keys or profile, optional
    ``endpoint_url`` for a compatible gateway or the test fake).
    """

    def __init__(
        self,
        config: AwsEc2PowerConfig,
        *,
        client: Any = None,
        max_attempts: int = 3,
    ):
        self._config = config
        self._max_attempts = max(1, max_attempts)
        self._terminated = False
        self._client = client if client is not None else self._build_client()

    def _build_client(self) -> Any:
        """Builds a real boto3 EC2 client (requires the relay[aws] extra)."""
        import boto3
        from botocore.config import Config as BotoConfig

        config = self._config
        if config.profile:
            session = boto3.Session(profile_name=config.profile, region_name=config.region)
        else:
            session = boto3.Session(
                region_name=config.region,
                aws_access_key_id=config.access_key,
                aws_secret_access_key=config.secret_key,
                aws_session_token=config.session_token,
            )
        # Adaptive retry mode = the default backoff plus client-side pacing
        # and throttling-aware retries (phases.md: "retry on throttling").
        boto_config = BotoConfig(retries={"max_attempts": self._max_attempts, "mode": "adaptive"})
        client_kwargs: dict[str, Any] = {"config": boto_config}
        if config.endpoint_url:
            client_kwargs["endpoint_url"] = config.endpoint_url
        return session.client("ec2", **client_kwargs)

    def _describe_state(self) -> str | None:
        """Synchronous: the instance's EC2 state name, or None if absent."""
        instance_id = self._config.instance_id
        response = self._client.describe_instances(InstanceIds=[instance_id])
        for reservation in response.get("Reservations", []):
            for instance in reservation.get("Instances", []):
                if instance.get("InstanceId") == instance_id:
                    return instance.get("State", {}).get("Name")
        return None

    async def power_state(self) -> PowerState:
        try:
            state = await asyncio.to_thread(self._describe_state)
        except Exception as e:
            logger.warning(
                "EC2 API unreachable for %s (%s); power state unknown.",
                self._config.instance_id,
                e,
            )
            return PowerState.UNKNOWN
        if state == "terminated":
            self._terminated = True
        return _map_state(state)

    async def power_on(self) -> bool:
        if self._terminated:
            logger.error(
                "EC2 instance %s is terminated and can never be started again "
                "(off_action: terminate is irreversible).",
                self._config.instance_id,
            )
            return False
        try:
            await asyncio.to_thread(
                self._client.start_instances, InstanceIds=[self._config.instance_id]
            )
            return True
        except Exception as e:
            logger.error("EC2 start_instances failed for %s: %s", self._config.instance_id, e)
            return False

    async def power_off(self) -> bool:
        if self._config.off_action == "terminate":
            action = "terminate_instances"
        else:
            action = "stop_instances"
        try:
            await asyncio.to_thread(
                getattr(self._client, action), InstanceIds=[self._config.instance_id]
            )
            if action == "terminate_instances":
                self._terminated = True
            return True
        except Exception as e:
            logger.error("EC2 %s failed for %s: %s", action, self._config.instance_id, e)
            return False

    async def close(self) -> None:
        # The boto3 client holds nothing that needs explicit release.
        return None
