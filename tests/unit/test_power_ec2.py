"""AwsEc2Backend state mapping and action selection (D10, Phase 2).

The backend is exercised against the in-process ``FakeEc2Client`` (never
real AWS); ``relay[aws]``/boto3 is only needed to import the module's real
client builder, which these tests do not call.
"""

import pytest

from relay.models import AwsEc2PowerConfig
from relay.power.aws_ec2 import AwsEc2Backend
from relay.power.base import PowerState
from tests.mocks.fake_ec2 import FakeEc2Client

pytestmark = pytest.mark.unit


def make_backend(*, off_action: str = "stop"):
    """(backend, fake) pair: the backend talks to the injected fake client."""
    config = AwsEc2PowerConfig(
        region="us-east-1",
        instance_id="i-fake",
        access_key="test",
        secret_key="test",
        off_action=off_action,
    )
    fake = FakeEc2Client()
    return AwsEc2Backend(config, client=fake), fake


@pytest.mark.parametrize(
    ("ec2_state", "expected"),
    [
        # pending = booting: ON so the queue manager does not re-issue
        # start_instances while the instance comes up (D8).
        ("pending", PowerState.ON),
        ("running", PowerState.ON),
        ("stopping", PowerState.OFF),
        ("stopped", PowerState.OFF),
        ("shutting-down", PowerState.OFF),
        ("terminated", PowerState.OFF),
        # Unrecognized states are never acted on (D14).
        ("weird-state", PowerState.UNKNOWN),
    ],
)
async def test_state_map(ec2_state, expected):
    backend, fake = make_backend()
    fake.state = ec2_state
    assert await backend.power_state() is expected


async def test_power_on_issues_start_instances():
    backend, fake = make_backend()
    assert await backend.power_on() is True
    assert fake.actions == ["start_instances"]


async def test_power_off_issues_stop_by_default():
    backend, fake = make_backend()
    assert await backend.power_off() is True
    assert fake.actions == ["stop_instances"]


async def test_power_off_issues_terminate_when_configured():
    backend, fake = make_backend(off_action="terminate")
    assert await backend.power_off() is True
    assert fake.actions == ["terminate_instances"]


async def test_terminated_instance_refuses_power_on():
    # A describe that reports terminated sets the flag; start is refused
    # without touching the API (a terminated instance can never start).
    backend, fake = make_backend()
    fake.state = "terminated"
    assert await backend.power_state() is PowerState.OFF
    assert await backend.power_on() is False
    assert fake.actions == []


async def test_power_on_refused_after_successful_terminate():
    backend, fake = make_backend(off_action="terminate")
    assert await backend.power_off() is True
    assert await backend.power_on() is False
    assert fake.actions == ["terminate_instances"]


async def test_unreachable_api_is_unknown_and_recovers():
    backend, fake = make_backend()
    fake.fail_next = "describe_instances"
    assert await backend.power_state() is PowerState.UNKNOWN
    # The API comes back -> the real state on the next poll.
    fake.state = "running"
    assert await backend.power_state() is PowerState.ON


async def test_power_on_error_is_false():
    backend, fake = make_backend()
    fake.fail_next = "start_instances"
    assert await backend.power_on() is False


async def test_power_off_error_is_false():
    backend, fake = make_backend()
    fake.fail_next = "stop_instances"
    assert await backend.power_off() is False


async def test_close_is_idempotent():
    backend, _fake = make_backend()
    await backend.close()
    await backend.close()
