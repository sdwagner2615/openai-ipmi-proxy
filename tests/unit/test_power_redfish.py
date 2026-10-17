"""RedfishBackend state mapping and action payloads (O1, parity §power)."""

import base64
import json

import httpx
import pytest

from relay.models import RedfishPowerConfig
from relay.power.base import PowerState
from relay.power.ipmi import BmcClient
from relay.power.redfish import RedfishBackend

pytestmark = pytest.mark.unit


class BmcHandler:
    def __init__(self, power_state: str = "On", fail_times: int = 0):
        self.state = power_state
        self.calls: list = []
        self.fail_times = fail_times
        self.transport = httpx.MockTransport(self.handle)

    def handle(self, request: httpx.Request) -> httpx.Response:
        if self.fail_times > 0:
            self.fail_times -= 1
            raise httpx.ConnectError("bmc unreachable")
        self.calls.append((request.method, request.url.path, request.content))
        if request.method == "GET":
            return httpx.Response(200, json={"Id": "Self", "PowerState": self.state})
        if request.url.path.endswith("ComputerSystem.Reset"):
            body = json.loads(request.content)
            reset_type = body.get("ResetType")
            self.state = "On" if reset_type == "On" else "Off"
            return httpx.Response(200, json={"status": "accepted"})
        return httpx.Response(404, json={"error": "not found"})


def make_backend(
    handler: BmcHandler,
    *,
    host: str = "127.0.0.1",
    base_url: str | None = None,
    user: str = "admin",
    password: str = "secret",
    system_path: str = "/redfish/v1/Systems/Self",
    **client_kwargs,
) -> RedfishBackend:
    config = RedfishPowerConfig(
        host=host, user=user, password=password, base_url=base_url, system_path=system_path
    )
    client_kwargs.setdefault("timeout", 1.0)
    client_kwargs.setdefault("retry_backoff", 0.01)
    bmc = BmcClient(
        base_url=base_url or f"https://{host}",
        user=user,
        password=password,
        http_client=httpx.AsyncClient(transport=handler.transport),
        **client_kwargs,
    )
    return RedfishBackend(config, bmc=bmc)


async def test_state_mapping():
    handler = BmcHandler("On")
    backend = make_backend(handler)
    assert await backend.power_state() is PowerState.ON
    handler.state = "Off"
    assert await backend.power_state() is PowerState.OFF
    handler.state = "Degraded"
    assert await backend.power_state() is PowerState.UNKNOWN
    handler.state = "On"
    assert await backend.power_state() is PowerState.ON


async def test_unreachable_bmc_is_unknown_and_recovers():
    # Both attempts fail -> UNKNOWN (a dead BMC must not wedge the proxy).
    handler = BmcHandler(fail_times=2)
    backend = make_backend(handler, max_attempts=2)
    assert await backend.power_state() is PowerState.UNKNOWN
    # The BMC comes back -> ON on the next poll.
    assert await backend.power_state() is PowerState.ON
    await backend.close()


async def test_power_on_posts_reset_on_to_the_system_path():
    handler = BmcHandler("Off")
    backend = make_backend(handler)
    assert await backend.power_on() is True
    method, path, content = handler.calls[-1]
    assert method == "POST"
    assert path == "/redfish/v1/Systems/Self/Actions/ComputerSystem.Reset"
    assert json.loads(content) == {"ResetType": "On"}


async def test_power_off_posts_graceful_shutdown():
    handler = BmcHandler("On")
    backend = make_backend(handler)
    assert await backend.power_off() is True
    method, _path, content = handler.calls[-1]
    assert method == "POST"
    assert json.loads(content) == {"ResetType": "GracefulShutdown"}


async def test_custom_system_path_is_used():
    handler = BmcHandler("Off")
    backend = make_backend(handler, system_path="/redfish/v1/Systems/1")
    await backend.power_state()
    assert handler.calls[-1][1] == "/redfish/v1/Systems/1"


async def test_base_url_override_wins_over_host():
    handler = BmcHandler("On")
    backend = make_backend(handler, base_url="http://127.0.0.1:8102")
    await backend.power_state()
    assert handler.calls[-1][1] == "/redfish/v1/Systems/Self"
    assert backend.bmc.base_url == "http://127.0.0.1:8102"


async def test_basic_auth_is_sent():
    handler = BmcHandler("On")
    backend = make_backend(handler, user="admin", password="secret")
    await backend.power_state()
    # The MockTransport does not expose the auth header on the response side,
    # so verify via a capturing transport.
    seen = {}

    def capture(request: httpx.Request) -> httpx.Response:
        seen["auth"] = request.headers.get("authorization")
        return httpx.Response(200, json={"PowerState": "On"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(capture))
    backend2 = RedfishBackend(
        RedfishPowerConfig(host="h", user="admin", password="secret"),
        bmc=BmcClient(base_url="https://h", user="admin", password="secret", http_client=client),
    )
    await backend2.power_state()
    expected = "Basic " + base64.b64encode(b"admin:secret").decode()
    assert seen["auth"] == expected


async def test_bmc_http_error_maps_to_unknown():
    def not_found(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, text="nope")

    client = httpx.AsyncClient(transport=httpx.MockTransport(not_found))
    backend = RedfishBackend(
        RedfishPowerConfig(host="h", user="u", password="p"),
        bmc=BmcClient(base_url="https://h", user="u", password="p", http_client=client),
    )
    assert await backend.power_state() is PowerState.UNKNOWN
    assert await backend.power_on() is False
