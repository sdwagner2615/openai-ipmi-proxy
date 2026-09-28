"""Redfish power backend (D9) — migrates the original main.py logic.

GET  {system_path}                                  -> PowerState
POST {system_path}/Actions/ComputerSystem.Reset     {"ResetType": ...}
"""

import logging

import httpx

from relay.models import RedfishPowerConfig
from relay.power.base import PowerState
from relay.power.ipmi import BmcClient, IpmiBackend

logger = logging.getLogger("relay.power.redfish")

# The BMC firmware quirk that made the old code hardcode
# /redfish/v1/Systems/Self (many MegaRAC units expose the single system as
# "Self"); now a config field with this default.
DEFAULT_SYSTEM_PATH = "/redfish/v1/Systems/Self"


class RedfishBackend(IpmiBackend):
    """Power backend for `type: redfish` servers (MegaRAC-style BMCs)."""

    def __init__(self, config: RedfishPowerConfig, *, http_client: httpx.AsyncClient | None = None):
        # Most BMCs use self-signed certs on HTTPS; verify_ssl is a
        # per-server config (default false, kept from the old behavior).
        base_url = config.base_url or f"https://{config.host}"
        super().__init__(
            BmcClient(
                base_url=base_url,
                user=config.user,
                password=config.password,
                verify_ssl=config.verify_ssl,
                http_client=http_client,
            )
        )
        self._system_path = config.system_path

    async def _read_power_state(self) -> PowerState:
        response = await self._bmc.request("GET", self._system_path)
        if response is None or response.status_code != 200:
            return PowerState.UNKNOWN
        try:
            data = response.json()
        except ValueError:
            return PowerState.UNKNOWN
        if not isinstance(data, dict):
            return PowerState.UNKNOWN
        state = data.get("PowerState")
        if state == "On":
            return PowerState.ON
        if state == "Off":
            return PowerState.OFF
        return PowerState.UNKNOWN

    async def _send_reset(self, reset_type: str) -> bool:
        endpoint = f"{self._system_path}/Actions/ComputerSystem.Reset"
        logger.info("Triggering Redfish reset %s using %s...", reset_type, self._system_path)
        response = await self._bmc.request("POST", endpoint, json_body={"ResetType": reset_type})
        if response is None:
            return False
        if response.status_code in (200, 202, 204):
            return True
        logger.error("Redfish reset %s failed: HTTP %s", reset_type, response.status_code)
        return False
