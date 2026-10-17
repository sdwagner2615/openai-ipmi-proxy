"""Shared BMC management logic (D9): the IpmiBackend middle layer.

Redfish is technically a separate REST protocol coexisting with IPMI on the
same BMC, so this middle layer is really "BMC power management": it owns the
BMC client (host/creds/verify_ssl/timeout), retry/backoff on management
calls, the graceful-off mapping, and raw-response normalization. Subclasses
implement only their protocol's send/read.
"""

import asyncio
import logging
from abc import abstractmethod

import httpx

from relay.power.base import PowerBackend, PowerState

logger = logging.getLogger("relay.power.ipmi")


class BmcClient:
    """A small authenticated HTTP client for one BMC.

    Owns its own `httpx.AsyncClient` (verify is a client-level setting in
    httpx, so per-server `verify_ssl` needs a per-server client). BMC calls
    are rare; the high-volume proxy traffic uses the app's shared client.
    """

    def __init__(
        self,
        *,
        base_url: str,
        user: str,
        password: str,
        verify_ssl: bool = False,
        timeout: float = 10.0,
        max_attempts: int = 2,
        retry_backoff: float = 1.0,
        http_client: httpx.AsyncClient | None = None,
    ):
        self._base_url = base_url.rstrip("/")
        self._auth = (user, password)
        self._timeout = httpx.Timeout(timeout)
        self._max_attempts = max(1, max_attempts)
        self._retry_backoff = retry_backoff
        if http_client is not None:
            self._http = http_client
        else:
            self._http = httpx.AsyncClient(verify=verify_ssl)
        self._owns_http = http_client is None

    @property
    def base_url(self) -> str:
        return self._base_url

    async def request(
        self, method: str, path: str, *, json_body: dict | None = None
    ) -> httpx.Response | None:
        """Performs one management call; returns None when unreachable.

        Retries with backoff on transport errors only (an unreachable BMC
        must not wedge the proxy); HTTP error responses are returned as-is
        so callers can normalize them.
        """
        url = f"{self._base_url}/{path.lstrip('/')}"
        for attempt in range(1, self._max_attempts + 1):
            try:
                if method == "POST":
                    response = await self._http.post(
                        url, json=json_body, timeout=self._timeout, auth=self._auth
                    )
                else:
                    response = await self._http.get(url, timeout=self._timeout, auth=self._auth)
                if response.status_code >= 400:
                    logger.error(
                        "BMC API error %s during %s %s (URL: %s): %s",
                        response.status_code,
                        method,
                        path,
                        url,
                        response.text,
                    )
                return response
            except httpx.TransportError as e:
                if attempt < self._max_attempts:
                    logger.warning(
                        "BMC network error during %s %s (URL: %s): %s; retrying",
                        method,
                        path,
                        url,
                        e,
                    )
                    await asyncio.sleep(self._retry_backoff)
        logger.error("BMC network error during %s %s (URL: %s)", method, path, url)
        return None

    async def close(self) -> None:
        if self._owns_http:
            await self._http.aclose()


class IpmiBackend(PowerBackend):
    """Abstract middle layer over a BMC (D9).

    Subclasses implement `_send_reset` (protocol-specific reset action) and
    `_read_power_state` (protocol-specific state read).
    """

    #: Reset action used for a graceful (OS-level) shutdown.
    GRACEFUL_OFF_RESET_TYPE = "GracefulShutdown"

    def __init__(self, bmc: BmcClient):
        self._bmc = bmc

    @property
    def bmc(self) -> BmcClient:
        return self._bmc

    async def power_on(self) -> bool:
        return await self._send_reset("On")

    async def power_off(self) -> bool:
        # Graceful shutdown: ask the OS to shut down, never hard-cut.
        return await self._send_reset(self.GRACEFUL_OFF_RESET_TYPE)

    async def power_state(self) -> PowerState:
        return await self._read_power_state()

    async def close(self) -> None:
        await self._bmc.close()

    @abstractmethod
    async def _send_reset(self, reset_type: str) -> bool:
        """Issues a reset action; True when the BMC accepted it."""

    @abstractmethod
    async def _read_power_state(self) -> PowerState:
        """Reads the power state; UNKNOWN when unreachable or unparseable."""
