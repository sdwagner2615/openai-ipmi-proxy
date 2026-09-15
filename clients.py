"""
Known-client providers and their status APIs.

Queuing needs to know when a session is truly done with the model (not just
"the last HTTP response finished"), because the client may still be inside a
long tool call and we want to keep its K/V cache warm by holding its spot.

Known clients can therefore report their own session state over an external
API. OpenCode is the first such client: every LLM request it makes carries
an "x-opencode-session" header (per-conversation id), and its local server
exposes GET /session/status returning
    { <sessionID>: {"type": "busy"} | {"type": "idle"}
        | {"type": "retry", "attempt", "message", "next"} }
"busy" is held for the whole turn, including tool execution, which is
exactly the window we want to protect.

Discovery: the proxy probes <client-source-IP>:<OPENCODE_STATUS_PORT>.
The client must therefore run its opencode server on a reachable interface
(e.g. "opencode serve --hostname 0.0.0.0 --port 4096"); the TUI default
(127.0.0.1, random port) is not reachable from the proxy.
"""

import logging
from dataclasses import dataclass
from typing import Optional

import httpx

logger = logging.getLogger("ipmi-proxy.clients")

__all__ = ["ClientProvider", "CLIENT_PROVIDERS", "detect_client", "StatusPoller"]

# Grace period (seconds) a session keeps its last known status when the
# client's status API becomes unreachable or stops reporting the session
# (e.g. the client restarted). After the grace it is treated as idle.
STATUS_UNREACHABLE_GRACE = 30.0


@dataclass(frozen=True)
class ClientProvider:
    name: str
    # Header whose presence identifies this client and whose value is the
    # session id.
    session_header: str
    # Path on the client's local server that reports all session statuses.
    status_path: str


OPENCODE = ClientProvider(
    name="opencode",
    session_header="x-opencode-session",
    status_path="/session/status",
)

CLIENT_PROVIDERS: tuple[ClientProvider, ...] = (OPENCODE,)


def detect_client(headers: dict) -> Optional[ClientProvider]:
    """
    Returns the provider for a request's headers (case-insensitive), or
    None for clients without a known provider.
    """
    lowered = {k.lower(): v for k, v in headers.items()}
    for provider in CLIENT_PROVIDERS:
        if provider.session_header in lowered:
            return provider
    return None


class StatusPoller:
    """
    Polls known clients' status APIs. One base URL per client machine
    (derived from the client's source IP); a single GET returns the status
    of every session on that machine, so polling cost stays at one request
    per client per interval no matter how many sessions it has.
    """

    def __init__(
        self,
        http_client: httpx.AsyncClient,
        port: int,
        password: str = "",
        poll_interval: float = 5.0,
        timeout: float = 2.0,
    ):
        self.http_client = http_client
        self.port = port
        self.poll_interval = poll_interval
        self.timeout = httpx.Timeout(timeout)
        # Basic auth: opencode servers protected with OPENCODE_SERVER_PASSWORD
        # use the fixed username "opencode".
        self.auth = ("opencode", password) if password else None
        self.last_poll: dict[str, float] = {}

    def base_url(self, client_ip: str) -> str:
        return f"http://{client_ip}:{self.port}"

    def due(self, base: str, now: float) -> bool:
        return now - self.last_poll.get(base, 0.0) >= self.poll_interval

    async def fetch(self, base: str) -> Optional[dict]:
        """
        GETs the status map for one client base. Returns a dict of
        session-id -> status-object, or None when the client is unreachable
        or answers with garbage (callers keep last-known state in that case).
        The caller records last_poll[base] after the call.
        """
        url = base + OPENCODE.status_path
        try:
            response = await self.http_client.get(
                url, timeout=self.timeout, auth=self.auth
            )
            if response.status_code != 200:
                logger.debug("Status poll %s: HTTP %s", url, response.status_code)
                return None
            data = response.json()
            if not isinstance(data, dict):
                return None
            return data
        except Exception as e:
            logger.debug("Status poll %s failed: %s", url, e)
            return None
