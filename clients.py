"""
Known-client providers and their status APIs.

Queuing needs to know when a session is truly done with the model (not just
"the last HTTP response finished"), because the client may still be inside a
long tool call and we want to keep its K/V cache warm by holding its spot.

Known clients can therefore report their own session state over an external
API. OpenCode is the first such client. Its local server exposes
GET /session/status returning
    { <sessionID>: {"type": "busy"} | {"type": "retry", "attempt", "message", "next"} }
"busy" is held for the whole turn, including tool execution, which is
exactly the window we want to protect. Note that an idle session is NOT
present in the map at all - opencode deletes its entry the moment the
session goes idle, so "absent from a fresh status map" is the idle report.

Session identification: opencode only sends the "x-opencode-session" header
when talking to OpenCode's own hosted provider; for every other provider
(e.g. a self-hosted llama.cpp server) it sends "X-Session-Id" (and
"x-session-affinity") instead. Both are checked, gated on the opencode
User-Agent, so other tools that happen to send an X-Session-Id header are
not misattributed.

The status map is per-directory (one opencode "instance" per working
directory), so the session's directory is resolved first via
GET /session/{id} (which works without a directory and returns it) and the
status is then polled with GET /session/status?directory=<dir>.

Sub-agents: opencode's task tool creates sub-agent sessions that carry a
"parentID" pointing at the session that spawned them (also returned by
GET /session/{id}). The proxy uses this to let a sub-agent share the spot
of its tracked ancestor instead of waiting for one of its own.

Waiting for user input: while a session is blocked mid-turn on a pending
permission approval or a question answer, opencode keeps it "busy" in the
status map, so the proxy also polls the client's pending-request endpoints
(GET /permission and GET /question, both per-directory like the status
map) and treats sessions with a pending request as "waiting for input".

Discovery: the proxy probes <client-source-IP>:<OPENCODE_STATUS_PORT>.
The client must therefore run its opencode server on a reachable interface
(e.g. "opencode serve --hostname 0.0.0.0 --port 4096"); the TUI default
(127.0.0.1, random port) is not reachable from the proxy.
"""

import logging
from dataclasses import dataclass
from typing import Optional
from urllib.parse import quote

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
    # Headers that identify this client on their own (uniquely owned by the
    # client; checked in order, first present carries the session id).
    session_headers: tuple
    # Headers that identify this client only when the User-Agent matches
    # ua_prefix (these are generic headers other tools may send too).
    gated_session_headers: tuple = ()
    # UA prefix required for the gated headers to count.
    ua_prefix: str = ""
    # Path on the client's local server that reports session statuses.
    status_path: str = "/session/status"
    # Path on the client's local server that returns a single session
    # (used to resolve its directory).
    session_path: str = "/session"
    # (path, kind) pairs of endpoints listing requests a session is blocked
    # on while awaiting user input; each item carries a "sessionID".
    pending_paths: tuple = ()


OPENCODE = ClientProvider(
    name="opencode",
    # Only sent when using OpenCode's own hosted provider.
    session_headers=("x-opencode-session",),
    # Sent for every other provider (e.g. llama.cpp); gated on the
    # opencode User-Agent so other tools using the same headers are not
    # misattributed.
    gated_session_headers=("x-session-affinity", "x-session-id"),
    ua_prefix="opencode/",
    pending_paths=(
        ("/permission", "permission"),
        ("/question", "question"),
    ),
)

CLIENT_PROVIDERS: tuple[ClientProvider, ...] = (OPENCODE,)


def detect_client(headers: dict) -> Optional[ClientProvider]:
    """
    Returns the provider for a request's headers (case-insensitive), or
    None for clients without a known provider.
    """
    lowered = {k.lower(): v for k, v in headers.items()}
    ua = lowered.get("user-agent", "").lower()
    for provider in CLIENT_PROVIDERS:
        if any(lowered.get(h) for h in provider.session_headers):
            return provider
        if (
            provider.ua_prefix
            and ua.startswith(provider.ua_prefix)
            and any(lowered.get(h) for h in provider.gated_session_headers)
        ):
            return provider
    return None


def session_header_value(provider: ClientProvider, headers: dict) -> Optional[str]:
    """The first non-empty session header value for a provider, else None."""
    lowered = {k.lower(): v for k, v in headers.items()}
    for header in provider.session_headers + provider.gated_session_headers:
        value = lowered.get(header)
        if value:
            return value
    return None


class StatusPoller:
    """
    Polls known clients' status APIs. One base URL per client machine
    (derived from the client's source IP). Because the status map is
    per-directory, each session's directory is resolved once (GET
    /session/{id}) and cached; one status GET is then made per distinct
    directory per poll interval.
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
        # (base, session-id) -> working directory; a session's directory
        # never changes, so entries live until the session is gone.
        self.session_dir: dict[tuple, str] = {}
        # (base, session-id) -> parent session id (None = not a sub-agent).
        self.session_parent: dict[tuple, Optional[str]] = {}

    def base_url(self, client_ip: str) -> str:
        return f"http://{client_ip}:{self.port}"

    def due(self, base: str, now: float) -> bool:
        return now - self.last_poll.get(base, 0.0) >= self.poll_interval

    def forget(self, base: str, session_id: str) -> None:
        self.session_dir.pop((base, session_id), None)
        self.session_parent.pop((base, session_id), None)

    async def fetch_session_info(self, base: str, session_id: str) -> Optional[tuple]:
        """
        Resolves a session's (working directory, parent session id) via
        GET /session/{id}. The parent id is None for regular sessions and
        set for sub-agent sessions (opencode's task tool). Returns None
        when the session is unknown or the client is unreachable (callers
        skip that session until the next poll).
        """
        url = f"{base}{OPENCODE.session_path}/{quote(session_id, safe='')}"
        try:
            response = await self.http_client.get(
                url, timeout=self.timeout, auth=self.auth
            )
            if response.status_code != 200:
                logger.debug("Session lookup %s: HTTP %s", url, response.status_code)
                return None
            data = response.json()
            if not isinstance(data, dict) or not isinstance(data.get("directory"), str) or not data["directory"]:
                return None
            parent = data.get("parentID")
            return data["directory"], (parent if isinstance(parent, str) and parent else None)
        except Exception as e:
            logger.debug("Session lookup %s failed: %s", url, e)
            return None

    async def fetch_statuses(self, base: str, directory: Optional[str]) -> Optional[dict]:
        """
        GETs the status map for one client base and directory. Returns a
        dict of session-id -> status-object, or None when the client is
        unreachable or answers with garbage (callers keep last-known state
        in that case). The caller records last_poll[base] after the call.
        """
        url = base + OPENCODE.status_path
        params = {"directory": directory} if directory else None
        try:
            response = await self.http_client.get(
                url, params=params, timeout=self.timeout, auth=self.auth
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

    async def fetch_pending(self, base: str, directory: Optional[str]) -> Optional[dict]:
        """
        GETs the pending user-input endpoints (permission / question) for
        one client base and directory. Returns {session-id: kind} for the
        sessions currently blocked awaiting a human, or None when any of
        the endpoints is unreachable or answers with garbage (callers keep
        last-known state; a partial answer must never clear a session's
        "waiting" flag).
        """
        params = {"directory": directory} if directory else None
        pending: dict = {}
        for path, kind in OPENCODE.pending_paths:
            url = base + path
            try:
                response = await self.http_client.get(
                    url, params=params, timeout=self.timeout, auth=self.auth
                )
                if response.status_code != 200:
                    logger.debug("Pending poll %s: HTTP %s", url, response.status_code)
                    return None
                data = response.json()
                if not isinstance(data, list):
                    return None
                for item in data:
                    if isinstance(item, dict) and isinstance(item.get("sessionID"), str):
                        pending[item["sessionID"]] = kind
            except Exception as e:
                logger.debug("Pending poll %s failed: %s", url, e)
                return None
        return pending
