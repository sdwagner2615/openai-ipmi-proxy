"""The opencode StatusSource (D21) — the parity reference implementation.

OpenCode's local server exposes GET /session/status returning
    { <sessionID>: {"type": "busy"} | {"type": "retry", "attempt", "message", "next"} }
"busy" is held for the whole turn, including tool execution, which is
exactly the window we want to protect. Note that an idle session is NOT
present in the map at all - opencode deletes its entry the moment the
session goes idle, so "absent from a fresh status map" is the idle report
(StatusSource contract).

The status map is per-directory (one opencode "instance" per working
directory), so a session's directory is resolved once via
GET /session/{id} (which works without a directory and returns it) and the
status is then polled with GET /session/status?directory=<dir>.

Sub-agents: opencode's task tool creates sub-agent sessions that carry a
"parentID" pointing at the session that spawned them (also returned by
GET /session/{id}); the proxy uses it to let a sub-agent share the spot of
its tracked ancestor instead of waiting for one of its own (Q8).

Waiting for user input: while a session is blocked mid-turn on a pending
permission approval or a question answer, opencode keeps it "busy" in the
status map, so the pending-request endpoints (GET /permission and
GET /question, both per-directory like the status map) are the tie-breaker:
a session with a pending request counts as "waiting for input".

Discovery: the proxy probes <client-source-IP>:<port>. The client must
therefore run its opencode server on a reachable interface (e.g.
"opencode serve --hostname 0.0.0.0 --port 4096"); the TUI default
(127.0.0.1, random port) is not reachable from the proxy.
"""

import logging
from urllib.parse import quote

import httpx

from relay.clients import StatusReport, StatusSource
from relay.models import ClientConfig

logger = logging.getLogger("relay.status_opencode")

__all__ = ["OpencodeStatusSource", "create_status_source"]


class OpencodeStatusSource(StatusSource):
    """Polls opencode's local server (one "instance" per working directory)."""

    def __init__(
        self,
        http_client: httpx.AsyncClient,
        *,
        port: int,
        password: str = "",
        poll_interval: float = 5.0,
        status_path: str = "/session/status",
        session_path: str = "/session",
        pending_paths: tuple[str, ...] = ("/permission", "/question"),
        timeout: float = 2.0,
    ):
        super().__init__(http_client, poll_interval)
        self.port = port
        self.status_path = status_path
        self.session_path = session_path
        # (path, kind) pairs of endpoints listing requests a session is
        # blocked on while awaiting user input; each item carries a
        # "sessionID".
        self.pending_paths = tuple(
            (path, path.strip("/").rsplit("/", 1)[-1]) for path in pending_paths
        )
        self.timeout = httpx.Timeout(timeout)
        # Basic auth: opencode servers protected with a password use the
        # fixed username "opencode".
        self.auth = ("opencode", password) if password else None

    def base_url(self, client_ip: str) -> str:
        return f"http://{client_ip}:{self.port}"

    async def fetch_session_info(self, base: str, session_id: str) -> tuple[str, str | None] | None:
        """
        Resolves a session's (working directory, parent session id) via
        GET /session/{id}. The parent id is None for regular sessions and
        set for sub-agent sessions (opencode's task tool). Returns None
        when the session is unknown or the client is unreachable (callers
        skip that session until the next poll).
        """
        url = f"{base}{self.session_path}/{quote(session_id, safe='')}"
        try:
            response = await self.http_client.get(url, timeout=self.timeout, auth=self.auth)
            if response.status_code != 200:
                logger.debug("Session lookup %s: HTTP %s", url, response.status_code)
                return None
            data = response.json()
            if (
                not isinstance(data, dict)
                or not isinstance(data.get("directory"), str)
                or not data["directory"]
            ):
                return None
            parent = data.get("parentID")
            return data["directory"], (parent if isinstance(parent, str) and parent else None)
        except Exception as e:
            logger.debug("Session lookup %s failed: %s", url, e)
            return None

    async def fetch_statuses(self, base: str, directory: str) -> dict[str, StatusReport] | None:
        """
        GETs the per-directory status map and normalizes it. Returns a dict
        of session-id -> StatusReport for the sessions opencode reports as
        active (busy / retry; a literal "idle" entry is passed through), or
        None when the client is unreachable or answers with garbage
        (callers keep last-known state in that case).
        """
        url = base + self.status_path
        try:
            response = await self.http_client.get(
                url,
                params={"directory": directory},
                timeout=self.timeout,
                auth=self.auth,
            )
            if response.status_code != 200:
                logger.debug("Status poll %s: HTTP %s", url, response.status_code)
                return None
            data = response.json()
            if not isinstance(data, dict):
                return None
            reports: dict[str, StatusReport] = {}
            for sid, st in data.items():
                if not isinstance(st, dict):
                    continue
                stype = st.get("type")
                if stype == "busy":
                    reports[sid] = StatusReport("busy")
                elif stype == "idle":
                    reports[sid] = StatusReport("idle")
                elif stype == "retry":
                    reports[sid] = StatusReport("retry", f"attempt {st.get('attempt', '?')}")
                # Unrecognized types are not reported: to the caller the
                # session is simply not active in this map.
            return reports
        except Exception as e:
            logger.debug("Status poll %s failed: %s", url, e)
            return None

    async def fetch_pending(self, base: str, directory: str) -> dict[str, str] | None:
        """
        GETs the pending user-input endpoints (permission / question) for
        one client base and directory. Returns {session-id: kind} for the
        sessions currently blocked awaiting a human, or None when any of
        the endpoints is unreachable or answers with garbage (callers keep
        last-known state; a partial answer must never clear a session's
        "waiting" flag).
        """
        pending: dict[str, str] = {}
        for path, kind in self.pending_paths:
            url = base + path
            try:
                response = await self.http_client.get(
                    url,
                    params={"directory": directory},
                    timeout=self.timeout,
                    auth=self.auth,
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


def create_status_source(
    client: ClientConfig, http_client: httpx.AsyncClient
) -> StatusSource | None:
    """
    Builds the configured client's status source, or None for
    ``status.kind: none`` (a client that is identified but gets no reports:
    its sessions use busy-window inference).
    """
    status = client.status
    if status.kind == "opencode":
        return OpencodeStatusSource(
            http_client,
            port=status.port,
            password=status.password,
            poll_interval=status.poll_interval,
            status_path=status.status_path,
            session_path=status.session_path,
            pending_paths=tuple(status.pending_paths),
        )
    return None
