"""White-glove client identification and the status-source interface (D20/D21).

Queuing needs to know when a session is truly done with the model (not just
"the last HTTP response finished"), because the client may still be inside a
long tool call and we want to keep its K/V cache warm by holding its spot.

Known clients can therefore report their own session state over an external
API. White-glove clients are configured (D20/D21): identified by header/UA
rules (``ClientProvider`` / ``ClientRegistry``), and carrying a pluggable
``StatusSource`` — where to poll (a base URL on the client's own source IP)
and how to interpret the answers. The opencode source
(``relay/status_opencode.py``) is the reference implementation and must
reproduce every current opencode behavior (parity.md §client-status).

A configured client without a status source (``status.kind: none``) gets no
reports at all: its sessions fall back to busy-window inference, like
unknown clients.
"""

import json
import logging
from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import httpx

from relay.models import ClientConfig

logger = logging.getLogger("relay.clients")

__all__ = [
    "STATUS_UNREACHABLE_GRACE",
    "ClientProvider",
    "ClientRegistry",
    "StatusReport",
    "StatusSource",
    "detect_client",
    "extract_body_field",
    "resolve_shared_spot",
    "session_header_value",
]

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
    # Child-session slot sharing rule: none | parent-chain.
    children_kind: str = "none"
    # parent-chain walk cap.
    children_depth: int = 10


def provider_from_config(client: ClientConfig) -> ClientProvider:
    """Builds a ClientProvider from a configured client entry."""
    pending_paths = tuple(
        (path, path.strip("/").rsplit("/", 1)[-1]) for path in client.status.pending_paths
    )
    return ClientProvider(
        name=client.name,
        session_headers=tuple(h.lower() for h in client.match.session_headers),
        gated_session_headers=tuple(h.lower() for h in client.match.gated_session_headers),
        ua_prefix=client.match.ua_prefix.lower(),
        status_path=client.status.status_path,
        session_path=client.status.session_path,
        pending_paths=pending_paths,
        children_kind=client.children.kind,
        children_depth=client.children.depth,
    )


def detect_client(headers: dict, providers: list[ClientProvider]) -> ClientProvider | None:
    """
    Returns the provider for a request's headers (case-insensitive), or
    None for clients without a known provider.
    """
    lowered = {k.lower(): v for k, v in headers.items()}
    ua = lowered.get("user-agent", "").lower()
    for provider in providers:
        if any(lowered.get(h) for h in provider.session_headers):
            return provider
        if (
            provider.ua_prefix
            and ua.startswith(provider.ua_prefix)
            and any(lowered.get(h) for h in provider.gated_session_headers)
        ):
            return provider
    return None


def session_header_value(provider: ClientProvider, headers: dict) -> str | None:
    """The first non-empty session header value for a provider, else None."""
    lowered = {k.lower(): v for k, v in headers.items()}
    for header in provider.session_headers + provider.gated_session_headers:
        value = lowered.get(header)
        if value:
            return value
    return None


class ClientRegistry:
    """White-glove client matching and session-id resolution (D20/D22).

    Built from the ``clients:`` config entries (config order — first match
    wins) and the proxy's generic session-id headers. A matched client's
    status source (D21) is looked up separately, by client name.
    """

    def __init__(self, clients: list[ClientConfig], generic_headers: list[str]):
        self.providers = [provider_from_config(client) for client in clients]
        self.by_name: dict[str, ClientProvider] = {
            provider.name: provider for provider in self.providers
        }
        self.generic_headers = tuple(h.lower() for h in generic_headers)

    def match(self, headers: dict) -> ClientProvider | None:
        """The white-glove client identified by the headers, else None."""
        return detect_client(headers, self.providers)

    def session_id(self, provider: ClientProvider, headers: dict) -> str | None:
        """The session id carried in the provider's headers, else None."""
        return session_header_value(provider, headers)

    def resolve(
        self,
        headers: dict,
        client_ip: str,
        body: bytes | None = None,
        body_fields: tuple[str, ...] = (),
        query: Mapping[str, str] | None = None,
    ) -> tuple[str, str]:
        """Identifies the (client, session id) for a request (S1, D22).

        Precedence: (1) the matched white-glove client's own session headers
        (plain, then UA-gated), (2) the configured generic session headers,
        (3) the endpoint's body fields (HTTP only), (4) query params mirroring
        those header names (WebSockets only — WS URLs carry ids/tokens in the
        query string), (5) ``ua:<UA>|ip:<IP>`` as a last resort.
        """
        lowered = {k.lower(): v for k, v in headers.items()}
        user_agent = lowered.get("user-agent", "")
        provider = self.match(lowered)
        if provider is not None:
            value = self.session_id(provider, lowered)
            if value:
                return provider.name, value
        for header in self.generic_headers:
            value = lowered.get(header)
            if value:
                return "unknown", value
        if body is not None:
            for dotted in body_fields:
                value = extract_body_field(body, dotted)
                if value is not None:
                    return "unknown", value
        if query is not None:
            if provider is not None:
                for header in provider.session_headers + provider.gated_session_headers:
                    value = query.get(header)
                    if value:
                        return provider.name, value
            for header in self.generic_headers:
                value = query.get(header)
                if value:
                    return "unknown", value
        return "unknown", f"ua:{user_agent}|ip:{client_ip}"


def extract_body_field(body: bytes, dotted_path: str) -> str | None:
    """Walks a dotted path (e.g. "metadata.user_id") into a JSON body (S3).

    Returns the value as a string when it is a non-empty string (or a
    number), else None. Never raises on malformed input.
    """
    try:
        data: Any = json.loads(body)
    except (ValueError, TypeError):
        return None
    for part in dotted_path.split("."):
        if not isinstance(data, dict) or part not in data:
            return None
        data = data[part]
    if isinstance(data, str) and data:
        return data
    if isinstance(data, (int, float)) and not isinstance(data, bool):
        return str(data)
    return None


@dataclass(frozen=True)
class StatusReport:
    """One session's normalized status as reported by a source."""

    # busy | idle | retry ("waiting" is applied by the caller from the
    # pending-input poll, not reported in the status map).
    status: str
    # Source-specific detail (e.g. the attempt count for a retry).
    detail: str = ""


class StatusSource(ABC):
    """Pluggable per-client status API (D21).

    One source per configured white-glove client that has a status source.
    The proxy polls the client's own machine (``base_url(client_ip)`` —
    e.g. ``http://<client-ip>:4096`` for opencode) at the source's
    ``poll_interval``, and drives each session's state from the client's
    own reports instead of from the proxy's HTTP timing.

    Contract:

    - ``fetch_statuses`` returns the sessions the client reports as ACTIVE,
      normalized. A session missing from a fresh map IS the client's idle
      report (sources only list non-idle sessions).
    - A ``None`` return means the client is unreachable or answered with
      garbage: callers keep the session's last-known status for
      ``STATUS_UNREACHABLE_GRACE`` and then fall back to idle. A partial
      answer must never clear a session's state.
    - A session's context (e.g. opencode's working directory) is resolved
      once via ``fetch_session_info`` and cached by the source
      (``resolve_context``); the parent-id cache (``parent_of``) is what
      the parent-chain child rule walks.
    - ``last_poll[base]`` is stamped by the caller at poll START (before
      the fetches) so the interval is measured start-to-start.
    """

    def __init__(self, http_client: httpx.AsyncClient, poll_interval: float):
        self.http_client = http_client
        self.poll_interval = poll_interval
        # base (client machine) -> last poll start, stamped by the caller.
        self.last_poll: dict[str, float] = {}
        # (base, session-id) -> context (e.g. working directory); a session's
        # context never changes, so entries live until the session is gone.
        self._context: dict[tuple, str] = {}
        # (base, session-id) -> parent session id (None = not a child session).
        self._parent: dict[tuple, str | None] = {}

    def due(self, base: str, now: float) -> bool:
        return now - self.last_poll.get(base, 0.0) >= self.poll_interval

    def parent_of(self, base: str, session_id: str) -> str | None:
        """The cached parent id for a session (None = unknown or no parent)."""
        return self._parent.get((base, session_id))

    def forget(self, base: str, session_id: str) -> None:
        """Drops the session's context + parent caches."""
        self._context.pop((base, session_id), None)
        self._parent.pop((base, session_id), None)

    def forget_gone(self, base: str, live: set[str]) -> None:
        """Drops the caches of this base's sessions that have vanished."""
        for key in [k for k in self._context if k[0] == base and k[1] not in live]:
            self.forget(key[0], key[1])

    async def resolve_context(self, base: str, session_id: str) -> tuple[str, str | None] | None:
        """The session's (context, parent-id), cached after the first fetch.

        Returns None when the session is unknown or the client is
        unreachable (callers skip that session until the next poll).
        """
        key = (base, session_id)
        if key in self._context:
            return self._context[key], self._parent.get(key)
        info = await self.fetch_session_info(base, session_id)
        if info is None:
            return None
        context, parent_id = info
        self._context[key] = context
        self._parent[key] = parent_id
        return context, parent_id

    @abstractmethod
    def base_url(self, client_ip: str) -> str:
        """The status-API base for one client machine (its source IP)."""

    @abstractmethod
    async def fetch_session_info(self, base: str, session_id: str) -> tuple[str, str | None] | None:
        """Resolves a session's (context, parent-id); None = unknown/unreachable."""

    @abstractmethod
    async def fetch_statuses(self, base: str, context: str) -> dict[str, StatusReport] | None:
        """The client's active-session status map for one context; None = unreachable."""

    @abstractmethod
    async def fetch_pending(self, base: str, context: str) -> dict[str, str] | None:
        """{session-id: kind} for sessions blocked awaiting user input; None = unreachable."""


def resolve_shared_spot(
    base: str,
    session_id: str,
    source: StatusSource,
    sessions: dict,
    spots: dict,
    client_name: str,
    depth: int,
) -> tuple | None:
    """
    Sub-agent spot sharing: walks the cached parent-id chain of a session
    and returns the queue key of the spot it may run on - the spot of the
    closest tracked ancestor (an ancestor that made requests through the
    proxy). If that ancestor is itself a sub-agent, the spot it shares is
    returned instead. None means no sharing: the session needs a spot of
    its own. Recomputed on every poll; no network I/O (the cache is filled
    by the status source). `sessions`/`spots` are the owning endpoint
    queue's dicts (passed explicitly to avoid a circular import).
    """
    current = session_id
    seen = {current}
    for _ in range(depth):  # depth cap; real sub-agent chains are 1-2 levels
        parent = source.parent_of(base, current)
        if not parent or parent in seen:
            return None  # not a child, a cycle, or the chain is unresolved yet
        seen.add(parent)
        ancestor = sessions.get((client_name, parent))
        if ancestor is not None:
            if ancestor.key in spots:
                return ancestor.key
            if ancestor.shared_spot_key is not None and ancestor.shared_spot_key in spots:
                return ancestor.shared_spot_key
            return ancestor.key
        current = parent
    return None
