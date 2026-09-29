"""Endpoint runtime: readiness probing, per-endpoint queues, and routing.

One EndpointRuntime per configured endpoint (D2/D15/D16). Owns:

- readiness state + probe cadence (``readiness.interval`` normally, fast
  <=2s while its queue is non-empty — the queue manager owns that cadence),
- its ``EndpointQueue`` (slots, FIFO wait, per-session in-flight cap,
  atomic mode, spot expiry — all semantics in ``queue.py`` / ``parity.md``),
- ``wait_policy`` (wait/error) and ``queue_timeout`` enforcement,
- the per-endpoint queue manager (1s tick, Q13).

Readiness is the second of the two signals (D8): power state (is the box
on) comes from the server's PowerBackend; readiness (is the service
answering) comes from this endpoint's probe. ``ready`` implies the box is
on, so a successful probe updates the server's power state.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import TYPE_CHECKING, Any

import httpx
from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.responses import Response

from relay.clients import StatusPoller, detect_client, resolve_shared_spot, session_header_value
from relay.power.base import PowerState
from relay.queue import EndpointQueue, QueueEntry, UnknownTracker
from relay.transport.http import forward_request

if TYPE_CHECKING:
    from relay.models import EndpointConfig
    from relay.servers import ServerRuntime
    from relay.store import Store

logger = logging.getLogger("relay.endpoints")

__all__ = [
    "EndpointRuntime",
    "extract_body_field",
    "resolve_session_id",
    "route",
]


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


def resolve_session_id(
    request: Request,
    body: bytes,
    endpoint: EndpointRuntime,
    providers: list,
    generic_headers: list[str],
) -> tuple[str, str]:
    """Identifies the session for a request (S1, D22).

    Precedence:
      1. a white-glove client's own session headers (plain, then UA-gated),
      2. the configured generic session headers,
      3. the endpoint's body fields (e.g. OpenAI "user", Anthropic
         "metadata.user_id"),
      4. User-Agent + client source IP as a last resort.
    (WS query params join at step 4 with the Phase-1 WS transport.)
    """
    lowered = {k.lower(): v for k, v in request.headers.items()}
    client_ip = request.client.host if request.client else "unknown"
    user_agent = lowered.get("user-agent", "")

    provider = detect_client(lowered, providers)
    if provider is not None:
        value = session_header_value(provider, lowered)
        if value:
            return provider.name, value
    for header in generic_headers:
        value = lowered.get(header)
        if value:
            return "unknown", value
    for dotted in endpoint.config.session_id_body_fields:
        value = extract_body_field(body, dotted)
        if value is not None:
            return "unknown", value
    return "unknown", f"ua:{user_agent}|ip:{client_ip}"


def route(path: str, endpoints: dict[str, EndpointRuntime]) -> EndpointRuntime | None:
    """Longest path-prefix match across all endpoints (D15)."""
    if not path.startswith("/"):
        path = "/" + path
    best: EndpointRuntime | None = None
    best_len = -1
    for endpoint in endpoints.values():
        prefix = endpoint.config.path_prefix
        if path.startswith(prefix) and len(prefix) > best_len:
            best, best_len = endpoint, len(prefix)
    return best


class EndpointRuntime:
    """Runtime state + background loops for one configured endpoint."""

    def __init__(
        self,
        config: EndpointConfig,
        server: ServerRuntime,
        *,
        http_client: httpx.AsyncClient,
        store: Store,
        pollers: dict[str, StatusPoller],
        providers: list,
        generic_headers: list[str],
        unknown_tracker: UnknownTracker,
        read_timeout: float = 0,
    ):
        self.config = config
        self.server = server
        self.http_client = http_client
        self.store = store
        self.pollers = pollers
        self.providers = providers
        self.generic_headers = generic_headers
        self.unknown_tracker = unknown_tracker
        # Live proxy-to-target read timeout (X4); the app refreshes it on
        # every endpoint when the monitor changes it.
        self.read_timeout = read_timeout
        self.queue = EndpointQueue(
            max_spots=config.concurrency,
            max_inflight_per_session=config.session.per_session_requests,
            busy_window=config.session.busy_window,
            session_expiry=config.session.expiry,
            atomic_requests=config.session.request_mode == "atomic",
            immediate_idle_release=config.session.immediate_idle_release,
        )
        # Readiness bookkeeping (D8). ready_since/last_check_at are epochs
        # (the store's clock); the idle/queue logic never uses them.
        self.ready_since: float | None = None
        self.last_check_at: float | None = None
        # In-flight forwards that the queue does not track
        # (concurrent / passthrough routing).
        self.active_forwards = 0
        # Last queue-manager tick (monitor context; Q13).
        self.last_manager_tick: float | None = None
        self._readiness_task: asyncio.Task | None = None
        self._manager_task: asyncio.Task | None = None

    @property
    def name(self) -> str:
        return self.config.name

    def has_activity(self) -> bool:
        """An endpoint is idle when its queue is empty and nothing runs (D13)."""
        return self.queue.has_activity() or self.active_forwards > 0

    # -- readiness (the D8 "is the service answering" signal) ---------------

    async def check_readiness(self) -> bool:
        """Probes the readiness path and applies any transition."""
        rc = self.config.readiness
        url = self.server.config.service_url + rc.path
        try:
            if rc.method == "GET":
                response = await self.http_client.get(url, timeout=httpx.Timeout(rc.timeout))
            else:
                response = await self.http_client.request(
                    rc.method, url, timeout=httpx.Timeout(rc.timeout)
                )
            ok = response.status_code in rc.healthy_statuses
        except Exception:
            ok = False
        self.last_check_at = time.time()
        if ok != self.queue.ready:
            self.queue.ready = ok
            if ok:
                self.ready_since = time.time()
                logger.info("Endpoint %s is READY.", self.name)
                await self.server.note_service_ready()
            else:
                logger.info("Endpoint %s is NOT ready.", self.name)
            await self.store.set_endpoint_runtime(
                self.name,
                ready=ok,
                ready_since=self.ready_since,
                last_check_at=self.last_check_at,
            )
        return ok

    async def readiness_loop(self) -> None:
        """Per-endpoint readiness poll (architecture.md loop table).

        Normal cadence (``readiness.interval``); while the queue is
        non-empty the queue manager owns the fast <=2s cadence, so this
        loop idles until the queue drains. A transient error never kills
        the loop.
        """
        while True:
            if self.queue.queue:
                await asyncio.sleep(2.0)
                continue
            await asyncio.sleep(self.config.readiness.interval)
            try:
                await self.check_readiness()
            except Exception:
                logger.exception("Readiness probe failed for endpoint %s; continuing.", self.name)

    # -- queue manager (1s tick) ----------------------------------------------

    async def queue_manager(self) -> None:
        """Per-endpoint queue manager (1s tick, Q13).

        1. polls known clients' status APIs (one call per client machine),
        2. recomputes session statuses and surrenders expired spots,
        3. while requests wait: keeps the boot cycle alive (cooldown-
           deduped power-on) and promotes once ready.
        """
        last_check = 0.0
        while True:
            await asyncio.sleep(1.0)
            try:
                last_check = await self._queue_manager_tick(last_check)
                self.last_manager_tick = time.monotonic()
            except Exception:
                # A transient error must never kill the manager: without it,
                # spots are never surrendered and queued requests are never
                # promoted or woken.
                logger.exception(
                    "Queue manager tick failed for endpoint %s; continuing.", self.name
                )

    async def _queue_manager_tick(self, last_check: float) -> float:
        now = time.monotonic()

        # 1) Client status polling (grouped per client machine).
        bases: dict[tuple, list] = {}
        for session in self.queue.sessions.values():
            if session.client == "unknown":
                continue
            poller = self.pollers.get(session.client)
            if poller is None:
                continue
            base = poller.base_url(session.client_ip)
            bases.setdefault((poller, base), []).append(session)
        for (poller, base), sessions in bases.items():
            if not poller.due(base, now):
                continue
            # Stamp the interval at poll start, not completion: stamping after
            # the fetches makes the gap between polls tick period + sleep
            # overshoot - fetch duration, which dips below poll_interval and
            # silently skips every other tick (2s cadence instead of 1s).
            poller.last_poll[base] = time.monotonic()
            # The status map is per-directory (one opencode "instance" per
            # working directory), so first resolve each session's directory
            # (GET /session/{id}, cached) and then poll one map per
            # directory. Unreachable lookups are skipped; tick() keeps each
            # session's last-known status for the grace period.
            by_dir: dict[str, list] = {}
            for session in sessions:
                key = (base, session.session_id)
                directory = poller.session_dir.get(key)
                if directory is None:
                    info = await poller.fetch_session_info(base, session.session_id)
                    if info is None:
                        continue
                    directory, parent_id = info
                    poller.session_dir[key] = directory
                    poller.session_parent[key] = parent_id
                by_dir.setdefault(directory, []).append(session)
                # Sub-agent slot sharing (Q8): a session runs on the spot of
                # its tracked ancestor, recomputed every poll.
                if poller.provider.children_kind == "parent-chain":
                    session.shared_spot_key = resolve_shared_spot(
                        base,
                        session.session_id,
                        poller,
                        self.queue.sessions,
                        self.queue.spots,
                        session.client,
                        poller.provider.children_depth,
                    )
                else:
                    session.shared_spot_key = None
            for directory, dir_sessions in by_dir.items():
                data = await poller.fetch_statuses(base, directory)
                # Sessions blocked on a pending permission or question stay
                # "busy" in the status map, so the pending-request endpoints
                # are the tie-breaker (C3). A failed poll (None) keeps the
                # last-known state.
                pending = await poller.fetch_pending(base, directory)
                if data is not None or pending is not None:
                    reported = time.monotonic()
                    for session in dir_sessions:
                        reason = pending.get(session.session_id) if pending is not None else None
                        if reason is not None:
                            if session.client_status != "waiting":
                                logger.info(
                                    "Session %s (%s) waiting for user input: %s.",
                                    session.session_id,
                                    session.client,
                                    reason,
                                )
                            session.client_status = "waiting"
                            session.client_status_detail = reason
                            session.client_status_at = reported
                            continue
                        if data is None:
                            continue
                        st = data.get(session.session_id)
                        if not isinstance(st, dict):
                            # A fresh map that lacks this session IS the
                            # client's idle report (C2).
                            session.client_status = "idle"
                            session.client_status_detail = "absent from status map (idle)"
                            session.client_status_at = reported
                            continue
                        stype = st.get("type")
                        if stype not in ("busy", "idle", "retry"):
                            continue
                        session.client_status = stype
                        session.client_status_at = reported
                        session.client_status_detail = (
                            f"attempt {st.get('attempt', '?')}" if stype == "retry" else ""
                        )
            # Forget directory cache entries of sessions that are gone.
            live = {s.session_id for s in sessions}
            for key in [k for k in poller.session_dir if k[0] == base and k[1] not in live]:
                poller.forget(key[0], key[1])

        # 2) Status recompute + spot surrender.
        for key, reason in self.queue.tick(now):
            logger.info(
                "Session %s (%s): spot released, session removed (%s).", key[1], key[0], reason
            )

        # 3) While requests wait: keep the boot cycle alive and promote.
        if self.queue.queue:
            if now - last_check >= 2.0:
                await self.check_readiness()
                last_check = now
            # The first waiter triggered the initial power-on in the
            # admission path; this keeps the single boot cycle alive
            # (re-issuing the power-on on cooldown) while later requests
            # simply wait. An ON box is booting (model loading) — just wait
            # (D8).
            if not self.queue.ready and self.server.power_state in (
                PowerState.OFF,
                PowerState.UNKNOWN,
            ):
                await self.server.maybe_power_on()
            if self.queue.ready:
                # Ready may have just flipped (probe above or readiness
                # loop): entries already in the queue are only promoted by
                # _try_promote, and nothing else re-runs it for them.
                self.queue._try_promote()

        # 4) Prune passthrough/catch-all activity (the catch-all endpoint
        # owns the shared tracker).
        if self.config.catch_all:
            self.unknown_tracker.prune(now, self.config.session.expiry)

        return last_check

    # -- admission ---------------------------------------------------------

    async def _ensure_wake(self) -> None:
        """Triggers the power-on while the server is off/unknown (D8).

        Cooldown-deduped in the server runtime (P2).
        """
        if self.queue.ready:
            return
        if self.server.power_state in (PowerState.OFF, PowerState.UNKNOWN):
            await self.server.maybe_power_on()

    async def wait_until_ready(self, request: Request, timeout: float | None = None) -> str:
        """Holds a request until the endpoint is ready (wait policy, P1).

        Returns "ok" when ready, "disconnected" when the client hangs up,
        or "timed_out" when the hold deadline elapses. No 503 is ever
        returned while the machine boots.
        """
        deadline: float | None = None
        if timeout and timeout > 0:
            deadline = time.monotonic() + timeout
        while True:
            if self.queue.ready:
                return "ok"
            await self._ensure_wake()
            if await request.is_disconnected():
                return "disconnected"
            if deadline is not None and time.monotonic() >= deadline:
                return "timed_out"
            await asyncio.sleep(1.0)

    async def admit_catch_all(self, request: Request, full_path: str) -> Response:
        """Catch-all traffic (D17): passthrough semantics, no queueing and no
        session tracking, regardless of the endpoint's routing mode."""
        try:
            body = await request.body()
        except Exception as e:
            logger.error("Failed to read request body: %s", e)
            return JSONResponse(status_code=400, content={"error": "Failed to read request body."})
        return await self.admit_unqueued(request, full_path, body)

    async def admit(self, request: Request, full_path: str) -> Response:
        """Admits one routed request (architecture.md request flow, steps 2-6)."""
        try:
            body = await request.body()
        except Exception as e:
            logger.error("Failed to read request body: %s", e)
            return JSONResponse(status_code=400, content={"error": "Failed to read request body."})

        if self.config.routing == "queued":
            return await self._admit_queued(request, full_path, body)
        return await self.admit_unqueued(request, full_path, body)

    async def admit_unqueued(self, request: Request, full_path: str, body: bytes) -> Response:
        """Forwards without queueing or session tracking (D16, D17).

        Used for ``concurrent``/``passthrough`` routing and for catch-all
        traffic (unmatched paths routed to the catch_all endpoint with
        passthrough semantics). Readiness gate (D8) first; the held request
        is not in the queue, so it is counted separately for idle-off
        accounting.
        """
        if not self.queue.queue:
            await self.check_readiness()
        if not self.queue.ready:
            if self.config.wait_policy == "error":
                return self._not_ready()
            result = await self.wait_until_ready(request, self.config.queue_timeout or None)
            if result != "ok":
                return self._hold_rejected(result, full_path)

        self.active_forwards += 1

        def _finish(code: int | None) -> None:
            self.active_forwards = max(0, self.active_forwards - 1)

        return await self._forward(request, full_path, body, _finish)

    async def _admit_queued(self, request: Request, full_path: str, body: bytes) -> Response:
        # Readiness gate (D8): a fresh probe on an empty queue, always (P3 —
        # the manager's bookkeeping may be stale in BOTH directions: the
        # target may have just died while `ready` is still True), then the
        # wait policy. Under the wait policy the request joins the queue
        # while it waits, so it stays visible to the monitor and the manager
        # keeps the boot cycle alive (re-issuing the power-on on cooldown)
        # until it is served.
        if not self.queue.queue:
            await self.check_readiness()
        if not self.queue.ready:
            if self.config.wait_policy == "error":
                return self._not_ready()
            await self._ensure_wake()

        lowered = {k.lower(): v for k, v in request.headers.items()}
        client_ip = request.client.host if request.client else "unknown"
        client_name, session_id = resolve_session_id(
            request, body, self, self.providers, self.generic_headers
        )
        session = self.queue.get_or_create_session(
            client_name, session_id, self.name, client_ip, lowered.get("user-agent", "")
        )
        entry = QueueEntry(
            session=session,
            path=full_path,
            body=body,
            request=request,
            enqueued_at=time.monotonic(),
        )
        self.queue.enqueue(entry)
        logger.debug(
            "Session %s (%s/%s) queued at position %d.",
            session_id,
            client_name,
            self.name,
            len(self.queue.queue),
        )

        result = await self.queue.wait_for_slot(entry, self.config.queue_timeout or None)
        if result != "ok":
            if entry.done:
                # Lost a race: the entry was promoted in the same instant
                # the client went away (or the timeout fired). The slot is
                # ours, so release it rather than abandoning (Q11).
                self.queue.release(entry, None)
            else:
                self.queue.abandon(entry)
            if result == "disconnected":
                logger.info(
                    "Client %s hung up while %s was waiting in queue; dropping request.",
                    client_ip,
                    session_id,
                )
                return JSONResponse(
                    status_code=503,
                    content={"error": "Client disconnected while request was queued."},
                )
            return self._hold_rejected("timed_out", full_path)

        return await self._forward(
            request,
            full_path,
            body,
            lambda code: self.queue.release(entry, code),
        )

    def _not_ready(self) -> Response:
        return JSONResponse(
            status_code=503,
            content={
                "error": {
                    "message": f"Service for endpoint '{self.name}' is not ready; retry later.",
                    "type": "server_error",
                    "param": None,
                    "code": "service_not_ready",
                }
            },
        )

    def _hold_rejected(self, result: str, full_path: str) -> Response:
        """The 503/504 responses for a request the hold did not admit."""
        if result == "disconnected":
            return JSONResponse(
                status_code=503,
                content={"error": "Client disconnected while request was queued."},
            )
        timeout = self.config.queue_timeout
        logger.warning(
            "Request to %s was held in the queue for %.0fs and timed out; dropping it.",
            full_path,
            timeout,
        )
        return JSONResponse(
            status_code=504,
            content={
                "error": {
                    "message": f"Request was held in the queue for {timeout}s and timed out.",
                    "type": "server_error",
                    "param": None,
                    "code": "queue_timeout",
                }
            },
        )

    async def _forward(
        self,
        request: Request,
        full_path: str,
        body: bytes,
        release,
    ) -> Response:
        return await forward_request(
            self.http_client,
            self.server.config.service_url,
            request,
            full_path,
            body=body,
            read_timeout=self.read_timeout,
            release=release,
        )

    # -- lifecycle ---------------------------------------------------------

    def start(self) -> None:
        """Starts the per-endpoint background loops (called from the app lifespan)."""
        self._readiness_task = asyncio.create_task(
            self.readiness_loop(), name=f"readiness-{self.name}"
        )
        self._manager_task = asyncio.create_task(
            self.queue_manager(), name=f"queue-manager-{self.name}"
        )

    async def stop(self) -> None:
        for task in (self._readiness_task, self._manager_task):
            if task is not None:
                task.cancel()
        self._readiness_task = None
        self._manager_task = None
