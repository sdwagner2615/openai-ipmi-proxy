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
import contextlib
import logging
import time
from typing import TYPE_CHECKING

import httpx
from fastapi import Request, WebSocket
from fastapi.responses import JSONResponse
from starlette.responses import Response

from relay.clients import ClientRegistry, StatusSource, resolve_shared_spot
from relay.power.base import PowerState
from relay.queue import EndpointQueue, QueueEntry, UnknownTracker
from relay.store import RequestRecord
from relay.transport.http import forward_request
from relay.transport.ws import read_client, to_ws_url, tunnel

if TYPE_CHECKING:
    from relay.models import EndpointConfig
    from relay.servers import ServerRuntime
    from relay.store import Store

logger = logging.getLogger("relay.endpoints")

__all__ = [
    "EndpointRuntime",
    "route",
]


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
        sources: dict[str, StatusSource],
        registry: ClientRegistry,
        unknown_tracker: UnknownTracker,
        read_timeout: float = 0,
    ):
        self.config = config
        self.server = server
        self.http_client = http_client
        self.store = store
        self.sources = sources
        self.registry = registry
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
            source = self.sources.get(session.client)
            if source is None:
                # "unknown" and kind:none clients have no status source.
                continue
            base = source.base_url(session.client_ip)
            bases.setdefault((source, base), []).append(session)
        for (source, base), sessions in bases.items():
            if not source.due(base, now):
                continue
            # Stamp the interval at poll start, not completion: stamping after
            # the fetches makes the gap between polls tick period + sleep
            # overshoot - fetch duration, which dips below poll_interval and
            # silently skips every other tick (2s cadence instead of 1s).
            source.last_poll[base] = time.monotonic()
            # A session's context (e.g. opencode's working directory) is
            # resolved once (cached in the source), then one status map is
            # polled per context. Unreachable lookups are skipped; tick()
            # keeps each session's last-known status for the grace period.
            by_context: dict[str, list] = {}
            for session in sessions:
                resolved = await source.resolve_context(base, session.session_id)
                if resolved is None:
                    continue
                context, _parent_id = resolved
                by_context.setdefault(context, []).append(session)
                # Sub-agent slot sharing (Q8): a session runs on the spot of
                # its tracked ancestor, recomputed every poll.
                provider = self.registry.by_name.get(session.client)
                if provider is not None and provider.children_kind == "parent-chain":
                    session.shared_spot_key = resolve_shared_spot(
                        base,
                        session.session_id,
                        source,
                        self.queue.sessions,
                        self.queue.spots,
                        session.client,
                        provider.children_depth,
                    )
                else:
                    session.shared_spot_key = None
            for context, ctx_sessions in by_context.items():
                statuses = await source.fetch_statuses(base, context)
                # Sessions blocked on a pending user-input request stay
                # "busy" in the status map, so the pending endpoints are the
                # tie-breaker (C3). A failed poll (None) keeps the
                # last-known state.
                pending = await source.fetch_pending(base, context)
                if statuses is None and pending is None:
                    continue  # unreachable: last-known status is kept
                reported = time.monotonic()
                for session in ctx_sessions:
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
                    if statuses is None:
                        continue
                    report = statuses.get(session.session_id)
                    if report is None:
                        # A fresh map that lacks this session IS the
                        # client's idle report (C2).
                        session.client_status = "idle"
                        session.client_status_detail = "absent from status map (idle)"
                        session.client_status_at = reported
                        continue
                    session.client_status = report.status
                    session.client_status_detail = report.detail
                    session.client_status_at = reported
            # Forget context/parent caches of sessions that are gone.
            source.forget_gone(base, {s.session_id for s in sessions})

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

        lowered = {k.lower(): v for k, v in request.headers.items()}
        client_ip = request.client.host if request.client else "unknown"
        client_name, session_id = self.registry.resolve(
            lowered, client_ip, body=body, body_fields=tuple(self.config.session_id_body_fields)
        )
        client = self._client_display(client_name, client_ip, lowered.get("user-agent", ""))
        return await self._forward(
            request, full_path, body, _finish, self._make_record(client, session_id, 0.0)
        )

    # -- WebSocket admission (D18) -------------------------------------------

    async def admit_ws(
        self, websocket: WebSocket, full_path: str, *, passthrough: bool = False
    ) -> None:
        """Admits a routed WebSocket connection (D18).

        Accepts the upgrade, enforces the wait policy (hold with keep-alives
        until ready, or close 1013 on ``error``), holds a spot for the
        connection's lifetime when the endpoint is ``queued`` (unless
        ``passthrough``), then tunnels. The inbound connection is always
        closed exactly once on exit.
        """
        lowered = {k.lower(): v for k, v in websocket.headers.items()}
        client_ip = websocket.client.host if websocket.client else "unknown"
        client_name, session_id = self.registry.resolve(
            lowered, client_ip, query=websocket.query_params
        )
        await websocket.accept()
        client_q: asyncio.Queue = asyncio.Queue()
        disconnected = asyncio.Event()
        reader = asyncio.create_task(
            read_client(websocket, client_q, disconnected), name=f"ws-read-{self.name}"
        )
        target_url = to_ws_url(self.server.config.service_url, full_path)
        try:
            if not self.queue.queue:
                await self.check_readiness()
            if not self.queue.ready:
                if self.config.wait_policy == "error":
                    await websocket.close(code=1013, reason="not ready; retry later")
                    return
                result = await self._wait_ws_ready(
                    websocket, disconnected, self.config.queue_timeout or None
                )
                if result != "ok":
                    await self._close_ws(websocket)
                    return
            if self.config.routing == "queued" and not passthrough:
                session = self.queue.get_or_create_session(
                    client_name,
                    session_id,
                    self.name,
                    client_ip,
                    lowered.get("user-agent", ""),
                    known=client_name in self.sources,
                )
                held = await self._hold_ws_spot(
                    websocket, session, disconnected, self.config.queue_timeout or None
                )
                if not held:
                    await self._close_ws(websocket)
                    return
                await tunnel(
                    websocket,
                    target_url,
                    client_q,
                    disconnected,
                    release=lambda: self._release_ws_spot(session),
                )
            else:
                self.active_forwards += 1
                try:
                    await tunnel(websocket, target_url, client_q, disconnected)
                finally:
                    self.active_forwards = max(0, self.active_forwards - 1)
        finally:
            reader.cancel()
            await self._close_ws(websocket)

    async def _wait_ws_ready(
        self, websocket: WebSocket, disconnected: asyncio.Event, timeout: float | None
    ) -> str:
        """Holds an accepted WS until the endpoint is ready (wait policy, P1)."""
        deadline: float | None = None
        if timeout and timeout > 0:
            deadline = time.monotonic() + timeout
        while not self.queue.ready:
            if disconnected.is_set():
                return "disconnected"
            if deadline is not None and time.monotonic() >= deadline:
                return "timed_out"
            await self._ws_keep_alive(websocket)
            await asyncio.sleep(1.0)
        return "ok"

    async def _hold_ws_spot(
        self,
        websocket: WebSocket,
        session,
        disconnected: asyncio.Event,
        timeout: float | None,
    ) -> bool:
        """Acquires a spot for a queued WS connection (held for its lifetime).

        Runs on the session's own or shared spot when available, otherwise
        takes a free one; when none is free it keeps the connection alive and
        waits. True when a spot is held, False on disconnect/timeout.
        """
        deadline: float | None = None
        if timeout and timeout > 0:
            deadline = time.monotonic() + timeout
        while True:
            if session.key in self.queue.spots:
                session.inflight += 1
                return True
            if session.shared_spot_key is not None and session.shared_spot_key in self.queue.spots:
                session.inflight += 1
                return True
            if len(self.queue.spots) < self.queue.max_spots:
                now = time.monotonic()
                self.queue.spots[session.key] = now
                session.spot_held = True
                session.spot_acquired_at = now
                session.inflight += 1
                return True
            if deadline is not None and time.monotonic() >= deadline:
                return False
            if disconnected.is_set():
                return False
            await self._ws_keep_alive(websocket)
            await asyncio.sleep(1.0)

    def _release_ws_spot(self, session) -> None:
        """Releases a queued WS connection's spot (once, on close)."""
        if session.inflight > 0:
            session.inflight -= 1
        if session.key in self.queue.spots:
            self.queue.spots.pop(session.key)
            session.spot_held = False
            self.queue._try_promote()

    async def _ws_keep_alive(self, websocket: WebSocket) -> None:
        """Best-effort WS ping keep-alive (ASGI websocket.ping) while waiting.

        A missing/unsupported ping must not break the hold: the accepted
        connection stays open in the wait loop regardless.
        """
        with contextlib.suppress(Exception):
            await websocket._send({"type": "websocket.ping", "bytes": b"\x00" * 16})

    async def _close_ws(self, websocket: WebSocket) -> None:
        """Closes the inbound connection once (idempotent, error-suppressed)."""
        with contextlib.suppress(Exception):
            await websocket.close()

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
        client_name, session_id = self.registry.resolve(
            lowered, client_ip, body=body, body_fields=tuple(self.config.session_id_body_fields)
        )
        session = self.queue.get_or_create_session(
            client_name,
            session_id,
            self.name,
            client_ip,
            lowered.get("user-agent", ""),
            known=client_name in self.sources,
        )
        client = self._client_display(client_name, client_ip, lowered.get("user-agent", ""))
        await self.store.upsert_session(client_name, session_id, self.name)
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

        wait_seconds = time.monotonic() - entry.enqueued_at
        return await self._forward(
            request,
            full_path,
            body,
            lambda code: self.queue.release(entry, code),
            self._make_record(client, session_id, wait_seconds),
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

    @staticmethod
    def _client_display(client_name: str, client_ip: str, user_agent: str) -> str:
        """The client column of the request log (schema: white-glove name or ip/ua)."""
        if client_name != "unknown":
            return client_name
        return f"ip:{client_ip} ({user_agent})"

    def _make_record(self, client: str, session_id: str, wait_seconds: float):
        """Builds the per-request record callback that stages a RequestRecord."""
        server = self.server.name
        endpoint = self.name
        store = self.store

        async def _record(status_code, request_bytes, response_bytes, active_seconds):
            await store.enqueue_request(
                RequestRecord(
                    ts=time.time(),
                    server=server,
                    endpoint=endpoint,
                    client=client,
                    session_id=session_id,
                    wait_seconds=wait_seconds,
                    active_seconds=active_seconds,
                    status_code=status_code,
                    request_bytes=request_bytes,
                    response_bytes=response_bytes,
                )
            )

        return _record

    async def _forward(
        self,
        request: Request,
        full_path: str,
        body: bytes,
        release,
        record=None,
    ) -> Response:
        return await forward_request(
            self.http_client,
            self.server.config.service_url,
            request,
            full_path,
            body=body,
            read_timeout=self.read_timeout,
            release=release,
            record=record,
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
