"""
Session queue with FIFO ordering and spot-based concurrency.

Concurrency is modelled with "spots": a session acquires one of
CONCURRENT_SESSIONS spots when its first request is promoted, and keeps it
until it is genuinely done with the model. "Genuinely done" depends on the
client:

- known clients (e.g. OpenCode) report their real session state (busy /
  idle / retry) over their own API, so a session that finished its HTTP
  response but is still inside a long tool call keeps its spot and protects
  its K/V cache;
- unknown clients are assumed busy for CLIENT_BUSY_WINDOW seconds after
  their last request.

A spot is surrendered SESSION_EXPIRY seconds after the session went idle
(unknown clients: SESSION_EXPIRY after their last request, whichever is
longer than the busy window). When a spot is surrendered the session is
forgotten, so any future request with the same id is queued at the back as
a new session.

Within a session, CONCURRENT_SESSION_REQUESTS bounds in-flight requests
(-1 unlimited, 0 strictly serialized). The waiting queue itself is a plain
global FIFO: an entry is promoted when its session may run (spot available
or already held, per-session cap not hit).

Request mode (atomic_requests):
- parallel (default): spot-holding sessions run their in-flight requests
  concurrently;
- atomic: at most one in-flight request globally at any moment. Spots still
  overlap (each session keeps its K/V cache warm), but requests alternate:
  a waiting entry is only promoted while no request anywhere is in flight.
"""

import asyncio
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

from clients import STATUS_UNREACHABLE_GRACE

__all__ = ["Session", "QueueEntry", "SessionQueue", "UnknownTracker"]


@dataclass
class Session:
    key: tuple  # (client, session_id)
    client: str  # known client name ("opencode") or "unknown"
    session_id: str
    api: str  # "openai" / "anthropic"
    client_ip: str
    user_agent: str
    created_at: float
    last_request_at: float
    last_path: str = ""
    status: str = "busy"
    status_detail: str = ""
    inflight: int = 0
    waiting: int = 0
    spot_held: bool = False
    spot_acquired_at: float = 0.0
    idle_since: Optional[float] = None
    # Last state reported by the client's status API (known clients only).
    client_status: Optional[str] = None
    client_status_detail: str = ""
    client_status_at: float = 0.0


@dataclass
class QueueEntry:
    session: Session
    path: str  # full path, leading slash included
    body: bytes
    # The Starlette Request, kept so a held request can notice its client
    # hanging up while it waits its turn (method/headers stay readable on
    # it for the whole handler lifetime).
    request: object
    enqueued_at: float
    go: asyncio.Event = field(default_factory=asyncio.Event)
    done: bool = False  # True once promoted (left the waiting queue)


class SessionQueue:
    def __init__(
        self,
        max_spots: int,
        max_inflight_per_session: int,
        busy_window: float,
        session_expiry: float,
        atomic_requests: bool = False,
    ):
        self.max_spots = max(1, max_spots)
        # -1 = unlimited per-session concurrency, 0 = serialized.
        self.max_inflight_per_session = max_inflight_per_session
        self.busy_window = busy_window
        self.session_expiry = session_expiry
        # atomic: admit at most one in-flight request globally at a time.
        self.atomic_requests = atomic_requests
        self.inflight_total = 0
        self.sessions: dict[tuple, Session] = {}
        # spot-holding sessions, keyed by session key; dict order is the
        # order in which spots were acquired (used for monitor ordering).
        self.spots: dict[tuple, float] = {}
        self.queue: deque[QueueEntry] = deque()
        # Whether the target server is reachable. Promotion is only allowed
        # while healthy so requests wait in the queue (instead of failing)
        # while the proxy wakes the machine up.
        self.healthy: bool = False

    # -- session handling -------------------------------------------------

    def get_or_create_session(
        self, client: str, session_id: str, api: str, client_ip: str, user_agent: str
    ) -> Session:
        key = (client, session_id)
        now = time.monotonic()
        session = self.sessions.get(key)
        if session is None:
            session = Session(
                key=key,
                client=client,
                session_id=session_id,
                api=api,
                client_ip=client_ip,
                user_agent=user_agent,
                created_at=now,
                last_request_at=now,
            )
            self.sessions[key] = session
        else:
            # A new request revives a session that was in its idle
            # countdown: the spot (if held) is kept and the countdown
            # restarts from scratch.
            session.api = api
            session.client_ip = client_ip
            session.last_request_at = now
            session.idle_since = None
        return session

    # -- queue handling ----------------------------------------------------

    def enqueue(self, entry: QueueEntry) -> None:
        session = entry.session
        session.waiting += 1
        session.last_path = entry.path
        self.queue.append(entry)
        self._try_promote()

    def _eligible(self, entry: QueueEntry) -> bool:
        # atomic mode: nothing runs while any request anywhere is in flight.
        if self.atomic_requests and self.inflight_total > 0:
            return False
        session = entry.session
        if session.key in self.spots:
            cap = self.max_inflight_per_session
            if cap < 0:
                return True
            return session.inflight < cap
        return len(self.spots) < self.max_spots

    def _try_promote(self) -> None:
        """
        Walks the FIFO queue front-to-back and promotes the first entries
        that may run. Entries whose session is at its per-session cap are
        skipped and keep their position (FIFO is preserved among eligible
        entries).
        """
        if not self.healthy:
            return
        now = time.monotonic()
        while True:
            target = None
            for entry in self.queue:
                if self._eligible(entry):
                    target = entry
                    break
            if target is None:
                return
            self.queue.remove(target)
            target.done = True
            session = target.session
            session.waiting -= 1
            session.inflight += 1
            self.inflight_total += 1
            session.idle_since = None
            if session.key not in self.spots:
                self.spots[session.key] = now
                session.spot_held = True
                session.spot_acquired_at = now
            session.status = "busy"
            session.status_detail = "in-flight request"
            target.go.set()

    def _remove_waiting(self, entry: QueueEntry) -> None:
        if entry.done:
            return
        try:
            self.queue.remove(entry)
        except ValueError:
            pass
        entry.session.waiting = max(0, entry.session.waiting - 1)

    def abandon(self, entry: QueueEntry) -> None:
        """Drops a waiting entry (client disconnected or queue timeout)."""
        self._remove_waiting(entry)

    def release_session(self, client: str, session_id: str) -> bool:
        """
        Manually surrenders a session's spot right now (same effect as
        SESSION_EXPIRY elapsing): the spot is freed for the next waiting
        session and the session is forgotten, so future requests with the
        same id queue at the back as a new session. Returns True when a
        spot was actually released.
        """
        key = (client, session_id)
        session = self.sessions.get(key)
        if session is None or not session.spot_held:
            return False
        self._release_spot(session, key)
        return True

    def release(self, entry: QueueEntry, status_code: Optional[int]) -> None:
        """
        Called exactly once when an in-flight request finishes (the
        status_code is kept for future use; status is otherwise driven by
        the client's reported state or the busy window).
        """
        session = entry.session
        if session.inflight > 0:
            session.inflight -= 1
            self.inflight_total = max(0, self.inflight_total - 1)
        session.last_request_at = time.monotonic()
        self._try_promote()

    async def wait_for_slot(self, entry: QueueEntry, timeout: Optional[float]) -> str:
        """
        Blocks until the entry is promoted ("ok"), its client goes away
        ("disconnected"), or the queue hold timeout elapses ("timed_out").
        Clients are expected to run with no timeout, so the default hold is
        unbounded.

        Promotion is a separate coroutine, so it can win a race against a
        disconnect/timeout: if the result is not "ok" but entry.done is
        True, the entry was already promoted and the caller must release it
        instead of abandoning it.
        """
        deadline = None
        if timeout and timeout > 0:
            deadline = entry.enqueued_at + timeout
        while True:
            if entry.go.is_set():
                return "ok"
            if await entry.request.is_disconnected():
                return "disconnected"
            if deadline is not None and time.monotonic() >= deadline:
                return "timed_out"
            await asyncio.sleep(1.0)

    # -- status / spot lifecycle -------------------------------------------

    def _current_status(self, session: Session, now: float) -> tuple:
        if session.inflight > 0:
            return "busy", "in-flight request"
        if session.client != "unknown":
            if session.client_status and now - session.client_status_at < STATUS_UNREACHABLE_GRACE:
                return session.client_status, session.client_status_detail
            return "idle", ""
        if now - session.last_request_at < self.busy_window:
            return "busy", ""
        return "idle", ""

    def _release_deadline(self, session: Session) -> Optional[float]:
        """Monotonic deadline at which the session's spot is surrendered."""
        if session.inflight > 0:
            return None
        if session.client != "unknown":
            if session.status != "idle" or session.idle_since is None:
                return None
            return session.idle_since + self.session_expiry
        # Unknown clients: busy window and expiry are both measured from
        # the last request; whichever is longer wins.
        return session.last_request_at + max(self.busy_window, self.session_expiry)

    def _release_spot(self, session: Session, key: tuple) -> None:
        self.spots.pop(key, None)
        session.spot_held = False
        session.idle_since = None
        # A session is only forgotten once it has no requests left at all;
        # otherwise it lingers (spotless) until its waiting entries are
        # promoted or abandoned, keeping a single Session object per key.
        if session.waiting == 0 and session.inflight == 0:
            self.sessions.pop(key, None)
        self._try_promote()

    def tick(self, now: float) -> list:
        """
        Recomputes statuses, surrenders expired spots, and drops dead
        session records. Returns the session keys released. Called ~1/s by
        the queue manager after client-status polling.
        """
        released = []
        for key, session in list(self.sessions.items()):
            status, detail = self._current_status(session, now)
            session.status = status
            session.status_detail = detail
            if status == "idle":
                if session.idle_since is None:
                    session.idle_since = now
            else:
                session.idle_since = None
            if session.spot_held:
                deadline = self._release_deadline(session)
                if deadline is not None and now >= deadline:
                    self._release_spot(session, key)
                    released.append(key)
            elif session.waiting == 0 and session.inflight == 0:
                # No spot, nothing waiting: the session is gone (e.g. its
                # only queued request was abandoned).
                self.sessions.pop(key, None)
        return released

    def has_activity(self) -> bool:
        """True while any request is queued or any spot is held."""
        return bool(self.queue) or bool(self.spots)

    # -- monitoring ----------------------------------------------------------

    def snapshot(self, now: float) -> list:
        """
        Sessions ordered by queue position: spot holders first (in spot
        acquisition order), then sessions with waiting requests in FIFO
        order of their earliest waiting entry.
        """
        first_pos: dict = {}
        for i, entry in enumerate(self.queue, start=1):
            first_pos.setdefault(entry.session.key, i)

        ordered: list = []
        for key in list(self.spots.keys()):
            session = self.sessions.get(key)
            if session is not None:
                ordered.append(session)
        for key in first_pos:
            session = self.sessions.get(key)
            if session is not None and all(s is not session for s in ordered):
                ordered.append(session)

        rows = []
        for idx, session in enumerate(ordered, start=1):
            releases_in = None
            if session.spot_held:
                deadline = self._release_deadline(session)
                if deadline is not None:
                    releases_in = round(max(0.0, deadline - now), 1)
            rows.append(
                {
                    "position": idx,
                    "session": session.session_id,
                    "client": session.client,
                    "api": session.api,
                    "status": session.status,
                    "detail": session.status_detail,
                    "spot": "held" if session.spot_held else "none",
                    "spot_releases_in": releases_in,
                    "inflight": session.inflight,
                    "waiting": session.waiting,
                    "queue_position": first_pos.get(session.key),
                    "last_path": session.last_path,
                    "client_ip": session.client_ip,
                    "active_seconds": round(now - session.created_at, 1),
                }
            )
        return rows


class UnknownTracker:
    """
    Tracks requests that do not match any known API and are allowed through
    unqueued. Keyed by client (source IP + user agent); shown in the
    monitor's second list with the URL they are targeting.
    """

    def __init__(self):
        self.entries: dict[tuple, dict] = {}

    def record(
        self, ip: str, user_agent: str, method: str, path: str, target_url: str
    ) -> None:
        key = (ip, user_agent or "-")
        now = time.monotonic()
        entry = self.entries.get(key)
        if entry is None:
            self.entries[key] = {
                "ip": ip,
                "ua": user_agent or "-",
                "method": method,
                "path": path,
                "target_url": target_url,
                "first_at": now,
                "last_at": now,
                "count": 1,
            }
        else:
            entry["method"] = method
            entry["path"] = path
            entry["target_url"] = target_url
            entry["last_at"] = now
            entry["count"] += 1

    def prune(self, now: float, expiry: float) -> None:
        for key in [
            k
            for k, e in self.entries.items()
            if now - e["last_at"] >= expiry
        ]:
            del self.entries[key]

    def snapshot(self, now: float) -> list:
        rows = [
            {
                "id": f"{e['ip']} ({e['ua']})",
                "method": e["method"],
                "path": e["path"],
                "target_url": e["target_url"],
                "requests": e["count"],
                "last_activity_seconds_ago": round(now - e["last_at"], 1),
            }
            for e in self.entries.values()
        ]
        rows.sort(key=lambda r: r["last_activity_seconds_ago"])
        return rows
