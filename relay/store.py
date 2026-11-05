"""SQLite store (D5): WAL, Alembic migrations, reconciliation, retention.

Rule: config owns *what exists and the policy*; this store owns *what is
happening and what has happened*. The app never reads policy from the DB.

Schema: the models in `relay.db` are the source of truth. `open()` applies
pending Alembic migrations (`relay/migrations`) to file databases — a fresh
path is created at head, an existing one is upgraded; `:memory:` (tests) is
built straight from the same metadata.

Single async writer: all writes are serialized on one lock; WAL mode allows
concurrent readers during writes.

Degraded mode (D33): the store is a telemetry/audit mirror — routing never
depends on it. A database-level write failure (disk full, I/O error, locked,
corruption) therefore degrades the store instead of propagating: writes are
dropped (counted, never re-queued), the first failure logs one ERROR, and the
first success after a degradation logs the recovery. Proxying is unaffected.
"""

import asyncio
import logging
import sqlite3
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from alembic import command as alembic_command
from alembic.config import Config as AlembicConfig
from sqlalchemy import delete, select, tuple_, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from relay.db import (
    Base,
    EndpointRuntime,
    PowerEvent,
    RequestLog,
    ServerRuntime,
    SessionRecord,
    make_engine,
)

logger = logging.getLogger("relay.store")

__all__ = ["RequestRecord", "ServerRuntimeRow", "Store"]

_UNSET: Any = object()

#: Retry cadence while the database cannot be opened at boot (disk full).
OPEN_RETRY_INTERVAL = 10.0
#: Bounded staged-request buffer: over capacity sheds the OLDEST records
#: (drop, never re-queue — re-queueing would grow RAM without bound while
#: the disk is full). ~10k rows * ~150 B keeps worst-case memory ~1.5 MB.
REQUEST_BUFFER_MAX = 10_000


@dataclass
class RequestRecord:
    """One completed proxied request, staged for batched insert (D13)."""

    ts: float
    server: str
    endpoint: str
    client: str
    session_id: str
    wait_seconds: float
    active_seconds: float
    status_code: int | None
    request_bytes: int
    response_bytes: int


class ServerRuntimeRow:
    """A server_runtime row (coarse state restored at startup)."""

    def __init__(
        self,
        name: str,
        power_state: str | None,
        owned: bool,
        shutdown_override: bool | None,
        cycle_id: str | None,
        updated_at: float | None,
    ):
        self.name = name
        self.power_state = power_state
        self.owned = owned
        self.shutdown_override = shutdown_override
        self.cycle_id = cycle_id
        self.updated_at = updated_at


class Store:
    """The relay SQLite database (one per deployment; `:memory:` for tests)."""

    def __init__(self, path: str):
        self._path = path
        self._engine: AsyncEngine | None = None
        self._sessionmaker: async_sessionmaker[AsyncSession] | None = None
        self._write_lock = asyncio.Lock()
        # Staged request rows, flushed to disk in batches by request_flush_loop.
        self._request_buffer: deque[RequestRecord] = deque(maxlen=REQUEST_BUFFER_MAX)
        # Degraded mode (D33): True while database writes are failing.
        self._degraded = False
        self._dropped_writes = 0
        self._dropped_requests = 0

    @property
    def path(self) -> str:
        return self._path

    @property
    def degraded(self) -> bool:
        """True while database writes are failing (proxying is unaffected)."""
        return self._degraded

    @property
    def dropped_writes(self) -> int:
        """Total store writes dropped while degraded."""
        return self._dropped_writes

    @property
    def dropped_requests(self) -> int:
        """Total request records dropped (failed flush batches + buffer shed)."""
        return self._dropped_requests

    # -- degraded-mode helpers (D33) ------------------------------------------

    @staticmethod
    def _db_failure(exc: BaseException) -> bool:
        """True for database-level environmental failures (disk full, I/O
        error, locked, read-only file, corruption) that degrade the store
        instead of propagating. Code-level SQL bugs still surface.

        In Python's sqlite3 the environmental conditions are all subclasses
        of ``DatabaseError`` — except the bug-indicating classes
        (``ProgrammingError``, ``IntegrityError``, ``NotSupportedError``),
        which are excluded here.
        """
        orig = exc.orig if isinstance(exc, DBAPIError) else None
        if not isinstance(orig, sqlite3.DatabaseError):
            return False
        return not isinstance(
            orig, (sqlite3.ProgrammingError, sqlite3.IntegrityError, sqlite3.NotSupportedError)
        )

    def _note_write_failure(self, exc: DBAPIError) -> None:
        """Records a dropped write; logs the transition to degraded once."""
        self._dropped_writes += 1
        if not self._degraded:
            self._degraded = True
            logger.error(
                "Store degraded: database writes are failing (%s: %s). Proxying "
                "continues; record-keeping is dropped (not re-queued) until "
                "writes succeed again.",
                type(exc.orig).__name__,
                exc.orig,
            )

    def _note_write_success(self) -> None:
        """Marks recovery: the first successful write after a degradation."""
        if self._degraded:
            self._degraded = False
            logger.info(
                "Store recovered: writes are succeeding again (%d writes and %d "
                "request records were dropped while degraded).",
                self._dropped_writes,
                self._dropped_requests,
            )

    async def open(self) -> None:
        if self._path == ":memory:":
            engine = make_engine(self._path)
            # No Alembic here: an in-memory DB lives on one connection, which
            # the command API can't share — build the same metadata directly.
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
        else:
            await self._open_with_retry()
            engine = make_engine(self._path)
        self._engine = engine
        self._sessionmaker = async_sessionmaker(engine, expire_on_commit=False)

    async def _open_with_retry(self) -> None:
        """Applies migrations, retrying while the database cannot be written
        (D33). A full disk at boot must not crash-loop the proxy: startup
        simply waits, and the proxy serves as soon as the store is writable.
        """
        warned = False
        while True:
            try:
                await asyncio.to_thread(self._upgrade_schema)
                return
            except Exception as e:
                # A one-time ERROR is the operator's signal that startup is
                # blocked on a (full) disk; the store's degraded flag stays
                # untouched until a real write is attempted.
                if not warned:
                    warned = True
                    logger.error(
                        "Store cannot be opened (%s: %s); retrying every %.0fs. "
                        "Startup is delayed until the database is writable.",
                        type(e).__name__,
                        e,
                        OPEN_RETRY_INTERVAL,
                    )
                await asyncio.sleep(OPEN_RETRY_INTERVAL)

    def _upgrade_schema(self) -> None:
        """Applies pending Alembic migrations (no-op when already at head)."""
        cfg = AlembicConfig()
        cfg.set_main_option("script_location", str(Path(__file__).parent / "migrations"))
        cfg.set_main_option("sqlalchemy.url", f"sqlite:///{self._path}")
        alembic_command.upgrade(cfg, "head")

    async def close(self) -> None:
        if self._engine is not None:
            try:
                await self._engine.dispose()
            except Exception as e:
                # A full disk must not abort the shutdown (D33).
                logger.error("Store close failed: %s", e)
            self._engine = None
            self._sessionmaker = None

    def _sessions(self) -> async_sessionmaker[AsyncSession]:
        if self._sessionmaker is None:
            raise RuntimeError("store is not open")
        return self._sessionmaker

    def _engine_live(self) -> AsyncEngine:
        if self._engine is None:
            raise RuntimeError("store is not open")
        return self._engine

    # -- server_runtime ------------------------------------------------------

    async def reconcile_servers(self, names: list[str]) -> dict[str, ServerRuntimeRow]:
        """Startup reconciliation (storage.md).

        - row missing  -> insert (owned=0, shutdown_override=NULL,
          power_state=unknown); the normal power-state sync then applies, and
          adoption happens only on traffic, never on startup;
        - row exists   -> restore `owned` and `shutdown_override` — this is
          what makes restarts safe (a server the proxy brought up before a
          restart is still owned after it; a manually-on server stays
          unowned);
        - removed servers' rows are left in place (harmless history).
        """
        rows: dict[str, ServerRuntimeRow] = {}
        now = time.time()
        try:
            async with self._write_lock, self._sessions()() as session, session.begin():
                for name in names:
                    row = await session.get(ServerRuntime, name)
                    if row is None:
                        row = ServerRuntime(
                            name=name, power_state="unknown", owned=False, updated_at=now
                        )
                        session.add(row)
                        await session.flush()
                    rows[name] = ServerRuntimeRow(
                        name=name,
                        power_state=row.power_state,
                        owned=bool(row.owned),
                        shutdown_override=row.shutdown_override,
                        cycle_id=row.cycle_id,
                        updated_at=row.updated_at,
                    )
        except DBAPIError as e:
            if self._db_failure(e):
                # The store is the mirror, not the source: start UNOWNED (the
                # safe direction per D12 — the proxy never powers off a box it
                # did not bring up) and let adoption happen on traffic.
                self._note_write_failure(e)
                return {
                    name: ServerRuntimeRow(name, "unknown", False, None, None, None)
                    for name in names
                }
            raise
        self._note_write_success()
        return rows

    async def set_server_runtime(
        self,
        name: str,
        *,
        power_state: str | None = None,
        owned: bool | None = None,
        shutdown_override: Any = _UNSET,
        cycle_id: Any = _UNSET,
    ) -> None:
        """Persists part of a server's runtime state (reconcile first)."""
        values: dict[str, Any] = {}
        if power_state is not None:
            values["power_state"] = power_state
        if owned is not None:
            values["owned"] = owned
        if shutdown_override is not _UNSET:
            values["shutdown_override"] = shutdown_override
        if cycle_id is not _UNSET:
            values["cycle_id"] = cycle_id
        if not values:
            return
        values["updated_at"] = time.time()
        try:
            async with self._write_lock, self._engine_live().begin() as conn:
                result = await conn.execute(
                    update(ServerRuntime).where(ServerRuntime.name == name).values(**values)
                )
                if result.rowcount == 0:
                    logger.warning("set_server_runtime: no row for server %r", name)
        except DBAPIError as e:
            if self._db_failure(e):
                self._note_write_failure(e)
                return
            raise
        self._note_write_success()

    # -- endpoint_runtime ------------------------------------------------------

    async def set_endpoint_runtime(
        self,
        name: str,
        *,
        ready: bool,
        ready_since: float | None,
        last_check_at: float | None,
    ) -> None:
        stmt = sqlite_insert(EndpointRuntime).values(
            name=name, ready=ready, ready_since=ready_since, last_check_at=last_check_at
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=["name"],
            set_={
                "ready": stmt.excluded.ready,
                "ready_since": stmt.excluded.ready_since,
                "last_check_at": stmt.excluded.last_check_at,
            },
        )
        try:
            async with self._write_lock, self._engine_live().begin() as conn:
                await conn.execute(stmt)
        except DBAPIError as e:
            if self._db_failure(e):
                self._note_write_failure(e)
                return
            raise
        self._note_write_success()

    async def get_endpoint_runtime(self) -> dict[str, tuple[bool, float | None]]:
        async with self._sessions()() as session:
            result = await session.execute(
                select(EndpointRuntime.name, EndpointRuntime.ready, EndpointRuntime.ready_since)
            )
            rows = result.all()
        return {name: (bool(ready), ready_since) for name, ready, ready_since in rows}

    # -- power_events ----------------------------------------------------------

    async def log_power_event(
        self, server: str, action: str, reason: str, initiated_by: str, success: bool
    ) -> None:
        try:
            async with self._write_lock, self._sessions()() as session, session.begin():
                session.add(
                    PowerEvent(
                        ts=time.time(),
                        server=server,
                        action=action,
                        reason=reason,
                        initiated_by=initiated_by,
                        success=success,
                    )
                )
        except DBAPIError as e:
            if self._db_failure(e):
                self._note_write_failure(e)
                return
            raise
        self._note_write_success()

    async def recent_power_events(self, limit: int = 50) -> list[dict]:
        async with self._sessions()() as session:
            result = await session.execute(
                select(PowerEvent).order_by(PowerEvent.id.desc()).limit(limit)
            )
            events = result.scalars().all()
        return [
            {
                "ts": e.ts,
                "server": e.server,
                "action": e.action,
                "reason": e.reason,
                "initiated_by": e.initiated_by,
                "success": bool(e.success),
            }
            for e in reversed(events)
        ]

    # -- requests / sessions (the store flusher, Phase 1 task 7) ---------------

    async def enqueue_request(self, record: RequestRecord) -> None:
        """Stages one completed request for batched insert (single-loop safe).

        Over capacity the OLDEST record is shed and counted (D33): the buffer
        stays bounded even if the flusher is down for an extended period."""
        if len(self._request_buffer) >= REQUEST_BUFFER_MAX:
            self._dropped_requests += 1
        self._request_buffer.append(record)

    async def flush_requests(self) -> int:
        """Writes the staged request buffer in one transaction; returns the count.

        A failed flush drops the batch (counted in ``bulk_log_requests``); it
        is never re-queued — that would grow RAM without bound while the disk
        is full."""
        records, self._request_buffer = list(self._request_buffer), deque(
            maxlen=REQUEST_BUFFER_MAX
        )
        if not records:
            return 0
        return await self.bulk_log_requests(records)

    async def bulk_log_requests(self, records: list[RequestRecord]) -> int:
        """Inserts a batch of request rows in a single transaction."""
        if not records:
            return 0
        try:
            async with self._write_lock, self._sessions()() as session, session.begin():
                session.add_all(
                    RequestLog(
                        ts=r.ts,
                        server=r.server,
                        endpoint=r.endpoint,
                        client=r.client,
                        session_id=r.session_id,
                        wait_seconds=r.wait_seconds,
                        active_seconds=r.active_seconds,
                        status_code=r.status_code,
                        request_bytes=r.request_bytes,
                        response_bytes=r.response_bytes,
                    )
                    for r in records
                )
        except DBAPIError as e:
            if self._db_failure(e):
                self._note_write_failure(e)
                self._dropped_requests += len(records)
                return 0
            raise
        self._note_write_success()
        return len(records)

    async def upsert_session(self, client: str, session_id: str, endpoint: str) -> None:
        """Touches the session-history row for a tracked session (create/update)."""
        now = time.time()
        stmt = sqlite_insert(SessionRecord).values(
            client=client,
            session_id=session_id,
            endpoint=endpoint,
            first_seen=now,
            last_seen=now,
            request_count=1,
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=["client", "session_id", "endpoint"],
            set_={"last_seen": now, "request_count": SessionRecord.request_count + 1},
        )
        try:
            async with self._write_lock, self._engine_live().begin() as conn:
                await conn.execute(stmt)
        except DBAPIError as e:
            if self._db_failure(e):
                self._note_write_failure(e)
                return
            raise
        self._note_write_success()

    async def request_flush_loop(self, interval: float = 1.0) -> None:
        """Flushes staged request rows on a fixed cadence (the store flusher).

        A transient error must never kill the loop.
        """
        while True:
            await asyncio.sleep(interval)
            try:
                await self.flush_requests()
            except Exception:
                logger.exception("Request flush failed; continuing.")

    async def get_requests(self, limit: int = 100) -> list[dict]:
        """Most recent request rows (monitor / tests)."""
        async with self._sessions()() as session:
            result = await session.execute(
                select(RequestLog).order_by(RequestLog.id.desc()).limit(limit)
            )
            rows = result.scalars().all()
        return [
            {
                "ts": r.ts,
                "server": r.server,
                "endpoint": r.endpoint,
                "client": r.client,
                "session_id": r.session_id,
                "wait_seconds": r.wait_seconds,
                "active_seconds": r.active_seconds,
                "status_code": r.status_code,
                "request_bytes": r.request_bytes,
                "response_bytes": r.response_bytes,
            }
            for r in reversed(rows)
        ]

    # -- retention -------------------------------------------------------------

    async def prune(self, retention_days: int) -> dict[str, int]:
        """Deletes history rows older than `retention_days` (batched).

        `server_runtime` / `endpoint_runtime` are never pruned (one row per
        entity). Returns per-table delete counts.
        """
        cutoff = time.time() - retention_days * 86400
        batch = 10_000
        deleted: dict[str, int] = {}
        try:
            async with self._write_lock, self._engine_live().begin() as conn:
                for model in (RequestLog, PowerEvent):
                    total = 0
                    while True:
                        stale_ids = select(model.id).where(model.ts < cutoff).limit(batch)
                        result = await conn.execute(delete(model).where(model.id.in_(stale_ids)))
                        total += result.rowcount
                        if result.rowcount < batch:
                            break
                    deleted[model.__tablename__] = total
                total = 0
                while True:
                    stale_keys = (
                        select(
                            SessionRecord.client,
                            SessionRecord.session_id,
                            SessionRecord.endpoint,
                        )
                        .where(SessionRecord.last_seen < cutoff)
                        .limit(batch)
                    )
                    result = await conn.execute(
                        delete(SessionRecord).where(
                            tuple_(
                                SessionRecord.client,
                                SessionRecord.session_id,
                                SessionRecord.endpoint,
                            ).in_(stale_keys)
                        )
                    )
                    total += result.rowcount
                    if result.rowcount < batch:
                        break
                deleted["sessions"] = total
        except DBAPIError as e:
            if self._db_failure(e):
                # Return {} (not a partial count) so the retention loop's
                # "deleted ..." log line is never misleading.
                self._note_write_failure(e)
                return {}
            raise
        self._note_write_success()
        return deleted

    async def retention_loop(self, retention_days: int) -> None:
        """Hourly retention prune (the store flusher loop, Phase 0 scope).

        A transient error must never kill the loop.
        """
        while True:
            await asyncio.sleep(3600)
            try:
                deleted = await self.prune(retention_days)
                if any(deleted.values()):
                    logger.info("Retention prune deleted %s", deleted)
            except Exception:
                logger.exception("Retention prune failed; continuing.")
