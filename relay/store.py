"""SQLite store (D5): schema, WAL, startup reconciliation, retention.

Rule: config owns *what exists and the policy*; this store owns *what is
happening and what has happened*. The app never reads policy from the DB.

Single async writer: all writes go through one connection on the app's
asyncio loop (WAL mode allows concurrent readers during writes).
"""

import asyncio
import logging
import time
from typing import Any

import aiosqlite

logger = logging.getLogger("relay.store")

__all__ = ["ServerRuntimeRow", "Store"]

SCHEMA = """
-- One row per server: current runtime state, reconciled with config at startup.
CREATE TABLE IF NOT EXISTS server_runtime (
    name              TEXT PRIMARY KEY,
    power_state       TEXT,              -- on | off | unknown (coarse; transient
                                         --   powering_on/off lives in memory)
    owned             INTEGER NOT NULL,  -- 1 = proxy owns the power lifecycle (D12)
    shutdown_override INTEGER,           -- NULL = use config default; else 0/1 for
                                         --   the current power cycle (monitor toggle)
    cycle_id          TEXT,              -- uuid per proxy-initiated power-on
    updated_at        REAL
);

-- One row per endpoint: latest readiness bookkeeping.
CREATE TABLE IF NOT EXISTS endpoint_runtime (
    name          TEXT PRIMARY KEY,
    ready         INTEGER,
    ready_since   REAL,
    last_check_at REAL
);

-- Append-only audit of every power action (success AND failure).
CREATE TABLE IF NOT EXISTS power_events (
    id           INTEGER PRIMARY KEY,
    ts           REAL,
    server       TEXT,
    action       TEXT,    -- power_on | power_off | schedule_off | idle_off
                     -- | state_sync | external_change
    reason       TEXT,    -- request_wake | idle_timeout | schedule | manual
                     -- | adopted | startup_sync | ...
    initiated_by TEXT,    -- proxy | monitor-ui | startup | external
    success      INTEGER
);

-- Append-only activity log: metrics + future cost basis (D13: never gates
-- shutdown decisions).
CREATE TABLE IF NOT EXISTS requests (
    id             INTEGER PRIMARY KEY,
    ts             REAL,
    server         TEXT,
    endpoint       TEXT,
    client         TEXT,        -- white-glove client name, or "ip:<ip> (<ua>)"
    session_id     TEXT,
    wait_seconds   REAL,        -- time held in the queue before promotion
    active_seconds REAL,        -- time in flight (forward start -> response done)
    status_code    INTEGER,
    request_bytes  INTEGER,
    response_bytes INTEGER
);

-- History per (client, session_id, endpoint).
CREATE TABLE IF NOT EXISTS sessions (
    client        TEXT,
    session_id    TEXT,
    endpoint      TEXT,
    first_seen    REAL,
    last_seen     REAL,
    request_count INTEGER,
    PRIMARY KEY (client, session_id, endpoint)
);

CREATE INDEX IF NOT EXISTS idx_requests_ts          ON requests(ts);
CREATE INDEX IF NOT EXISTS idx_requests_endpoint    ON requests(endpoint, ts);
CREATE INDEX IF NOT EXISTS idx_power_events_server  ON power_events(server, ts);
"""

_UNSET: Any = object()


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
        self._db: aiosqlite.Connection | None = None
        self._write_lock = asyncio.Lock()

    @property
    def path(self) -> str:
        return self._path

    @property
    def db(self) -> aiosqlite.Connection:
        if self._db is None:
            raise RuntimeError("store is not open")
        return self._db

    async def open(self) -> None:
        db = await aiosqlite.connect(self._path)
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA synchronous=NORMAL")
        await db.executescript(SCHEMA)
        await db.commit()
        self._db = db

    async def close(self) -> None:
        if self._db is not None:
            await self._db.commit()
            await self._db.close()
            self._db = None

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
        db = self.db
        async with self._write_lock:
            for name in names:
                cursor = await db.execute(
                    "SELECT power_state, owned, shutdown_override, cycle_id, updated_at "
                    "FROM server_runtime WHERE name = ?",
                    (name,),
                )
                row = await cursor.fetchone()
                if row is None:
                    await db.execute(
                        "INSERT INTO server_runtime "
                        "(name, power_state, owned, shutdown_override, updated_at) "
                        "VALUES (?, 'unknown', 0, NULL, ?)",
                        (name, now),
                    )
                    cursor = await db.execute(
                        "SELECT power_state, owned, shutdown_override, cycle_id, "
                        "updated_at FROM server_runtime WHERE name = ?",
                        (name,),
                    )
                    row = await cursor.fetchone()
                    assert row is not None  # just inserted
                power_state, owned, override, cycle_id, updated_at = row
                rows[name] = ServerRuntimeRow(
                    name=name,
                    power_state=power_state,
                    owned=bool(owned),
                    shutdown_override=None if override is None else bool(override),
                    cycle_id=cycle_id,
                    updated_at=updated_at,
                )
            await db.commit()
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
        sets: list[str] = []
        args: list[object] = []
        if power_state is not None:
            sets.append("power_state = ?")
            args.append(power_state)
        if owned is not None:
            sets.append("owned = ?")
            args.append(int(owned))
        if shutdown_override is not _UNSET:
            sets.append("shutdown_override = ?")
            args.append(None if shutdown_override is None else int(shutdown_override))
        if cycle_id is not _UNSET:
            sets.append("cycle_id = ?")
            args.append(cycle_id)
        if not sets:
            return
        db = self.db
        sets.append("updated_at = ?")
        args.append(time.time())
        args.append(name)
        async with self._write_lock:
            cursor = await db.execute(
                f"UPDATE server_runtime SET {', '.join(sets)} WHERE name = ?", args
            )
            if cursor.rowcount == 0:
                logger.warning("set_server_runtime: no row for server %r", name)
            await db.commit()

    # -- endpoint_runtime ------------------------------------------------------

    async def set_endpoint_runtime(
        self,
        name: str,
        *,
        ready: bool,
        ready_since: float | None,
        last_check_at: float | None,
    ) -> None:
        db = self.db
        async with self._write_lock:
            await db.execute(
                "INSERT INTO endpoint_runtime (name, ready, ready_since, last_check_at) "
                "VALUES (?, ?, ?, ?) "
                "ON CONFLICT(name) DO UPDATE SET "
                "ready = excluded.ready, ready_since = excluded.ready_since, "
                "last_check_at = excluded.last_check_at",
                (name, int(ready), ready_since, last_check_at),
            )
            await db.commit()

    async def get_endpoint_runtime(self) -> dict[str, tuple[bool, float | None]]:
        db = self.db
        cursor = await db.execute("SELECT name, ready, ready_since FROM endpoint_runtime")
        rows = await cursor.fetchall()
        return {name: (bool(ready), ready_since) for name, ready, ready_since in rows}

    # -- power_events ----------------------------------------------------------

    async def log_power_event(
        self, server: str, action: str, reason: str, initiated_by: str, success: bool
    ) -> None:
        db = self.db
        async with self._write_lock:
            await db.execute(
                "INSERT INTO power_events (ts, server, action, reason, initiated_by, success) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (time.time(), server, action, reason, initiated_by, int(success)),
            )
            await db.commit()

    async def recent_power_events(self, limit: int = 50) -> list[dict]:
        db = self.db
        cursor = await db.execute(
            "SELECT ts, server, action, reason, initiated_by, success "
            "FROM power_events ORDER BY id DESC LIMIT ?",
            (limit,),
        )
        rows = list(await cursor.fetchall())
        return [
            {
                "ts": ts,
                "server": server,
                "action": action,
                "reason": reason,
                "initiated_by": initiated_by,
                "success": bool(success),
            }
            for ts, server, action, reason, initiated_by, success in rows[::-1]
        ]

    # -- requests / sessions (written from Phase 1 onward) ---------------------

    async def log_request(
        self,
        *,
        server: str,
        endpoint: str,
        client: str,
        session_id: str,
        wait_seconds: float,
        active_seconds: float,
        status_code: int,
        request_bytes: int,
        response_bytes: int,
    ) -> None:
        db = self.db
        async with self._write_lock:
            await db.execute(
                "INSERT INTO requests "
                "(ts, server, endpoint, client, session_id, wait_seconds, "
                " active_seconds, status_code, request_bytes, response_bytes) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    time.time(),
                    server,
                    endpoint,
                    client,
                    session_id,
                    wait_seconds,
                    active_seconds,
                    status_code,
                    request_bytes,
                    response_bytes,
                ),
            )
            await db.commit()

    # -- retention -------------------------------------------------------------

    async def prune(self, retention_days: int) -> dict[str, int]:
        """Deletes history rows older than `retention_days` (batched).

        `server_runtime` / `endpoint_runtime` are never pruned (one row per
        entity). Returns per-table delete counts.
        """
        cutoff = time.time() - retention_days * 86400
        batch = 10_000
        deleted: dict[str, int] = {}
        db = self.db
        async with self._write_lock:
            for table in ("requests", "power_events"):
                total = 0
                while True:
                    cursor = await db.execute(
                        f"DELETE FROM {table} WHERE id IN "
                        f"(SELECT id FROM {table} WHERE ts < ? LIMIT ?)",
                        (cutoff, batch),
                    )
                    count = cursor.rowcount
                    total += count
                    if count < batch:
                        break
                await db.commit()
                deleted[table] = total
            total = 0
            while True:
                cursor = await db.execute(
                    "DELETE FROM sessions WHERE (client, session_id, endpoint) IN "
                    "(SELECT client, session_id, endpoint FROM sessions "
                    "WHERE last_seen < ? LIMIT ?)",
                    (cutoff, batch),
                )
                count = cursor.rowcount
                total += count
                if count < batch:
                    break
            await db.commit()
            deleted["sessions"] = total
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
