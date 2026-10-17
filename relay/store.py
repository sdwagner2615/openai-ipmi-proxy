"""SQLite store (D5): WAL, Alembic migrations, reconciliation, retention.

Rule: config owns *what exists and the policy*; this store owns *what is
happening and what has happened*. The app never reads policy from the DB.

Schema: the models in `relay.db` are the source of truth. `open()` applies
pending Alembic migrations (`relay/migrations`) to file databases — a fresh
path is created at head, an existing one is upgraded; `:memory:` (tests) is
built straight from the same metadata.

Single async writer: all writes are serialized on one lock; WAL mode allows
concurrent readers during writes.
"""

import asyncio
import logging
import time
from pathlib import Path
from typing import Any

from alembic import command as alembic_command
from alembic.config import Config as AlembicConfig
from sqlalchemy import delete, select, tuple_, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
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

__all__ = ["ServerRuntimeRow", "Store"]

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
        self._engine: AsyncEngine | None = None
        self._sessionmaker: async_sessionmaker[AsyncSession] | None = None
        self._write_lock = asyncio.Lock()

    @property
    def path(self) -> str:
        return self._path

    async def open(self) -> None:
        if self._path == ":memory:":
            engine = make_engine(self._path)
            # No Alembic here: an in-memory DB lives on one connection, which
            # the command API can't share — build the same metadata directly.
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
        else:
            await asyncio.to_thread(self._upgrade_schema)
            engine = make_engine(self._path)
        self._engine = engine
        self._sessionmaker = async_sessionmaker(engine, expire_on_commit=False)

    def _upgrade_schema(self) -> None:
        """Applies pending Alembic migrations (no-op when already at head)."""
        cfg = AlembicConfig()
        cfg.set_main_option("script_location", str(Path(__file__).parent / "migrations"))
        cfg.set_main_option("sqlalchemy.url", f"sqlite:///{self._path}")
        alembic_command.upgrade(cfg, "head")

    async def close(self) -> None:
        if self._engine is not None:
            await self._engine.dispose()
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
        async with self._write_lock, self._engine_live().begin() as conn:
            result = await conn.execute(
                update(ServerRuntime).where(ServerRuntime.name == name).values(**values)
            )
            if result.rowcount == 0:
                logger.warning("set_server_runtime: no row for server %r", name)

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
        async with self._write_lock, self._engine_live().begin() as conn:
            await conn.execute(stmt)

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
        async with self._write_lock, self._sessions()() as session, session.begin():
            session.add(
                RequestLog(
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

    # -- retention -------------------------------------------------------------

    async def prune(self, retention_days: int) -> dict[str, int]:
        """Deletes history rows older than `retention_days` (batched).

        `server_runtime` / `endpoint_runtime` are never pruned (one row per
        entity). Returns per-table delete counts.
        """
        cutoff = time.time() - retention_days * 86400
        batch = 10_000
        deleted: dict[str, int] = {}
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
