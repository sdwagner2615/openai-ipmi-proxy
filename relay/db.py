"""SQLAlchemy models and engine factory for the relay store (D5).

The mapped models in this module are the single source of truth for the
SQLite schema. Alembic migrations in `relay/migrations` are generated from
`Base.metadata` and applied automatically when the store opens. WAL mode +
`synchronous=NORMAL` are set on every connection via an engine event.
"""

from sqlalchemy import Boolean, Float, Index, Integer, Text, event
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.pool import StaticPool

__all__ = [
    "Base",
    "EndpointRuntime",
    "PowerEvent",
    "RequestLog",
    "ServerRuntime",
    "SessionRecord",
    "make_engine",
]


class Base(DeclarativeBase):
    pass


class ServerRuntime(Base):
    """One row per server: current runtime state, reconciled with config at startup."""

    __tablename__ = "server_runtime"

    name: Mapped[str] = mapped_column(Text, primary_key=True)
    # on | off | unknown (coarse; transient powering_on/off lives in memory)
    power_state: Mapped[str | None] = mapped_column(Text)
    # 1 = proxy owns the power lifecycle (D12)
    owned: Mapped[bool] = mapped_column(Boolean)
    # None = use config default; else 0/1 for the current power cycle (monitor)
    shutdown_override: Mapped[bool | None] = mapped_column(Boolean)
    # uuid per proxy-initiated power-on
    cycle_id: Mapped[str | None] = mapped_column(Text)
    updated_at: Mapped[float | None] = mapped_column(Float)


class EndpointRuntime(Base):
    """One row per endpoint: latest readiness bookkeeping."""

    __tablename__ = "endpoint_runtime"

    name: Mapped[str] = mapped_column(Text, primary_key=True)
    ready: Mapped[bool | None] = mapped_column(Boolean)
    ready_since: Mapped[float | None] = mapped_column(Float)
    last_check_at: Mapped[float | None] = mapped_column(Float)


class PowerEvent(Base):
    """Append-only audit of every power action (success AND failure)."""

    __tablename__ = "power_events"
    __table_args__ = (Index("idx_power_events_server", "server", "ts"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    ts: Mapped[float | None] = mapped_column(Float)
    server: Mapped[str | None] = mapped_column(Text)
    # power_on | power_off | schedule_off | idle_off | state_sync | external_change
    action: Mapped[str | None] = mapped_column(Text)
    # request_wake | idle_timeout | schedule | manual | adopted | startup_sync | ...
    reason: Mapped[str | None] = mapped_column(Text)
    # proxy | monitor-ui | startup | external
    initiated_by: Mapped[str | None] = mapped_column(Text)
    success: Mapped[bool | None] = mapped_column(Boolean)


class RequestLog(Base):
    """Append-only activity log: metrics + future cost basis (D13)."""

    __tablename__ = "requests"
    __table_args__ = (
        Index("idx_requests_ts", "ts"),
        Index("idx_requests_endpoint", "endpoint", "ts"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    ts: Mapped[float | None] = mapped_column(Float)
    server: Mapped[str | None] = mapped_column(Text)
    endpoint: Mapped[str | None] = mapped_column(Text)
    # white-glove client name, or "ip:<ip> (<ua>)"
    client: Mapped[str | None] = mapped_column(Text)
    session_id: Mapped[str | None] = mapped_column(Text)
    # time held in the queue before promotion
    wait_seconds: Mapped[float | None] = mapped_column(Float)
    # time in flight (forward start -> response done)
    active_seconds: Mapped[float | None] = mapped_column(Float)
    status_code: Mapped[int | None] = mapped_column(Integer)
    request_bytes: Mapped[int | None] = mapped_column(Integer)
    response_bytes: Mapped[int | None] = mapped_column(Integer)


class SessionRecord(Base):
    """History per (client, session_id, endpoint)."""

    __tablename__ = "sessions"

    client: Mapped[str] = mapped_column(Text, primary_key=True)
    session_id: Mapped[str] = mapped_column(Text, primary_key=True)
    endpoint: Mapped[str] = mapped_column(Text, primary_key=True)
    first_seen: Mapped[float | None] = mapped_column(Float)
    last_seen: Mapped[float | None] = mapped_column(Float)
    request_count: Mapped[int | None] = mapped_column(Integer)


def make_engine(path: str) -> AsyncEngine:
    """Builds the async engine for `path` (WAL + synchronous=NORMAL)."""
    if path == ":memory:":
        # An in-memory DB exists per connection, so share one.
        engine = create_async_engine("sqlite+aiosqlite://", poolclass=StaticPool)
    else:
        engine = create_async_engine(f"sqlite+aiosqlite:///{path}")

    @event.listens_for(engine.sync_engine, "connect")
    def _set_pragmas(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.close()

    return engine
