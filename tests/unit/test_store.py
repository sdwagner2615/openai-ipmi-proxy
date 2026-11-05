"""SQLite store: migrations, reconciliation, retention, in-memory safety (D5),
and degraded mode (D33: a full disk never blocks proxying)."""

import sqlite3
import time

import aiosqlite
import pytest
from sqlalchemy.exc import DBAPIError

from relay.store import REQUEST_BUFFER_MAX, RequestRecord, Store

pytestmark = pytest.mark.unit


def _fail_commits(store: Store) -> None:
    """Makes every commit raise 'database or disk is full' (deterministic disk-full simulation)."""

    def _raise(connection):
        raise sqlite3.OperationalError("database or disk is full")

    store._engine.sync_engine.dialect.do_commit = _raise


def _restore_commits(store: Store) -> None:
    del store._engine.sync_engine.dialect.do_commit


def _record(**overrides) -> RequestRecord:
    base = {
        "ts": time.time(),
        "server": "ws",
        "endpoint": "llm",
        "client": "c",
        "session_id": "s",
        "wait_seconds": 1.5,
        "active_seconds": 2.5,
        "status_code": 200,
        "request_bytes": 10,
        "response_bytes": 42,
    }
    base.update(overrides)
    return RequestRecord(**base)


async def test_open_applies_migrations_to_fresh_db(tmp_path):
    db_path = str(tmp_path / "relay.db")
    store = Store(db_path)
    await store.open()
    async with aiosqlite.connect(db_path) as db:
        cursor = await db.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        tables = {row[0] for row in await cursor.fetchall()}
        assert {
            "server_runtime",
            "endpoint_runtime",
            "power_events",
            "requests",
            "sessions",
            "alembic_version",
        } <= tables
        cursor = await db.execute("SELECT name FROM sqlite_master WHERE type = 'index'")
        indexes = {row[0] for row in await cursor.fetchall()}
        assert {
            "idx_requests_ts",
            "idx_requests_endpoint",
            "idx_power_events_server",
        } <= indexes
    await store.close()


async def test_reopen_is_idempotent(tmp_path):
    db_path = str(tmp_path / "relay.db")
    store = Store(db_path)
    await store.open()
    await store.reconcile_servers(["ws"])
    await store.close()
    # A second boot migrates nothing and restores state.
    store2 = Store(db_path)
    await store2.open()
    rows = await store2.reconcile_servers(["ws"])
    assert rows["ws"].owned is False
    await store2.close()


async def test_open_retries_while_db_unwritable(tmp_path):
    """A full disk at boot delays startup instead of crashing (D33)."""
    import relay.store as store_module

    store = Store(str(tmp_path / "relay.db"))
    attempts = {"n": 0}
    real_upgrade = store._upgrade_schema

    def _flaky_upgrade():
        attempts["n"] += 1
        if attempts["n"] <= 2:
            raise sqlite3.OperationalError("database or disk is full")
        real_upgrade()

    store._upgrade_schema = _flaky_upgrade
    original = store_module.OPEN_RETRY_INTERVAL
    store_module.OPEN_RETRY_INTERVAL = 0.01
    try:
        await store.open()
    finally:
        store_module.OPEN_RETRY_INTERVAL = original
    assert attempts["n"] == 3  # two failures, then success
    assert store.degraded is False  # a healthy open leaves no degraded state
    await store.close()


async def test_open_creates_schema_and_round_trips(tmp_path):
    store = Store(str(tmp_path / "relay.db"))
    await store.open()
    rows = await store.reconcile_servers(["a", "b"])
    assert set(rows) == {"a", "b"}
    assert rows["a"].owned is False
    assert rows["a"].power_state == "unknown"
    await store.close()


async def test_reconcile_restores_persisted_state(tmp_path):
    store = Store(str(tmp_path / "relay.db"))
    await store.open()
    await store.reconcile_servers(["ws"])
    await store.set_server_runtime("ws", power_state="on", owned=True, shutdown_override=False)
    rows = await store.reconcile_servers(["ws"])
    assert rows["ws"].owned is True
    assert rows["ws"].shutdown_override is False
    assert rows["ws"].power_state == "on"
    await store.close()


async def test_reconcile_leaves_removed_servers_in_place(tmp_path):
    store = Store(str(tmp_path / "relay.db"))
    await store.open()
    await store.reconcile_servers(["ws"])
    rows = await store.reconcile_servers(["other"])
    assert set(rows) == {"other"}
    await store.set_server_runtime("ws", power_state="off")
    await store.close()


async def test_set_server_runtime_partial_updates(tmp_path):
    store = Store(str(tmp_path / "relay.db"))
    await store.open()
    await store.reconcile_servers(["ws"])
    await store.set_server_runtime("ws", owned=True)
    rows = await store.reconcile_servers(["ws"])
    assert rows["ws"].owned is True and rows["ws"].shutdown_override is None
    # None clears the override (monitor toggle off -> back to config default).
    await store.set_server_runtime("ws", shutdown_override=None)
    rows = await store.reconcile_servers(["ws"])
    assert rows["ws"].shutdown_override is None
    # Unknown server names do not raise.
    await store.set_server_runtime("ghost", owned=True)
    await store.close()


async def test_endpoint_runtime_upserts(tmp_path):
    store = Store(str(tmp_path / "relay.db"))
    await store.open()
    await store.set_endpoint_runtime("llm", ready=False, ready_since=None, last_check_at=1.0)
    await store.set_endpoint_runtime("llm", ready=True, ready_since=2.0, last_check_at=3.0)
    got = await store.get_endpoint_runtime()
    assert got == {"llm": (True, 2.0)}
    await store.close()


async def test_power_events_logged_and_ordered(tmp_path):
    store = Store(str(tmp_path / "relay.db"))
    await store.open()
    await store.log_power_event("ws", "power_on", "request_wake", "proxy", True)
    await store.log_power_event("ws", "power_off", "idle_timeout", "proxy", False)
    events = await store.recent_power_events()
    assert [e["action"] for e in events] == ["power_on", "power_off"]
    assert events[0]["success"] is True and events[1]["success"] is False
    await store.close()


async def test_prune_respects_retention(tmp_path):
    db_path = str(tmp_path / "relay.db")
    store = Store(db_path)
    await store.open()
    await store.reconcile_servers(["ws"])
    now = time.time()
    # Seed history rows through a second connection (WAL allows it).
    async with aiosqlite.connect(db_path) as db:
        await db.execute(
            "INSERT INTO requests (ts, server, endpoint, client, session_id, "
            "wait_seconds, active_seconds, status_code, request_bytes, response_bytes) "
            "VALUES (?, 'ws', 'llm', 'c', 's', 0, 0, 200, 1, 1)",
            (now - 100 * 86400,),  # 100 days old
        )
        await db.execute(
            "INSERT INTO requests (ts, server, endpoint, client, session_id, "
            "wait_seconds, active_seconds, status_code, request_bytes, response_bytes) "
            "VALUES (?, 'ws', 'llm', 'c', 's', 0, 0, 200, 1, 1)",
            (now - 1.0,),  # fresh
        )
        await db.execute(
            "INSERT INTO power_events (ts, server, action, reason, initiated_by, success) "
            "VALUES (?, 'ws', 'state_sync', 'poll', 'proxy', 1)",
            (now - 100 * 86400,),
        )
        await db.execute(
            "INSERT INTO sessions "
            "(client, session_id, endpoint, first_seen, last_seen, request_count) "
            "VALUES ('c', 's', 'llm', ?, ?, 1)",
            (now - 100 * 86400, now - 100 * 86400),
        )
        await db.commit()
    deleted = await store.prune(retention_days=90)
    assert deleted["requests"] == 1
    assert deleted["power_events"] == 1
    assert deleted["sessions"] == 1
    async with aiosqlite.connect(db_path) as db:
        cursor = await db.execute("SELECT COUNT(*) AS n FROM requests")
        row = await cursor.fetchone()
        assert row[0] == 1  # only the fresh row survives
    await store.close()


async def test_in_memory_store_is_safe(tmp_path):
    store = Store(":memory:")
    await store.open()
    rows = await store.reconcile_servers(["ws"])
    assert rows["ws"].owned is False
    await store.set_server_runtime("ws", owned=True)
    await store.close()
    # A second open on :memory: is a fresh database (documented behavior).
    store2 = Store(":memory:")
    await store2.open()
    rows2 = await store2.reconcile_servers(["ws"])
    assert rows2["ws"].owned is False
    await store2.close()


async def test_enqueue_stages_until_flush(tmp_path):
    store = Store(str(tmp_path / "relay.db"))
    await store.open()
    await store.enqueue_request(_record())
    await store.enqueue_request(_record(status_code=502, response_bytes=0))
    # Nothing is persisted until the flusher runs.
    assert await store.get_requests() == []
    flushed = await store.flush_requests()
    assert flushed == 2
    got = await store.get_requests()
    assert len(got) == 2
    assert {r["status_code"] for r in got} == {200, 502}
    assert all(r["endpoint"] == "llm" and r["server"] == "ws" for r in got)
    # The buffer is drained by the flush.
    assert len(store._request_buffer) == 0
    await store.close()


async def test_flush_requests_empty_is_a_noop(tmp_path):
    store = Store(str(tmp_path / "relay.db"))
    await store.open()
    assert await store.flush_requests() == 0
    assert await store.get_requests() == []
    await store.close()


async def test_upsert_session_creates_and_increments(tmp_path):
    db_path = str(tmp_path / "relay.db")
    store = Store(db_path)
    await store.open()
    for _ in range(3):
        await store.upsert_session("opencode", "abc", "llm")
    async with aiosqlite.connect(db_path) as db:
        cursor = await db.execute(
            "SELECT client, session_id, endpoint, request_count FROM sessions"
        )
        row = await cursor.fetchone()
    assert row == ("opencode", "abc", "llm", 3)
    await store.close()


# -- degraded mode (D33): a full disk never blocks proxying -------------------


async def test_write_failure_degrades_store(tmp_path):
    store = Store(str(tmp_path / "relay.db"))
    await store.open()
    _fail_commits(store)
    # The request-path write must NOT raise, even with the disk "full".
    await store.upsert_session("opencode", "abc", "llm")
    await store.log_power_event("ws", "power_on", "request_wake", "proxy", True)
    await store.set_server_runtime("ws", power_state="on")
    await store.set_endpoint_runtime("llm", ready=True, ready_since=1.0, last_check_at=2.0)
    assert store.degraded is True
    assert store.dropped_writes == 4
    await store.close()


async def test_store_recovers_when_writes_succeed_again(tmp_path):
    store = Store(str(tmp_path / "relay.db"))
    await store.open()
    _fail_commits(store)
    await store.upsert_session("opencode", "abc", "llm")
    assert store.degraded is True
    # Disk space freed: the next successful write marks recovery.
    _restore_commits(store)
    await store.upsert_session("opencode", "abc", "llm")
    assert store.degraded is False
    assert store.dropped_writes == 1  # the one dropped write stays counted
    async with aiosqlite.connect(str(tmp_path / "relay.db")) as db:
        cursor = await db.execute("SELECT request_count FROM sessions")
        row = await cursor.fetchone()
    assert row == (1,)  # only the post-recovery write landed
    await store.close()


async def test_programming_error_still_raises(tmp_path):
    store = Store(str(tmp_path / "relay.db"))
    await store.open()

    def _raise(connection):
        raise sqlite3.ProgrammingError("a code-level SQL bug")

    store._engine.sync_engine.dialect.do_commit = _raise
    try:
        with pytest.raises(DBAPIError):
            await store.upsert_session("opencode", "abc", "llm")
    finally:
        _restore_commits(store)
    # It did not degrade the store: this is a bug to surface, not a condition.
    assert store.degraded is False
    await store.close()


async def test_corruption_degrades_store(tmp_path):
    """A corrupted DB ('file is not a database', a plain DatabaseError) also
    degrades rather than 500ing the request path."""
    store = Store(str(tmp_path / "relay.db"))
    await store.open()

    def _raise(connection):
        raise sqlite3.DatabaseError("file is not a database")

    store._engine.sync_engine.dialect.do_commit = _raise
    try:
        await store.upsert_session("opencode", "abc", "llm")
    finally:
        _restore_commits(store)
    assert store.degraded is True
    await store.close()


async def test_reconcile_degrades_to_unowned_rows(tmp_path):
    store = Store(str(tmp_path / "relay.db"))
    await store.open()
    _fail_commits(store)
    rows = await store.reconcile_servers(["ws", "other"])
    # The safe default per D12: nothing is owned, state unknown.
    assert set(rows) == {"ws", "other"}
    for row in rows.values():
        assert row.owned is False
        assert row.power_state == "unknown"
        assert row.shutdown_override is None
    assert store.degraded is True
    await store.close()


async def test_flush_failure_drops_batch_and_counts(tmp_path):
    store = Store(str(tmp_path / "relay.db"))
    await store.open()
    _fail_commits(store)
    for _ in range(3):
        await store.enqueue_request(_record())
    # A failed flush drops the batch (no exception, no re-queue).
    assert await store.flush_requests() == 0
    assert store.degraded is True
    assert store.dropped_requests == 3
    _restore_commits(store)
    await store.enqueue_request(_record())
    assert await store.flush_requests() == 1
    assert store.degraded is False
    assert len(await store.get_requests()) == 1
    await store.close()


async def test_buffer_sheds_oldest_when_full(tmp_path):
    store = Store(str(tmp_path / "relay.db"))
    await store.open()
    for i in range(REQUEST_BUFFER_MAX + 5):
        await store.enqueue_request(_record(session_id=f"s{i}"))
    # Bounded: the 5 oldest records were shed and counted.
    assert len(store._request_buffer) == REQUEST_BUFFER_MAX
    assert store._request_buffer[0].session_id == "s5"  # s0..s4 shed
    assert store._request_buffer[-1].session_id == f"s{REQUEST_BUFFER_MAX + 4}"
    assert store.dropped_requests == 5
    await store.close()
