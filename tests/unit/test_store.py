"""SQLite store: migrations, reconciliation, retention, in-memory safety (D5)."""

import time

import aiosqlite
import pytest

from relay.store import RequestRecord, Store

pytestmark = pytest.mark.unit


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
    assert store._request_buffer == []
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
