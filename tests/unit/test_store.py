"""SQLite store: schema, reconciliation, retention, in-memory safety (D5)."""

import time

import pytest

from relay.store import Store

pytestmark = pytest.mark.unit


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
    store = Store(str(tmp_path / "relay.db"))
    await store.open()
    await store.reconcile_servers(["ws"])
    now = time.time()
    db = store.db
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
        "INSERT INTO sessions (client, session_id, endpoint, first_seen, last_seen, request_count) "
        "VALUES ('c', 's', 'llm', ?, ?, 1)",
        (now - 100 * 86400, now - 100 * 86400),
    )
    await db.commit()
    deleted = await store.prune(retention_days=90)
    assert deleted["requests"] == 1
    assert deleted["power_events"] == 1
    assert deleted["sessions"] == 1
    cursor = await store.db.execute("SELECT COUNT(*) AS n FROM requests")
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
