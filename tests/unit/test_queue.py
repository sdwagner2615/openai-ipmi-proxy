"""Unit tests for the per-endpoint slot queue (Q1-Q13, T22 ported)."""

import asyncio
import time

import pytest

from relay.queue import EndpointQueue, QueueEntry

pytestmark = pytest.mark.unit


class FakeRequest:
    def __init__(self, disconnected: bool = False):
        self._disconnected = disconnected

    def set_disconnected(self, value: bool) -> None:
        self._disconnected = value

    async def is_disconnected(self) -> bool:
        return self._disconnected


def make_queue(**overrides) -> EndpointQueue:
    params = {
        "max_spots": 1,
        "max_inflight_per_session": -1,
        "busy_window": 2,
        "session_expiry": 4,
        "immediate_idle_release": True,
    }
    params.update(overrides)
    queue = EndpointQueue(**params)
    queue.ready = True
    return queue


def make_entry(queue: EndpointQueue, client: str, session_id: str, request=None) -> QueueEntry:
    session = queue.get_or_create_session(client, session_id, "openai", "1.2.3.4", "ua")
    return QueueEntry(
        session=session,
        path="/v1/chat/completions",
        body=b"{}",
        request=request or FakeRequest(),
        enqueued_at=time.monotonic(),
    )


def test_promotion_acquires_a_spot_and_invariants_hold():
    # T22a/T22b ported.
    q = make_queue()
    e = make_entry(q, "opencode", "unit-A")
    q.enqueue(e)
    assert e.done and e.session.spot_held and len(q.spots) == 1
    assert set(q.spots) <= set(q.sessions)
    q.release(e, 200)


def test_fifo_ordering_across_sessions():
    q = make_queue(max_spots=1)
    e1 = make_entry(q, "unknown", "s1")
    q.enqueue(e1)
    e2 = make_entry(q, "unknown", "s2")
    q.enqueue(e2)
    assert e1.done and not e2.done
    assert len(q.queue) == 1 and q.queue[0] is e2


def test_per_session_cap_serializes():
    # cap 0: a spot-holding session never runs a second request; once the
    # session is genuinely done and its spot is surrendered, the next
    # request is promoted as a spot-less session.
    q = make_queue(max_inflight_per_session=0, immediate_idle_release=True)
    e1 = make_entry(q, "opencode", "s1")
    q.enqueue(e1)
    e2 = make_entry(q, "opencode", "s1")
    q.enqueue(e2)
    assert e1.done and not e2.done
    q.release(e1, 200)
    q._try_promote()
    assert not e2.done  # still holds the spot, so still capped
    now = time.monotonic()
    e1.session.client_status = "idle"
    e1.session.client_status_at = now
    q.tick(now + 0.1)  # fresh idle report: spot surrendered immediately
    assert e2.done


def test_two_spots_run_two_sessions():
    q = make_queue(max_spots=2)
    e1 = make_entry(q, "unknown", "s1")
    e2 = make_entry(q, "unknown", "s2")
    q.enqueue(e1)
    q.enqueue(e2)
    assert e1.done and e2.done
    assert len(q.spots) == 2


def test_atomic_mode_allows_one_inflight_globally():
    q = make_queue(max_spots=2, atomic_requests=True)
    e1 = make_entry(q, "unknown", "s1")
    q.enqueue(e1)
    e2 = make_entry(q, "unknown", "s2")
    q.enqueue(e2)
    assert e1.done and not e2.done  # a free spot exists, but one in-flight max
    q.release(e1, 200)
    q._try_promote()
    assert e2.done


def test_not_ready_holds_everything():
    q = make_queue()
    q.ready = False
    e = make_entry(q, "unknown", "s1")
    q.enqueue(e)
    assert not e.done
    q.ready = True
    q._try_promote()
    assert e.done


def test_abandon_drops_a_waiting_entry():
    q = make_queue()
    e1 = make_entry(q, "unknown", "s1")
    q.enqueue(e1)
    e2 = make_entry(q, "unknown", "s2")
    q.enqueue(e2)
    q.abandon(e2)
    assert not e2.done and len(q.queue) == 0 and e2.session.waiting == 0
    q.release(e1, 200)


def test_unknown_client_release_deadline_is_max_busy_window_expiry():
    q = make_queue(busy_window=2, session_expiry=4)
    e = make_entry(q, "unknown", "s1")
    q.enqueue(e)
    q.release(e, 200)
    now = time.monotonic()
    assert q._release_deadline(e.session, now) == e.session.last_request_at + 4


def test_known_client_idle_release_deadlines():
    q = make_queue(immediate_idle_release=True)
    e = make_entry(q, "opencode", "s1")
    q.enqueue(e)
    q.release(e, 200)
    now = time.monotonic()
    # Client reports idle (fresh): immediate release (deadline = idle_since).
    e.session.client_status = "idle"
    e.session.client_status_at = now
    e.session.status = "idle"
    e.session.idle_since = now
    assert q._release_deadline(e.session, now) == now
    # A fresh "waiting" report always releases immediately, flag or no flag.
    e2 = make_entry(q, "opencode", "s2")
    q.enqueue(e2)
    q.release(e2, 200)
    e2.session.client_status = "waiting"
    e2.session.client_status_detail = "permission"
    e2.session.client_status_at = now
    e2.session.status = "waiting"
    e2.session.idle_since = now
    assert q._release_deadline(e2.session, now) == now


def test_stale_client_report_falls_back_to_expiry():
    q = make_queue(immediate_idle_release=True)
    e = make_entry(q, "opencode", "s1")
    q.enqueue(e)
    q.release(e, 200)
    now = time.monotonic()
    e.session.client_status = "idle"
    e.session.client_status_at = now - 60  # stale (beyond the unreachable grace)
    e.session.status = "idle"
    e.session.idle_since = now - 1
    assert q._release_deadline(e.session, now) == (now - 1) + 4


def test_tick_releases_and_forgets_sessions():
    # T22c-T22e ported.
    q = make_queue()
    e = make_entry(q, "opencode", "unit-A")
    q.enqueue(e)
    q.release(e, 200)
    now = time.monotonic()
    e.session.client_status = "idle"
    e.session.client_status_detail = "absent from status map (idle)"
    e.session.client_status_at = now
    released = q.tick(now + 0.1)
    assert len(released) == 1 and len(q.spots) == 0
    assert "idle" in released[0][1]
    assert all(k[1] != "unit-A" for k in q.sessions)


def test_tick_releases_waiting_for_input():
    # T22f/T22g ported.
    q = make_queue(immediate_idle_release=False)
    e = make_entry(q, "opencode", "unit-B")
    q.enqueue(e)
    q.release(e, 200)
    now = time.monotonic()
    e.session.client_status = "waiting"
    e.session.client_status_detail = "permission"
    e.session.client_status_at = now
    released = q.tick(now + 0.1)
    assert len(released) == 1 and "waiting" in released[0][1]


def test_revival_keeps_the_spot_and_restarts_the_countdown():
    q = make_queue()
    e1 = make_entry(q, "opencode", "s1")
    q.enqueue(e1)
    q.release(e1, 200)
    now = time.monotonic()
    e1.session.status = "idle"
    e1.session.idle_since = now
    e2 = make_entry(q, "opencode", "s1")  # new request for the same session
    assert e1.session.idle_since is None and e1.session.spot_held
    assert e2.session is e1.session


def test_release_session_manual():
    q = make_queue()
    e = make_entry(q, "opencode", "s1")
    q.enqueue(e)
    q.release(e, 200)
    assert q.release_session("opencode", "s1") is True
    assert not q.spots
    assert q.release_session("opencode", "s1") is False


def test_shared_spot_lets_a_child_run_on_the_ancestors_spot():
    q = make_queue(max_spots=1)
    parent = make_entry(q, "opencode", "parent")
    q.enqueue(parent)
    child_session = q.get_or_create_session("opencode", "child", "openai", "1.2.3.4", "ua")
    child_session.shared_spot_key = parent.session.key
    child = QueueEntry(
        session=child_session,
        path="/v1/chat/completions",
        body=b"{}",
        request=FakeRequest(),
        enqueued_at=time.monotonic(),
    )
    q.enqueue(child)
    assert parent.done and child.done
    assert child_session.spot_held is False  # no spot of its own
    assert len(q.spots) == 1


def test_wait_for_slot_returns_ok_on_promotion():
    q = make_queue()
    e = make_entry(q, "unknown", "s1")
    q.enqueue(e)

    async def run():
        return await q.wait_for_slot(e, None)

    assert asyncio.run(run()) == "ok"


def test_wait_for_slot_detects_disconnect():
    q = make_queue()
    q.ready = False  # nothing will promote while the endpoint is not ready
    request = FakeRequest()
    e = make_entry(q, "unknown", "s1", request=request)
    q.enqueue(e)
    request.set_disconnected(True)

    async def run():
        return await q.wait_for_slot(e, None)

    assert asyncio.run(run()) == "disconnected"


def test_wait_for_slot_times_out():
    q = make_queue()
    other = make_entry(q, "unknown", "other")
    q.enqueue(other)  # takes the only spot
    e = make_entry(q, "unknown", "s1")
    q.enqueue(e)  # waits behind it

    async def run():
        return await q.wait_for_slot(e, 1.0)

    start = time.monotonic()
    assert asyncio.run(run()) == "timed_out"
    assert time.monotonic() - start >= 0.9


def test_promote_disconnect_race_releases_not_abandons():
    # Q11: promoted in the same instant the client went away.
    q = make_queue()
    e = make_entry(q, "unknown", "s1")
    q.enqueue(e)
    assert e.done  # promoted
    q.release(e, None)
    assert e.session.inflight == 0 and not q.queue
