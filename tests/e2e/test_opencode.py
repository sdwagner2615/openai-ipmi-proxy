"""E2E: opencode parity against the generalized, config-driven client stack.

Covers every behavior in parity.md §client-status (C1-C8) plus the Phase-3
generalization itself: clients are config entries, the opencode source is a
pluggable StatusSource, and a second client with ``status.kind: none`` runs
alongside opencode on busy-window inference only (D20/D21).

Each scenario gets an isolated environment from ``oc_env.make()`` (own
target, mock BMC, and BOTH opencode status mocks), so per-mock request
counters and the /offline switch start clean.
"""

import asyncio
import time

import httpx
import pytest

pytestmark = pytest.mark.e2e

CHAT = "/v1/chat/completions"
BODY = {"model": "mock", "messages": [{"role": "user", "content": "hi"}], "stream": False}


def oc_headers(sid: str) -> dict:
    return {"x-opencode-session": sid, "user-agent": "opencode/1.18.23"}


def oc_b_headers(sid: str) -> dict:
    return {"x-opencode-b-session": sid, "user-agent": "opencode-b/1.0"}


def mytool_headers(sid: str) -> dict:
    return {"x-mytool-session": sid, "user-agent": "mytool/1.0"}


def find(data: dict, sid: str) -> dict | None:
    for row in data["sessions"]:
        if row["session"] == sid:
            return row
    return None


async def until(monitor, predicate, timeout: float, what: str) -> dict:
    """Polls the monitor until predicate(data) holds (bounded, per e2e rules)."""
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = await monitor()
        if predicate(last):
            return last
        await asyncio.sleep(0.25)
    raise AssertionError(f"timed out waiting for {what}: {last}")


def mock_set(port: int, sid: str, status_type: str, directory: str | None = None) -> None:
    params = {"sid": sid, "type": status_type}
    if directory:
        params["directory"] = directory
    httpx.post(f"http://127.0.0.1:{port}/set", params=params, timeout=3)


def mock_delete(port: int, sid: str) -> None:
    httpx.delete(f"http://127.0.0.1:{port}/set", params={"sid": sid}, timeout=3)


def mock_parent(port: int, sid: str, parent: str) -> None:
    httpx.post(f"http://127.0.0.1:{port}/parent", params={"sid": sid, "parent": parent}, timeout=3)


def mock_permission(port: int, sid: str, pending: bool = True) -> None:
    method = httpx.post if pending else httpx.delete
    method(f"http://127.0.0.1:{port}/permission", params={"sid": sid}, timeout=3)


def mock_question(port: int, sid: str, pending: bool = True) -> None:
    method = httpx.post if pending else httpx.delete
    method(f"http://127.0.0.1:{port}/question", params={"sid": sid}, timeout=3)


async def test_c1_per_directory_status_maps(oc_env):
    # C1: one client, two working directories (two opencode "instances"):
    # the context is resolved once per session (cached) and one status map
    # is polled per distinct (client machine, directory) per interval.
    env = await oc_env.make("c1", concurrency=2)
    try:
        port = env.status_port
        mock_set(port, "c1-A", "busy", directory="/work/A")
        mock_set(port, "c1-B", "busy", directory="/work/B")
        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{env.port}", timeout=30) as c:
            ra = asyncio.create_task(c.post(CHAT, json=BODY, headers=oc_headers("c1-A")))
            await asyncio.sleep(0.3)
            rb = c.post(CHAT, json=BODY, headers=oc_headers("c1-B"))
            await ra
            resp_b = await rb
            assert resp_b.status_code == 200, f"C1a both requests served: {resp_b.text[:120]}"
            await asyncio.sleep(4)  # several poll intervals (poll=1s)
            d = await env.monitor()
            sa, sb = find(d, "c1-A"), find(d, "c1-B")
            assert sa is not None and sa["client"] == "opencode" and sa["spot"] == "held", (
                f"C1b A holds its spot (busy in /work/A): {sa}"
            )
            assert sb is not None and sb["client"] == "opencode" and sb["spot"] == "held", (
                f"C1c B holds its spot (busy in /work/B): {sb}"
            )
            counts = await env.mock_requests(port)
            assert counts.get("/session?c1-A") == 1, f"C1d A's context resolved once: {counts}"
            assert counts.get("/session?c1-B") == 1, f"C1e B's context resolved once: {counts}"
            polls_a = counts.get("/session/status?directory=/work/A", 0)
            polls_b = counts.get("/session/status?directory=/work/B", 0)
            assert polls_a >= 2 and polls_b >= 2, f"C1f both directories polled: {counts}"
            assert counts.get("/session/status?directory=mock-dir", 0) == 0, (
                f"C1g no poll for a directory without sessions: {counts}"
            )
            # Absence in ONE directory idles only that session.
            mock_delete(port, "c1-B")
            await until(env.monitor, lambda d: find(d, "c1-B") is None, 10, "C1h B released")
            d = await env.monitor()
            sa = find(d, "c1-A")
            assert sa is not None and sa["spot"] == "held" and sa["status"] == "busy", (
                f"C1i A unaffected by B's directory going idle: {sa}"
            )
    finally:
        env.stop()


async def test_c2_absence_from_map_is_idle_report(oc_env):
    # C2: opencode deletes idle entries, so a fresh map lacking the session
    # IS the idle report (with immediate release the spot goes at once).
    env = await oc_env.make("c2")
    try:
        port = env.status_port
        mock_set(port, "c2-A", "busy")
        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{env.port}", timeout=30) as c:
            resp = await c.post(CHAT, json=BODY, headers=oc_headers("c2-A"))
            assert resp.status_code == 200, f"C2a request ok: {resp.text[:120]}"
            d = await until(
                env.monitor,
                lambda d: (s := find(d, "c2-A")) is not None and s["spot"] == "held",
                10,
                "C2b spot held while busy",
            )
            assert d["config"]["immediate_idle_release"] is True, (
                f"C2c flag on for this scenario: {d['config']}"
            )
            mock_delete(port, "c2-A")  # the session goes idle (entry deleted)
            await until(
                env.monitor,
                lambda d: find(d, "c2-A") is None,
                10,
                "C2d fresh absence released the spot",
            )
    finally:
        env.stop()


async def test_c3_pending_input_forces_waiting(oc_env):
    # C3: a session blocked on a pending permission stays "busy" in the
    # status map, so the pending endpoint is the tie-breaker: the confirmed
    # pending request forces status "waiting" no matter what the map says.
    # delay=2 keeps the request in-flight across a status tick: the waiting
    # state is visible while the request runs (a 0-delay request would finish
    # before the first tick, which sets and releases the spot in one tick).
    env = await oc_env.make("c3", delay=2)
    try:
        port = env.status_port
        mock_set(port, "c3-A", "busy")
        mock_permission(port, "c3-A")
        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{env.port}", timeout=30) as c:
            req = asyncio.create_task(c.post(CHAT, json=BODY, headers=oc_headers("c3-A")))
            d = await until(
                env.monitor,
                lambda d: (s := find(d, "c3-A")) is not None and s["client_status"] == "waiting",
                8,
                "C3a session recorded as waiting",
            )
            s = find(d, "c3-A")
            assert (
                s["client_status_detail"] == "permission"
                and s["spot"] == "held"
                and s["inflight"] == 1
            ), f"C3b waiting (permission) wins over the map's busy: {s}"
            resp = await req
            assert resp.status_code == 200, f"C3c request ok: {resp.text[:120]}"
            # A confirmed pending request also releases the spot as soon as
            # the in-flight request ends (C7), so the session is gone.
            await until(
                env.monitor,
                lambda d: find(d, "c3-A") is None,
                10,
                "C3d waiting session released",
            )
    finally:
        env.stop()


async def test_c4_unreachable_keeps_last_status_for_grace(oc_env):
    # C4: while the status API is unreachable, a session keeps its
    # last-known status for the 30s grace, then falls back to idle (and the
    # spot goes out after the session's expiry). Driven with the second
    # client entry (opencode-b) and its own status mock.
    env = await oc_env.make("c4", expiry=4)
    try:
        port = env.status_b_port
        mock_set(port, "c4-A", "busy")
        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{env.port}", timeout=30) as c:
            resp = await c.post(CHAT, json=BODY, headers=oc_b_headers("c4-A"))
            assert resp.status_code == 200, f"C4a request ok: {resp.text[:120]}"
            d = await until(
                env.monitor,
                lambda d: (
                    (s := find(d, "c4-A")) is not None
                    and s["client_status"] == "busy"
                    and s["spot"] == "held"
                ),
                10,
                "C4b busy + spot held before the outage",
            )
            assert find(d, "c4-A")["client"] == "opencode-b"
            await env.mock_offline(port, True)  # status API unreachable from now on
            # After expiry (4s) would long have fired, the last-known busy
            # status still holds the spot (inside the 30s grace).
            await asyncio.sleep(10)
            d = await env.monitor()
            s = find(d, "c4-A")
            assert s is not None and s["spot"] == "held", (
                f"C4c grace keeps the last-known busy status (spot held at ~10s): {s}"
            )
            assert s["client_status"] == "busy" and (s["client_status_age"] or 0) > 10, (
                f"C4d the report is stale-but-graced, not re-polled: {s}"
            )
            # After the 30s grace the session falls back to idle and the
            # spot is surrendered after its (4s) expiry.
            await until(
                env.monitor,
                lambda d: find(d, "c4-A") is None,
                45,
                "C4e released after grace + expiry",
            )
    finally:
        env.stop()


async def test_c5_poll_grouping_and_cache_pruning(oc_env):
    # C5: one status base per client machine (here: 127.0.0.1), polled at
    # the configured interval; context + parent caches are pruned when the
    # sessions vanish (no further lookups for gone sessions).
    env = await oc_env.make("c5", concurrency=2)
    try:
        port = env.status_port
        mock_set(port, "c5-A", "busy", directory="/work/A")
        mock_set(port, "c5-B", "busy", directory="/work/B")
        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{env.port}", timeout=30) as c:
            ra = asyncio.create_task(c.post(CHAT, json=BODY, headers=oc_headers("c5-A")))
            rb = asyncio.create_task(c.post(CHAT, json=BODY, headers=oc_headers("c5-B")))
            await ra, await rb
            await asyncio.sleep(4)
            counts = await env.mock_requests(port)
            assert counts.get("/session?c5-A") == 1, f"C5a context cached: {counts}"
            assert counts.get("/session?c5-B") == 1, f"C5b context cached: {counts}"
            assert counts.get("/session/status?directory=/work/A", 0) >= 3, f"C5c: {counts}"
            assert counts.get("/session/status?directory=/work/B", 0) >= 3, f"C5d: {counts}"
            # Both sessions idle at once: released, forgotten, caches pruned.
            mock_delete(port, "c5-A")
            mock_delete(port, "c5-B")
            await until(
                env.monitor,
                lambda d: find(d, "c5-A") is None and find(d, "c5-B") is None,
                10,
                "C5e both released",
            )
            before = await env.mock_requests(port)
            await asyncio.sleep(3)
            after = await env.mock_requests(port)
            for key in (
                "/session?c5-A",
                "/session?c5-B",
                "/session/status?directory=/work/A",
                "/session/status?directory=/work/B",
            ):
                assert after.get(key, 0) == before.get(key, 0), (
                    f"C5f no polls for vanished sessions ({key}): {before} -> {after}"
                )
    finally:
        env.stop()


async def test_c7_waiting_for_input_releases_regardless_of_flag(oc_env):
    # C7: a fresh "waiting" report releases the spot immediately regardless
    # of immediate_idle_release (here OFF, with a long expiry that a plain
    # idle report would wait out): a blocked session makes no model call
    # until a human acts, so there is no cache to protect.
    env = await oc_env.make("c7", immediate=False, expiry=8, delay=2)
    try:
        port = env.status_port
        mock_set(port, "c7-A", "busy")
        mock_set(port, "c7-B", "busy")
        mock_question(port, "c7-A")
        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{env.port}", timeout=30) as c:
            ra = asyncio.create_task(c.post(CHAT, json=BODY, headers=oc_headers("c7-A")))
            await asyncio.sleep(0.3)
            rb = asyncio.create_task(c.post(CHAT, json=BODY, headers=oc_headers("c7-B")))
            d = await until(
                env.monitor,
                lambda d: (
                    (s := find(d, "c7-A")) is not None
                    and s["client_status"] == "waiting"
                    and s["client_status_detail"] == "question"
                ),
                8,
                "C7a recorded as waiting (question)",
            )
            assert find(d, "c7-A")["spot"] == "held", f"C7b spot held while in-flight: {d}"
            assert (await env.monitor())["config"]["immediate_idle_release"] is False, (
                "C7c flag is off in this scenario"
            )
            t0 = time.monotonic()
            resp_a = await ra
            resp_b = await rb
            wait_b = time.monotonic() - t0
            assert resp_a.status_code == 200 and resp_b.status_code == 200 and wait_b < 5, (
                f"C7d waiting release beat the 8s cooldown (B ran in {wait_b:.1f}s): "
                f"a={resp_a.status_code} b={resp_b.status_code}"
            )
            mock_question(port, "c7-A", pending=False)
            mock_delete(port, "c7-B")
            await until(
                env.monitor,
                lambda d: find(d, "c7-A") is None and find(d, "c7-B") is None,
                15,
                "C7e both released afterwards",
            )
    finally:
        env.stop()


async def test_c8_retry_status_surfaced_and_keeps_spot(oc_env):
    # C8: {"type": "retry"} is surfaced (the monitor detail shows the
    # attempt count) and a retrying session keeps its spot — past expiry.
    env = await oc_env.make("c8")
    try:
        port = env.status_port
        mock_set(port, "c8-A", "retry")
        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{env.port}", timeout=30) as c:
            resp = await c.post(CHAT, json=BODY, headers=oc_headers("c8-A"))
            assert resp.status_code == 200, f"C8a request ok: {resp.text[:120]}"
            d = await until(
                env.monitor,
                lambda d: (s := find(d, "c8-A")) is not None and s["client_status"] == "retry",
                8,
                "C8b retry surfaced",
            )
            s = find(d, "c8-A")
            assert "attempt 1" in s["client_status_detail"], f"C8c detail carries attempt: {s}"
            assert s["spot"] == "held", f"C8d retry session holds its spot: {s}"
            await asyncio.sleep(6)  # > expiry (4s): a busy/idle session would be gone
            d = await env.monitor()
            s = find(d, "c8-A")
            assert s is not None and s["spot"] == "held" and s["status"] == "retry", (
                f"C8e retry keeps the spot past expiry: {s}"
            )
            mock_delete(port, "c8-A")  # retrying ends, session goes idle
            await until(
                env.monitor, lambda d: find(d, "c8-A") is None, 10, "C8f released when idle"
            )
    finally:
        env.stop()


async def test_sub_agent_shares_parent_spot(oc_env):
    # Q8 (via the generalized parent-chain rule): a sub-agent whose parent
    # chain leads to a tracked session runs on the closest tracked
    # ancestor's spot without taking one of its own.
    env = await oc_env.make("q8", concurrency=1)
    try:
        port = env.status_port
        mock_set(port, "q8-P", "busy")
        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{env.port}", timeout=30) as c:
            rp = await c.post(CHAT, json=BODY, headers=oc_headers("q8-P"))
            assert rp.status_code == 200, f"Q8a parent running: {rp.text[:120]}"
            d = await until(
                env.monitor,
                lambda d: (s := find(d, "q8-P")) is not None and s["spot"] == "held",
                10,
                "Q8b parent holds the only spot",
            )
            assert d["config"]["active_sessions"] == 1
            mock_parent(port, "q8-C", "q8-P")
            rc = asyncio.create_task(c.post(CHAT, json=BODY, headers=oc_headers("q8-C")))
            d = await until(
                env.monitor,
                lambda d: (s := find(d, "q8-C")) is not None and s["spot"] == "shared",
                10,
                "Q8c child running on the parent's spot",
            )
            sp = find(d, "q8-P")
            assert (
                sp is not None and sp["spot"] == "held" and d["config"]["active_sessions"] == 1
            ), f"Q8d the shared spot takes no extra slot: {sp}"
            resp_c = await rc
            assert resp_c.status_code == 200, "Q8e child served on the shared spot"
            mock_delete(port, "q8-P")
            mock_delete(port, "q8-C")
            await until(
                env.monitor,
                lambda d: find(d, "q8-P") is None and find(d, "q8-C") is None,
                15,
                "Q8f all released",
            )
    finally:
        env.stop()


async def test_kind_none_client_uses_busy_window_alongside_opencode(oc_env):
    # Phase 3: a configured client with status.kind: none is identified by
    # its match rule (named, scoped sessions) but carries no status source:
    # its sessions use busy-window inference, exactly like unknown clients —
    # while an opencode session on the same endpoint follows its reports.
    env = await oc_env.make("none", busy_window=2, expiry=4)
    try:
        port = env.status_port
        mock_set(port, "k0-OC", "busy")
        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{env.port}", timeout=30) as c:
            # Sequential, on purpose: k0-MT takes the only spot first.
            r_tool = await c.post(CHAT, json=BODY, headers=mytool_headers("k0-MT"))
            assert r_tool.status_code == 200, f"N0 mytool request ok: {r_tool.text[:120]}"
            # Within the busy window (2s) the kind:none session is busy —
            # with no status source at all (client_status stays None), and
            # it is NOT released immediately as a report-less "known"
            # client used to be.
            await asyncio.sleep(1)
            d = await env.monitor()
            smt = find(d, "k0-MT")
            assert (
                smt is not None
                and smt["client"] == "mytool"
                and smt["status"] == "busy"
                and smt["spot"] == "held"
            ), f"N1a named kind:none client busy inside its window: {smt}"
            assert smt["client_status"] is None, f"N1b no status source reports it: {smt}"
            # The opencode session queues behind mytool's spot...
            r_oc = asyncio.create_task(c.post(CHAT, json=BODY, headers=oc_headers("k0-OC")))
            await asyncio.sleep(0.5)
            soc = find(await env.monitor(), "k0-OC")
            assert (
                soc is not None
                and soc["client"] == "opencode"
                and soc["waiting"] == 1
                and soc["spot"] == "none"
            ), f"N1c opencode session on the same endpoint queues behind it: {soc}"
            # ...and after max(busy_window, expiry) the kind:none session is
            # gone while opencode (still busy in the map) holds the spot.
            resp_oc = await r_oc
            assert resp_oc.status_code == 200, (
                f"N2a opencode served after mytool released: {resp_oc.text[:120]}"
            )
            d = await until(
                env.monitor,
                lambda d: (
                    find(d, "k0-MT") is None
                    and (s := find(d, "k0-OC")) is not None
                    and s["spot"] == "held"
                ),
                15,
                "N2b kind:none released, opencode still held",
            )
            mock_delete(port, "k0-OC")
            await until(env.monitor, lambda d: find(d, "k0-OC") is None, 10, "N2c cleanup")
    finally:
        env.stop()
