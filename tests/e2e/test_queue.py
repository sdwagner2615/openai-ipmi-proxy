"""E2E parity suite: faithful port of scripts/test_queue.py T1-T21, T23, T24.

Tests run in definition order against one shared environment (the `env`
fixture); the settle sleeps between sections are part of the parity
contract (spot expiry 4s, busy window 2s, poll 1s, mock delay 2s).
"""

import asyncio
import os
import signal
import time

import httpx
import pytest

pytestmark = pytest.mark.e2e

CHAT = "/v1/chat/completions"
BODY = {"model": "mock", "messages": [{"role": "user", "content": "hi"}], "stream": False}


def client(env, name: str) -> httpx.AsyncClient:
    return httpx.AsyncClient(base_url=f"http://127.0.0.1:{env.proxies[name].port}", timeout=30)


async def test_t1_known_client_status_keeps_spot(env):
    env.mock_set("ses-A", "busy")
    async with client(env, "base") as c:
        r = asyncio.create_task(c.post(CHAT, json=BODY, headers={"x-opencode-session": "ses-A"}))
        await asyncio.sleep(0.7)
        s = env.find_session(await env.monitor("base"), "ses-A")
        assert s and s["status"] == "busy" and s["spot"] == "held" and s["inflight"] == 1, (
            f"T1a in-flight request is busy with spot held: {s}"
        )
        resp = await r
        assert (
            resp.status_code == 200 and resp.json()["choices"][0]["message"]["content"] == "done"
        ), f"T1b request succeeded: {resp.text[:200]}"
        await asyncio.sleep(6)  # > SESSION_EXPIRY(4): time alone must not release the spot
        s = env.find_session(await env.monitor("base"), "ses-A")
        assert s is not None and s["spot"] == "held" and s["status"] == "busy", (
            f"T1c client-reported busy keeps spot after expiry window: {s}"
        )
        env.mock_set("ses-A", "idle")
        await asyncio.sleep(7)  # idle + SESSION_EXPIRY + ticks
        assert env.find_session(await env.monitor("base"), "ses-A") is None, (
            "T1d idle client releases spot and session is removed"
        )


async def test_t2_fifo_queue_second_session_waits(env):
    env.mock_set("ses-B", "busy")
    env.mock_set("ses-C", "busy")
    async with client(env, "base") as c:
        rb = asyncio.create_task(c.post(CHAT, json=BODY, headers={"x-opencode-session": "ses-B"}))
        await asyncio.sleep(0.5)
        rc = asyncio.create_task(c.post(CHAT, json=BODY, headers={"x-opencode-session": "ses-C"}))
        await asyncio.sleep(0.5)
        d = await env.monitor("base")
        sb, sc = env.find_session(d, "ses-B"), env.find_session(d, "ses-C")
        assert (
            sb
            and sb["inflight"] == 1
            and sc
            and sc["waiting"] == 1
            and sc["queue_position"] == 1
            and sb["position"] < sc["position"]
        ), f"T2a B running, C queued at position 1: {sb} / {sc}"
        await rb
        await asyncio.sleep(1)
        sc = env.find_session(await env.monitor("base"), "ses-C")
        assert sc is not None and sc["waiting"] == 1, (
            f"T2b C still waiting while B stays client-busy: {sc}"
        )
        env.mock_set("ses-B", "idle")
        resp_c = await rc
        assert resp_c.status_code == 200, (
            f"T2c C promoted after B's spot released: {resp_c.text[:200]}"
        )
        env.mock_set("ses-C", "idle")
        await asyncio.sleep(7)


async def test_t3_per_session_cap_serializes(env):
    async with client(env, "serial") as c:
        rd1 = asyncio.create_task(c.post(CHAT, json=BODY, headers={"x-opencode-session": "ses-D"}))
        await asyncio.sleep(0.7)
        rd2 = asyncio.create_task(c.post(CHAT, json=BODY, headers={"x-opencode-session": "ses-D"}))
        await asyncio.sleep(0.5)
        sd = env.find_session(await env.monitor("serial"), "ses-D")
        assert (
            sd is not None
            and sd["inflight"] == 1
            and sd["waiting"] == 1
            and sd["queue_position"] == 1
        ), f"T3a second request of same session waits (cap 0): {sd}"
        resp1, resp2 = await rd1, await rd2
        assert resp1.status_code == 200 and resp2.status_code == 200, (
            "T3b both serialized requests succeed"
        )
        await asyncio.sleep(7)


async def test_t4_unknown_clients_use_busy_window(env):
    async with client(env, "base") as c:
        p1 = await c.post(CHAT, json=BODY, headers={"x-session-id": "pyagent-1"})
        assert p1.status_code == 200, f"T4a unknown-client request ok: {p1.text[:200]}"
        t0 = time.monotonic()
        p2 = asyncio.create_task(c.post(CHAT, json=BODY, headers={"x-session-id": "pyagent-2"}))
        await asyncio.sleep(0.6)
        d = await env.monitor("base")
        s1, s2 = env.find_session(d, "pyagent-1"), env.find_session(d, "pyagent-2")
        assert (
            s1
            and s1["status"] == "busy"
            and s1["spot"] == "held"
            and s2
            and s2["queue_position"] == 1
        ), f"T4b pyagent-1 busy (window), pyagent-2 queued: {s1} / {s2}"
        resp2 = await p2
        wait_s = time.monotonic() - t0
        assert resp2.status_code == 200 and wait_s > 4, (
            f"T4c pyagent-2 ran only after window+expiry: waited {wait_s:.1f}s"
        )
        await asyncio.sleep(7)


async def test_t5_client_hang_up_removes_queued_request(env):
    env.mock_set("ses-E", "busy")
    async with client(env, "base") as c:
        e1 = await c.post(CHAT, json=BODY, headers={"x-opencode-session": "ses-E"})
        assert e1.status_code == 200, f"T5a E running: {e1.text[:120]}"
    try:
        async with httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{env.proxies['base'].port}", timeout=1.2
        ) as short:
            await short.post(CHAT, json=BODY, headers={"x-opencode-session": "ses-F"})
        hung_up = False
    except httpx.TimeoutException:
        hung_up = True
    assert hung_up, "T5b client actually timed out/hung up"
    await asyncio.sleep(3)
    d = await env.monitor("base")
    assert (
        env.find_session(d, "ses-F") is None
        and len(d["sessions"]) == 1
        and env.find_session(d, "ses-E") is not None
    ), f"T5c queued request removed after hang-up: {d['sessions']}"
    env.mock_set("ses-E", "idle")
    await asyncio.sleep(7)


async def test_t6_unknown_api_allowed_through(env):
    async with client(env, "base") as c:
        r = await c.get("/custom/thing", params={"x": "1"})
        assert r.status_code == 200 and r.json().get("echo") is True, (
            f"T6a unknown path proxied: {r.text[:200]}"
        )
        d = await env.monitor("base")
        u = d["unknown"]
        assert any(
            u0["target_url"] == f"http://127.0.0.1:{env.target_port}/custom/thing" for u0 in u
        ), f"T6b listed in unknown-sessions with target URL: {u}"
        long_ua = "very-long-agent/" + "x" * 400
        r = await c.get("/custom/long/" + "y" * 120, headers={"User-Agent": long_ua})
        assert r.status_code == 200, f"T6c long UA + long path proxied: {r.status_code}"
        d = await env.monitor("base")
        u = d["unknown"]
        assert any("x" * 100 in u0["id"] and "y" * 100 in u0["target_url"] for u0 in u), (
            f"T6d long UA + long path listed: {str(u)[:400]}"
        )
        page = (await c.get("/monitor")).text
        assert 'class="tablewrap"' in page and "td.wrap" in page and "td.nw" in page, (
            "T6e monitor page has overflow guard + wrap cells"
        )


async def test_t7_block_policy_and_queue_timeout(env):
    env.mock_set("ses-G", "busy")
    async with client(env, "block") as c:
        r = await c.get("/custom/thing")
        assert r.status_code == 403 and r.json()["error"]["code"] == "unknown_api_blocked", (
            f"T7a unknown path blocked with 403: {r.text[:200]}"
        )
        g = await c.post(CHAT, json=BODY, headers={"x-opencode-session": "ses-G"})
        assert g.status_code == 200, f"T7b G running (holds the spot): {g.text[:120]}"
        t0 = time.monotonic()
        h = await c.post(CHAT, json=BODY, headers={"x-opencode-session": "ses-H"})
        wait_s = time.monotonic() - t0
        assert (
            h.status_code == 504
            and h.json()["error"]["code"] == "queue_timeout"
            and 2.5 < wait_s < 10
        ), f"T7c H dropped with 504 after QUEUE_TIMEOUT: {h.status_code} after {wait_s:.1f}s"
        assert env.find_session(await env.monitor("block"), "ses-H") is None, (
            "T7d H removed from the queue"
        )
    env.mock_set("ses-G", "idle")
    await asyncio.sleep(7)


async def test_t8_target_down_requests_wait_no_503(env):
    # Kill the mock target: the BMC still reports On, so this is a pure
    # readiness wait (the queue manager keeps the cycle alive).
    os.killpg(os.getpgid(env.target.pid), signal.SIGTERM)
    env.target.wait(timeout=5)
    t0 = time.monotonic()
    async with client(env, "base") as c:
        ri = asyncio.create_task(c.post(CHAT, json=BODY, headers={"x-opencode-session": "ses-I"}))
        await asyncio.sleep(3)
        si = env.find_session(await env.monitor("base"), "ses-I")
        assert si is not None and si["waiting"] == 1 and si["queue_position"] == 1, (
            f"T8a request held in queue while target is off: {si}"
        )
        env.target = env.spawn_target()
        await env.wait_http(f"http://127.0.0.1:{env.target_port}/health")
        resp_i = await ri
        total = time.monotonic() - t0
        assert resp_i.status_code == 200 and total > 3, (
            f"T8b no 503; served once target is healthy: {resp_i.status_code} after {total:.1f}s"
        )
    env.mock_set("ses-I", "idle")
    await asyncio.sleep(7)


async def test_t9_monitor_page(env):
    async with client(env, "base") as c:
        r = await c.get("/monitor")
        assert r.status_code == 200 and "Relay Monitor" in r.text, "T9a /monitor serves HTML"


async def test_t10_sse_streaming_passes_through(env):
    sbody = {**BODY, "stream": True}
    async with client(env, "base") as c:
        sr = await c.post(CHAT, json=sbody, headers={"x-opencode-session": "ses-J"})
        assert sr.status_code == 200 and "text/event-stream" in sr.headers.get(
            "content-type", ""
        ), f"T10a stream response is SSE: {sr.status_code} {sr.headers.get('content-type')}"
        assert "tok0" in sr.text and "tok4" in sr.text and "data: [DONE]" in sr.text, (
            f"T10b stream carries chunks and [DONE]: {sr.text[:200]}"
        )
        await asyncio.sleep(7)


async def test_t11_anthropic_profile_and_body_field(env):
    abody = {
        "model": "claude-mock",
        "messages": [{"role": "user", "content": "hi"}],
        "metadata": {"user_id": "anth-ses-1"},
    }
    async with client(env, "base") as c:
        ar = await c.post("/v1/messages", json=abody, headers={"x-api-key": "sk-mock"})
        assert ar.status_code == 200, (
            f"T11a anthropic path proxied: {ar.status_code} {ar.text[:200]}"
        )
        sa = env.find_session(await env.monitor("base"), "anth-ses-1")
        assert sa is not None and sa["api"] == "anthropic" and sa["client"] == "unknown", (
            f"T11b session id from metadata.user_id, api=anthropic: {sa}"
        )
        await asyncio.sleep(7)


async def test_t12_manual_spot_release(env):
    env.mock_set("ses-K", "busy")
    async with client(env, "base") as c:
        kr = await c.post(CHAT, json=BODY, headers={"x-opencode-session": "ses-K"})
        assert kr.status_code == 200, f"T12a K running: {kr.text[:120]}"
        lr = asyncio.create_task(c.post(CHAT, json=BODY, headers={"x-opencode-session": "ses-L"}))
        await asyncio.sleep(0.6)
        sl = env.find_session(await env.monitor("base"), "ses-L")
        assert sl is not None and sl["waiting"] == 1 and sl["queue_position"] == 1, (
            f"T12b L queued while K stays client-busy: {sl}"
        )
        rel = await c.post("/monitor/release", json={"client": "opencode", "session": "ses-K"})
        assert rel.status_code == 200 and rel.json().get("released") is True, (
            f"T12c release endpoint frees K's spot: {rel.text[:120]}"
        )
        t0 = time.monotonic()
        resp_l = await lr
        wait_s = time.monotonic() - t0
        assert resp_l.status_code == 200 and wait_s < 6, (
            f"T12d L promoted promptly after release: {resp_l.status_code} after {wait_s:.1f}s"
        )
        bad = await c.post("/monitor/release", json={"client": "opencode", "session": "ses-ghost"})
        assert bad.status_code == 404, (
            f"T12e releasing a non-spot-holding session -> 404: {bad.status_code} {bad.text[:120]}"
        )
    env.mock_set("ses-L", "idle")
    await asyncio.sleep(7)


async def test_t13_opencode_via_gated_session_header(env):
    # Busy is set before the request (like T1): a real opencode keeps its
    # running session in the status map as busy across our response, and the
    # mock only carries an entry once set - a report polled between the
    # response and the set would be a spurious "absent = idle".
    env.mock_set("ses-O", "busy")
    oc_headers = {"X-Session-Id": "ses-O", "User-Agent": "opencode/1.18.23"}
    async with client(env, "base") as c:
        orr = await c.post(CHAT, json=BODY, headers=oc_headers)
        assert orr.status_code == 200, (
            f"T13a X-Session-Id + opencode UA request ok: {orr.text[:200]}"
        )
        so = env.find_session(await env.monitor("base"), "ses-O")
        assert so is not None and so["client"] == "opencode", (
            f"T13b identified as client=opencode, not unknown: {so}"
        )
        env.mock_set("ses-O", "busy")
        await asyncio.sleep(6)  # > SESSION_EXPIRY(4): per-directory poll must keep it busy
        so = env.find_session(await env.monitor("base"), "ses-O")
        assert so is not None and so["spot"] == "held" and so["status"] == "busy", (
            f"T13c directory-aware poll keeps spot held (client busy): {so}"
        )
        env.mock_set("ses-O", "idle")
        await asyncio.sleep(7)
        assert env.find_session(await env.monitor("base"), "ses-O") is None, (
            "T13d idle report releases the X-Session-Id session"
        )


async def test_t14_atomic_mode_one_inflight_at_a_time(env):
    async with client(env, "atomic") as c:
        p1 = asyncio.create_task(c.post(CHAT, json=BODY, headers={"x-session-id": "ses-P"}))
        await asyncio.sleep(0.5)
        q1 = asyncio.create_task(c.post(CHAT, json=BODY, headers={"x-session-id": "ses-Q"}))
        await asyncio.sleep(0.5)
        p2 = asyncio.create_task(c.post(CHAT, json=BODY, headers={"x-session-id": "ses-P"}))
        await asyncio.sleep(0.5)
        d = await env.monitor("atomic")
        sp, sq = env.find_session(d, "ses-P"), env.find_session(d, "ses-Q")
        assert (
            sq is not None
            and sq["waiting"] == 1
            and sq["queue_position"] == 1
            and sq["spot"] == "none"
        ), f"T14a Q queued despite a free spot (atomic: one in-flight max): {sq}"
        assert (
            sp is not None
            and sp["inflight"] == 1
            and sp["waiting"] == 1
            and sp["queue_position"] == 2
        ), f"T14b P's 2nd request queued despite P holding a spot: {sp}"
        t_p2 = time.monotonic()
        resp_q = await q1
        t_q_done = time.monotonic()
        resp_p2 = await p2
        t_p2_done = time.monotonic()
        await p1
        ok = resp_q.status_code == 200 and resp_p2.status_code == 200
        assert ok and t_q_done < t_p2_done, (
            f"T14c Q's request ran before P's 2nd (FIFO alternation): "
            f"q {t_q_done:.1f} p2 {t_p2_done:.1f}"
        )
        assert resp_p2.status_code == 200 and (t_p2_done - t_p2) > 3, (
            f"T14d P's 2nd waited out both P1 and Q1: waited {t_p2_done - t_p2:.1f}s"
        )
        await asyncio.sleep(7)
        d = await env.monitor("atomic")
        assert len(d["sessions"]) == 0, (
            f"T14e all spots released after idle expiry: {d['sessions']}"
        )


async def test_t15_parallel_mode_two_spots(env):
    async with client(env, "parallel") as c:
        r1 = asyncio.create_task(c.post(CHAT, json=BODY, headers={"x-session-id": "ses-R"}))
        await asyncio.sleep(0.5)
        s1 = asyncio.create_task(c.post(CHAT, json=BODY, headers={"x-session-id": "ses-S"}))
        await asyncio.sleep(1.0)
        s2 = asyncio.create_task(c.post(CHAT, json=BODY, headers={"x-session-id": "ses-S"}))
        # S's two requests overlap only while the first is still running, so
        # poll for the overlap instead of taking one snapshot.
        sr = ss = None
        t_wait = time.monotonic()
        while time.monotonic() - t_wait < 4.0:
            d = await env.monitor("parallel")
            sr, ss = env.find_session(d, "ses-R"), env.find_session(d, "ses-S")
            if ss is not None and ss["inflight"] == 2:
                break
            await asyncio.sleep(0.2)
        assert ss is not None and ss["inflight"] == 2 and ss["spot"] == "held", (
            f"T15a S has 2 in-flight at once (parallel): {ss}"
        )
        assert sr is not None and sr["inflight"] == 1 and sr["spot"] == "held", (
            f"T15b both sessions hold spots at the same time: {sr} / {ss}"
        )
        await r1, await s1, await s2
        await asyncio.sleep(7)


async def test_t16_sub_agent_shares_parent_spot(env):
    env.mock_set("ses-PP", "busy")
    async with client(env, "base") as c:
        ppr = await c.post(CHAT, json=BODY, headers={"x-opencode-session": "ses-PP"})
        assert ppr.status_code == 200, (
            f"T16a parent running, holding the only spot: {ppr.text[:120]}"
        )
        d = await env.monitor("base")
        sp = env.find_session(d, "ses-PP")
        assert sp is not None and sp["spot"] == "held" and d["config"]["active_sessions"] == 1, (
            f"T16b parent holds the spot (client busy): {sp}"
        )
        env.mock_parent("ses-PC", "ses-PP")
        t0 = time.monotonic()
        pcr = asyncio.create_task(c.post(CHAT, json=BODY, headers={"x-opencode-session": "ses-PC"}))
        await asyncio.sleep(1.5)
        d = await env.monitor("base")
        sp, sc = env.find_session(d, "ses-PP"), env.find_session(d, "ses-PC")
        assert (
            sp is not None
            and sp["spot"] == "held"
            and sc is not None
            and sc["spot"] == "shared"
            and (sc["inflight"] + sc["waiting"]) >= 1
        ), f"T16c child listed on the parent's spot (shared): {sp} / {sc}"
        assert d["config"]["active_sessions"] == 1, (
            f"T16d still only one spot in use overall: {d['config']['active_sessions']}"
        )
        resp_c = await pcr
        wait_s = time.monotonic() - t0
        assert resp_c.status_code == 200 and wait_s < 5, (
            f"T16e child ran without waiting for its own spot: {wait_s:.1f}s"
        )
        env.mock_set("ses-PC", "busy")
        env.mock_set("ses-PP", "idle")
        await asyncio.sleep(7)
        assert env.find_session(await env.monitor("base"), "ses-PP") is None, (
            "T16f parent spot released after idle"
        )
        pcr2 = await c.post(CHAT, json=BODY, headers={"x-opencode-session": "ses-PC"})
        assert pcr2.status_code == 200, (
            f"T16g child request ok after parent release: {pcr2.text[:120]}"
        )
        sc = env.find_session(await env.monitor("base"), "ses-PC")
        assert sc is not None and sc["spot"] == "held", (
            f"T16h child now holds a spot of its own: {sc}"
        )
        env.mock_set("ses-PC", "idle")
        await asyncio.sleep(7)


async def test_t17_immediate_idle_release(env):
    async with client(env, "immediate") as c:
        ra = await c.post(CHAT, json=BODY, headers={"x-opencode-session": "ses-IA"})
        env.mock_set("ses-IA", "idle")
        assert ra.status_code == 200, f"T17a request ok: {ra.text[:120]}"
        await asyncio.sleep(3.5)  # < SESSION_EXPIRY(4): the cooldown would still hold it
        d = await env.monitor("immediate")
        assert env.find_session(d, "ses-IA") is None, (
            f"T17b fresh idle report released the spot before the cooldown: {d['sessions']}"
        )
        rb = await c.post(CHAT, json=BODY, headers={"x-opencode-session": "ses-IB"})
        assert rb.status_code == 200, (
            f"T17c second request ok (status never reported): {rb.text[:120]}"
        )
        await asyncio.sleep(
            5
        )  # old behavior (stale-busy grace + cooldown) would still hold the spot
        d = await env.monitor("immediate")
        assert env.find_session(d, "ses-IB") is None, (
            f"T17d absent from the map is a confirmed idle: {d['sessions']}"
        )
        assert d["config"].get("immediate_idle_release") is True, (
            f"T17e monitor config shows the flag: {d['config'].get('immediate_idle_release')}"
        )


async def test_t18_immediate_idle_release_off_restores_cooldown(env):
    async with client(env, "cooldown") as c:
        rc = await c.post(CHAT, json=BODY, headers={"x-opencode-session": "ses-ID"})
        env.mock_set("ses-ID", "idle")
        assert rc.status_code == 200, f"T18a request ok: {rc.text[:120]}"
        await asyncio.sleep(3.5)  # < SESSION_EXPIRY(4): with the flag on it would be gone by now
        sd = env.find_session(await env.monitor("cooldown"), "ses-ID")
        assert sd is not None and sd["spot"] == "held", (
            f"T18b fresh idle still waits out the cooldown (flag off): {sd}"
        )
        await asyncio.sleep(4)
        d = await env.monitor("cooldown")
        assert env.find_session(d, "ses-ID") is None, (
            f"T18c ...and is released after expiry: {d['sessions']}"
        )
        assert d["config"].get("immediate_idle_release") is False, (
            f"T18d monitor config shows the flag off: {d['config'].get('immediate_idle_release')}"
        )


async def test_t19_absent_from_map_is_confirmed_idle(env):
    async with client(env, "immediate") as c:
        r19a = asyncio.create_task(
            c.post(CHAT, json=BODY, headers={"x-opencode-session": "ses-NA"})
        )
        await asyncio.sleep(0.7)
        r19b = asyncio.create_task(
            c.post(CHAT, json=BODY, headers={"x-opencode-session": "ses-NB"})
        )
        await asyncio.sleep(0.7)
        d = await env.monitor("immediate")
        s19a, s19b = env.find_session(d, "ses-NA"), env.find_session(d, "ses-NB")
        assert (
            s19a is not None
            and s19a["spot"] == "held"
            and s19b is not None
            and s19b["waiting"] == 1
            and s19b["queue_position"] == 1
        ), f"T19a A running (absent from map), B queued behind it: {s19a} / {s19b}"
        t0 = time.monotonic()
        resp19a = await r19a
        resp19b = await r19b
        wait_b = time.monotonic() - t0
        assert resp19a.status_code == 200 and resp19b.status_code == 200 and wait_b < 10, (
            f"T19b A's absent-idle released the spot promptly and B ran: "
            f"a={resp19a.status_code} b={resp19b.status_code} after {wait_b:.1f}s"
        )
        await asyncio.sleep(3)  # let both sessions' spots settle (B also absent -> released)


async def test_t20_absent_idle_flips_immediately_cooldown_applies(env):
    async with client(env, "cooldown") as c:
        r20 = asyncio.create_task(c.post(CHAT, json=BODY, headers={"x-opencode-session": "ses-OC"}))
        await asyncio.sleep(0.7)
        s20 = env.find_session(await env.monitor("cooldown"), "ses-OC")
        assert s20 is not None and s20["status"] == "busy" and s20["spot"] == "held", (
            f"T20a in-flight request shows busy: {s20}"
        )
        await r20
        await asyncio.sleep(2)  # > poll interval, < SESSION_EXPIRY(4)
        s20 = env.find_session(await env.monitor("cooldown"), "ses-OC")
        assert s20 is not None and s20["status"] == "idle" and s20["spot"] == "held", (
            f"T20b absent session is idle right after the response (no stale-busy grace): {s20}"
        )
        assert (
            s20 is not None
            and s20["spot_releases_in"] is not None
            and 0 < s20["spot_releases_in"] <= 4
        ), f"T20c spot counts down the cooldown: {s20}"
        await asyncio.sleep(4)
        d = await env.monitor("cooldown")
        assert env.find_session(d, "ses-OC") is None, (
            f"T20d released after SESSION_EXPIRY (flag off): {d['sessions']}"
        )


async def test_t21_waiting_for_input_releases_spot(env):
    async with client(env, "cooldown") as c:
        r21a = asyncio.create_task(
            c.post(CHAT, json=BODY, headers={"x-opencode-session": "ses-WP"})
        )
        await asyncio.sleep(0.7)
        env.mock_set("ses-WP", "busy")  # opencode stays "busy" in the map while blocked
        env.mock_permission("ses-WP")
        r21b = asyncio.create_task(
            c.post(CHAT, json=BODY, headers={"x-opencode-session": "ses-WQ"})
        )
        # The poller records the pending permission as client_status "waiting"
        # (opencode keeps the session "busy" in its status map while blocked).
        # The spot itself is released on the next tick once the in-flight
        # request ends, so the observable proof is the client_status, which
        # is visible for the whole window.
        saw_waiting = None
        t_wait = time.monotonic()
        while time.monotonic() - t_wait < 4.0:
            sw = env.find_session(await env.monitor("cooldown"), "ses-WP")
            if sw is not None and sw["client_status"] == "waiting" and sw["spot"] == "held":
                saw_waiting = sw
                break
            await asyncio.sleep(0.2)
        assert saw_waiting is not None and saw_waiting["client_status_detail"] == "permission", (
            f"T21a session listed as waiting (permission) with its spot held: {saw_waiting}"
        )
        t0 = time.monotonic()
        resp21a = await r21a
        resp21b = await r21b
        wait_b = time.monotonic() - t0
        # With the flag off the cooldown alone would hold the spot until
        # idle+4s, so B finishing within ~6s proves the waiting-for-input
        # release fired, not the cooldown.
        assert resp21a.status_code == 200 and resp21b.status_code == 200 and wait_b < 6, (
            f"T21b waiting-for-input released the spot before the cooldown and B ran: "
            f"a={resp21a.status_code} b={resp21b.status_code} after {wait_b:.1f}s"
        )
        env.mock_permission("ses-WP", pending=False)  # the user answers the prompt
        r21c = await c.post(CHAT, json=BODY, headers={"x-opencode-session": "ses-WP"})
        assert r21c.status_code == 200, (
            f"T21c answered session re-enters the queue and runs: {r21c.text[:120]}"
        )
        env.mock_set("ses-WP", "idle")
        env.mock_set("ses-WQ", "idle")
        await asyncio.sleep(7)
        d = await env.monitor("cooldown")
        assert env.find_session(d, "ses-WP") is None and env.find_session(d, "ses-WQ") is None, (
            f"T21d all spots released afterwards: {d['sessions']}"
        )


async def test_t23_monitor_controls(env):
    async with client(env, "base") as c:
        d = await env.monitor("base")
        assert (
            d["config"].get("shutdown_enabled") is True
            and d["config"].get("target_read_timeout") == 0
            and d["config"].get("idle_timeout") == 3600
        ), f"T23a defaults: shutdown on, read timeout none (0), idle timeout shown: {d['config']}"
        page = (await c.get("/monitor")).text
        assert (
            'id="shutdown-toggle"' in page
            and 'id="read-timeout"' in page
            and 'id="timeout-apply"' in page
        ), "T23b monitor page wires the toggle + timeout input"
        r = await c.post("/monitor/shutdown", json={"enabled": False})
        d = await env.monitor("base")
        assert (
            r.status_code == 200
            and r.json().get("shutdown_enabled") is False
            and d["config"].get("shutdown_enabled") is False
        ), f"T23c toggle off is applied: {r.status_code} {d['config'].get('shutdown_enabled')}"
        bad = await c.post("/monitor/shutdown", json={"enabled": "yes"})
        assert bad.status_code == 400, (
            f"T23d non-bool toggle body -> 400: {bad.status_code} {bad.text[:120]}"
        )
        r = await c.post("/monitor/shutdown", json={"enabled": True})
        d = await env.monitor("base")
        assert r.status_code == 200 and d["config"].get("shutdown_enabled") is True, (
            f"T23e toggle back on: {r.status_code} {d['config'].get('shutdown_enabled')}"
        )
        r = await c.post("/monitor/timeout", json={"read_timeout": 5})
        d = await env.monitor("base")
        assert r.status_code == 200 and d["config"].get("target_read_timeout") == 5, (
            f"T23f read timeout set to 5: {r.status_code} {d['config'].get('target_read_timeout')}"
        )
        bad = None
        for bad_body in ({"read_timeout": -1}, {"read_timeout": "abc"}, {"read_timeout": True}):
            bad = await c.post("/monitor/timeout", json=bad_body)
            if bad.status_code != 400:
                break
        assert bad.status_code == 400, (
            f"T23g invalid read-timeout bodies -> 400: {bad.status_code} {bad.text[:120]}"
        )
        r = await c.post("/monitor/timeout", json={"read_timeout": 0})
        d = await env.monitor("base")
        assert r.status_code == 200 and d["config"].get("target_read_timeout") == 0, (
            f"T23h read timeout back to none (0): {r.status_code}"
        )


async def test_t24_target_read_timeout(env):
    async with client(env, "readtimeout") as c:
        t0 = time.monotonic()
        r = await c.post(CHAT, json=BODY, headers={"x-session-id": "ses-TD"})
        wait_s = time.monotonic() - t0
        assert r.status_code == 502 and 0.8 < wait_s < 6, (
            f"T24a 502 at the read timeout: {r.status_code} after {wait_s:.1f}s"
        )
        r = await c.post("/monitor/timeout", json={"read_timeout": 0})
        d = await env.monitor("readtimeout")
        assert r.status_code == 200 and d["config"].get("target_read_timeout") == 0, (
            f"T24b runtime 0 = no timeout: {r.status_code} {d['config'].get('target_read_timeout')}"
        )
        t0 = time.monotonic()
        r = await c.post(CHAT, json=BODY, headers={"x-session-id": "ses-TD"})
        wait_s = time.monotonic() - t0
        assert r.status_code == 200 and 1.8 < wait_s < 10, (
            f"T24c 2s generation succeeds once lifted: {r.status_code} after {wait_s:.1f}s"
        )
