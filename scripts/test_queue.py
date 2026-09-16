"""
Local integration test for the session queue.

Spawns (all on 127.0.0.1, non-default ports, no docker, no real hardware):
  - a mock LLM target            (port 8100)
  - a mock opencode status server (port 8101)
  - the proxy under test         (port 8123, + 8124/8125 for alternate config)

Run:  venv/bin/python scripts/test_queue.py
"""

import asyncio
import os
import signal
import socket
import subprocess
import sys
import time

import httpx

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOGDIR = "/tmp/opencode/proxy-test"
os.makedirs(LOGDIR, exist_ok=True)

TARGET_PORT, STATUS_PORT = 8100, 8101
PROXY_PORT, PROXY2_PORT, PROXY3_PORT = 8123, 8124, 8125

BASE_ENV = {
    # Fake BMC: power-on attempts fail fast with connection refused and can
    # never touch real hardware. Explicit vars also shield us from a local
    # .env (load_dotenv does not override existing environment).
    "IPMI_HOST": "127.0.0.1",
    "IPMI_USER": "test",
    "IPMI_PASS": "test",
    "TARGET_SERVER_URL": f"http://127.0.0.1:{TARGET_PORT}",
    "HEALTH_PATH": "/health",
    "OPENCODE_STATUS_PORT": str(STATUS_PORT),
    "CONCURRENT_SESSIONS": "1",
    "CONCURRENT_SESSION_REQUESTS": "-1",
    "UNKNOWN_API_POLICY": "allow",
    "SESSION_EXPIRY": "4",
    "CLIENT_BUSY_WINDOW": "2",
    "CLIENT_STATUS_POLL": "1",
    "QUEUE_TIMEOUT": "0",
    "IDLE_TIMEOUT": "3600",
    "SHUTDOWN_ENABLED": "true",
}

processes = []
results = []


def spawn(name, args, env):
    log = open(f"{LOGDIR}/{name}.log", "ab")
    p = subprocess.Popen(
        args,
        cwd=REPO,
        env={**os.environ, **env},
        stdout=log,
        stderr=subprocess.STDOUT,
        preexec_fn=os.setsid,
    )
    processes.append((name, p))
    print(f"  spawned {name} (pid {p.pid})")
    return p


async def wait_http(url, timeout=20):
    async with httpx.AsyncClient() as c:
        t0 = time.time()
        while time.time() - t0 < timeout:
            try:
                r = await c.get(url, timeout=1.0)
                if r.status_code < 500:
                    return True
            except Exception:
                pass
            await asyncio.sleep(0.3)
    raise TimeoutError(f"{url} not ready")


async def monitor(port=PROXY_PORT):
    async with httpx.AsyncClient() as c:
        r = await c.get(f"http://127.0.0.1:{port}/monitor/data", timeout=3)
        r.raise_for_status()
        return r.json()


def find_session(data, sid, port=None):
    for s in data["sessions"]:
        if s["session"] == sid:
            return s
    return None


def mock_set(status_sid, status_type):
    return httpx.post(
        f"http://127.0.0.1:{STATUS_PORT}/set",
        params={"sid": status_sid, "type": status_type},
        timeout=3,
    )


def check(name, cond, extra=""):
    results.append((name, bool(cond)))
    print(f"  {'PASS' if cond else 'FAIL'}: {name}" + (f"  [{extra}]" if extra and not cond else ""))


CHAT = "/v1/chat/completions"
BODY = {"model": "mock", "messages": [{"role": "user", "content": "hi"}], "stream": False}


def check_ports_free(ports):
    for port in ports:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                s.bind(("127.0.0.1", port))
            except OSError:
                print(f"port {port} is already in use - aborting")
                sys.exit(2)


async def main():
    py = sys.executable
    skip_proxy = os.getenv("SKIP_PROXY") == "1"
    # In SKIP_PROXY mode the proxies run externally (e.g. docker), so only
    # the mock ports need to be free.
    ports = [TARGET_PORT, STATUS_PORT] if skip_proxy else [
        TARGET_PORT, STATUS_PORT, PROXY_PORT, PROXY2_PORT, PROXY3_PORT
    ]
    check_ports_free(ports)
    print("== spawning mocks ==")
    spawn("mock-target", [py, "scripts/mock_target.py"], {"MOCK_TARGET_PORT": str(TARGET_PORT), "MOCK_DELAY": "2"})
    spawn("mock-status", [py, "scripts/mock_opencode_status.py"], {"MOCK_STATUS_PORT": str(STATUS_PORT)})
    await wait_http(f"http://127.0.0.1:{TARGET_PORT}/health")
    await wait_http(f"http://127.0.0.1:{STATUS_PORT}/session/status")

    if skip_proxy:
        print("== SKIP_PROXY=1: using externally-launched proxies ==")
    else:
        print("== spawning proxies ==")
        spawn("proxy1", [py, "-m", "uvicorn", "main:app", "--host", "127.0.0.1", "--port", str(PROXY_PORT)], dict(BASE_ENV))
        spawn(
            "proxy2",
            [py, "-m", "uvicorn", "main:app", "--host", "127.0.0.1", "--port", str(PROXY2_PORT)],
            {**BASE_ENV, "QUEUE_TIMEOUT": "3", "UNKNOWN_API_POLICY": "block"},
        )
        spawn(
            "proxy3",
            [py, "-m", "uvicorn", "main:app", "--host", "127.0.0.1", "--port", str(PROXY3_PORT)],
            {**BASE_ENV, "CONCURRENT_SESSION_REQUESTS": "0"},
        )
    await wait_http(f"http://127.0.0.1:{PROXY_PORT}/monitor/data")
    await wait_http(f"http://127.0.0.1:{PROXY2_PORT}/monitor/data")
    await wait_http(f"http://127.0.0.1:{PROXY3_PORT}/monitor/data")

    client = httpx.AsyncClient(base_url=f"http://127.0.0.1:{PROXY_PORT}", timeout=30)

    print("== T1: known-client status keeps the spot held ==")
    mock_set("ses-A", "busy")
    r = asyncio.create_task(client.post(CHAT, json=BODY, headers={"x-opencode-session": "ses-A"}))
    await asyncio.sleep(0.7)
    d = await monitor()
    s = find_session(d, "ses-A")
    check("T1a in-flight request is busy with spot held", s and s["status"] == "busy" and s["spot"] == "held" and s["inflight"] == 1, str(s))
    resp = await r
    check("T1b request succeeded", resp.status_code == 200 and resp.json()["choices"][0]["message"]["content"] == "done", resp.text[:200])
    await asyncio.sleep(6)  # > SESSION_EXPIRY(4): time alone must not release the spot
    d = await monitor()
    s = find_session(d, "ses-A")
    check("T1c client-reported busy keeps spot after expiry window", s is not None and s["spot"] == "held" and s["status"] == "busy", str(s))
    mock_set("ses-A", "idle")
    await asyncio.sleep(7)  # idle + SESSION_EXPIRY + ticks
    d = await monitor()
    check("T1d idle client releases spot and session is removed", find_session(d, "ses-A") is None)

    print("== T2: FIFO queue, second session waits for the spot ==")
    mock_set("ses-B", "busy")
    mock_set("ses-C", "busy")
    rb = asyncio.create_task(client.post(CHAT, json=BODY, headers={"x-opencode-session": "ses-B"}))
    await asyncio.sleep(0.5)
    rc = asyncio.create_task(client.post(CHAT, json=BODY, headers={"x-opencode-session": "ses-C"}))
    await asyncio.sleep(0.5)
    d = await monitor()
    sb, sc = find_session(d, "ses-B"), find_session(d, "ses-C")
    check("T2a B running, C queued at position 1", sb and sb["inflight"] == 1 and sc and sc["waiting"] == 1 and sc["queue_position"] == 1 and sb["position"] < sc["position"], f"{sb} / {sc}")
    await rb
    await asyncio.sleep(1)
    d = await monitor()
    sc = find_session(d, "ses-C")
    check("T2b C still waiting while B stays client-busy", sc is not None and sc["waiting"] == 1, str(sc))
    mock_set("ses-B", "idle")
    resp_c = await rc
    check("T2c C promoted after B's spot released", resp_c.status_code == 200, resp_c.text[:200])
    mock_set("ses-C", "idle")
    await asyncio.sleep(7)

    print("== T3: per-session cap 0 serializes requests of one session (proxy3) ==")
    client3 = httpx.AsyncClient(base_url=f"http://127.0.0.1:{PROXY3_PORT}", timeout=30)
    rd1 = asyncio.create_task(client3.post(CHAT, json=BODY, headers={"x-opencode-session": "ses-D"}))
    await asyncio.sleep(0.7)
    rd2 = asyncio.create_task(client3.post(CHAT, json=BODY, headers={"x-opencode-session": "ses-D"}))
    await asyncio.sleep(0.5)
    d = await monitor(PROXY3_PORT)
    sd = find_session(d, "ses-D")
    check("T3a second request of same session waits (cap 0)", sd is not None and sd["inflight"] == 1 and sd["waiting"] == 1 and sd["queue_position"] == 1, str(sd))
    resp1, resp2 = await rd1, await rd2
    check("T3b both serialized requests succeed", resp1.status_code == 200 and resp2.status_code == 200)
    await asyncio.sleep(7)

    print("== T4: unknown clients use the busy window ==")
    p1 = await client.post(CHAT, json=BODY, headers={"x-session-id": "pyagent-1"})
    check("T4a unknown-client request ok", p1.status_code == 200)
    t0 = time.monotonic()
    p2 = asyncio.create_task(client.post(CHAT, json=BODY, headers={"x-session-id": "pyagent-2"}))
    await asyncio.sleep(0.6)
    d = await monitor()
    s1, s2 = find_session(d, "pyagent-1"), find_session(d, "pyagent-2")
    check("T4b pyagent-1 busy (window), pyagent-2 queued", s1 and s1["status"] == "busy" and s1["spot"] == "held" and s2 and s2["queue_position"] == 1, f"{s1} / {s2}")
    resp2 = await p2
    wait_s = time.monotonic() - t0
    check("T4c pyagent-2 ran only after window+expiry", resp2.status_code == 200 and wait_s > 4, f"waited {wait_s:.1f}s")
    await asyncio.sleep(7)

    print("== T5: client hang-up removes a queued request ==")
    mock_set("ses-E", "busy")
    e1 = await client.post(CHAT, json=BODY, headers={"x-opencode-session": "ses-E"})
    check("T5a E running", e1.status_code == 200)
    try:
        async with httpx.AsyncClient(timeout=1.2) as short:
            await short.post(
                f"http://127.0.0.1:{PROXY_PORT}{CHAT}",
                json=BODY,
                headers={"x-opencode-session": "ses-F"},
            )
        hung_up = False
    except httpx.TimeoutException:
        hung_up = True
    check("T5b client actually timed out/hung up", hung_up)
    await asyncio.sleep(3)
    d = await monitor()
    check("T5c queued request removed after hang-up", find_session(d, "ses-F") is None and len(d["sessions"]) == 1 and find_session(d, "ses-E") is not None)
    mock_set("ses-E", "idle")
    await asyncio.sleep(7)

    print("== T6: unknown API allowed through unqueued ==")
    r = await client.get("/custom/thing", params={"x": "1"})
    check("T6a unknown path proxied", r.status_code == 200 and r.json().get("echo") is True, r.text[:200])
    d = await monitor()
    u = d["unknown"]
    check(
        "T6b listed in unknown-sessions with target URL",
        any(u0["target_url"] == f"http://127.0.0.1:{TARGET_PORT}/custom/thing" for u0 in u),
        str(u),
    )
    long_ua = "very-long-agent/" + "x" * 400
    r = await client.get("/custom/long/" + "y" * 120, headers={"User-Agent": long_ua})
    check("T6c long UA + long path proxied", r.status_code == 200, str(r.status_code))
    d = await monitor()
    u = d["unknown"]
    check(
        "T6d long UA + long path listed",
        any("x" * 100 in u0["id"] and "y" * 100 in u0["target_url"] for u0 in u),
        str(u)[:400],
    )
    page = (await client.get("/monitor")).text
    check(
        "T6e monitor page has overflow guard + wrap cells",
        'class="tablewrap"' in page and "td.wrap" in page and "td.nw" in page,
    )

    print("== T7: block policy + queue timeout (proxy2) ==")
    client2 = httpx.AsyncClient(base_url=f"http://127.0.0.1:{PROXY2_PORT}", timeout=30)
    r = await client2.get("/custom/thing")
    check("T7a unknown path blocked with 403", r.status_code == 403 and r.json()["error"]["code"] == "unknown_api_blocked", r.text[:200])
    mock_set("ses-G", "busy")
    g = await client2.post(CHAT, json=BODY, headers={"x-opencode-session": "ses-G"})
    check("T7b G running (holds the spot)", g.status_code == 200)
    t0 = time.monotonic()
    h = await client2.post(CHAT, json=BODY, headers={"x-opencode-session": "ses-H"})
    wait_s = time.monotonic() - t0
    check("T7c H dropped with 504 after QUEUE_TIMEOUT", h.status_code == 504 and h.json()["error"]["code"] == "queue_timeout" and 2.5 < wait_s < 10, f"{h.status_code} after {wait_s:.1f}s: {h.text[:120]}")
    d = await monitor(PROXY2_PORT)
    check("T7d H removed from the queue", find_session(d, "ses-H") is None)
    mock_set("ses-G", "idle")
    await asyncio.sleep(7)

    print("== T8: server off -> requests wait, no 503, single boot cycle ==")
    target = next(p for name, p in processes if name == "mock-target")
    os.killpg(os.getpgid(target.pid), signal.SIGTERM)
    target.wait(timeout=5)
    t0 = time.monotonic()
    ri = asyncio.create_task(client.post(CHAT, json=BODY, headers={"x-opencode-session": "ses-I"}))
    await asyncio.sleep(3)
    d = await monitor()
    si = find_session(d, "ses-I")
    check("T8a request held in queue while target is off", si is not None and si["waiting"] == 1 and si["queue_position"] == 1, str(si))
    spawn("mock-target2", [py, "scripts/mock_target.py"], {"MOCK_TARGET_PORT": str(TARGET_PORT), "MOCK_DELAY": "2"})
    await wait_http(f"http://127.0.0.1:{TARGET_PORT}/health")
    resp_i = await ri
    total = time.monotonic() - t0
    check("T8b no 503; served once target is healthy", resp_i.status_code == 200 and total > 3, f"{resp_i.status_code} after {total:.1f}s")
    mock_set("ses-I", "idle")
    await asyncio.sleep(7)

    print("== T9: monitor page ==")
    r = await client.get("/monitor")
    check("T9a /monitor serves HTML", r.status_code == 200 and "IPMI Proxy Monitor" in r.text)

    print("== T10: SSE streaming passes through the queue ==")
    sbody = {**BODY, "stream": True}
    sr = await client.post(CHAT, json=sbody, headers={"x-opencode-session": "ses-J"})
    check("T10a stream response is SSE", sr.status_code == 200 and "text/event-stream" in sr.headers.get("content-type", ""), f"{sr.status_code} {sr.headers.get('content-type')}")
    check("T10b stream carries chunks and [DONE]", "tok0" in sr.text and "tok4" in sr.text and "data: [DONE]" in sr.text, sr.text[:200])
    await asyncio.sleep(7)

    print("== T11: anthropic profile + metadata.user_id extraction ==")
    abody = {
        "model": "claude-mock",
        "messages": [{"role": "user", "content": "hi"}],
        "metadata": {"user_id": "anth-ses-1"},
    }
    ar = await client.post("/v1/messages", json=abody, headers={"x-api-key": "sk-mock"})
    check("T11a anthropic path proxied", ar.status_code == 200, f"{ar.status_code} {ar.text[:200]}")
    d = await monitor()
    sa = find_session(d, "anth-ses-1")
    check("T11b session id from metadata.user_id, api=anthropic", sa is not None and sa["api"] == "anthropic" and sa["client"] == "unknown", str(sa))
    await asyncio.sleep(7)

    print("== T12: manual spot release via /monitor/release ==")
    mock_set("ses-K", "busy")
    kr = await client.post(CHAT, json=BODY, headers={"x-opencode-session": "ses-K"})
    check("T12a K running", kr.status_code == 200, kr.text[:120])
    lr = asyncio.create_task(client.post(CHAT, json=BODY, headers={"x-opencode-session": "ses-L"}))
    await asyncio.sleep(0.6)
    d = await monitor()
    sl = find_session(d, "ses-L")
    check("T12b L queued while K stays client-busy", sl is not None and sl["waiting"] == 1 and sl["queue_position"] == 1, str(sl))
    rel = await client.post("/monitor/release", json={"client": "opencode", "session": "ses-K"})
    check("T12c release endpoint frees K's spot", rel.status_code == 200 and rel.json().get("released") is True, rel.text[:120])
    t0 = time.monotonic()
    resp_l = await lr
    wait_s = time.monotonic() - t0
    check("T12d L promoted promptly after manual release", resp_l.status_code == 200 and wait_s < 6, f"{resp_l.status_code} after {wait_s:.1f}s")
    bad = await client.post("/monitor/release", json={"client": "opencode", "session": "ses-ghost"})
    check("T12e releasing a non-spot-holding session -> 404", bad.status_code == 404, f"{bad.status_code} {bad.text[:120]}")
    mock_set("ses-L", "idle")
    await asyncio.sleep(7)

    print("== T13: opencode detected via X-Session-Id (non-hosted provider) ==")
    oc_headers = {"X-Session-Id": "ses-O", "User-Agent": "opencode/1.18.23"}
    orr = await client.post(CHAT, json=BODY, headers=oc_headers)
    check("T13a X-Session-Id + opencode UA request ok", orr.status_code == 200, orr.text[:200])
    d = await monitor()
    so = find_session(d, "ses-O")
    check("T13b identified as client=opencode, not unknown", so is not None and so["client"] == "opencode", str(so))
    mock_set("ses-O", "busy")
    await asyncio.sleep(6)  # > SESSION_EXPIRY(4): per-directory poll must keep it busy
    d = await monitor()
    so = find_session(d, "ses-O")
    check("T13c directory-aware poll keeps spot held (client busy)", so is not None and so["spot"] == "held" and so["status"] == "busy", str(so))
    mock_set("ses-O", "idle")
    await asyncio.sleep(7)
    d = await monitor()
    check("T13d idle report releases the X-Session-Id session", find_session(d, "ses-O") is None)

    await client.aclose()
    await client2.aclose()
    await client3.aclose()

    print()
    failed = [n for n, ok in results if not ok]
    print(f"== {len(results) - len(failed)}/{len(results)} checks passed ==")
    if failed:
        print("failed:", ", ".join(failed))
        for name, p in processes:
            log = f"{LOGDIR}/{name}.log"
            if os.path.exists(log):
                print(f"--- last lines of {name}.log ---")
                with open(log, "rb") as f:
                    f.seek(0, os.SEEK_END)
                    size = f.tell()
                    f.seek(max(0, size - 2000))
                    sys.stdout.write(f.read().decode(errors="replace"))
    return 1 if failed else 0


if __name__ == "__main__":
    try:
        rc = asyncio.run(main())
    finally:
        for name, p in processes:
            try:
                os.killpg(os.getpgid(p.pid), signal.SIGTERM)
            except Exception:
                pass
        for name, p in processes:
            try:
                p.wait(timeout=5)
            except Exception:
                try:
                    os.killpg(os.getpgid(p.pid), signal.SIGKILL)
                except Exception:
                    pass
        print("all test processes stopped")
    sys.exit(rc)
