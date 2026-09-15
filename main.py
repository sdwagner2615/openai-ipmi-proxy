import os
import asyncio
import time
import httpx
import logging
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from dotenv import load_dotenv
from contextlib import asynccontextmanager

from apis import detect_api, session_id_from_body
from clients import detect_client, StatusPoller
from session_queue import SessionQueue, QueueEntry, UnknownTracker
from monitor import build_data, HTML_PAGE

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("ipmi-proxy")

load_dotenv()

# Configuration
IPMI_HOST = os.getenv("IPMI_HOST")
IPMI_USER = os.getenv("IPMI_USER")
IPMI_PASS = os.getenv("IPMI_PASS")
TARGET_SERVER_URL = os.getenv("TARGET_SERVER_URL", "").rstrip("/")
# Path of the target's liveness endpoint. The proxy itself is path-transparent
# (it forwards whatever API the client speaks - OpenAI chat, Anthropic
# Messages, etc.), but the power-on decision depends on this one route, so it
# must match the target server (llama.cpp/vLLM: /health, LiteLLM:
# /health/liveliness).
HEALTH_PATH = "/" + os.getenv("HEALTH_PATH", "/health").lstrip("/")
IDLE_TIMEOUT = int(os.getenv("IDLE_TIMEOUT", 3600))
# Kill switch for the idle auto-shutdown. Power-on still works when disabled;
# it only stops the proxy from ever shutting the workstation down.
SHUTDOWN_ENABLED = os.getenv("SHUTDOWN_ENABLED", "true").lower() in ("1", "true", "yes", "on")

# --- Queuing configuration -------------------------------------------------
# How many distinct sessions may hold a spot (run against the target) at once.
CONCURRENT_SESSIONS = max(1, int(os.getenv("CONCURRENT_SESSIONS", "1")))
# Max in-flight requests per session: -1 unlimited (default), N a cap,
# 0 strictly serialized (one request at a time).
CONCURRENT_SESSION_REQUESTS = int(os.getenv("CONCURRENT_SESSION_REQUESTS", "-1"))
# Requests that match no known API: "allow" passes them through unqueued
# (and lists them on the monitor page), "block" rejects them with 403.
UNKNOWN_API_POLICY = os.getenv("UNKNOWN_API_POLICY", "allow").lower()
# Seconds a session stays idle (client-reported, or busy-window based for
# unknown clients) before its spot is surrendered and the session is
# forgotten. Future requests with the same id queue at the back as new.
SESSION_EXPIRY = max(1, int(os.getenv("SESSION_EXPIRY", "300")))
# Unknown clients have no status API: they are considered busy this long
# after their last request (a heuristic for in-progress tool calls).
CLIENT_BUSY_WINDOW = max(0, int(os.getenv("CLIENT_BUSY_WINDOW", "120")))
# Seconds between polls of known clients' status APIs.
CLIENT_STATUS_POLL = max(1, int(os.getenv("CLIENT_STATUS_POLL", "5")))
# Max seconds a request may wait in the queue before being dropped with a
# 504. 0 = no timeout (default): clients are expected to run with no
# timeout and simply wait for their turn.
QUEUE_TIMEOUT = max(0, int(os.getenv("QUEUE_TIMEOUT", "0")))
# Port of the opencode server probed on each client's source IP
# (GET /session/status). The client must bind it to a reachable interface.
OPENCODE_STATUS_PORT = int(os.getenv("OPENCODE_STATUS_PORT", "4096"))
# Optional basic-auth password for opencode servers (fixed user "opencode").
OPENCODE_SERVER_PASSWORD = os.getenv("OPENCODE_SERVER_PASSWORD", "")
# Generic fallback session headers, checked after a known client's own
# header and before API body fields. Comma separated, case-insensitive.
SESSION_ID_HEADERS = tuple(
    h.strip().lower()
    for h in os.getenv("SESSION_ID_HEADERS", "x-session-id").split(",")
    if h.strip()
)

# Global Client
# Using a single AsyncClient globally enables connection pooling, which is critical
# for a proxy service to minimize latency and avoid socket exhaustion.
http_client: httpx.AsyncClient = None

# Central state object to track the physical server status and activity across async tasks.
#
# Why time.monotonic() instead of time.time(): the idle timer must measure how long the
# proxy itself has been active, not wall-clock time. time.monotonic() (CLOCK_MONOTONIC on
# Linux) freezes while the host laptop is asleep and is immune to NTP step adjustments, so
# a long sleep cannot make the proxy think the server has been idle and shut it down on wake.
#
# manage_power_with_proxy: the proxy only ever powers the server OFF if it is managing the
# power lifecycle. It takes ownership when it powers the server ON, or when any request is
# routed through it (adopting an already-running server). A server turned on manually and
# never used through the proxy is left alone.
state = {
    "last_request_time": time.monotonic(),
    "is_powered_on": None,
    "is_healthy": None,
    "manage_power_with_proxy": False,
    "last_power_on_attempt": 0,
    "power_on_cooldown": 30,
    "discovered_system_path": "/redfish/v1/Systems/Self" # Hardcoded after discovery of BMC firmware behavior
}

# Queueing state.
queue = SessionQueue(
    max_spots=CONCURRENT_SESSIONS,
    max_inflight_per_session=CONCURRENT_SESSION_REQUESTS,
    busy_window=CLIENT_BUSY_WINDOW,
    session_expiry=SESSION_EXPIRY,
)
unknown_tracker = UnknownTracker()
status_poller: StatusPoller = None

PROXY_CONFIG = {
    "concurrent_sessions": CONCURRENT_SESSIONS,
    "concurrent_session_requests": CONCURRENT_SESSION_REQUESTS,
    "unknown_api_policy": UNKNOWN_API_POLICY,
    "session_expiry": SESSION_EXPIRY,
    "client_busy_window": CLIENT_BUSY_WINDOW,
    "client_status_poll": CLIENT_STATUS_POLL,
    "queue_timeout": QUEUE_TIMEOUT,
    "opencode_status_port": OPENCODE_STATUS_PORT,
}


async def redfish_request(method: str, endpoint: str, body: dict = None):
    """
    Executes an authenticated request to the MegaRAC Redfish API.

    Args:
        method (str): HTTP method (GET, POST, etc.)
        endpoint (str): Redfish API endpoint path
        body (dict, optional): JSON payload for POST requests

    Returns:
        httpx.Response: The response object if successful, None otherwise.
    """
    url = f"https://{IPMI_HOST}{endpoint}"
    auth = (IPMI_USER, IPMI_PASS)
    try:
        # Explicit timeout prevents the proxy from hanging if the BMC is unresponsive.
        timeout = httpx.Timeout(10.0)
        if method == "POST":
            response = await http_client.post(url, json=body, timeout=timeout, auth=auth)
        else:
            response = await http_client.get(url, timeout=timeout, auth=auth)

        if response.status_code >= 400:
            logger.error(f"IPMI API Error {response.status_code} during {method} {endpoint} (URL: {url}): {response.text}")

        return response
    except Exception as e:
        logger.error(f"IPMI Network Error during {method} {endpoint} (URL: {url}): {e}")
        return None


async def get_power_state():
    """
    Queries the BMC to determine if the server is currently powered on.

    Returns:
        bool: True if powered on, False if powered off, None if state is unknown.
    """
    path = state["discovered_system_path"]
    response = await redfish_request("GET", path)
    if response and response.status_code == 200:
        data = response.json()
        return data.get("PowerState") == "On"
    return None


async def power_on():
    """
    Issues a Redfish command to power on the server.

    Returns:
        httpx.Response: The result of the IPMI API call.
    """
    logger.info(f"Triggering IPMI Power On using {state['discovered_system_path']}...")
    endpoint = f"{state['discovered_system_path']}/Actions/ComputerSystem.Reset"
    res = await redfish_request("POST", endpoint, {"ResetType": "On"})
    if res and res.status_code in (200, 202, 204):
        state["is_powered_on"] = True
        # The proxy initiated this power-on, so it now owns the power lifecycle
        # and is allowed to shut the server down again after idle timeout.
        state["manage_power_with_proxy"] = True
    return res


async def power_off():
    """
    Issues a Redfish command for a graceful shutdown of the server.

    Returns:
        httpx.Response: The result of the IPMI API call.
    """
    logger.info(f"Triggering IPMI Graceful Shutdown using {state['discovered_system_path']}...")
    endpoint = f"{state['discovered_system_path']}/Actions/ComputerSystem.Reset"
    res = await redfish_request("POST", endpoint, {"ResetType": "GracefulShutdown"})
    if res and res.status_code in (200, 202, 204):
        state["is_powered_on"] = False
        # Ownership ends when the proxy shuts the server down. If it is turned
        # back on manually afterwards, the proxy must not shut it down again.
        state["manage_power_with_proxy"] = False
    return res


async def check_health():
    """
    Polls the target AI server's health endpoint.

    Returns:
        bool: True if the server is responsive and healthy, False otherwise.
    """
    url = f"{TARGET_SERVER_URL}{HEALTH_PATH}"
    try:
        # Short timeout to avoid blocking the main request flow.
        response = await http_client.get(url, timeout=httpx.Timeout(2.0))
        is_healthy = response.status_code == 200
        state["is_healthy"] = is_healthy
        if is_healthy:
            state["is_powered_on"] = True
        return is_healthy
    except Exception:
        state["is_healthy"] = False
        return False


async def sync_state():
    """
    Synchronizes the internal state with the actual hardware state at startup.
    """
    logger.info("Synchronizing current server state...")
    state["is_healthy"] = await check_health()
    state["is_powered_on"] = await get_power_state()

    if state["is_healthy"]:
        logger.info("Server state: ONLINE and HEALTHY")
    elif state["is_powered_on"]:
        logger.info("Server state: POWERED ON but NOT HEALTHY (Booting?)")
    elif state["is_powered_on"] is False:
        logger.info("Server state: POWERED OFF")
    else:
        logger.info("Server state: UNKNOWN")


def resolve_session_id(request: Request, body: bytes) -> tuple:
    """
    Identifies the session for a request. Returns (client_name, session_id)
    where client_name is a known client ("opencode") or "unknown".

    Precedence:
      1. a known client's own session header (x-opencode-session),
      2. a generic configured header (SESSION_ID_HEADERS),
      3. the API-native body field (OpenAI "user", Anthropic
         "metadata.user_id"),
      4. User-Agent + client source IP as a last resort.
    """
    lowered = {k.lower(): v for k, v in request.headers.items()}
    client_ip = request.client.host if request.client else "unknown"
    user_agent = lowered.get("user-agent", "")

    provider = detect_client(lowered)
    if provider is not None and lowered.get(provider.session_header):
        return provider.name, lowered[provider.session_header]
    for header in SESSION_ID_HEADERS:
        value = lowered.get(header)
        if value:
            return "unknown", value
    api = detect_api(request.url.path)
    value = session_id_from_body(api, body)
    if value is not None:
        return "unknown", value
    return "unknown", f"ua:{user_agent}|ip:{client_ip}"


async def forward_request(
    request: Request, path: str, body: bytes = None, entry: QueueEntry = None
):
    """
    Forwards a request to the target server and streams the response back
    (SSE-safe). When called with a queue entry, the entry is released
    exactly once when the response is finished.
    """
    if body is None:
        body = await request.body()
    headers = dict(request.headers)
    # Remove host header to prevent the target server from rejecting the request due to host mismatch.
    headers.pop("host", None)
    url = f"{TARGET_SERVER_URL}{path}"

    try:
        # We define the timeout on the Request object.
        # a read timeout of 300s is used to accommodate long LLM generation times.
        req = http_client.build_request(
            method=request.method,
            url=url,
            headers=headers,
            content=body,
            timeout=httpx.Timeout(None, read=300.0)
        )

        response = await http_client.send(req, stream=True)

    except Exception as e:
        if entry is not None:
            queue.release(entry, None)
        logger.error(f"Proxy error: {e}")
        return JSONResponse(status_code=502, content={"error": f"Proxy error: {str(e)}"})

    async def stream_generator():
        """
        Generator to forward raw bytes from the target server to the client.
        This enables SSE (Server-Sent Events) support for streaming LLM responses.
        """
        try:
            async for chunk in response.aiter_raw():
                yield chunk
        except httpx.ReadTimeout:
            logger.error("Read timeout occurred during streaming from AI server")
            yield b" [Error: Read Timeout] "
        except Exception as e:
            logger.error(f"Unexpected error during streaming: {e}")
            yield f" [Error: {str(e)}] ".encode()
        finally:
            # Ensure the connection is closed.
            await response.aclose()
            if entry is not None:
                queue.release(entry, response.status_code)
            logger.debug(f"Request for {path} finished.")

    return StreamingResponse(
        stream_generator(),
        status_code=response.status_code,
        headers=dict(response.headers)
    )


async def idle_monitor():
    """
    Background task that shuts down the server after a period of inactivity.

    The server is only shut down if the proxy manages its power lifecycle
    (state["manage_power_with_proxy"]). A server that was turned on manually
    and never used through the proxy is never powered off by this monitor.
    While the queue has work (queued requests or held spots) the machine is
    never taken down and the idle timer is restarted.
    """
    while True:
        await asyncio.sleep(60)
        # Auto-shutdown disabled via SHUTDOWN_ENABLED: power-on still works,
        # we just never take the server down.
        if not SHUTDOWN_ENABLED:
            continue
        if queue.has_activity():
            state["last_request_time"] = time.monotonic()
            continue
        elapsed = time.monotonic() - state["last_request_time"]
        if elapsed > IDLE_TIMEOUT:
            # Verify actual power state before attempting shutdown to avoid redundant API calls.
            actual_power = await get_power_state()
            if actual_power is True:
                if state["manage_power_with_proxy"]:
                    logger.info(f"Server idle for {elapsed:.0f}s. Actual state: ON. Shutting down...")
                    await power_off()
                else:
                    logger.info(f"Server idle for {elapsed:.0f}s but was powered on outside the proxy. Leaving it on.")
                # Reset timer to prevent immediate repeated shutdown attempts (or re-polls).
                state["last_request_time"] = time.monotonic()
            elif actual_power is False:
                logger.debug("Server already off, skipping shutdown.")
            else:
                logger.warning("Could not determine power state, skipping shutdown to be safe.")


async def queue_manager():
    """
    Background task driving the queue:

    1. polls known clients' status APIs (one call per client machine),
    2. recomputes session statuses and surrenders expired spots,
    3. while requests wait in the queue: wakes the machine (a single
       power-on, re-issued on the cooldown if it did not take) and polls
       the target's health every 2s until it comes up - queued requests
       simply wait, no 503s are returned,
    4. promotes waiting requests once the target is healthy.
    """
    last_health_check = 0.0
    while True:
        await asyncio.sleep(1.0)
        now = time.monotonic()

        # 1) Client status polling.
        bases: dict[str, list] = {}
        for session in queue.sessions.values():
            if session.client != "unknown":
                base = status_poller.base_url(session.client_ip)
                bases.setdefault(base, []).append(session)
        for base, sessions in bases.items():
            if not status_poller.due(base, now):
                continue
            data = await status_poller.fetch(base)
            status_poller.last_poll[base] = time.monotonic()
            if data is None:
                # Unreachable: tick() keeps each session's last-known status
                # for the grace period, then treats it as idle.
                continue
            for session in sessions:
                st = data.get(session.session_id)
                if not isinstance(st, dict):
                    continue
                stype = st.get("type")
                if stype not in ("busy", "idle", "retry"):
                    continue
                session.client_status = stype
                session.client_status_at = time.monotonic()
                session.client_status_detail = (
                    f"attempt {st.get('attempt', '?')}" if stype == "retry" else ""
                )

        # 2) Status recompute + spot surrender.
        for key in queue.tick(now):
            logger.info(
                f"Session {key[1]} ({key[0]}) idle for {SESSION_EXPIRY}s: spot released, session removed."
            )

        # 3) Wake the machine / promote requests while the queue is not empty.
        if queue.queue:
            if now - last_health_check >= 2.0:
                await check_health()
                last_health_check = now
            queue.healthy = state["is_healthy"]
            if not queue.healthy:
                # The first waiting request triggered the initial power-on
                # in the request handler; this keeps the single boot cycle
                # alive (re-issuing on cooldown) while later requests simply
                # wait their turn.
                if now - state["last_power_on_attempt"] > state["power_on_cooldown"]:
                    await power_on()
                    state["last_power_on_attempt"] = now
            else:
                queue._try_promote()

        # 4) Prune stale unknown-API entries.
        unknown_tracker.prune(now, SESSION_EXPIRY)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI lifespan handler for async resource setup and teardown.
    """
    global http_client, status_poller
    # verify=False is required because most IPMI/BMC interfaces use self-signed certificates.
    http_client = httpx.AsyncClient(verify=False)
    status_poller = StatusPoller(
        http_client,
        port=OPENCODE_STATUS_PORT,
        password=OPENCODE_SERVER_PASSWORD,
        poll_interval=CLIENT_STATUS_POLL,
    )

    await sync_state()
    queue.healthy = state["is_healthy"]

    monitor_task = asyncio.create_task(idle_monitor())
    manager_task = asyncio.create_task(queue_manager())
    logger.info(
        f"Queuing enabled: {CONCURRENT_SESSIONS} spot(s), per-session requests={CONCURRENT_SESSION_REQUESTS}, "
        f"unknown API policy={UNKNOWN_API_POLICY}, session expiry={SESSION_EXPIRY}s, queue timeout={QUEUE_TIMEOUT or 'none'}."
    )
    yield

    monitor_task.cancel()
    manager_task.cancel()
    await http_client.aclose()
    logger.info("Idle monitor stopped.")


app = FastAPI(lifespan=lifespan)


@app.get("/monitor")
async def monitor_page():
    """Simple self-contained monitoring page (polls /monitor/data)."""
    return HTMLResponse(HTML_PAGE)


@app.get("/monitor/data")
async def monitor_data():
    """JSON snapshot of configuration, sessions and unknown-API activity."""
    config = {**PROXY_CONFIG, "target_server_url": TARGET_SERVER_URL}
    return JSONResponse(build_data(queue, unknown_tracker, config, state))


@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def proxy(request: Request, path: str):
    """
    Queueing proxy endpoint.

    Requests matching a known API are identified as sessions (per client
    and API, see resolve_session_id), enqueued FIFO, and held until their
    session may run (a free spot and the per-session in-flight cap) and the
    target server is healthy. While the target is off the first waiting
    request powers it on and all waiting requests simply wait - clients are
    expected to run without a timeout. A client that hangs up while waiting
    is dropped from the queue.

    Requests matching no known API are passed through unqueued (and tracked
    on the monitor page) or rejected, depending on UNKNOWN_API_POLICY.
    """
    state["last_request_time"] = time.monotonic()
    # Any request routed through the proxy means the proxy is now serving this
    # server, so it adopts power management (even if the server was already on).
    state["manage_power_with_proxy"] = True

    full_path = "/" + path
    api = detect_api(full_path)

    if api is None:
        if UNKNOWN_API_POLICY != "allow":
            return JSONResponse(
                status_code=403,
                content={
                    "error": {
                        "message": f"Unknown API path '{full_path}' is blocked (UNKNOWN_API_POLICY=block)",
                        "type": "policy_error",
                        "param": None,
                        "code": "unknown_api_blocked"
                    }
                }
            )
        lowered = {k.lower(): v for k, v in request.headers.items()}
        client_ip = request.client.host if request.client else "unknown"
        unknown_tracker.record(
            client_ip,
            lowered.get("user-agent", ""),
            request.method,
            full_path,
            f"{TARGET_SERVER_URL}{full_path}",
        )
        return await forward_request(request, full_path)

    try:
        body = await request.body()
    except Exception as e:
        logger.error(f"Failed to read request body: {e}")
        return JSONResponse(status_code=400, content={"error": "Failed to read request body."})

    client_name, session_id = resolve_session_id(request, body)
    lowered = {k.lower(): v for k, v in request.headers.items()}
    client_ip = request.client.host if request.client else "unknown"
    session = queue.get_or_create_session(
        client_name, session_id, api.name, client_ip, lowered.get("user-agent", "")
    )
    entry = QueueEntry(
        session=session,
        path=full_path,
        body=body,
        request=request,
        enqueued_at=time.monotonic(),
    )

    # When this request is the first in line, the manager's health
    # bookkeeping may be stale (it only refreshes while the queue is
    # non-empty), so do a fresh check before enqueueing: a healthy target
    # promotes without waiting for the manager tick (same latency profile
    # as before queuing was added), and a dead one triggers the initial
    # power-on. While the target is down the queue manager owns the single
    # boot cycle; the cooldown below de-duplicates it with the manager's
    # re-issues.
    if not queue.queue:
        await check_health()
        queue.healthy = state["is_healthy"]
        if not queue.healthy:
            now = time.monotonic()
            if now - state["last_power_on_attempt"] > state["power_on_cooldown"]:
                await power_on()
                state["last_power_on_attempt"] = now

    queue.enqueue(entry)
    logger.debug(f"Session {session_id} ({client_name}/{api.name}) queued at position {len(queue.queue)}.")

    result = await queue.wait_for_slot(entry, QUEUE_TIMEOUT or None)
    if result != "ok":
        if entry.done:
            # Lost a race: the entry was promoted in the same instant the
            # client went away (or the timeout fired). The slot is ours, so
            # release it rather than abandoning.
            queue.release(entry, None)
        else:
            queue.abandon(entry)
        if result == "disconnected":
            logger.info(f"Client {client_ip} hung up while {session_id} was waiting in queue; dropping request.")
            return JSONResponse(status_code=503, content={"error": "Client disconnected while request was queued."})
        logger.warning(f"Session {session_id} waited {QUEUE_TIMEOUT}s in queue and timed out; dropping request.")
        return JSONResponse(
            status_code=504,
            content={
                "error": {
                    "message": f"Request was held in the queue for {QUEUE_TIMEOUT}s and timed out.",
                    "type": "server_error",
                    "param": None,
                    "code": "queue_timeout"
                }
            }
        )

    return await forward_request(request, full_path, body=body, entry=entry)
