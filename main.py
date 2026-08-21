import os
import asyncio
import time
import httpx
import logging
from fastapi import FastAPI, Request, Response, BackgroundTasks
from fastapi.responses import JSONResponse, StreamingResponse
from dotenv import load_dotenv
from contextlib import asynccontextmanager

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
BOOT_WAIT_TIMEOUT = int(os.getenv("BOOT_WAIT_TIMEOUT", 300))
# Kill switch for the idle auto-shutdown. Power-on still works when disabled;
# it only stops the proxy from ever shutting the workstation down.
SHUTDOWN_ENABLED = os.getenv("SHUTDOWN_ENABLED", "true").lower() in ("1", "true", "yes", "on")

# Global Client
# Using a single AsyncClient globally enables connection pooling, which is critical
# for a proxy service to minimize latency and avoid socket exhaustion.
http_client: httpx.AsyncClient = None

# State
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
        httpx.Response: The result of the Redfish API call.
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
        httpx.Response: The result of the Redfish API call.
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

async def idle_monitor():
    """
    Background task that shuts down the server after a period of inactivity.

    The server is only shut down if the proxy manages its power lifecycle
    (state["manage_power_with_proxy"]). A server that was turned on manually
    and never used through the proxy is never powered off by this monitor.
    """
    while True:
        await asyncio.sleep(60)
        # Auto-shutdown disabled via SHUTDOWN_ENABLED: power-on still works,
        # we just never take the server down.
        if not SHUTDOWN_ENABLED:
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

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI lifespan handler for async resource setup and teardown.
    """
    global http_client
    # verify=False is required because most IPMI/BMC interfaces use self-signed certificates.
    http_client = httpx.AsyncClient(verify=False)
    
    await sync_state()
    
    monitor_task = asyncio.create_task(idle_monitor())
    logger.info("Idle monitor started.")
    yield
    
    await http_client.aclose()
    monitor_task.cancel()
    logger.info("Idle monitor stopped.")

app = FastAPI(lifespan=lifespan)

@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def proxy(request: Request, path: str):
    """
    OpenAI-compatible proxy endpoint that manages server power state.

    This endpoint forwards requests to the target AI server if it is healthy.
    If the server is off, it triggers a power-on and polls the health endpoint
    for a configurable timeout before giving up.
    """
    state["last_request_time"] = time.monotonic()
    logger.debug(f"Request received for {path}, resetting idle timer.")
    # Any request routed through the proxy means the proxy is now serving this
    # server, so it adopts power management (even if the server was already on).
    # This is what makes the idle monitor allowed to shut it down later.
    state["manage_power_with_proxy"] = True
    
    # Initial health check and potential power-on trigger
    if not await check_health():
        now = time.monotonic()
        if now - state["last_power_on_attempt"] > state["power_on_cooldown"]:
            await power_on()
            state["last_power_on_attempt"] = now
        
        # Polling loop: wait for the server to become healthy within the BOOT_WAIT_TIMEOUT window
        start_poll = time.monotonic()
        while (time.monotonic() - start_poll) < BOOT_WAIT_TIMEOUT:
            await asyncio.sleep(2)
            if await check_health():
                logger.info(f"Server became healthy after {time.monotonic() - start_poll:.1f}s polling.")
                break
        
        # If still unhealthy after polling, return the loading error
        if not await check_health():
            return JSONResponse(
                status_code=503,
                content={
                    "error": {
                        "message": "The model is still loading. Please retry in a few moments.",
                        "type": "server_error",
                        "param": None,
                        "code": "model_loading"
                    }
                }
            )

    # Proceed to proxy the request now that the server is confirmed healthy
    url = f"{TARGET_SERVER_URL}/{path}"
    body = await request.body()
    headers = dict(request.headers)
    # Remove host header to prevent the target server from rejecting the request due to host mismatch.
    headers.pop("host", None)

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
                logger.debug(f"Request for {path} finished.")

        return StreamingResponse(
            stream_generator(),
            status_code=response.status_code,
            headers=dict(response.headers)
        )

    except Exception as e:
        logger.error(f"Proxy error: {e}")
        return JSONResponse(status_code=502, content={"error": f"Proxy error: {str(e)}"})
