import os
import asyncio
import time
import httpx
import logging
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
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
IDLE_TIMEOUT = int(os.getenv("IDLE_TIMEOUT", 3600))

# State
state = {
    "last_request_time": time.time(),
    "is_powered_on": None,
    "is_healthy": None,
    "last_power_on_attempt": 0,
    "power_on_cooldown": 30,
    "discovered_system_path": "/redfish/v1/Systems/Self"
}

async def redfish_request(method: str, endpoint: str, body: dict = None):
    url = f"https://{IPMI_HOST}{endpoint}"
    auth = (IPMI_USER, IPMI_PASS)
    async with httpx.AsyncClient(verify=False, auth=auth) as client:
        try:
            if method == "POST":
                response = await client.post(url, json=body, timeout=10.0)
            else:
                response = await client.get(url, timeout=10.0)
            
            if response.status_code >= 400:
                logger.error(f"IPMI API Error {response.status_code} during {method} {endpoint} (URL: {url}): {response.text}")
            
            return response
        except Exception as e:
            logger.error(f"IPMI Network Error during {method} {endpoint} (URL: {url}): {e}")
            return None

async def get_power_state():
    path = state["discovered_system_path"]
    response = await redfish_request("GET", path)
    if response and response.status_code == 200:
        data = response.json()
        return data.get("PowerState") == "On"
    return None

async def power_on():
    logger.info(f"Triggering IPMI Power On using {state['discovered_system_path']}...")
    endpoint = f"{state['discovered_system_path']}/Actions/ComputerSystem.Reset"
    res = await redfish_request("POST", endpoint, {"ResetType": "On"})
    if res and res.status_code in (200, 204):
        state["is_powered_on"] = True
    return res

async def power_off():
    logger.info(f"Triggering IPMI Graceful Shutdown using {state['discovered_system_path']}...")
    endpoint = f"{state['discovered_system_path']}/Actions/ComputerSystem.Reset"
    res = await redfish_request("POST", endpoint, {"ResetType": "GracefulShutdown"})
    if res and res.status_code in (200, 204):
        state["is_powered_on"] = False
    return res

async def check_health():
    url = f"{TARGET_SERVER_URL}/health"
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url, timeout=2.0)
            is_healthy = response.status_code == 200
            state["is_healthy"] = is_healthy
            if is_healthy:
                state["is_powered_on"] = True
            return is_healthy
        except Exception:
            state["is_healthy"] = False
            return False

async def sync_state():
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
    while True:
        await asyncio.sleep(60)
        elapsed = time.time() - state["last_request_time"]
        if elapsed > IDLE_TIMEOUT:
            if state["is_powered_on"]:
                logger.info(f"Server idle for {elapsed:.0f}s. Shutting down...")
                await power_off()
                state["last_request_time"] = time.time()
            else:
                logger.debug("Server already off, skipping shutdown.")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Sync state at startup
    await sync_state()
    
    # Start background monitor
    monitor_task = asyncio.create_task(idle_monitor())
    logger.info("Idle monitor started.")
    yield
    monitor_task.cancel()
    logger.info("Idle monitor stopped.")

app = FastAPI(lifespan=lifespan)

@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def proxy(request: Request, path: str):
    state["last_request_time"] = time.time()
    
    if await check_health():
        # Proxy to Target Server
        url = f"{TARGET_SERVER_URL}/{path}"
        body = await request.body()
        headers = dict(request.headers)
        # Remove host header to avoid conflicts
        headers.pop("host", None)

        async with httpx.AsyncClient() as client:
            try:
                proxy_res = await client.request(
                    method=request.method,
                    url=url,
                    headers=headers,
                    content=body,
                    timeout=None # AI responses can take a long time
                )
                return Response(
                    content=proxy_res.content,
                    status_code=proxy_res.status_code,
                    headers=dict(proxy_res.headers)
                )
            except Exception as e:
                logger.error(f"Proxy error: {e}")
                return JSONResponse(status_code=502, content={"error": f"Proxy error: {str(e)}"})
    else:
        # Server is down or loading
        now = time.time()
        if now - state["last_power_on_attempt"] > state["power_on_cooldown"]:
            await power_on()
            state["last_power_on_attempt"] = now
            
        return JSONResponse(
            status_code=503,
            content={
                "error": {
                    "message": "model still loading",
                    "type": "server_error",
                    "param": None,
                    "code": "model_loading"
                }
            }
        )
