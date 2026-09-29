"""
Scriptable Redfish BMC mock for local proxy tests (no real hardware).

Models the MegaRAC-style Redfish surface the proxy talks to:
  GET  /redfish/v1/Systems/Self
       -> {"PowerState": "On" | "Off"}
  POST /redfish/v1/Systems/Self/Actions/ComputerSystem.Reset
       {"ResetType": "On"}               -> state On
       {"ResetType": "GracefulShutdown"} -> state Off
Test inspection / scripting:
  GET  /actions              -> {"actions": [...], "count": N}
  POST /set?state=on|off     -> script the power state directly (no action
                                 recorded)

Basic auth from the proxy is accepted and ignored.

Env: MOCK_BMC_PORT (default 8102), MOCK_BMC_INITIAL (default "on")
"""

import os

import uvicorn
from fastapi import FastAPI, Query, Request

PORT = int(os.getenv("MOCK_BMC_PORT", "8102"))
INITIAL = os.getenv("MOCK_BMC_INITIAL", "on").lower()

app = FastAPI()
_power_state = "On" if INITIAL == "on" else "Off"
actions: list = []


@app.get("/redfish/v1/Systems/Self")
async def system():
    return {"Id": "Self", "PowerState": _power_state}


@app.post("/redfish/v1/Systems/Self/Actions/ComputerSystem.Reset")
async def reset(request: Request):
    global _power_state
    body = await request.json()
    reset_type = body.get("ResetType")
    actions.append(reset_type)
    if reset_type == "On":
        _power_state = "On"
    elif reset_type in ("GracefulShutdown", "ForceOff", "Off"):
        _power_state = "Off"
    return {"status": "accepted"}


@app.get("/actions")
async def list_actions():
    return {"actions": actions, "count": len(actions)}


@app.post("/set")
async def set_state(state: str = Query(...)):
    global _power_state
    _power_state = "On" if state.lower() == "on" else "Off"
    return {"PowerState": _power_state}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=PORT, log_level="warning")
