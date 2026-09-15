"""
Mock OpenCode status server for local proxy tests.

  GET  /session/status         -> {sid: {"type": ...}} for all sessions
  POST /set?sid=X&type=busy    -> set a session's status
                                  (type: busy | idle | retry)

Env: MOCK_STATUS_PORT (default 8101)
"""

import os

import uvicorn
from fastapi import FastAPI, Query

PORT = int(os.getenv("MOCK_STATUS_PORT", "8101"))

app = FastAPI()
statuses: dict = {}


@app.get("/session/status")
async def session_status():
    return statuses


@app.post("/set")
async def set_status(sid: str = Query(...), type: str = Query(..., alias="type")):
    if type == "busy":
        statuses[sid] = {"type": "busy"}
    elif type == "idle":
        statuses[sid] = {"type": "idle"}
    elif type == "retry":
        statuses[sid] = {"type": "retry", "attempt": 1, "message": "mock", "next": 0}
    else:
        raise ValueError(f"unknown type {type}")
    return statuses


@app.delete("/set")
async def delete_status(sid: str = Query(...)):
    statuses.pop(sid, None)
    return statuses


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=PORT, log_level="warning")
