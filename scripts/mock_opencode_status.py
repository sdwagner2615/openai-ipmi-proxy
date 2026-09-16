"""
Mock OpenCode status server for local proxy tests.

Models the real opencode server's per-directory (per-instance) behavior:
  GET  /session/status?directory=<dir> -> {sid: {"type": ...}}
                                            ({} unless directory matches)
  GET  /session/{sid}                   -> {"id": sid, "directory": <dir>,
                                            "parentID": <p> (if sub-agent)}
  POST /set?sid=X&type=busy|idle|retry  -> set a session's status
  POST /parent?sid=X&parent=Y           -> mark X a sub-agent of Y
  DELETE /parent?sid=X                  -> clear X's parent

Env: MOCK_STATUS_PORT (default 8101), MOCK_STATUS_DIR (default "mock-dir")
"""

import os

import uvicorn
from fastapi import FastAPI, Query

PORT = int(os.getenv("MOCK_STATUS_PORT", "8101"))
MOCK_DIR = os.getenv("MOCK_STATUS_DIR", "mock-dir")

app = FastAPI()
statuses: dict = {}
parents: dict = {}


@app.get("/session/status")
async def session_status(directory: str = Query(default="")):
    # The real server only reports statuses for the requested directory.
    # (Declared before /session/{session_id} so "status" is not captured
    # as a session id.)
    if directory != MOCK_DIR:
        return {}
    return statuses


@app.get("/session/{session_id}")
async def session_info(session_id: str):
    # The real server resolves sessions across instances without a
    # directory parameter and reports the session's working directory.
    # Sub-agent sessions additionally carry a parentID.
    info = {"id": session_id, "directory": MOCK_DIR}
    if session_id in parents:
        info["parentID"] = parents[session_id]
    return info


@app.post("/parent")
async def set_parent(sid: str = Query(...), parent: str = Query(...)):
    parents[sid] = parent
    return parents


@app.delete("/parent")
async def delete_parent(sid: str = Query(...)):
    parents.pop(sid, None)
    return parents


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
