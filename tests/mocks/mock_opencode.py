"""
Mock OpenCode status server for local proxy tests.

Models the real opencode server's per-directory (per-instance) behavior:
  GET  /session/status?directory=<dir> -> {sid: {"type": ...}}
                                               (idle sessions are ABSENT;
                                                only this directory's sessions)
  GET  /session/{sid}                   -> {"id": sid, "directory": <dir>,
                                               "parentID": <p> (if sub-agent)}
  GET  /permission?directory=<dir>      -> pending permission requests
  GET  /question?directory=<dir>        -> pending question requests
  POST /set?sid=X&type=busy|idle|retry[&directory=<dir>]
                                           -> set X's status (and directory;
                                              default: MOCK_STATUS_DIR)
  DELETE /set?sid=X                     -> drop X from the status map
  POST /permission?sid=X                -> X has a pending permission
  DELETE /permission?sid=X              -> X's permission was answered
  POST /question?sid=X                  -> X has a pending question
  DELETE /question?sid=X                -> X's question was answered
  POST /parent?sid=X&parent=Y           -> mark X a sub-agent of Y
  DELETE /parent?sid=X                  -> clear X's parent
  POST /offline                         -> every GET answers 503 (unreachable)
  DELETE /offline                       -> back online
  POST /fail?path=/question             -> that GET endpoint answers 503
  DELETE /fail?path=/question           -> endpoint back up (partial failure)
  GET  /requests                        -> per-endpoint GET counter (test hook)

Env: MOCK_STATUS_PORT (default 8101), MOCK_STATUS_DIR (default "mock-dir")
"""

import os

import uvicorn
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

PORT = int(os.getenv("MOCK_STATUS_PORT", "8101"))
MOCK_DIR = os.getenv("MOCK_STATUS_DIR", "mock-dir")

app = FastAPI()
statuses: dict = {}
dirs: dict = {}  # sid -> working directory (default: MOCK_DIR)
parents: dict = {}
pending_permissions: dict = {}
pending_questions: dict = {}
state = {"offline": False, "failing": {}}
counts: dict = {}


def _dir_of(sid: str) -> str:
    return dirs.get(sid, MOCK_DIR)


def _count(path: str, detail: str) -> None:
    key = f"{path}?{detail}"
    counts[key] = counts.get(key, 0) + 1


def _dead() -> bool:
    return state["offline"]


@app.get("/session/status")
async def session_status(directory: str = Query(default="")):
    # The real server only reports statuses for the requested directory.
    # (Declared before /session/{session_id} so "status" is not captured
    # as a session id.)
    if _dead():
        return JSONResponse(status_code=503, content={"error": "offline"})
    _count("/session/status", f"directory={directory}")
    return {sid: st for sid, st in statuses.items() if _dir_of(sid) == directory}


@app.get("/permission")
async def permission_list(directory: str = Query(default="")):
    if _dead() or state["failing"].get("/permission"):
        return JSONResponse(status_code=503, content={"error": "unavailable"})
    _count("/permission", f"directory={directory}")
    return [
        {"id": f"per_{sid}", "sessionID": sid, "permission": "edit"}
        for sid in pending_permissions
        if _dir_of(sid) == directory
    ]


@app.delete("/permission")
async def permission_clear(sid: str = Query(...)):
    pending_permissions.pop(sid, None)
    return pending_permissions


@app.post("/permission")
async def permission_set(sid: str = Query(...)):
    pending_permissions[sid] = True
    return pending_permissions


@app.get("/question")
async def question_list(directory: str = Query(default="")):
    if _dead() or state["failing"].get("/question"):
        return JSONResponse(status_code=503, content={"error": "unavailable"})
    _count("/question", f"directory={directory}")
    return [
        {"id": f"que_{sid}", "sessionID": sid, "questions": []}
        for sid in pending_questions
        if _dir_of(sid) == directory
    ]


@app.delete("/question")
async def question_clear(sid: str = Query(...)):
    pending_questions.pop(sid, None)
    return pending_questions


@app.post("/question")
async def question_set(sid: str = Query(...)):
    pending_questions[sid] = True
    return pending_questions


@app.get("/session/{session_id}")
async def session_info(session_id: str):
    # The real server resolves sessions across instances without a
    # directory parameter and reports the session's working directory.
    # Sub-agent sessions additionally carry a parentID.
    if _dead():
        return JSONResponse(status_code=503, content={"error": "offline"})
    _count("/session", session_id)
    info = {"id": session_id, "directory": _dir_of(session_id)}
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
async def set_status(
    sid: str = Query(...),
    type: str = Query(..., alias="type"),
    directory: str = Query(default=""),
):
    if directory:
        dirs[sid] = directory
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


@app.post("/offline")
async def go_offline():
    state["offline"] = True
    return {"offline": True}


@app.delete("/offline")
async def go_online():
    state["offline"] = False
    return {"offline": False}


@app.post("/fail")
async def fail_endpoint(path: str = Query(...)):
    state["failing"][path] = True
    return state["failing"]


@app.delete("/fail")
async def unfail_endpoint(path: str = Query(...)):
    state["failing"].pop(path, None)
    return state["failing"]


@app.get("/requests")
async def request_counts():
    return counts


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=PORT, log_level="warning")
