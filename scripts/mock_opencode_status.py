"""
Mock OpenCode status server for local proxy tests.

Models the real opencode server's per-directory (per-instance) behavior:
  GET  /session/status?directory=<dir> -> {sid: {"type": ...}}
                                             ({} unless directory matches;
                                              idle sessions are ABSENT)
  GET  /session/{sid}                   -> {"id": sid, "directory": <dir>,
                                             "parentID": <p> (if sub-agent)}
  GET  /permission?directory=<dir>      -> pending permission requests
  GET  /question?directory=<dir>        -> pending question requests
  POST /set?sid=X&type=busy|idle|retry  -> set a session's status
  DELETE /set?sid=X                     -> drop X from the status map
  POST /permission?sid=X                -> X has a pending permission
  DELETE /permission?sid=X              -> X's permission was answered
  POST /question?sid=X                  -> X has a pending question
  DELETE /question?sid=X                -> X's question was answered
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
pending_permissions: dict = {}
pending_questions: dict = {}


@app.get("/session/status")
async def session_status(directory: str = Query(default="")):
    # The real server only reports statuses for the requested directory.
    # (Declared before /session/{session_id} so "status" is not captured
    # as a session id.)
    if directory != MOCK_DIR:
        return {}
    return statuses


@app.get("/permission")
async def permission_list(directory: str = Query(default="")):
    if directory != MOCK_DIR:
        return []
    return [
        {"id": f"per_{sid}", "sessionID": sid, "permission": "edit"}
        for sid in pending_permissions
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
    if directory != MOCK_DIR:
        return []
    return [
        {"id": f"que_{sid}", "sessionID": sid, "questions": []}
        for sid in pending_questions
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
