"""
Scriptable fake EC2 API for local proxy tests (no real AWS, D10).

Two faces, both driven by the same one-instance state machine:

1. ``FakeEc2Client`` — an in-process stand-in for a boto3 EC2 client,
   injected into ``AwsEc2Backend`` by the unit tests (set ``state`` to any
   EC2 state; ``actions`` records issued actions; ``fail_next`` makes one
   call raise to simulate an unreachable API).

2. A standalone HTTP server (run as a subprocess by the e2e suite, like
   ``mock_bmc.py``) that speaks the minimal AWS Query protocol surface the
   relay talks to, so a REAL boto3 client can be pointed at it via the
   aws-ec2 power block's ``endpoint_url``:
       POST /?Action=DescribeInstances|StartInstances|StopInstances|
            TerminateInstances   -> EC2 Query-protocol XML
       GET  /state               -> {"state": "running"}
       GET  /actions             -> {"actions": [...], "count": N}
       POST /set?state=on|...    -> script the state directly (no action
                                    recorded)

State machine (one instance):
    stopped --start--> pending --(FAKE_EC2_BOOT_DELAY)--> running
    running --stop-->  stopping --(FAKE_EC2_STOP_DELAY)--> stopped
    (any)   --terminate--> terminated   (irreversible)

Env: FAKE_EC2_PORT (default 8103), FAKE_EC2_INITIAL (default "stopped"),
     FAKE_EC2_BOOT_DELAY (default 2.0), FAKE_EC2_STOP_DELAY (default 0.5),
     FAKE_EC2_INSTANCE_ID (default "i-fake")
"""

import asyncio
import os
from urllib.parse import parse_qs

import uvicorn
from fastapi import FastAPI, Query, Request, Response

PORT = int(os.getenv("FAKE_EC2_PORT", "8103"))
INITIAL = os.getenv("FAKE_EC2_INITIAL", "stopped").lower()
BOOT_DELAY = float(os.getenv("FAKE_EC2_BOOT_DELAY", "2.0"))
STOP_DELAY = float(os.getenv("FAKE_EC2_STOP_DELAY", "0.5"))
INSTANCE_ID = os.getenv("FAKE_EC2_INSTANCE_ID", "i-fake")

#: EC2 instance state -> protocol status code (cosmetic; the parser reads
#: the ``name`` element).
STATE_CODES = {
    "pending": 0,
    "running": 16,
    "shutting-down": 32,
    "terminated": 48,
    "stopping": 64,
    "stopped": 80,
}

NS = "http://ec2.amazonaws.com/doc/2016-11-15/"


class FakeEc2Client:
    """In-process stand-in for a boto3 EC2 client (unit tests)."""

    def __init__(self, *, instance_id: str = "i-fake", state: str = "stopped"):
        self.instance_id = instance_id
        self.state = state
        # Issued actions, in order: "start_instances" / "stop_instances" /
        # "terminate_instances" (reads are not recorded).
        self.actions: list[str] = []
        # Name of the next action to fail with a transport error.
        self.fail_next: str | None = None

    def _maybe_fail(self, action: str) -> None:
        if self.fail_next == action:
            self.fail_next = None
            raise ConnectionError(f"fake EC2 API unreachable during {action}")

    def _instance(self) -> dict:
        return {"InstanceId": self.instance_id, "State": {"Name": self.state}}

    def describe_instances(self, InstanceIds=None):
        self._maybe_fail("describe_instances")
        return {"Reservations": [{"Instances": [self._instance()]}]}

    def start_instances(self, InstanceIds=None):
        self._maybe_fail("start_instances")
        self.actions.append("start_instances")
        self.state = "pending"
        return {"Instances": [self._instance()]}

    def stop_instances(self, InstanceIds=None):
        self._maybe_fail("stop_instances")
        self.actions.append("stop_instances")
        self.state = "stopped"
        return {"Instances": [self._instance()]}

    def terminate_instances(self, InstanceIds=None):
        self._maybe_fail("terminate_instances")
        self.actions.append("terminate_instances")
        self.state = "terminated"
        return {"Instances": [self._instance()]}


# ---------------------------------------------------------------------------
# Subprocess server (e2e): the minimal EC2 Query protocol.
# ---------------------------------------------------------------------------

app = FastAPI()
_state = INITIAL
_actions: list[str] = []
_tasks: set[asyncio.Task] = set()
_request_id = 0


async def _transition(delay: float, src: str, dst: str) -> None:
    global _state
    await asyncio.sleep(delay)
    if _state == src:
        _state = dst


def _spawn(task: asyncio.Task) -> None:
    _tasks.add(task)
    task.add_done_callback(_tasks.discard)


def _instance_xml() -> str:
    return (
        f"<instanceId>{INSTANCE_ID}</instanceId>"
        f"<instanceState><code>{STATE_CODES.get(_state, -1)}</code>"
        f"<name>{_state}</name></instanceState>"
    )


def _response(action: str, *, reservation: bool = False) -> Response:
    global _request_id
    _request_id += 1
    if reservation:
        body = (
            "<reservationSet><item><instancesSet>"
            f"<item>{_instance_xml()}</item>"
            "</instancesSet></item></reservationSet>"
        )
    else:
        body = f"<instancesSet><item>{_instance_xml()}</item></instancesSet>"
    xml = (
        f'<{action}Response xmlns="{NS}">'
        f"<requestId>fake-{_request_id:04d}</requestId>{body}"
        f"</{action}Response>"
    )
    return Response(content=xml.encode("utf-8"), media_type="text/xml")


def _error(code: str, message: str) -> Response:
    xml = (
        f'<Response xmlns="{NS}"><Errors><Error>'
        f"<Code>{code}</Code><Message>{message}</Message>"
        f"</Error></Errors><RequestId>fake-error</RequestId></Response>"
    )
    return Response(content=xml.encode("utf-8"), media_type="text/xml", status_code=400)


@app.api_route("/{path:path}", methods=["POST"])
async def ec2_api(path: str, request: Request):
    """The EC2 Query endpoint: dispatches on the ``Action`` form field."""
    global _state
    # The Query protocol body is plain application/x-www-form-urlencoded;
    # parse it directly (no python-multipart dependency).
    body = (await request.body()).decode("utf-8")
    form = {k: v[0] for k, v in parse_qs(body).items()}
    action = str(form.get("Action", ""))
    if action == "DescribeInstances":
        return _response("DescribeInstances", reservation=True)
    if action == "StartInstances":
        _actions.append("start_instances")
        if _state == "stopped":
            _state = "pending"
            _spawn(asyncio.create_task(_transition(BOOT_DELAY, "pending", "running")))
        return _response("StartInstances")
    if action == "StopInstances":
        _actions.append("stop_instances")
        if _state == "running":
            _state = "stopping"
            _spawn(asyncio.create_task(_transition(STOP_DELAY, "stopping", "stopped")))
        return _response("StopInstances")
    if action == "TerminateInstances":
        _actions.append("terminate_instances")
        _state = "terminated"
        return _response("TerminateInstances")
    return _error("InvalidAction", f"unknown Action {action!r}")


@app.get("/state")
async def get_state():
    return {"state": _state}


@app.get("/actions")
async def get_actions():
    return {"actions": _actions, "count": len(_actions)}


@app.post("/set")
async def set_state(state: str = Query(...)):
    global _state
    _state = state
    return {"state": _state}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=PORT, log_level="warning")
