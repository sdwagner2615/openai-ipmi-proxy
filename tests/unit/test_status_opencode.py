"""OpencodeStatusSource: fetch contracts and normalization (D21, C2-C8)."""

import base64

import httpx
import pytest

from relay.clients import StatusReport
from relay.models import ClientConfig, MatchConfig, StatusConfig
from relay.status_opencode import OpencodeStatusSource, create_status_source

pytestmark = pytest.mark.unit


class OpencodeHandler:
    """Scriptable opencode status server (in-process MockTransport)."""

    def __init__(self):
        self.sessions = {
            "s1": {"directory": "/work/a", "parentID": None},
            "s2": {"directory": "/work/b", "parentID": "s1"},
        }
        self.statuses = {
            "/work/a": {"s1": {"type": "busy"}},
            "/work/b": {"s2": {"type": "retry", "attempt": 3, "message": "x", "next": 0}},
        }
        self.pending = {
            "/work/a": {},
            "/work/b": {"s2": "permission"},
        }
        self.fail = False
        self.calls: list[str] = []

    def handle(self, request: httpx.Request) -> httpx.Response:
        if self.fail:
            raise httpx.ConnectError("client unreachable")
        path = request.url.path
        params = dict(request.url.params)
        self.calls.append(f"{path}?{params}")
        directory = params.get("directory", "")
        if path.startswith("/session/status"):
            return httpx.Response(200, json=self.statuses.get(directory, {}))
        if path in ("/permission", "/question"):
            kind = "permission" if path == "/permission" else "question"
            items = [
                {"id": f"x_{sid}", "sessionID": sid, "kind": kind}
                for sid in self.pending.get(directory, {})
                if self.pending[directory][sid] == kind
            ]
            return httpx.Response(200, json=items)
        if path.startswith("/session/"):
            sid = path[len("/session/") :]
            info = self.sessions.get(sid)
            if info is None:
                return httpx.Response(404, json={"error": "not found"})
            body = {"id": sid, "directory": info["directory"]}
            if info["parentID"]:
                body["parentID"] = info["parentID"]
            return httpx.Response(200, json=body)
        return httpx.Response(404, json={"error": "not found"})


def make_source(handler: OpencodeHandler, **kwargs) -> OpencodeStatusSource:
    kwargs.setdefault("port", 4096)
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler.handle))
    return OpencodeStatusSource(client, **kwargs)


async def test_base_url_uses_the_clients_source_ip_and_port():
    source = make_source(OpencodeHandler(), port=4096)
    assert source.base_url("10.0.0.5") == "http://10.0.0.5:4096"


async def test_fetch_session_info_parses_directory_and_parent():
    handler = OpencodeHandler()
    source = make_source(handler)
    assert await source.fetch_session_info("http://c:4096", "s2") == ("/work/b", "s1")
    assert await source.fetch_session_info("http://c:4096", "s1") == ("/work/a", None)


async def test_fetch_session_info_unknown_or_unreachable_is_none():
    handler = OpencodeHandler()
    source = make_source(handler)
    assert await source.fetch_session_info("http://c:4096", "nope") is None
    handler.fail = True
    assert await source.fetch_session_info("http://c:4096", "s1") is None


async def test_fetch_statuses_normalizes_busy_retry_idle():
    handler = OpencodeHandler()
    handler.statuses["/work/a"] = {
        "s1": {"type": "busy"},
        "s3": {"type": "idle"},
        "s4": {"type": "retry", "attempt": 7, "message": "m", "next": 1},
    }
    source = make_source(handler)
    reports = await source.fetch_statuses("http://c:4096", "/work/a")
    assert reports["s1"] == StatusReport("busy", "")
    assert reports["s3"] == StatusReport("idle", "")
    assert reports["s4"] == StatusReport("retry", "attempt 7")


async def test_fetch_statuses_ignores_unrecognized_types():
    handler = OpencodeHandler()
    handler.statuses["/work/a"] = {"s1": {"type": "mystery"}, "s5": "garbage"}
    source = make_source(handler)
    assert await source.fetch_statuses("http://c:4096", "/work/a") == {}


async def test_fetch_statuses_unreachable_or_garbage_is_none():
    handler = OpencodeHandler()
    source = make_source(handler)
    handler.fail = True
    assert await source.fetch_statuses("http://c:4096", "/work/a") is None
    handler.fail = False
    handler.statuses["/work/a"] = "not a dict"
    assert await source.fetch_statuses("http://c:4096", "/work/a") is None


async def test_fetch_statuses_polls_the_requested_context():
    handler = OpencodeHandler()
    source = make_source(handler)
    await source.fetch_statuses("http://c:4096", "/work/b")
    assert "/session/status?{'directory': '/work/b'}" in handler.calls


async def test_fetch_pending_maps_sessions_to_their_kind():
    handler = OpencodeHandler()
    handler.pending["/work/a"] = {"s9": "question"}
    source = make_source(handler)
    assert await source.fetch_pending("http://c:4096", "/work/a") == {"s9": "question"}
    assert await source.fetch_pending("http://c:4096", "/work/b") == {"s2": "permission"}


async def test_fetch_pending_partial_failure_is_none():
    # A partial answer must never clear a session's "waiting" flag (C3):
    # one bad endpoint poisons the whole pending poll.
    handler = OpencodeHandler()
    source = make_source(handler)
    real_get = source.http_client.get

    async def flaky_get(url, **kwargs):
        if url.endswith("/question"):
            return httpx.Response(500, json={"error": "boom"})
        return await real_get(url, **kwargs)

    source.http_client.get = flaky_get
    assert await source.fetch_pending("http://c:4096", "/work/b") is None


async def test_basic_auth_uses_the_fixed_opencode_user():
    # opencode servers protected with a password use the fixed username
    # "opencode".
    seen = {}

    def capture(request: httpx.Request) -> httpx.Response:
        seen["auth"] = request.headers.get("authorization")
        return httpx.Response(200, json={})

    source = OpencodeStatusSource(
        httpx.AsyncClient(transport=httpx.MockTransport(capture)), port=4096, password="pw"
    )
    await source.fetch_statuses("http://c:4096", "/work/a")
    assert seen["auth"] == f"Basic {base64.b64encode(b'opencode:pw').decode()}"


async def test_no_auth_header_without_password():
    seen = {}

    def capture(request: httpx.Request) -> httpx.Response:
        seen["auth"] = request.headers.get("authorization")
        return httpx.Response(200, json={})

    source = OpencodeStatusSource(
        httpx.AsyncClient(transport=httpx.MockTransport(capture)), port=4096
    )
    await source.fetch_statuses("http://c:4096", "/work/a")
    assert seen["auth"] is None


async def test_resolve_context_caches_and_forget_gone_prunes():
    handler = OpencodeHandler()
    source = make_source(handler)
    lookups_before = len(handler.calls)
    assert await source.resolve_context("http://c:4096", "s1") == ("/work/a", None)
    assert await source.resolve_context("http://c:4096", "s2") == ("/work/b", "s1")
    # Cached: no further /session/{id} lookups.
    assert await source.resolve_context("http://c:4096", "s1") == ("/work/a", None)
    assert len(handler.calls) == lookups_before + 2
    # Only s1 survives: s2's caches are dropped.
    source.forget_gone("http://c:4096", {"s1"})
    assert source.parent_of("http://c:4096", "s2") is None
    assert await source.resolve_context("http://c:4096", "s2") == ("/work/b", "s1")


async def test_due_tracks_poll_interval():
    handler = OpencodeHandler()
    source = make_source(handler, poll_interval=5.0)
    assert source.due("http://c:4096", 100.0)
    source.last_poll["http://c:4096"] = 100.0
    assert not source.due("http://c:4096", 104.0)
    assert source.due("http://c:4096", 105.0)


def _client(**status_kwargs) -> ClientConfig:
    return ClientConfig(
        name="opencode",
        match=MatchConfig(ua_prefix="opencode/", session_headers=["x-opencode-session"]),
        status=StatusConfig(**status_kwargs),
    )


async def test_create_status_source_opencode_uses_config():
    client = httpx.AsyncClient()
    source = create_status_source(_client(port=7777, password="pw", poll_interval=9), client)
    assert isinstance(source, OpencodeStatusSource)
    assert source.port == 7777
    assert source.poll_interval == 9
    assert source.auth == ("opencode", "pw")


async def test_create_status_source_none_kind_returns_none():
    assert create_status_source(_client(kind="none"), httpx.AsyncClient()) is None
