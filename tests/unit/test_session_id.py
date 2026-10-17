"""Session-id resolution precedence (S1-S3, D22)."""

import json
from types import SimpleNamespace

import pytest

from relay.clients import provider_from_config
from relay.config import load_config
from relay.endpoints import EndpointRuntime, extract_body_field
from relay.models import ClientConfig, MatchConfig, StatusConfig

pytestmark = pytest.mark.unit


def make_request(headers: dict, client_ip: str = "9.9.9.9") -> SimpleNamespace:
    return SimpleNamespace(headers=headers, client=SimpleNamespace(host=client_ip))


def make_endpoint(tmp_path, body_fields) -> EndpointRuntime:
    import tempfile
    from pathlib import Path

    text = f"""
proxy:
  port: 8000
servers:
  - name: devbox
    type: noop
    power:
      initial_state: on
    service_url: http://127.0.0.1:8100
endpoints:
  - name: llm
    server: devbox
    path_prefix: /v1
    catch_all: true
    session_id_body_fields: [{", ".join(repr(f) for f in body_fields)}]
"""
    path = Path(tempfile.mkdtemp()) / "config.yaml"
    path.write_text(text)
    config = load_config(path)
    return EndpointRuntime(
        config.endpoints[0],
        server=SimpleNamespace(config=config.servers[0]),
        http_client=None,
        store=None,
        pollers={},
        providers=[],
        generic_headers=["x-session-id"],
        unknown_tracker=None,
    )


OPENCODE = [
    provider_from_config(
        ClientConfig(
            name="opencode",
            match=MatchConfig(
                ua_prefix="opencode/",
                session_headers=["x-opencode-session"],
                gated_session_headers=["x-session-affinity", "x-session-id"],
            ),
            status=StatusConfig(),
        )
    )
]


def resolve(endpoint, headers, body=b"{}") -> tuple:
    from relay.endpoints import resolve_session_id

    return resolve_session_id(make_request(headers), body, endpoint, OPENCODE, ["x-session-id"])


def test_white_glove_header_wins_over_everything(tmp_path):
    endpoint = make_endpoint(tmp_path, ["user"])
    client, sid = resolve(
        endpoint,
        {
            "x-opencode-session": "oc-1",
            "x-session-id": "generic-1",
            "user-agent": "opencode/1.0",
        },
        json.dumps({"user": "body-1"}).encode(),
    )
    assert (client, sid) == ("opencode", "oc-1")


def test_ua_gated_header_counts_with_matching_ua(tmp_path):
    endpoint = make_endpoint(tmp_path, ["user"])
    client, sid = resolve(
        endpoint,
        {"X-Session-Id": "oc-2", "User-Agent": "opencode/1.18.23"},
    )
    assert (client, sid) == ("opencode", "oc-2")


def test_ua_gated_header_ignored_with_other_ua(tmp_path):
    # S2: other tools sending X-Session-Id are not misattributed.
    endpoint = make_endpoint(tmp_path, ["user"])
    client, sid = resolve(
        endpoint,
        {"X-Session-Id": "generic-3", "User-Agent": "curl/8.0"},
    )
    assert (client, sid) == ("unknown", "generic-3")


def test_generic_header_beats_body_field(tmp_path):
    endpoint = make_endpoint(tmp_path, ["user", "metadata.user_id"])
    client, sid = resolve(
        endpoint,
        {"x-session-id": "generic-4", "user-agent": "curl/8.0"},
        json.dumps({"user": "body-4", "metadata": {"user_id": "meta-4"}}).encode(),
    )
    assert (client, sid) == ("unknown", "generic-4")


def test_body_fields_tried_in_order(tmp_path):
    endpoint = make_endpoint(tmp_path, ["user", "metadata.user_id"])
    client, sid = resolve(
        endpoint,
        {"user-agent": "curl/8.0"},
        json.dumps({"metadata": {"user_id": "meta-5"}}).encode(),
    )
    assert (client, sid) == ("unknown", "meta-5")
    client, sid = resolve(
        endpoint,
        {"user-agent": "curl/8.0"},
        json.dumps({"user": "body-5"}).encode(),
    )
    assert (client, sid) == ("unknown", "body-5")


def test_anthropic_style_dotted_field(tmp_path):
    endpoint = make_endpoint(tmp_path, ["metadata.user_id"])
    client, sid = resolve(
        endpoint,
        {"user-agent": "curl/8.0"},
        json.dumps({"metadata": {"user_id": "anth-6"}}).encode(),
    )
    assert (client, sid) == ("unknown", "anth-6")


def test_fallback_is_ua_and_ip(tmp_path):
    endpoint = make_endpoint(tmp_path, ["user"])
    client, sid = resolve(endpoint, {"user-agent": "curl/8.0"})
    assert (client, sid) == ("unknown", "ua:curl/8.0|ip:9.9.9.9")


def test_extract_body_field_tolerates_malformed_input():
    assert extract_body_field(b"not json", "user") is None
    assert extract_body_field(b"{}", "user") is None
    assert extract_body_field(json.dumps({"user": ""}).encode(), "user") is None
    assert extract_body_field(json.dumps({"user": 42}).encode(), "user") == "42"
    assert extract_body_field(json.dumps({"user": True}).encode(), "user") is None
    assert extract_body_field(json.dumps({"a": {"b": "x"}}).encode(), "a.b") == "x"
    assert extract_body_field(json.dumps({"a": [1]}).encode(), "a.b") is None
    assert extract_body_field(b"", "user") is None
