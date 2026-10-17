"""Session-id resolution precedence (S1-S3, D22)."""

import json

import pytest

from relay.clients import ClientRegistry, extract_body_field
from relay.models import ClientConfig, MatchConfig, StatusConfig

pytestmark = pytest.mark.unit

OPENCODE = ClientConfig(
    name="opencode",
    match=MatchConfig(
        ua_prefix="opencode/",
        session_headers=["x-opencode-session"],
        gated_session_headers=["x-session-affinity", "x-session-id"],
    ),
    status=StatusConfig(),
)


def resolve(
    headers: dict,
    body: bytes | None = None,
    body_fields: tuple[str, ...] = (),
    query: dict | None = None,
    client_ip: str = "9.9.9.9",
) -> tuple:
    registry = ClientRegistry([OPENCODE], ["x-session-id"])
    return registry.resolve(headers, client_ip, body=body, body_fields=body_fields, query=query)


def test_white_glove_header_wins_over_everything():
    client, sid = resolve(
        {
            "x-opencode-session": "oc-1",
            "x-session-id": "generic-1",
            "user-agent": "opencode/1.0",
        },
        json.dumps({"user": "body-1"}).encode(),
        body_fields=("user",),
    )
    assert (client, sid) == ("opencode", "oc-1")


def test_ua_gated_header_counts_with_matching_ua():
    client, sid = resolve({"X-Session-Id": "oc-2", "User-Agent": "opencode/1.18.23"})
    assert (client, sid) == ("opencode", "oc-2")


def test_ua_gated_header_ignored_with_other_ua():
    # S2: other tools sending X-Session-Id are not misattributed.
    client, sid = resolve({"X-Session-Id": "generic-3", "User-Agent": "curl/8.0"})
    assert (client, sid) == ("unknown", "generic-3")


def test_generic_header_beats_body_field():
    client, sid = resolve(
        {"x-session-id": "generic-4", "user-agent": "curl/8.0"},
        json.dumps({"user": "body-4", "metadata": {"user_id": "meta-4"}}).encode(),
        body_fields=("user", "metadata.user_id"),
    )
    assert (client, sid) == ("unknown", "generic-4")


def test_body_fields_tried_in_order():
    client, sid = resolve(
        {"user-agent": "curl/8.0"},
        json.dumps({"metadata": {"user_id": "meta-5"}}).encode(),
        body_fields=("user", "metadata.user_id"),
    )
    assert (client, sid) == ("unknown", "meta-5")
    client, sid = resolve(
        {"user-agent": "curl/8.0"},
        json.dumps({"user": "body-5"}).encode(),
        body_fields=("user", "metadata.user_id"),
    )
    assert (client, sid) == ("unknown", "body-5")


def test_anthropic_style_dotted_field():
    client, sid = resolve(
        {"user-agent": "curl/8.0"},
        json.dumps({"metadata": {"user_id": "anth-6"}}).encode(),
        body_fields=("metadata.user_id",),
    )
    assert (client, sid) == ("unknown", "anth-6")


def test_fallback_is_ua_and_ip():
    client, sid = resolve({"user-agent": "curl/8.0"}, body_fields=("user",))
    assert (client, sid) == ("unknown", "ua:curl/8.0|ip:9.9.9.9")


def test_ws_query_params_generic_fallback():
    client, sid = resolve({"user-agent": "curl/8.0"}, query={"x-session-id": "ws-2"})
    assert (client, sid) == ("unknown", "ws-2")


def test_headers_beat_ws_query_params():
    client, sid = resolve(
        {"x-opencode-session": "hdr-3", "user-agent": "opencode/1.0"},
        query={"x-session-id": "ws-3"},
    )
    assert (client, sid) == ("opencode", "hdr-3")


def test_query_params_ignored_without_query():
    # The HTTP path never consults query params: with no headers/body field
    # the resolution falls straight through to ua|ip.
    client, sid = resolve({"user-agent": "curl/8.0"})
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
