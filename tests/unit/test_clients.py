"""White-glove client matching (S2, D20)."""

import pytest

from relay.clients import detect_client, provider_from_config, session_header_value
from relay.models import ClientConfig, MatchConfig, StatusConfig

pytestmark = pytest.mark.unit

OPENCODE = provider_from_config(
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


def test_plain_header_identifies_alone():
    provider = detect_client({"x-opencode-session": "s1"}, [OPENCODE])
    assert provider is not None and provider.name == "opencode"
    assert session_header_value(provider, {"x-opencode-session": "s1"}) == "s1"


def test_gated_header_needs_matching_ua():
    headers = {"x-session-id": "s2", "user-agent": "opencode/1.2.3"}
    provider = detect_client(headers, [OPENCODE])
    assert provider is not None and provider.name == "opencode"
    other_ua = {"x-session-id": "s2", "user-agent": "other/1.0"}
    assert detect_client(other_ua, [OPENCODE]) is None


def test_no_match_returns_none():
    assert detect_client({"user-agent": "curl/8.0"}, [OPENCODE]) is None
    assert detect_client({}, []) is None


def test_first_match_wins_in_config_order():
    second = provider_from_config(
        ClientConfig(
            name="second",
            match=MatchConfig(session_headers=["x-session-id"]),
            status=StatusConfig(),
        )
    )
    headers = {"x-opencode-session": "s1", "x-session-id": "s2"}
    provider = detect_client(headers, [OPENCODE, second])
    assert provider.name == "opencode"
    provider = detect_client(headers, [second, OPENCODE])
    assert provider.name == "second"


def test_matching_is_case_insensitive():
    provider = detect_client({"X-OpenCode-Session": "s1"}, [OPENCODE])
    assert provider is not None
    provider = detect_client({"x-session-id": "s2", "User-Agent": "OpenCode/9.9"}, [OPENCODE])
    assert provider is not None
