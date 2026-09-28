"""
API profiles and session-id extraction.

The proxy itself stays path-transparent, but the queuing mechanism needs to
know which API a request speaks (OpenAI, Anthropic, ...) because that is what
tells it where to find session information. Profiles are matched by the
longest path prefix, so the specific Anthropic route (/v1/messages) wins over
the broad OpenAI-compatible prefix (/v1/). Adding support for another API is
a matter of appending one profile below.
"""

import json
from dataclasses import dataclass
from typing import Any, Optional

__all__ = [
    "APIProfile",
    "API_PROFILES",
    "detect_api",
    "extract_body_field",
    "session_id_from_body",
]


@dataclass(frozen=True)
class APIProfile:
    name: str
    path_prefixes: tuple
    # Dotted paths into the JSON request body that may carry a session id
    # (OpenAI: "user", Anthropic: "metadata.user_id").
    body_sources: tuple


API_PROFILES: tuple[APIProfile, ...] = (
    APIProfile("anthropic", ("/v1/messages",), ("metadata.user_id",)),
    APIProfile("openai", ("/v1/",), ("user",)),
)


def detect_api(path: str) -> Optional[APIProfile]:
    """
    Returns the API profile for a request path (with or without leading
    slash), or None when no known API matches.
    """
    if not path.startswith("/"):
        path = "/" + path
    best: Optional[APIProfile] = None
    best_len = -1
    for profile in API_PROFILES:
        for prefix in profile.path_prefixes:
            if path.startswith(prefix) and len(prefix) > best_len:
                best, best_len = profile, len(prefix)
    return best


def extract_body_field(body: bytes, dotted_path: str) -> Optional[str]:
    """
    Walks a dotted path (e.g. "metadata.user_id") into a JSON body.
    Returns the value as a string when it is a non-empty string, else None.
    Never raises on malformed input.
    """
    try:
        data: Any = json.loads(body)
    except (ValueError, TypeError):
        return None
    for part in dotted_path.split("."):
        if not isinstance(data, dict) or part not in data:
            return None
        data = data[part]
    if isinstance(data, str) and data:
        return data
    if isinstance(data, (int, float)) and not isinstance(data, bool):
        return str(data)
    return None


def session_id_from_body(profile: Optional[APIProfile], body: bytes) -> Optional[str]:
    """
    Tries the profile's body sources in order and returns the first
    non-empty value found.
    """
    if profile is None or not body:
        return None
    for source in profile.body_sources:
        value = extract_body_field(body, source)
        if value is not None:
            return value
    return None
