"""Config loading, ${ENV} interpolation, and validation for relay.

Three inputs, one rule (D4): `config.yaml` owns topology + policy,
`.env` owns non-secret operational values, `secrets.env` owns secrets,
referenced from config via `${VAR}`.

Load order: `.env` then `secrets.env` are searched in the working
directory and next to the config file (secrets win on key overlap, and the
copy next to the config file wins over the CWD copy). Values never clobber
an explicitly set process environment variable, so a deployment `.env` in
the CWD cannot leak into a test that sets its env explicitly.
"""

import os
import re
from pathlib import Path
from typing import Any

import yaml
from croniter import croniter
from dotenv import dotenv_values

from relay.models import (
    AwsEc2PowerConfig,
    ChildrenConfig,
    ClientConfig,
    EndpointConfig,
    MatchConfig,
    NoopPowerConfig,
    ProxyConfig,
    ReadinessConfig,
    RedfishPowerConfig,
    RelayConfig,
    ServerConfig,
    SessionConfig,
    StatusConfig,
    StoreConfig,
)

__all__ = ["ConfigError", "load_config"]

_ENV_VAR_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")

UNKNOWN_PATH_POLICIES = ("allow", "block")
WAIT_POLICIES = ("wait", "error")
ROUTINGS = ("queued", "concurrent", "passthrough")
REQUEST_MODES = ("parallel", "atomic")
SERVER_TYPES = ("redfish", "aws-ec2", "noop")
OFF_ACTIONS = ("stop", "terminate")
STATUS_KINDS = ("opencode", "none")
CHILDREN_KINDS = ("none", "parent-chain")
READINESS_METHODS = ("GET", "POST", "PUT", "DELETE", "PATCH", "HEAD")
NOOP_STATES = ("on", "off")


class ConfigError(Exception):
    """The config file is missing, malformed, or fails validation."""


def load_config(path: str | Path) -> RelayConfig:
    """Loads, interpolates, and validates the relay config at `path`."""
    config_path = Path(path).expanduser()
    if not config_path.is_file():
        raise ConfigError(f"config file not found: {config_path}")
    _load_env_files(config_path.parent)
    try:
        raw: Any = yaml.safe_load(config_path.read_text())
    except yaml.YAMLError as e:
        raise ConfigError(f"invalid YAML in {config_path}: {e}") from e
    if not isinstance(raw, dict):
        raise ConfigError(f"{config_path} must contain a YAML mapping at the top level")
    data = _interpolate(raw, "$")
    return _build(data)


# --- env files / interpolation ---------------------------------------------


def _load_env_files(config_dir: Path) -> None:
    """Loads `.env` and `secrets.env` into the environment (no clobbering)."""
    merged: dict[str, str] = {}
    for filename in (".env", "secrets.env"):
        for directory in (Path.cwd(), config_dir):
            candidate = directory / filename
            if candidate.is_file():
                for key, value in dotenv_values(candidate).items():
                    if value is not None:
                        merged[key] = value
    for key, value in merged.items():
        os.environ.setdefault(key, value)


def _interpolate(node: Any, key_path: str) -> Any:
    """Recursively resolves ${VAR} references in string values."""
    if isinstance(node, dict):
        return {key: _interpolate(value, f"{key_path}.{key}") for key, value in node.items()}
    if isinstance(node, list):
        return [_interpolate(value, f"{key_path}[{i}]") for i, value in enumerate(node)]
    if isinstance(node, str):

        def _sub(match: re.Match[str]) -> str:
            name = match.group(1)
            value = os.environ.get(name)
            if value is None:
                raise ConfigError(
                    f"environment variable '{name}' referenced at '{key_path}' is not set "
                    "(check .env / secrets.env or the process environment)"
                )
            return value

        return _ENV_VAR_RE.sub(_sub, node)
    return node


# --- field helpers -----------------------------------------------------------


def _section(raw: Any, name: str, section: str) -> dict:
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ConfigError(f"'{section}' must be a mapping, got {type(raw).__name__}")
    return raw


def _list_section(raw: Any, name: str, section: str) -> list:
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise ConfigError(f"'{section}' must be a list, got {type(raw).__name__}")
    return raw


def _present(section: dict, key: str) -> bool:
    return key in section and section[key] is not None


def _get_str(section: dict, key: str, where: str, *, default: str | None = None) -> str:
    if not _present(section, key):
        if default is not None:
            return default
        raise ConfigError(f"missing required field '{key}' in {where}")
    value = section[key]
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"field '{key}' in {where} must be a non-empty string")
    return value


def _get_str_opt(section: dict, key: str, where: str) -> str | None:
    if not _present(section, key):
        return None
    value = section[key]
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"field '{key}' in {where} must be a non-empty string when set")
    return value


def _get_int(
    section: dict,
    key: str,
    where: str,
    *,
    default: int | None = None,
    minimum: int | None = None,
) -> int:
    if not _present(section, key):
        if default is not None:
            return default
        raise ConfigError(f"missing required field '{key}' in {where}")
    value = section[key]
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError(f"field '{key}' in {where} must be an integer, got {value!r}")
    if minimum is not None and value < minimum:
        raise ConfigError(f"field '{key}' in {where} must be >= {minimum}, got {value}")
    return value


def _get_float(
    section: dict,
    key: str,
    where: str,
    *,
    default: float | None = None,
    minimum: float | None = None,
) -> float:
    if not _present(section, key):
        if default is not None:
            return default
        raise ConfigError(f"missing required field '{key}' in {where}")
    value = section[key]
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConfigError(f"field '{key}' in {where} must be a number, got {value!r}")
    value = float(value)
    if minimum is not None and value < minimum:
        raise ConfigError(f"field '{key}' in {where} must be >= {minimum}, got {value}")
    return value


def _get_bool(section: dict, key: str, where: str, *, default: bool | None = None) -> bool:
    if not _present(section, key):
        if default is not None:
            return default
        raise ConfigError(f"missing required field '{key}' in {where}")
    value = section[key]
    if not isinstance(value, bool):
        raise ConfigError(f"field '{key}' in {where} must be a boolean, got {value!r}")
    return value


def _get_enum(
    section: dict, key: str, where: str, allowed: tuple[str, ...], *, default: str | None = None
) -> str:
    if not _present(section, key):
        if default is not None:
            return default
        raise ConfigError(f"missing required field '{key}' in {where}")
    value = section[key]
    if not isinstance(value, str) or value not in allowed:
        raise ConfigError(
            f"field '{key}' in {where} must be one of {', '.join(allowed)}, got {value!r}"
        )
    return value


def _get_str_list(
    section: dict, key: str, where: str, *, default: list[str] | None = None
) -> list[str]:
    value = section.get(key)
    if value is None:
        if default is not None:
            return list(default)
        return []
    if not isinstance(value, list) or any(not isinstance(v, str) or not v.strip() for v in value):
        raise ConfigError(f"field '{key}' in {where} must be a list of non-empty strings")
    return [v.strip() for v in value]


def _get_int_list(
    section: dict, key: str, where: str, *, default: list[int] | None = None
) -> list[int]:
    value = section.get(key)
    if value is None:
        if default is not None:
            return list(default)
        raise ConfigError(f"missing required field '{key}' in {where}")
    if not isinstance(value, list) or any(
        isinstance(v, bool) or not isinstance(v, int) for v in value
    ):
        raise ConfigError(f"field '{key}' in {where} must be a list of integers")
    return list(value)


# --- section builders ---------------------------------------------------------


def _build_proxy(raw: dict) -> ProxyConfig:
    where = "proxy"
    store_raw = _section(raw.get("store"), "store", "proxy.store")
    store_path = _get_str_opt(store_raw, "path", "proxy.store") or "./relay.db"
    _validate_store_path(store_path)
    return ProxyConfig(
        host=_get_str_opt(raw, "host", where) or "0.0.0.0",
        port=_get_int(raw, "port", where, default=8000, minimum=1),
        unknown_path_policy=_get_enum(
            raw, "unknown_path_policy", where, UNKNOWN_PATH_POLICIES, default="allow"
        ),
        target_read_timeout=_get_int(raw, "target_read_timeout", where, default=0, minimum=0),
        session_id_headers=[
            h.lower()
            for h in _get_str_list(raw, "session_id_headers", where, default=["x-session-id"])
        ],
        store=StoreConfig(
            path=store_path,
            retention_days=_get_int(
                store_raw, "retention_days", "proxy.store", default=90, minimum=0
            ),
        ),
    )


def _validate_schedule(schedule: str) -> None:
    fields = schedule.split()
    if len(fields) != 5:
        raise ConfigError(
            f"schedule must be a 5-field cron expression, got {schedule!r} ({len(fields)} fields)"
        )
    try:
        croniter(schedule)
    except (ValueError, KeyError) as e:
        raise ConfigError(f"schedule is not a valid cron expression: {schedule!r} ({e})") from e


def _build_power(raw: dict, server_type: str, where: str) -> Any:
    if server_type == "redfish":
        host = _get_str_opt(raw, "host", where)
        base_url = _get_str_opt(raw, "base_url", where)
        if host is None and base_url is None:
            raise ConfigError(
                f"power block in {where} (type redfish) requires 'host' (or an explicit 'base_url')"
            )
        return RedfishPowerConfig(
            host=host or "",
            user=_get_str(raw, "user", where),
            password=_get_str(raw, "password", where),
            base_url=base_url,
            system_path=_get_str_opt(raw, "system_path", where) or "/redfish/v1/Systems/Self",
            verify_ssl=_get_bool(raw, "verify_ssl", where, default=False),
        )
    if server_type == "noop":
        return NoopPowerConfig(
            initial_state=_get_enum(raw, "initial_state", where, NOOP_STATES, default="on"),
        )
    if server_type == "aws-ec2":
        access_key = _get_str_opt(raw, "access_key", where)
        secret_key = _get_str_opt(raw, "secret_key", where)
        profile = _get_str_opt(raw, "profile", where)
        has_keys = bool(access_key and secret_key)
        if not (has_keys or profile):
            raise ConfigError(
                f"power block in {where} (type aws-ec2) requires either "
                "access_key+secret_key or a profile"
            )
        if has_keys and (bool(access_key) != bool(secret_key)):
            raise ConfigError(f"power block in {where} needs both access_key and secret_key")
        return AwsEc2PowerConfig(
            region=_get_str(raw, "region", where),
            instance_id=_get_str(raw, "instance_id", where),
            access_key=access_key,
            secret_key=secret_key,
            session_token=_get_str_opt(raw, "session_token", where),
            profile=profile,
            off_action=_get_enum(raw, "off_action", where, OFF_ACTIONS, default="stop"),
        )
    raise ConfigError(f"unsupported server type {server_type!r} in {where}")


def _build_servers(raw: list) -> list[ServerConfig]:
    servers: list[ServerConfig] = []
    seen: set[str] = set()
    for i, entry in enumerate(raw):
        where = f"servers[{i}]"
        entry = _section(entry, "server", where)
        name = _get_str(entry, "name", where)
        if name in seen:
            raise ConfigError(f"duplicate server name {name!r} (in {where})")
        seen.add(name)
        server_type = _get_enum(entry, "type", where, SERVER_TYPES)
        power_raw = _section(entry.get("power"), "power", f"{where}.power")
        schedule = _get_str_opt(entry, "schedule", where)
        if schedule is not None:
            _validate_schedule(schedule)
        service_url = _get_str(entry, "service_url", where).rstrip("/")
        servers.append(
            ServerConfig(
                name=name,
                type=server_type,
                power=_build_power(power_raw, server_type, f"{where}.power"),
                service_url=service_url,
                idle_timeout=_get_float(entry, "idle_timeout", where, default=3600, minimum=0.001),
                shutdown_enabled=_get_bool(entry, "shutdown_enabled", where, default=True),
                adopt_on_traffic=_get_bool(entry, "adopt_on_traffic", where, default=True),
                schedule=schedule,
            )
        )
    return servers


def _build_readiness(raw: dict, where: str) -> ReadinessConfig:
    path = _get_str_opt(raw, "path", where) or "/health"
    if not path.startswith("/"):
        path = "/" + path
    return ReadinessConfig(
        path=path,
        method=_get_enum(raw, "method", where, READINESS_METHODS, default="GET").upper(),
        interval=_get_float(raw, "interval", where, default=5, minimum=0.1),
        timeout=_get_float(raw, "timeout", where, default=2, minimum=0.1),
        healthy_statuses=_get_int_list(raw, "healthy_statuses", where, default=[200]),
    )


def _build_session(raw: dict, where: str) -> SessionConfig:
    return SessionConfig(
        per_session_requests=_get_int(raw, "per_session_requests", where, default=-1),
        request_mode=_get_enum(raw, "request_mode", where, REQUEST_MODES, default="parallel"),
        busy_window=_get_float(raw, "busy_window", where, default=120, minimum=0.0),
        expiry=_get_float(raw, "expiry", where, default=300, minimum=1.0),
        immediate_idle_release=_get_bool(raw, "immediate_idle_release", where, default=True),
    )


def _build_endpoints(raw: list, server_names: set[str]) -> list[EndpointConfig]:
    endpoints: list[EndpointConfig] = []
    seen: set[str] = set()
    prefixes: dict[str, str] = {}
    catch_all = 0
    for i, entry in enumerate(raw):
        where = f"endpoints[{i}]"
        entry = _section(entry, "endpoint", where)
        name = _get_str(entry, "name", where)
        if name in seen:
            raise ConfigError(f"duplicate endpoint name {name!r} (in {where})")
        seen.add(name)
        server = _get_str(entry, "server", where)
        if server not in server_names:
            raise ConfigError(
                f"endpoint {name!r} references unknown server {server!r} (in {where})"
            )
        prefix = _get_str(entry, "path_prefix", where)
        if not prefix.startswith("/"):
            raise ConfigError(f"field 'path_prefix' in {where} must start with '/', got {prefix!r}")
        owner = prefixes.get(prefix)
        if owner is not None:
            raise ConfigError(
                f"identical path_prefix {prefix!r} on endpoints {owner!r} and {name!r}"
            )
        prefixes[prefix] = name
        if entry.get("catch_all"):
            catch_all += 1
            if catch_all > 1:
                raise ConfigError(
                    f"at most one catch_all endpoint per platform (D17); {name!r} is a second"
                )
        routing = _get_enum(entry, "routing", where, ROUTINGS, default="queued")
        concurrency = _get_int(entry, "concurrency", where, default=1)
        if routing == "queued" and concurrency < 1:
            raise ConfigError(f"endpoint {name!r}: routing 'queued' requires concurrency >= 1")
        readiness_raw = _section(entry.get("readiness"), "readiness", f"{where}.readiness")
        session_raw = _section(entry.get("session"), "session", f"{where}.session")
        endpoints.append(
            EndpointConfig(
                name=name,
                server=server,
                path_prefix=prefix,
                catch_all=bool(entry.get("catch_all", False)),
                readiness=_build_readiness(readiness_raw, f"{where}.readiness"),
                wait_policy=_get_enum(entry, "wait_policy", where, WAIT_POLICIES, default="wait"),
                routing=routing,
                concurrency=concurrency,
                queue_timeout=_get_float(entry, "queue_timeout", where, default=0.0),
                session=_build_session(session_raw, f"{where}.session"),
                session_id_body_fields=_get_str_list(entry, "session_id_body_fields", where),
            )
        )
    return endpoints


def _build_match(raw: dict, where: str) -> MatchConfig:
    return MatchConfig(
        ua_prefix=_get_str_opt(raw, "ua_prefix", where) or "",
        session_headers=_get_str_list(raw, "session_headers", where),
        gated_session_headers=_get_str_list(raw, "gated_session_headers", where),
    )


def _build_status(raw: dict, where: str) -> StatusConfig:
    return StatusConfig(
        kind=_get_enum(raw, "kind", where, STATUS_KINDS, default="opencode"),
        port=_get_int(raw, "port", where, default=4096, minimum=1),
        password=_get_str_opt(raw, "password", where) or "",
        poll_interval=_get_float(raw, "poll_interval", where, default=5, minimum=0.1),
        status_path=_get_str_opt(raw, "status_path", where) or "/session/status",
        session_path=_get_str_opt(raw, "session_path", where) or "/session",
        pending_paths=_get_str_list(raw, "pending_paths", where),
    )


def _build_children(raw: dict, where: str) -> ChildrenConfig:
    return ChildrenConfig(
        kind=_get_enum(raw, "kind", where, CHILDREN_KINDS, default="none"),
        depth=_get_int(raw, "depth", where, default=10, minimum=1),
    )


def _build_clients(raw: list) -> list[ClientConfig]:
    clients: list[ClientConfig] = []
    seen: set[str] = set()
    for i, entry in enumerate(raw):
        where = f"clients[{i}]"
        entry = _section(entry, "client", where)
        name = _get_str(entry, "name", where)
        if name in seen:
            raise ConfigError(f"duplicate client name {name!r} (in {where})")
        seen.add(name)
        match_raw = _section(entry.get("match"), "match", f"{where}.match")
        status_raw = _section(entry.get("status"), "status", f"{where}.status")
        children_raw = _section(entry.get("children"), "children", f"{where}.children")
        clients.append(
            ClientConfig(
                name=name,
                match=_build_match(match_raw, f"{where}.match"),
                status=_build_status(status_raw, f"{where}.status"),
                children=_build_children(children_raw, f"{where}.children"),
            )
        )
    return clients


def _validate_store_path(path: str) -> None:
    """The store.path directory must exist or be creatable (rule 10)."""
    parent = Path(path).expanduser().parent
    current = parent
    while not current.exists() and current != current.parent:
        current = current.parent
    if current.exists() and not current.is_dir():
        raise ConfigError(
            f"store.path {path!r}: parent directory {parent} is not creatable "
            f"(blocked by existing file {current})"
        )


def _build(data: dict) -> RelayConfig:
    proxy_raw = _section(data.get("proxy"), "proxy", "proxy")
    proxy = _build_proxy(proxy_raw)
    servers = _build_servers(_list_section(data.get("servers"), "servers", "servers"))
    server_names = {s.name for s in servers}
    endpoints = _build_endpoints(
        _list_section(data.get("endpoints"), "endpoints", "endpoints"), server_names
    )
    clients = _build_clients(_list_section(data.get("clients"), "clients", "clients"))
    return RelayConfig(proxy=proxy, servers=servers, endpoints=endpoints, clients=clients)
