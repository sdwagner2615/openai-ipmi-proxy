"""Configuration dataclasses for relay.

These model `config.yaml` (topology + policy). Runtime state lives in
`servers.py` / `endpoints.py`; the store owns *what is happening*, these
own *what exists and the policy* (D4/D5).
"""

from dataclasses import dataclass, field


@dataclass
class StoreConfig:
    """SQLite location and retention (see storage.md)."""

    path: str = "./relay.db"
    retention_days: int = 90


@dataclass
class ProxyConfig:
    """Platform-level proxy settings."""

    host: str = "0.0.0.0"
    port: int = 8000
    # allow -> route unmatched paths to the catch_all endpoint (D17)
    # block -> 403
    unknown_path_policy: str = "allow"
    # Seconds to wait for the next chunk from a target (per-chunk for SSE,
    # whole body otherwise); 0 = no timeout. Live-tunable from the monitor.
    target_read_timeout: int = 0
    # Generic fallback session headers (D22 step 2), checked in order.
    session_id_headers: list[str] = field(default_factory=lambda: ["x-session-id"])
    store: StoreConfig = field(default_factory=StoreConfig)


@dataclass
class ReadinessConfig:
    """HTTP readiness probe for an endpoint (what HEALTH_PATH becomes)."""

    path: str = "/health"
    method: str = "GET"
    interval: float = 5
    timeout: float = 2
    healthy_statuses: list[int] = field(default_factory=lambda: [200])


@dataclass
class SessionConfig:
    """Slot (spot) semantics for an endpoint's queued traffic."""

    # -1 unlimited, N cap, 0 serialized (per session).
    per_session_requests: int = -1
    # parallel | atomic (one in-flight request globally within the endpoint)
    request_mode: str = "parallel"
    # Seconds unknown clients count as busy after their last request.
    busy_window: float = 120
    # Seconds a session may stay idle before its slot is surrendered.
    expiry: float = 300
    # Known clients that truly report idle surrender their slot immediately.
    immediate_idle_release: bool = True


@dataclass
class EndpointConfig:
    """An API hosted on a server, published by the proxy under path_prefix."""

    name: str
    server: str
    path_prefix: str
    # At most ONE per platform (D17); unmatched paths route here with
    # passthrough semantics when unknown_path_policy is allow.
    catch_all: bool = False
    readiness: ReadinessConfig = field(default_factory=ReadinessConfig)
    # wait (hold until ready) | error (503 + retry hint / WS close 1013)
    wait_policy: str = "wait"
    # queued (concurrency slots, FIFO) | concurrent | passthrough
    routing: str = "queued"
    # Slots; required (>= 1) when routing is queued.
    concurrency: int = 1
    # Seconds a request may wait in the queue before a 504; 0 = none.
    queue_timeout: float = 0
    session: SessionConfig = field(default_factory=SessionConfig)
    # Dotted JSON body paths that may carry a session id
    # (OpenAI: "user", Anthropic: "metadata.user_id"), tried in order.
    session_id_body_fields: list[str] = field(default_factory=list)


@dataclass
class RedfishPowerConfig:
    """Power config for `type: redfish` servers (BMC Redfish API)."""

    host: str
    user: str
    password: str
    # Optional full base URL (e.g. "http://127.0.0.1:8000" for a plain-HTTP
    # mock BMC); defaults to https://<host>.
    base_url: str | None = None
    # BMC firmware quirk: many MegaRAC units expose the single system as
    # "Self" rather than a numeric id.
    system_path: str = "/redfish/v1/Systems/Self"
    # BMCs use self-signed certs; default false.
    verify_ssl: bool = False


@dataclass
class NoopPowerConfig:
    """Power config for `type: noop` servers (dev/tests, no hardware)."""

    # on | off: the scripted initial state.
    initial_state: str = "on"


@dataclass
class AwsEc2PowerConfig:
    """Power config for `type: aws-ec2` servers (Phase 2 backend)."""

    region: str
    instance_id: str
    access_key: str | None = None
    secret_key: str | None = None
    session_token: str | None = None
    # Alternative to explicit keys.
    profile: str | None = None
    # stop (default) | terminate
    off_action: str = "stop"


PowerConfig = RedfishPowerConfig | NoopPowerConfig | AwsEc2PowerConfig


@dataclass
class ServerConfig:
    """An upstream machine whose power the platform manages (D2/D3)."""

    name: str
    # redfish | aws-ec2 | noop (exactly one type per server)
    type: str
    power: PowerConfig
    # Base URL of the service(s) hosted on it.
    service_url: str
    # Seconds: sleep when ALL endpoints idle (D11).
    idle_timeout: float = 3600
    # Per-cycle default for the auto-off switch (D14).
    shutdown_enabled: bool = True
    # Routed traffic grants power ownership (D12).
    adopt_on_traffic: bool = True
    # 5-field cron off-time, defers while active (D11); None = no schedule.
    schedule: str | None = None


@dataclass
class MatchConfig:
    """How a white-glove client is identified (D20)."""

    # UA prefix required for the gated headers to count.
    ua_prefix: str = ""
    # Headers that identify this client on their own.
    session_headers: list[str] = field(default_factory=list)
    # Headers that identify this client only when the UA matches ua_prefix.
    gated_session_headers: list[str] = field(default_factory=list)


@dataclass
class StatusConfig:
    """Where/how to poll a white-glove client's status API (D21)."""

    # Status-source plugin: opencode | none.
    kind: str = "opencode"
    # Probed on the client's SOURCE IP.
    port: int = 4096
    # Optional basic auth (user "opencode" for kind=opencode).
    password: str = ""
    # Seconds between status polls per client machine.
    poll_interval: float = 5
    status_path: str = "/session/status"
    session_path: str = "/session"
    pending_paths: list[str] = field(default_factory=list)


@dataclass
class ChildrenConfig:
    """Child-session slot sharing rule (D20)."""

    # none | parent-chain (child runs on a tracked ancestor's slot)
    kind: str = "none"
    # Chain walk cap.
    depth: int = 10


@dataclass
class ClientConfig:
    """A configured client: generic by default, white-glove when listed."""

    name: str
    match: MatchConfig = field(default_factory=MatchConfig)
    status: StatusConfig = field(default_factory=StatusConfig)
    children: ChildrenConfig = field(default_factory=ChildrenConfig)


@dataclass
class RelayConfig:
    """The full parsed + validated topology."""

    proxy: ProxyConfig
    servers: list[ServerConfig]
    endpoints: list[EndpointConfig]
    clients: list[ClientConfig] = field(default_factory=list)
