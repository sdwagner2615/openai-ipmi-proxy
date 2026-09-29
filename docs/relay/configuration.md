# Configuration

Three inputs, one rule (D4): **`config.yaml` owns topology + policy**
(gitignored, shipped as `config.yaml.sample`), **`.env` owns non-secret
operational values** (from `.env.sample`), **`secrets.env` owns secrets**
(from `secrets.env.sample`) referenced from config via `${VAR}`.

```bash
cp config.yaml.sample config.yaml
cp .env.sample .env
cp secrets.env.sample secrets.env
```

Load order (in `config.py`): `load_dotenv(".env")` then
`load_dotenv("secrets.env")` — secrets win on any key overlap. `${VAR}`
references resolve against the combined `os.environ`.

## `config.yaml` — full reference

```yaml
# relay.yaml — topology + policy. Secrets come from the environment via ${VAR}.
# Static and version-controlled as a .sample; the live file is gitignored
# (it carries internal topology: addresses, instance IDs).

proxy:
  host: 0.0.0.0                # bind address
  port: 8000                   # bind port
  unknown_path_policy: allow   # allow → route to the catch_all endpoint (D17;
                                #   passthrough semantics regardless of that
                                #   endpoint's routing mode)
                                # block → 403 (a rejected request neither adopts
                                #   power ownership nor resets the idle timer)
  target_read_timeout: 0       # seconds to wait for the next chunk from a target
                               # (per-chunk for SSE, whole body otherwise); 0 = none
                               # Live-tunable from the monitor (applies to new requests)
  session_id_headers:          # generic fallback session headers (D22 step 2),
    - x-session-id             #   comma-insensitive list, checked in order
  store:
    path: ${RELAY_DB_PATH}     # SQLite file (default ./relay.db)
    retention_days: 90         # prune requests/sessions/power_events older than this

servers:
  - name: gpu-workstation      # unique
    type: redfish              # redfish | aws-ec2 | noop
    power:
      host: ${IPMI_HOST}
      user: ${IPMI_USER}
      password: ${IPMI_PASS}
      # base_url: https://192.168.1.124   # optional full base URL; defaults to
                                          # https://<host> (use for plain-HTTP BMCs)
      system_path: /redfish/v1/Systems/Self   # default; today's hardcoded
                                                # discovered_system_path
      verify_ssl: false        # BMCs use self-signed certs; default false
    service_url: http://192.168.1.186:80      # base URL of the service(s) on it
    idle_timeout: 3600         # seconds: sleep when ALL endpoints idle (D11)
    shutdown_enabled: true     # per-cycle default for the auto-off switch (D14)
    adopt_on_traffic: true     # routed traffic grants power ownership (D12)
    schedule: "0 2 * * *"      # 5-field cron off-time, defers while active (D11);
                               # null/omitted = no schedule

  - name: ec2-gpu
    type: aws-ec2
    power:
      region: us-east-1
      instance_id: i-0abcdef
      access_key: ${AWS_ACCESS_KEY_ID}
      secret_key: ${AWS_SECRET_ACCESS_KEY}
      # session_token: ${AWS_SESSION_TOKEN}   # optional
      # profile: my-aws-profile               # alternative to explicit keys
      off_action: stop             # stop (default) | terminate (D10)
    service_url: http://10.0.0.5:80
    idle_timeout: 1800
    shutdown_enabled: true
    adopt_on_traffic: true
    schedule: null

  - name: local-dev            # example: dev/test server, no real hardware
    type: noop
    power:
      initial_state: on        # on | off  (scriptable for tests/dev)
    service_url: http://127.0.0.1:8100
    idle_timeout: 3600
    shutdown_enabled: false

endpoints:
  - name: llama-cpp            # unique
    server: gpu-workstation    # must reference a configured server
    path_prefix: /v1           # must start with /
    catch_all: true            # at most ONE per platform (D17); unmatched paths
                               # route here with passthrough semantics
    readiness:
      path: /health            # probed on service_url
      method: GET              # default GET
      interval: 5              # seconds between probes (default 5)
      timeout: 2               # probe timeout seconds (default 2)
      healthy_statuses: [200]  # default [200]
    wait_policy: wait          # wait | error (D16)
    routing: queued            # queued | concurrent | passthrough (D16)
    concurrency: 1             # slots; required when routing=queued (>= 1)
    queue_timeout: 0           # seconds a request may wait before 504; 0 = none
    session:
      per_session_requests: -1 # -1 unlimited (default), N cap, 0 serialized
      request_mode: parallel   # parallel (default) | atomic (one in-flight
                               #   request globally within this endpoint)
      busy_window: 120         # seconds unknown clients count as busy after
                               #   their last request
      expiry: 300              # seconds a session may stay idle before its
                               #   slot is surrendered and it is forgotten
      immediate_idle_release: true   # known clients that truly report idle
                                #   surrender their slot immediately (the idle
                                #   report must postdate the last response we
                                #   served — a stale report waits out `expiry`)

  - name: litellm
    server: ec2-gpu
    path_prefix: /v1
    readiness:
      path: /health/liveliness
      interval: 5
    wait_policy: error         # 503 + retry hint; user handles backoff
    routing: concurrent        # no slots; proxy as received

clients:
  - name: opencode             # white-glove client (D20)
    match:
      ua_prefix: opencode/
      session_headers: [x-opencode-session]            # identify alone
      gated_session_headers: [x-session-affinity, x-session-id]
                                               # identify only when UA matches
    status:
      kind: opencode           # status-source plugin: opencode | none
      port: 4096               # probed on the client's SOURCE IP
      password: ${OPENCODE_SERVER_PASSWORD}   # optional basic auth (user "opencode")
      poll_interval: 5         # seconds between status polls per client machine
      status_path: /session/status
      session_path: /session
      pending_paths: [/permission, /question]   # default for kind: opencode;
                                                # explicit [] disables the poll
    children:
      kind: parent-chain       # child sessions share a tracked ancestor's slot
      depth: 10                # chain walk cap
```

### Field notes

- **`type: noop`** — `NoopBackend` for dev/tests: no hardware, scripted state
  (`initial_state`, and the e2e harness can poke it). Records issued actions
  for assertions.
- **`readiness`** is what today's `HEALTH_PATH` becomes, generalized: any
  HTTP method, any interval, any healthy statuses.
- **Endpoint session knobs** are per endpoint because concurrency/spot
  semantics are about the *service behind the endpoint* (e.g. one model's
  K/V cache), not the platform.
- **`clients`** entries are optional; without any, every client is generic
  (IP-identified, busy-window status).
- **`pending_paths`** — the endpoints that report a session blocked on user
  input (a pending entry releases the session's spot immediately: the
  "waiting" state). Defaults to `[/permission, /question]` for
  `kind: opencode`; an explicit `[]` disables the poll.

## `.env.sample` (non-secret)

```bash
# relay operational settings (non-secret).  cp .env.sample .env
RELAY_CONFIG=./config.yaml
RELAY_DB_PATH=./relay.db
LOG_LEVEL=INFO
# BMC address (an address, not a secret); credentials live in secrets.env
IPMI_HOST=192.168.1.124
```

## `secrets.env.sample` (secrets — never commit `secrets.env`)

```bash
# relay secrets.  cp secrets.env.sample secrets.env
IPMI_USER=admin
IPMI_PASS=password
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_SESSION_TOKEN=
OPENCODE_SERVER_PASSWORD=
```

## Validation (fail at startup, clear messages)

1. Names unique within each section (servers, endpoints, clients).
2. Every `endpoint.server` references a configured server.
3. `path_prefix` starts with `/`. Overlapping prefixes are legal (longest
   wins); an **identical** prefix on two endpoints is an error.
4. At most **one** `catch_all` endpoint per platform (D17).
5. `routing: queued` requires `concurrency >= 1`.
6. `wait_policy`, `routing`, `request_mode`, `type`, `off_action`,
   `kind` values within their enums.
7. `schedule`, when set, must parse as a 5-field cron expression.
8. Every `${VAR}` must resolve (missing var ⇒ startup error naming the var
   and the config key that used it).
9. Per-type power blocks must carry their required fields
   (redfish: host/user/password; aws-ec2: region/instance_id and either
   keys or profile; noop: nothing).
10. `store.path` directory must exist or be creatable.

## Migration map (old env var → new home)

| Old env var | New location |
|---|---|
| `TARGET_SERVER_URL` | `servers[].service_url` |
| `IPMI_HOST` / `IPMI_USER` / `IPMI_PASS` | `servers[].power.*` (values from env) |
| `HEALTH_PATH` | `endpoints[].readiness.path` |
| `IDLE_TIMEOUT` | `servers[].idle_timeout` |
| `SHUTDOWN_ENABLED` | `servers[].shutdown_enabled` |
| `CONCURRENT_SESSIONS` | `endpoints[].concurrency` |
| `CONCURRENT_SESSION_REQUESTS` | `endpoints[].session.per_session_requests` |
| `REQUEST_MODE` | `endpoints[].session.request_mode` |
| `UNKNOWN_API_POLICY` | `proxy.unknown_path_policy` (+ `catch_all`) |
| `SESSION_EXPIRY` | `endpoints[].session.expiry` |
| `CLIENT_BUSY_WINDOW` | `endpoints[].session.busy_window` |
| `CLIENT_STATUS_POLL` | `clients[].status.poll_interval` |
| `QUEUE_TIMEOUT` | `endpoints[].queue_timeout` |
| `OPENCODE_STATUS_PORT` | `clients[].status.port` |
| `OPENCODE_SERVER_PASSWORD` | `clients[].status.password` (env) |
| `SESSION_ID_HEADERS` | `proxy.session_id_headers` |
| `IMMEDIATE_IDLE_RELEASE` | `endpoints[].session.immediate_idle_release` |
| `TARGET_READ_TIMEOUT` | `proxy.target_read_timeout` |
| *(hardcoded `discovered_system_path`)* | `servers[].power.system_path` |
