# Decisions (single source of truth)

Every locked decision, made with the maintainer. Implement exactly these.

## Identity & platform

| # | Decision |
|---|----------|
| D1 | **Name: `relay`** — repo, package, `relay.yaml`-style config naming, `relay.db`, `RELAY_*` env prefix, logger name, Docker image. (GitHub repo rename is the maintainer's action; in-repo rename is Phase 0.) |
| D2 | The platform has three configured entity types: **servers** (upstream machines whose power we manage), **endpoints** (APIs hosted on servers, proxied by path prefix), **clients** (requestors; default generic, optional white-glove). |
| D3 | A server is exactly one type — `redfish`, `aws-ec2`, or `noop` (dev/test). It cannot be both a VM and a workstation. |
| D4 | **Config = YAML topology file + env secrets.** `config.yaml` (topology + policy, gitignored, shipped as `config.yaml.sample`), `.env` (non-secret operational, from `.env.sample`), `secrets.env` (secrets, from `secrets.env.sample`), referenced from config via `${VAR}`. **Clean break** from the old env-only config — no compat shim. |
| D5 | **SQLite from the start** (`aiosqlite`, WAL). Config owns *what exists and the policy*; SQLite owns *what is happening and what has happened*. See `storage.md`. |
| D6 | **Single instance.** No multi-replica support; document it. |
| D7 | **No auth now** (trusted network). Admin/login support is **parked** (Phase 4), as is **cost accounting** (power-state awareness only for now) and **config hot-reload**. |

## Power management

| # | Decision |
|---|----------|
| D8 | **Two distinct signals:** *power state* (is the box on — from BMC/CSP) and *readiness* (is the service answering — from the endpoint's HTTP readiness path). A box can be ON but not READY (model loading). Requests block on **readiness**; power-on is triggered by traffic to an **off** server. Today's single `healthy` flag is split. |
| D9 | **`PowerBackend` ABC → `IpmiBackend` middle → `RedfishBackend`.** The middle layer holds shared "talk to a BMC" logic (BMC client: host/creds/`verify_ssl`/timeout, reachability, retry/backoff on management calls, graceful-off helper, raw→`PowerState` normalization); subclasses implement only their protocol's send/read. Structured for a future classic-IPMI backend (`IpmitoolBackend`) but **not** implementing one now. Note: Redfish is technically a separate REST protocol coexisting with IPMI on the same BMC — the middle layer is "BMC power management" even though we call it `IpmiBackend`. |
| D10 | **AWS EC2 is the first (and only) CSP backend**, phase 2. boto3 as **optional extra** `relay[aws]` (included in the Docker image). `off_action: stop` by default (`stop` = compute billing stops, EBS persists, warm restart; `terminate` available per server). EC2 sits **directly** under `PowerBackend` (not under `IpmiBackend`). |
| D11 | **Shutdown triggers, per server:** (a) idle — all of the server's endpoints idle for `idle_timeout` seconds; (b) schedule — a **cron off-time** (`croniter`) that **defers while active** (if traffic is active when the cron fires, power-off is postponed until the server goes idle). Both fire **only** when the server is owned and its per-cycle `shutdown_enabled` is on. |
| D12 | **Power-ownership invariant (non-negotiable, not configurable off):** the proxy never powers off a server it did not bring up. `owned` becomes true only via (a) a proxy-initiated power-on, or (b) traffic routed through the proxy when that server's `adopt_on_traffic` is `true` (default `true`, parity with today). `owned` is cleared when the proxy powers the server off, and is **persisted in SQLite** across restarts. Scenario this protects: the operator powers a workstation on manually; with no proxy traffic, the proxy never touches it. "Re-engagement" (routing traffic through the proxy) grants ownership per (b). |
| D13 | **Shutdown decisions use live state, never the activity log.** "Idle" = per-endpoint queue empty + no in-flight + no held spots, aggregated per server. The `requests`/`power_events` tables are history/metrics only and must never gate a shutdown. |
| D14 | Keep today's shutdown safety behaviors: verify **actual** BMC/CSP power state before issuing off; skip when state is unknown; never shut down while any queue has activity; per-cycle `shutdown_enabled` toggle (monitor) resets to the config default on the next proxy-initiated power-on; monotonic idle clock. |

## Endpoints, routing, transport

| # | Decision |
|---|----------|
| D15 | **Routing by longest path-prefix** (extends today's `API_PROFILES` matching). Each endpoint declares `path_prefix`. |
| D16 | **Endpoint behaviors:** `wait_policy: wait \| error` — `wait`: the client (configured with no timeout) is held until the service is ready; `error`: 503 + retry hint (HTTP) / WS close 1013, the user handles backoff. `routing: queued \| concurrent \| passthrough` — `queued`: `concurrency` slots (session-based, today's spot model), excess requests queue FIFO; `concurrent`: all requests proxied as received; `passthrough`: proxied unqueued and untracked (today's unknown-API-allow behavior). |
| D17 | **Unknown paths:** `proxy.unknown_path_policy: allow \| block`. `allow` routes unmatched paths to **the** `catch_all` endpoint (at most **one per platform** — deterministic routing), treated as `passthrough` and adopting ownership of its server (parity with main.py:753). `block` ⇒ 403. |
| D18 | **Transports: HTTP + SSE and WebSockets.** SSE via streaming forward (today's `aiter_raw` model). WS via bidirectional tunnel (`websockets` lib): for `wait` — **accept the upgrade immediately, then send WS ping keep-alives** while waiting for readiness/power-on, then tunnel; for `error` — accept then `close(code=1013, reason="not ready; retry later")` (1013 *Try Again Later* is the WS-native 503). An open WS **holds its slot for the connection's lifetime** (released on close). WS session ids may come from **query params** (WS URLs carry ids/tokens there; no body). |
| D19 | Keep: path-transparent forwarding (forward the path verbatim), `host` header stripped, per-chunk target read timeout (`target_read_timeout`, 0 = none, live-tunable from the monitor), 502 on proxy send failure, queue-timeout 504, slot released exactly once. |

## Clients (white-glove)

| # | Decision |
|---|----------|
| D20 | **Default client = generic**, identified by source IP (plus UA for display). **White-glove clients** are pre-configured: identified by header/UA rules, carry a session id, and get extra benefits — chiefly **child-session slot sharing**: child requests from the same session/lineage run on an ancestor's slot instead of waiting for their own (today's opencode sub-agent behavior), generalized to any configured client. |
| D21 | **Status model: generalize "proxy polls a client status API".** Each white-glove client config declares a `StatusSource` (pluggable): where to poll (port probed on the client's source IP, paths, auth, interval) and how to interpret responses. **`OpencodeStatusSource` must reproduce every current opencode behavior** (see `parity.md` §client-status) — this is a parity requirement, not a nice-to-have. |
| D22 | Session-id resolution precedence (generalized, per request): (1) matched white-glove client's own headers (plain, then UA-gated), (2) configured generic headers (`proxy.session_id_headers`), (3) endpoint-configured body fields (e.g. OpenAI `user`, Anthropic `metadata.user_id`), (4) WS query params, (5) `ua:<UA>|ip:<IP>` fallback. |

## Tooling, testing, CI

| # | Decision |
|---|----------|
| D23 | **uv** for dependency management (committed `uv.lock`; `uv sync` in dev/CI/Docker). `pyproject.toml` replaces `requirements.txt`. |
| D24 | **Python ≥3.11**; Docker base `python:3.12-slim`; CI matrix 3.11–3.13. |
| D25 | **ruff** for lint + format (one tool); **mypy** non-strict baseline (ratchet up over phases). |
| D26 | **pytest** for everything. `scripts/test_queue.py` is **ported faithfully to `tests/e2e/`** (proxy as a real subprocess, mocks in `tests/mocks/`); unit tests cover config/queue/resolution/ownership/store/scheduler/power mappings. Markers `unit` / `e2e`. Coverage reported (no threshold until the suite matures). |
| D27 | **CI:** new `ci.yml` (lint → test matrix → container smoke on every PR + push to main); existing `publish.yml` **gated on lint + test**, image renamed to `relay`, version via setuptools-scm build-arg. Conventional commits stay (semver + changelog depend on them). |
| D28 | **Packaging:** `relay` package with `relay` console entry point (`relay --config …`); `create_app(config_path)` factory for embedding; version is dynamic via **setuptools-scm** (git tag = single source of truth); new `GET /healthz` self-health endpoint for the container `HEALTHCHECK`; non-root Docker image; `config.yaml` + `secrets.env` mounted in compose. |

## Parking lot (deferred — Phase 4 or later, do not build now)

- Admin login / token auth on `/monitor` + power routes.
- Cost accounting (`$/hr` per server, kWh estimates) on top of `power_events`/`requests`.
- Config hot-reload (watch mtime; additive changes applied, destructive changes rejected).
- Classic-IPMI `IpmitoolBackend` (structure exists via `IpmiBackend`; implement only if needed).
- Prometheus `/metrics` endpoint (data already lands in SQLite; endpoint is Phase 4).
