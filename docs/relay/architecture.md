# Target Architecture

## Glossary

- **Server** — an upstream machine whose power the platform manages. Exactly
  one `PowerBackend` type. Hosts one or more endpoints.
- **Endpoint** — an API hosted on a server, published by the proxy under a
  `path_prefix`. Carries readiness + wait/error + routing policy.
- **Client** — a requestor. Generic (source IP) or white-glove
  (pre-configured identification + status source + child-session rules).
- **Session** — `(client_name, session_id)` scoped to an **endpoint**. Today's
  session concept, now per endpoint.
- **Slot** — one unit of an endpoint's `concurrency` (today's "spot"). A
  session acquires a slot when its first request is promoted and keeps it
  until genuinely idle (client-reported or busy-window).
- **Power state** — per server: `on` / `off` / `unknown` (+ transient
  `powering_on` / `powering_off`). From the `PowerBackend`.
- **Readiness** — per endpoint: `ready` / `not_ready`. From the endpoint's
  HTTP readiness probe. `unknown` while the server is off.
- **Ownership** — per server: did the proxy bring this server up (or adopt
  it)? Gates all power-off decisions. Persisted.

## The two signals (D8)

```
                 ┌────────────────────────────┐
 traffic ───────►│ endpoint.ready ?           │  ← HTTP readiness probe
                 │   no → server.powered ?    │  ← PowerBackend.power_state()
                 │        off/unknown → power_on()  (cooldown-deduped)
                 │        on            → wait (booting / model loading)
                 │   yes → admit (routing policy)
                 └────────────────────────────┘
```

- Readiness is polled **per endpoint** (config `readiness.interval`, default
  5s), always, and more aggressively (≤2s) while that endpoint's queue is
  non-empty (today's manager behavior).
- `ready` ⇒ the server is implicitly `on` (a 200 from the service means the
  box is up) — update power state accordingly.
- Power-on is deduped by a per-server cooldown (default 30s, today's
  `power_on_cooldown`): the first waiter triggers it; background re-issues
  keep the boot attempt alive; later waiters simply wait.
- **No 503s during boot** for `wait` endpoints: requests are held until
  ready (D16, parity).

## Power backends (D9, D10)

```
PowerBackend (ABC)                      # power/ base.py
  async power_on()  -> bool
  async power_off() -> bool
  async power_state() -> PowerState     # on | off | unknown
  async close()
  │
  ├── IpmiBackend (abstract middle)     # power/ ipmi.py
  │     owns: BmcClient (host, user, password, verify_ssl, timeout),
  │           management-call retry/backoff, graceful-off helper,
  │           raw-response → PowerState normalization, error logging
  │     implements abstract: _send_reset(reset_type), _read_power_state()
  │     │
  │     ├── RedfishBackend              # power/ redfish.py  (today's logic)
  │     │     GET  {system_path}                                  → PowerState
  │     │     POST {system_path}/Actions/ComputerSystem.Reset
  │     │          {"ResetType": "On" | "GracefulShutdown"}
  │     └── (future) IpmitoolBackend
  │
  ├── AwsEc2Backend                     # power/ aws_ec2.py  (boto3, extra "aws")
  │     power_on  → start_instances      state map: pending→(booting) on
  │     power_off → stop_instances       running→on
  │                 (terminate if off_action=terminate)
  │                 stopping/shut-down→off; terminated→off (+ flag)
  │
  └── NoopBackend                       # power/ noop.py  (dev + tests)
        configurable scripted state (e.g. "off until poked", "always on");
        records issued actions for assertions
```

boto3 is synchronous → run calls in an executor (`asyncio.to_thread`). Power
operations are rare; latency is irrelevant.

## Server runtime & state machine

`ServerRuntime` (one per configured server) owns:

- `power_state` + `powered_on_at`/`powered_off_at` (transient states tracked
  for the monitor; persisted coarse state in `server_runtime` table)
- `owned` + `shutdown_override` (per-cycle toggle; persisted)
- its endpoints' readiness state
- the single power-on cooldown timer

Transitions:

```
off ──power_on()──► powering_on ──state=on──► on
on  ──power_off()──► powering_off ──state=off──► off
(any) ──poll──► unknown   (BMC/CSP unreachable; never act on unknown)
```

`owned` lifecycle (D12): `power_on()` success ⇒ `owned=True`,
`shutdown_override` reset to config default, new cycle. Routed traffic +
`adopt_on_traffic` ⇒ `owned=True`. `power_off()` success ⇒ `owned=False`.
Startup reconciliation restores `owned`/`shutdown_override` from SQLite
(never guess).

## Endpoint runtime

`EndpointRuntime` (one per configured endpoint) owns:

- readiness state + last-check timestamp (poll cadence: normal interval;
  fast cadence ≤2s while its queue is non-empty)
- its own `EndpointQueue` (today's `SessionQueue`, generalized):
  slots (`concurrency`), FIFO wait, per-session in-flight cap
  (`session.per_session_requests`), atomic mode
  (`session.request_mode=atomic` ⇒ one in-flight request globally **within
  this endpoint**), slot expiry / immediate-idle-release / waiting-for-input
  release (all semantics in `parity.md`)
- `wait_policy` enforcement at the admission point
- `queue_timeout` (504) enforcement

An endpoint is **idle** when: queue empty, no in-flight, no held slots.
A **server is idle** when all its endpoints are idle (D11, D13).

## Clients

`ClientRegistry` (from `clients:` config):

- **Matching** (per request, first match wins): white-glove clients by
  `match.session_headers` (present alone) or `match.gated_session_headers`
  (present **and** UA starts with `match.ua_prefix`); else generic.
- **Session id** (D22 precedence): white-glove client headers → generic
  headers → endpoint body fields → WS query params → `ua|ip` fallback.
- **StatusSource** ABC (pluggable per client, D21):
  ```
  StatusSource
    async fetch_session_info(sid) -> (context, parent_id) | None
    async fetch_statuses(context) -> {sid: status} | None     # None = unreachable
    async fetch_pending(context)  -> {sid: kind} | None
  ```
  `OpencodeStatusSource` implements this over `http://<client-ip>:<port>`
  with the opencode semantics (per-directory context, absence = idle,
  pending permission/question = waiting, 30s unreachable grace). A
  `None`-valued source (generic clients) yields "no report" ⇒ busy-window
  inference.
- **Child rule** (per client, D20): `parent-chain` — walk the cached
  `parent_id` chain (config depth cap, cycle-safe) to the closest tracked
  ancestor; the child runs on that ancestor's slot if it holds one
  (recomputed each status poll; if the ancestor's slot is gone the child
  queues normally). `none` — no sharing.

## Request flow (HTTP; WS mirrors it — see D18)

1. **Route** by longest `path_prefix` → endpoint. No match ⇒
   `unknown_path_policy`: `block` → 403; `allow` → the `catch_all` endpoint
   (passthrough semantics) (D17).
2. **Identify client** (registry match) and **session id** (D22).
3. **Admit:**
   - endpoint `ready` → proceed to routing.
   - not ready:
     - `wait_policy=wait` → if server off/unknown, trigger power-on
       (cooldown-deduped); enqueue/hold (WS: accept + ping keep-alives).
     - `wait_policy=error` → 503 + retry hint (WS: close 1013). No queueing.
4. **Route (ready):** `concurrent`/`passthrough` → forward now. `queued` →
   session slot logic: own/shared slot + per-session cap OK → forward; else
   enqueue FIFO (held per step 3's wait policy).
5. **Forward** (path-transparent, host stripped, SSE-safe, read timeout per
   `proxy.target_read_timeout`).
6. **Complete:** release the slot exactly once; refresh the endpoint's
   activity; log to `requests` (ts, server, endpoint, client, session_id,
   wait_s, active_s, status_code, bytes).

## Background loops (one asyncio task each, all exception-guarded)

| Loop | Cadence | Work |
|------|---------|------|
| per-server power sync | ~5s | poll `power_state()`; detect external on/off; persist transitions; log `power_events` |
| per-endpoint readiness poll | `readiness.interval` (≤2s while queued) | readiness probe; update state; wake the queue manager on transitions |
| per-endpoint queue manager | 1s tick | client status polling (grouped by client base URL, per source poll interval), session status recompute, slot surrender, promote when `ready` + slot free, power-on re-issue on cooldown while waiting & server not on |
| per-server shutdown engine | 60s | idle off (D11) + cron off (D11); gating: `owned` ∧ `shutdown_override-on` ∧ no activity; verify actual power state before off (D14); cron deferral while active |
| store flusher | 1s / 30s | batch-write `requests`; hourly retention prune |

A transient error in any loop must never kill it (today's queue-manager
discipline, kept).

## Module layout (final)

```
relay/
  __init__.py              # __version__
  __main__.py              # python -m relay
  main.py                  # create_app(config_path) + run() console entry
  config.py                # YAML load, ${ENV} interpolation, validation
  models.py                # config dataclasses + runtime state dataclasses
  store.py                 # SQLite (aiosqlite): schema, reconciliation, retention
  servers.py               # ServerRuntime + power sync loop + shutdown engine
  endpoints.py             # EndpointRuntime + routing table + readiness polling
  queue.py                 # generic slot queue (from session_queue.py)
  clients.py               # ClientRegistry, matching, session-id resolution
  status_opencode.py       # OpencodeStatusSource (parity implementation)
  scheduler.py             # cron off-time evaluation (croniter), deferral
  monitor.py               # monitor page + /monitor/data + admin POSTs
  power/
    __init__.py            # backend registry: type name → class
    base.py                # PowerBackend ABC, PowerState enum
    ipmi.py                # IpmiBackend middle + BmcClient
    redfish.py             # RedfishBackend
    aws_ec2.py             # AwsEc2Backend
    noop.py                # NoopBackend
  transport/
    __init__.py
    http.py                # HTTP/SSE streaming forwarder
    ws.py                  # WS tunnel (keep-alive, 1013, bidirectional pump)
```

`apis.py` and `session_queue.py` are dissolved into the above (API profiles →
endpoint config; queue → `queue.py`).

## Monitor v2 surface

- `GET /monitor` — page: **servers** (power state, owned, next scheduled off,
  idle countdown, per-cycle auto-off toggle, manual power on/off buttons),
  **endpoints** (ready, queue depth, free slots, policies), **sessions**
  (today's table, per endpoint), unknown/passthrough activity.
- `GET /monitor/data` — JSON snapshot (same structure, server/endpoint aware).
- `POST /monitor/release` (manual slot release), `POST /monitor/shutdown`
  (per-server, per-cycle toggle), `POST /monitor/timeout` (live read
  timeout), new: `POST /monitor/power` (manual on/off ⇒ updates ownership per
  D12: manual-on **via the proxy** counts as proxy-initiated).
- `GET /healthz` — 200 while the app + its loops are alive (container
  HEALTHCHECK).
- **No auth** (D7) — trusted network, documented.

## Concurrency & threading notes

- One process, one asyncio loop (uvicorn). All state mutations happen on the
  loop; boto3 calls hop to an executor.
- The `wait_for_slot` promote/disconnect race (today's `entry.done` check,
  main.py:823–833) must be preserved in `queue.py` — promotion can win the
  race against disconnect/timeout; a promoted-then-lost entry is **released,
  not abandoned**.
- WS tunnels: two pump tasks per connection (client→target, target→client);
  close both on first failure/close; release the slot in `finally`.
