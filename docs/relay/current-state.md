# Current State (as of the rework proposal)

What exists today, so the implementation can be reasoned about against real
code rather than the README alone. Line numbers refer to the pre-rework
files at the repo root.

## One-paragraph summary

An energy-efficient AI gateway for **one** Gigabyte workstation managed via
its MegaRAC SP-X BMC (Redfish API). Clients (notably opencode coding agents,
which have limited retries) point at the proxy; the proxy holds their
requests — powering the workstation on if needed — instead of returning 503s,
queues sessions against the model ("spots"), tracks when known clients are
genuinely done via the client's own status API, and powers the workstation
off after idle. It is path-transparent (forwards whatever API the target
serves), SSE-capable, and ships with a self-contained HTML monitor page.

## File-by-file

### `main.py` (847 lines) — the whole app

- **Config** (27–104): everything from `os.getenv` — single target
  (`TARGET_SERVER_URL`, `HEALTH_PATH`), single BMC (`IPMI_HOST/USER/PASS`),
  idle/shutdown knobs, ~15 queue knobs (see `configuration.md` migration map).
- **Global `httpx.AsyncClient`** (109): one shared client for connection
  pooling; created with `verify=False` (BMC self-signed certs) in lifespan.
- **`state` dict** (122–137): `last_request_time` (monotonic),
  `is_powered_on`, `is_healthy`, `manage_power_with_proxy` (ownership),
  `shutdown_enabled` (live per-cycle copy), `target_read_timeout` (live),
  `last_power_on_attempt`, `power_on_cooldown = 30`,
  `discovered_system_path = "/redfish/v1/Systems/Self"` (hardcoded after BMC
  firmware behavior discovery — becomes the config field `power.system_path`).
- **Redfish** (165–251): `redfish_request` (auth, 10s timeout),
  `get_power_state` (GET system → `PowerState == "On"`, `None` if unknown),
  `power_on` (`Reset` `{"ResetType": "On"}`; sets ownership true and resets
  `shutdown_enabled` to the env default), `power_off`
  (`{"ResetType": "GracefulShutdown"}`; clears ownership).
- **`check_health`** (254–272): GET `service_url + HEALTH_PATH`, 2s timeout,
  200 = healthy. **This single flag is used both as "box is up" and "safe to
  promote queued requests"** — the rework splits it.
- **`sync_state`** (275–292): startup log of ONLINE/POWERED-OFF/UNKNOWN.
- **`resolve_session_id`** (293–324): precedence — (1) known client's own
  headers (opencode: `x-opencode-session`, or `X-Session-Id`/`x-session-affinity`
  gated on `opencode/` UA), (2) generic `SESSION_ID_HEADERS`, (3) API body
  field (OpenAI `user`, Anthropic `metadata.user_id`), (4) `ua:<UA>|ip:<IP>`.
- **`resolve_shared_spot`** (327–358): walks the cached opencode `parentID`
  chain (depth cap 10, cycle-safe) and returns the queue key of the closest
  tracked ancestor's spot — sub-agents run on the ancestor's spot.
- **`forward_request`** (361–424): buffers body, strips `host` header,
  `http_client.send(req, stream=True)` with `timeout=httpx.Timeout(None,
  read=state["target_read_timeout"] or None)` (0 = no read timeout), returns
  `StreamingResponse` over `aiter_raw()` (SSE-safe). Releases the queue entry
  **exactly once** in the generator's `finally`. 502 JSON on send failure.
- **`idle_monitor`** (427–466): every 60s; skips if per-cycle
  `shutdown_enabled` off; skips (and resets the timer) if
  `queue.has_activity()` (queue non-empty or any spot held); after
  `IDLE_TIMEOUT` elapsed, verifies **actual** BMC power state — only shuts
  down when state is `True` **and** `manage_power_with_proxy`; logs and
  leaves alone if `False`; skips if `None` (unknown) to be safe.
- **`queue_manager` / `_queue_manager_tick`** (469–604): 1s tick.
  1) polls known clients' status APIs — groups sessions by client base URL
  (`http://<client-ip>:<OPENCODE_STATUS_PORT>`), resolves each session's
  directory via `GET /session/{id}` (cached), polls `GET /session/status?directory=…`
  **and** the pending endpoints (`/permission`, `/question`) per directory;
  pending ⇒ status `waiting` (overrides map); session **absent from a fresh
  map** ⇒ `idle`; unreachable keeps last-known state; recomputes each
  session's `shared_spot_key` every poll.
  2) `queue.tick(now)` — recompute statuses, surrender expired spots.
  3) while the queue is non-empty: health check every 2s; if not healthy,
  re-issue `power_on()` on the 30s cooldown (the *first* waiter triggered the
  initial power-on in the request handler); if healthy, `queue._try_promote()`.
  4) prune `UnknownTracker`.
- **lifespan** (607–636): builds http client + `StatusPoller`, `sync_state()`,
  starts the two background tasks.
- **Monitor endpoints** (642–731): `GET /monitor` (HTML page),
  `GET /monitor/data` (JSON: config + live values + sessions + unknown),
  `POST /monitor/release` (manual spot release), `POST /monitor/shutdown`
  (per-cycle auto-off toggle), `POST /monitor/timeout` (live read timeout).
  **No authentication** (trusted network).
- **`proxy`** (734–847): `@app.api_route("/{path:path}", methods=[GET,POST,PUT,DELETE,PATCH])`.
  Sets `last_request_time` and **adopts ownership on any request** (line 753,
  including unknown-API passthrough). Unknown path ⇒ 403 (block) or unqueued
  forward + `UnknownTracker.record` (allow). Known path ⇒ read body, resolve
  session, `get_or_create_session`, enqueue. If the queue was empty, does a
  **fresh** health check first (stale manager bookkeeping) and triggers the
  initial power-on if needed. `wait_for_slot` (unbounded by default; 1s poll
  for disconnect/timeout) → `"ok"` | `"disconnected"` (503) | `"timed_out"`
  (504). **Race note** (823–833): if result ≠ `"ok"` but `entry.done`, the
  entry was promoted in the same instant the client left/timeout fired —
  release, don't abandon.

### `session_queue.py` (536 lines)

- `Session` (70–97): key `(client, session_id)`, client name, api, ip, ua,
  timestamps, `status`, `inflight`, `waiting`, `spot_held`,
  `shared_spot_key`, `idle_since`, `client_status*` (last state from the
  client's status API).
- `QueueEntry` (100–111): session, path, body, the Starlette `Request`
  (kept so a held request can detect its client hanging up), `go` Event,
  `done` flag.
- `SessionQueue` (114–478):
  - `max_spots` (`CONCURRENT_SESSIONS`), `max_inflight_per_session`
    (`CONCURRENT_SESSION_REQUESTS`: −1 unlimited, N cap, 0 serialized),
    `atomic_requests` (`REQUEST_MODE=atomic` ⇒ at most one in-flight request
    **globally** at any moment), `immediate_idle_release`.
  - `enqueue` → `_try_promote`: walk the FIFO front-to-back, promote the
    first eligible entries; a session with a spot (own or shared) is bounded
    by the per-session cap; a spotless session needs a free spot.
  - `_release_deadline` (330–356): known client — `waiting` (fresh report) ⇒
    release **immediately**; fresh `idle` + flag ⇒ release immediately;
    otherwise `idle_since + SESSION_EXPIRY`. Unknown client —
    `last_request_at + max(busy_window, session_expiry)`.
  - `tick` (377–404): recompute statuses, surrender expired spots, drop dead
    session records (no spot, nothing waiting/in-flight).
  - `wait_for_slot` (285–307): loop — promoted? client disconnected
    (`entry.request.is_disconnected()`)? deadline? sleep 1s.
  - `snapshot` (412–478): monitor ordering — spot holders first (acquisition
    order), then waiting sessions (earliest queue position), then shared-spot
    sub-agents; rich row data incl. `spot_releases_in`.
- `UnknownTracker` (481–536): unqueued unknown-API requests keyed by
  `(ip, ua)`; last method/path/target url + count; pruned by expiry.

### `clients.py` (250 lines)

- `ClientProvider` dataclass (63–81): `name`, `session_headers` (identify
  alone), `gated_session_headers` + `ua_prefix` (identify only when UA
  matches), `status_path`, `session_path`, `pending_paths`.
- `OPENCODE` (84–97): the one known client. `session_headers =
  ("x-opencode-session",)`; gated: `x-session-affinity`, `x-session-id` with
  `ua_prefix = "opencode/"`; `pending_paths = ("/permission", "/question")`.
- `STATUS_UNREACHABLE_GRACE = 30.0` (60): a session keeps its last known
  client status this long after the status API goes unreachable.
- `StatusPoller` (131–250): one base URL per client machine
  (`http://<client-ip>:<port>`), optional basic auth (user `opencode`),
  per-base `last_poll` + interval; caches `(base, sid) → directory` and
  `(base, sid) → parentID` via `fetch_session_info` (`GET /session/{id}`);
  `fetch_statuses` (`GET /session/status?directory=…` → `{sid: {type,…}}`);
  `fetch_pending` (permission/question lists → `{sid: kind}`). All return
  `None` on unreachable/garbage so callers keep last-known state.
- **Key opencode semantics** (module docstring, 1–44): idle sessions are
  **removed** from the status map (absence = idle report); a session blocked
  on user input stays `busy` in the map, hence the pending-endpoint
  tie-breaker; the map is **per working directory** (one opencode "instance"
  per dir), so the directory must be resolved first; sub-agents carry a
  `parentID`; the client must run `opencode serve` on a reachable interface
  (TUI default 127.0.0.1:random-port is not reachable from the proxy).

### `apis.py` (88 lines)

- `APIProfile` (26–32) + `API_PROFILES` (34–37): `anthropic`
  (`/v1/messages`, body `metadata.user_id`), `openai` (`/v1/`, body
  `user`). Longest-prefix match (`detect_api`). `extract_body_field` walks
  dotted JSON paths safely. These profiles become **per-endpoint config**
  (path prefix + session-id body fields) in the rework — the proxy stays
  path-transparent.

### `monitor.py` (237 lines)

- `build_data` (17–32): `{config: {…live values, power flags, queue_manager
  age, active_sessions, queued_requests}, sessions: queue.snapshot, unknown:
  tracker.snapshot}`.
- `HTML_PAGE` (35+): self-contained dark page, polls `/monitor/data` every
  2s, session/unknown tables, release buttons, per-cycle auto-off toggle,
  read-timeout input.

### `scripts/` — the behavioral spec

- `mock_target.py` (55 lines): FastAPI mock LLM — `GET /health`,
  `POST /v1/chat/completions` (delayed JSON or SSE stream when
  `"stream": true`), catch-all JSON echo. Env: `MOCK_TARGET_PORT` (8100),
  `MOCK_DELAY` (2s).
- `mock_opencode_status.py` (136 lines): models the real opencode server —
  per-directory `GET /session/status` (idle sessions **absent**),
  `GET /session/{sid}` (returns `directory` + `parentID`), pending
  `/permission` + `/question` (GET list per directory, POST/DELETE set/clear
  by `sid`), control endpoints `POST /set?sid=&type=busy|idle|retry`,
  `DELETE /set?sid=`, `POST|DELETE /parent?sid=&parent=`. Env:
  `MOCK_STATUS_PORT` (8101), `MOCK_STATUS_DIR` (`mock-dir`).
- `test_queue.py` (727 lines): the integration harness. Spawns the mock
  target, mock status server, and the proxy **as subprocesses** on 127.0.0.1
  (proxy 8123–8130 for alternate configs), with a **fake BMC**
  (`IPMI_HOST=127.0.0.1` — power-on attempts fail fast with connection
  refused and can never touch real hardware). Every env var the tests depend
  on is set explicitly in `BASE_ENV` **to shield from a local `.env`**
  (`load_dotenv` does not override existing environment) — keep this
  discipline in the ported pytest suite.

### Deployment

- `Dockerfile`: `python:3.10-slim`, `pip install -r requirements.txt`
  (fastapi, uvicorn, httpx, python-dotenv — unpinned), `COPY . .`,
  `CMD uvicorn main:app --host 0.0.0.0 --port 8000`.
- `compose.yaml`: builds the image, maps 8000, `env_file: .env`,
  `restart: unless-stopped`.
- `.env` / `.env.example`: the live config (see migration map in
  `configuration.md`).
- `.github/workflows/publish.yml`: on push to `main` — derive semver from
  conventional commits since last tag, git-cliff changelog, push tag, build +
  push `ghcr.io/<owner>/openai-ipmi-proxy:{latest,tag}`, GitHub release.
  **Runs no tests/lint before shipping** — fixed by the new CI (see
  `packaging-ci.md`).
- `cliff.toml`: git-cliff config, conventional-commit grouping.
- Git tags exist up to `v0.4.0`.

## Constraints & gotchas (carry these into the rework)

1. **Single global async client** (`verify=False`) for pooling — keep the
   pattern; `verify_ssl` becomes per-redfish-server config (default false).
2. **Monotonic clocks everywhere** for idle/queue timing
   (`time.monotonic()`) — sleep-safe, NTP-immune. Never mix wall clock into
   these paths.
3. **Bodies are buffered** (session-id extraction) — held queued requests
   keep their body in RAM. Acceptable at current scale; note it.
4. **The queue is in-memory** — single instance only.
5. **`discovered_system_path` is hardcoded** (BMC firmware quirk) — becomes
   `power.system_path` config with that value as the documented default.
6. **Status poller probes the client's *source IP*** — white-glove clients
   must expose their status API on a reachable interface; the rework keeps
   this model (decision: generalize "poll a status API").
7. **No authentication anywhere** (proxy + monitor) — trusted-network
   assumption, kept for now (login is parked, `decisions.md`).
8. **`REQUEST_MODE=atomic`** in the live `.env` differs from the code default
   (`parallel`) — behavior must follow config, and tests must pin it.
9. Python: current code runs on 3.10 (Docker) / 3.11–3.12 (dev). New code
   targets **≥3.11** (decision).
