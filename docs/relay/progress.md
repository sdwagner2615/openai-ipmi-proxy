# Implementation Progress

Phase 0 status: **complete** — gate green (ruff + mypy, 80 unit tests, 26
e2e tests, container smoke). Phase 1 status: **complete** — gate green
(ruff + mypy, 104 unit tests, 39 e2e tests, container smoke). Phase 2
status: **complete** — gate green (ruff + mypy, 120 unit tests, 40 e2e
tests, container smoke). Phase 3 status: **complete** — gate green
(ruff + mypy, 144 unit tests, 49 e2e tests). This file tracks completed
tasks, deviations from the design docs, and open questions, per
`phases.md`.

## Completed (Phase 3)

| Area | What landed |
|---|---|
| `ClientRegistry` (`clients.py`, D20/D22) | white-glove matching (plain + UA-gated headers, config order, first match wins) and the full D22 session-id resolution (white-glove headers → generic headers → endpoint body fields → WS query params → `ua\|ip`) now live on one object; `extract_body_field` moved alongside it. `endpoints.py` no longer carries resolution logic — the endpoint runtime takes the registry instead of provider/header lists |
| `StatusSource` ABC (`clients.py`, D21) | the pluggable per-client status contract: `fetch_session_info` / `fetch_statuses` / `fetch_pending`, the normalized `StatusReport`, context + parent-id caches, start-to-start poll stamping. The queue manager drives status polling through the interface only — no opencode vocabulary left in `queue.py` / `main.py` / `endpoints.py`; `resolve_shared_spot` reads the parent chain via `source.parent_of` |
| `OpencodeStatusSource` (`status_opencode.py`) | the parity reference implementation, moved out of the core: per-directory context resolution, status-map normalization (busy / retry + attempt detail), pending endpoints, basic auth (fixed user `opencode`); `create_status_source` is the single place that reads `status.kind` |
| `kind: none` (feat) | configured clients without a status source: identified and scoped by name but busy-window inference only — `Session.known` records whether the client has a status source, and the queue's status/deadline logic keys off it (see note 17 for the behavior this fixed) |
| E2E `test_opencode.py` (9 tests) | every `parity.md` §client-status behavior against the generalized stack, with config-driven client entries: C1 per-directory maps + cached contexts, C2 absence = idle, C3 pending-input tie-breaker, C4 the 30s unreachable grace (dedicated killable status mock), C5 poll grouping + cache pruning (mock request counters), C7 waiting-for-input release regardless of the flag, C8 retry surfacing + spot retention past expiry, Q8 parent-chain shared spots, and a `kind: none` client alongside opencode |
| Mock + fixture | `mock_opencode.py` extended (backward-compatible): per-session directories, `/offline` switch, per-endpoint `/fail` (partial failures), `/requests` counter; new `oc_env` e2e fixture gives each scenario isolated target + BMC + both status mocks |
| Unit | `test_status_opencode.py` (15 tests, MockTransport: fetch contracts, normalization, auth, caching, factory); D22 resolution incl. WS query params in `test_session_id.py`; `clients:` validation in `test_config.py`; `kind: none` deadlines in `test_queue.py` |
| Docs | `configuration.md` `kind: none` note; README "White-glove clients (config-driven)" section |

## Completed (Phase 2)

| Area | What landed |
|---|---|
| EC2 power backend (`power/aws_ec2.py`, D10) | boto3 client (keys or profile; adaptive retries for throttling), every call via `asyncio.to_thread`; `start_instances` / `stop_instances` / `terminate_instances` per `off_action`; state map `pending/running→on`, `stopping/stopped/shutting-down→off`, `terminated→off` + irreversible flag, anything else→`unknown`; unreachable API → `unknown` (state) / refused (action) |
| Registry + config (`power/__init__.py`, `models.py`, `config.py`) | `aws-ec2` registered with a **lazy** boto3 import (the extra stays optional; configuring it without `relay[aws]` fails at startup with a clear error); new optional `power.endpoint_url` field (fake / localstack / gateway) |
| Fake EC2 (`tests/mocks/fake_ec2.py`) | in-process `FakeEc2Client` (unit tests) **and** a standalone AWS Query-protocol server a *real* boto3 client talks to (e2e, like `mock_bmc.py`): `stopped→pending→running` boot delay, `stopping→stopped`, `terminated`, plus `/state` `/actions` `/set` inspection |
| EC2 unit tests (`tests/unit/test_power_ec2.py`, 16 tests) | state map (incl. unknown string → `unknown`), start/stop/terminate action selection, terminated refuses power-on (via observed state and via a successful terminate), unreachable API → `unknown`/`False`, close idempotent |
| E2E boot latency (`test_multi_server.py::test_ec2_boot_latency`, `ec2_env` fixture) | real proxy subprocess + real boto3 client against the fake EC2 API: a request triggers `start_instances`, the instance boots `pending→running` while the target is DOWN (endpoint stays not-ready), and the request is served only after the target starts — the wait-and-poll path with a slow boot |
| Deps + docs | `boto3` in the dev dependency group (CI runs the EC2 tests via fakes; still an optional *runtime* extra); README "AWS EC2 servers" section with the manual real-instance smoke (out of CI); `endpoint_url` in `configuration.md` + `config.yaml.sample` |

## Completed (Phase 1)

| Area | What landed |
|---|---|
| Scheduler + shutdown engine (`scheduler.py`, `servers.py`) | 60s-per-server tick (configurable via `proxy.shutdown_tick`, default 60); idle off (owned, override on, actual-state verified, unknown skipped); cron off via croniter with **deferral while active**; fake-clock unit tests (`tests/unit/test_scheduler.py`, 13 tests) |
| Request logging + batched flusher (`store.py`, `transport/http.py`, `endpoints.py`) | `RequestRecord` staged per completed request (wait_s, active_s, status, bytes), flushed ~1/s in one transaction and drained on shutdown; per-endpoint session upsert |
| Monitor v2 (`monitor.py`, `main.py`) | servers table (power, owned, idle countdown, next scheduled off, per-cycle toggle, manual on/off), endpoints table (ready, queue depth, free slots, policies), sessions per endpoint, unknown activity, recent `power_events`; new `POST /monitor/power` (manual on/off ⇒ ownership per D12); v1 `config` surface preserved for parity |
| Shutdown e2e (`test_shutdown.py`) | idle off, manual-on untouched, per-cycle toggle blocks off, restart restores ownership + override, cron off (per-minute, ~60s) |
| WebSocket transport (`transport/ws.py`, `endpoints.py`, `main.py`) | catch-all `@app.websocket("/{path:path}")` mirroring HTTP routing; accept-then-ping keep-alive while waiting; close 1013 on the error policy; bidirectional frame pump; slot held for the connection lifetime; session id from headers **and** query params; `tests/mocks/mock_ws_target.py` |
| WS e2e (`test_ws.py`) | echo tunnel, keep-alive while the target boots, 1013 on error policy, slot held for connection lifetime, query-param session id |
| Multi-server e2e (`test_multi_server.py`, `multi_env` fixture) | two servers (redfish mock-BMC + noop) on one proxy: longest-prefix routing + independent readiness, isolated per-endpoint queues, independent per-server shutdowns (shutting one down never touches the other) |

## Completed (Phase 0)

| Area | Commits |
|---|---|
| uv packaging skeleton (`pyproject.toml` + `uv.lock`, dev group) | `3046ec3` |
| ruff config + cleanup to green | `c024a5a` |
| Config system (`config.py`, `models.py`: YAML, `${ENV}`, full validation) | `185aac3`, `ce5bdc3` |
| Power package (`power/{base,ipmi,redfish,noop}.py`) | `184fd14` |
| Store (`store.py`: schema, WAL, retention, startup reconciliation) | `2385f13` |
| Queue + clients (`queue.py`, `clients.py`: spots, FIFO, caps, status poller) | `79ab2c0` |
| Server runtime (`servers.py`: state machine, cooldown, ownership, sync loop) | `6ed9ba8` |
| Transport (`transport/http.py`: SSE forward, host strip, read timeout, release-once) | `2632199` |
| Endpoint runtime (`endpoints.py`: readiness probing, admission, routing, manager) | `927211f` |
| App factory, CLI (`relay --config`), v1 monitor, sample files, legacy modules deleted | `5b1f984` |
| Admission/promotion/status-report races found by the e2e port | `01dcab3` |
| Test suite (80 unit + 26 e2e + mock target/opencode/BMC) | `6757db5` |
| Legacy harness removed (superseded by `tests/`) | `adb333a` |
| CI workflow; publish gated on lint+test, renamed, version build-arg | `505ddfd` |
| Docker (uv, non-root, aws extra, healthcheck), compose mounts, Makefile | `50f6431` |

## Deviations from the design docs

1. **403-blocked requests neither adopt ownership nor reset the idle
   timer.** With `proxy.unknown_path_policy: block`, a rejected request
   returns 403 *before* `on_routed_traffic()`. The legacy `main.py` set
   `last_request_time` at the top of the catch-all route, so requests that
   were later rejected still counted as activity (idle reset + adoption).
   Maintainer decision (locked during Phase 0): blocked traffic is not
   traffic the platform is serving. Routed (allowed) traffic — including
   catch-all passthrough — still adopts ownership per D12/D17.

2. **Monitor title renamed to "Relay Monitor"** (rename task; the page
   previously said "OpenAI IPMI Proxy Monitor").

3. **D8 made explicit: ON-but-not-ready means "wait".** The queue manager
   re-issues power-on only for `OFF`/`UNKNOWN` power states. A server that
   is ON but not ready is booting (model loading): the readiness loop
   probes at fast cadence while the queue is non-empty, and no power action
   is ever issued against an ON server.

## New behaviors surfaced by the parity port

These fix real races the ported e2e scenarios exposed; they preserve the
*asserted* behavior of every legacy scenario while making the timing
deterministic.

4. **Stale idle report rule** (`queue.py:_release_deadline`). For
   `immediate_idle_release` to fire, the client's idle report must postdate
   the last response we served (`client_status_at > last_request_at`). A
   report recorded while we were still serving that request is stale and
   releases the spot only after the normal `session.expiry` cooldown —
   otherwise a stale map could evict the K/V cache the moment a long
   response lands. The waiting-for-input path is exempt: a pending
   permission/question is a live state and always releases immediately.

5. **`pending_paths` default.** For `status.kind: opencode` clients,
   `pending_paths` defaults to `["/permission", "/question"]` (the legacy
   client hardcoded these). An explicit `[]` disables the pending-input
   poll.

6. **Catch-all passthrough enforcement (D17).** Unmatched paths routed to
   the `catch_all` endpoint use passthrough semantics (no queue, no session
   tracking) *regardless of that endpoint's `routing` mode* — the D17 rule
   "treated as passthrough" is now enforced in code (`admit_catch_all`).

7. **Status poll cadence (start-to-start interval).** `last_poll` is
   stamped at poll *start*, not completion. The old stamping made the gap
   between polls `tick period + sleep overshoot − fetch duration`, which
   dips below `poll_interval` and silently skipped every other manager
   tick — the 1s client-status poll degraded to 2s. Observed as a T21
   flake in the ported suite (a waiting-for-input report landing after the
   response and releasing the spot in the same tick it was recorded).

8. **Manager promotion.** The queue manager re-runs `_try_promote()` when
   `ready` flips true during a tick; entries already in the queue were
   previously promoted only by a new admission.

## Phase 1 notes

9. **Shutdown `power_events` reason strings.** `phases.md` named the idle and
   cron off reasons `idle_off` / `schedule_off`; the implementation logs
   `idle_timeout` (idle off) and `schedule` (cron off), with `manual` for the
   monitor's `POST /monitor/power`. `reason` is a free-form `Text` column (no
   enum), so this is a naming choice, not a schema constraint. The e2e suite
   asserts on `schedule` / `idle_timeout`.

10. **WS keep-alive ping goes through Starlette's private `_send`.** While a
    connection is held (wait policy) the proxy emits `websocket.ping` ASGI
    frames to keep idle intermediaries from timing it out. Starlette's public
    `WebSocket.send()` rejects `websocket.ping` once the socket is `CONNECTED`,
    so the ping is written via `websocket._send(...)` wrapped in
    `contextlib.suppress(Exception)` — a missing/unsupported ping must never
    break the hold (the connection stays open regardless).

11. **WS session id mirrors the configured header names as query params.**
    `resolve_session_id_ws` resolves a session from (1) a white-glove client's
    headers, (2) the configured generic headers, (3) WS query params that
    mirror those same header names (white-glove first, then generic), and (4)
    UA + source IP. WS URLs commonly carry ids/tokens in the query string
    because many clients cannot set headers on an upgrade.

## Phase 2 notes

12. **The EC2 state map uses the real EC2 state names.** `phases.md` listed
    `shut-down` / `expired`, which are not EC2 instance states; the
    implementation maps the six real ones (`pending`, `running`,
    `shutting-down`, `stopping`, `stopped`, `terminated`) and treats any
    other string as `unknown` (never act on unknown, D14). `pending` maps to
    **on** (booting) so the queue manager does not re-issue
    `start_instances` while the instance comes up (D8); readiness — the
    other signal — gates actual serving.

13. **`terminated` is an irreversible off.** With `off_action: terminate`
    the backend tracks the terminated state (from a `describe_instances`
    observation or a successful `terminate_instances`); while terminated,
    `power_on` refuses (`False`, no API call) because a terminated instance
    can never be started.

14. **`endpoint_url` power field (config addition).** The aws-ec2 power
    block accepts an optional `endpoint_url` so the real boto3 client can
    point at the fake EC2 API (the e2e boot-latency case) or at localstack /
    a compatible gateway; unset = the real AWS endpoint for `region`.

15. **boto3 in the dev dependency group.** The dev group carries `boto3` so
    CI (`uv sync --frozen`) can run the EC2 unit/e2e tests via the fakes; it
    remains an optional *runtime* extra (`relay[aws]`) — the package imports
    it lazily, only when an aws-ec2 server is configured.

## Phase 3 notes

16. **Unrecognized status entries are not reported.** The legacy manager
    kept a session's last-known state when its map entry was present but its
    `type` was not busy/idle/retry; the normalized source only reports
    recognized types, so such an entry now counts as "not active in this
    map" (an idle report). Opencode only emits busy/retry (and the mock only
    ever does), so this is unobservable in practice — recorded for
    completeness.
17. **`kind: none` fix: named clients without a status source use
    busy-window inference.** Before Phase 3, any client with a configured
    name was treated as "known" by the queue (`client != "unknown"`): a
    `status.kind: none` client's sessions went idle the moment the last
    response finished, ignoring `busy_window` entirely. `Session.known`
    records whether the client has a status source, and the queue's
    status/deadline logic keys off it, so report-less named clients follow
    the unknown-client rule (busy window, then
    `max(busy_window, expiry)` release).
18. **`oc_env` e2e fixture: per-scenario isolated mocks.** Each scenario
    gets its own mock target, mock BMC, and both opencode status mocks, so
    the `/offline` switch, per-directory maps, and request counters start
    clean. The C4 unreachable-grace test takes ~40s of real time by design
    (the 30s grace is a parity constant, not configurable).

## Open / deferred

- E2E suite runtime: ~9 min locally (real subprocesses, by design; the cron
  and multi-server scenarios wait on real 1s/60s cadences, and the Phase-3
  unreachable-grace case waits out the 30s parity constant).

## Verification

- Gate: `uv run ruff check . && uv run ruff format --check . && uv run mypy`
  green; `uv run pytest -m unit` 144 passed; `uv run pytest -m e2e` 49 passed
  (multiple consecutive full runs).
- Parity: faithful port of every legacy harness scenario (T1–T21, T23, T24)
  plus boot-wait and adoption (unknown-path) suites; the old harness runs
  green on the pre-rework tree for comparison. Phase 1 adds the shutdown,
  WebSocket, and multi-server suites (5 + 5 + 3 e2e tests). Phase 2 adds the
  EC2 unit suite (16 tests) and the EC2 boot-latency e2e case (1 test, real
  boto3 against the fake EC2 API — never real AWS). Phase 3 adds
  `test_opencode.py` (9 e2e tests covering every §client-status behavior
  against the generalized stack, incl. a `kind: none` client alongside
  opencode) and the status-source unit suite (15 tests); every previously
  ported test stays green untouched.
- Container: image builds (uv, non-root, aws extra), boots with the noop
  smoke config, `/healthz` 200, `/monitor/data` parses, default DB path
  exercised.
