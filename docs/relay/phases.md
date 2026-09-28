# Implementation Phases

Work on the `relay` branch. Each phase ends with a **gate**; a phase is not
done until its gate is green. Phases are sequential — later phases assume
earlier gates passed.

## First actions for the new agent

1. Read all docs in this directory (order in `README.md`).
2. `uv venv && uv sync` — establish the dev environment.
3. Run the existing harness once to see the current behavior live:
   `venv/bin/python scripts/test_queue.py` (it self-manages ports/processes;
   read it first).
4. Skim the four core files: `main.py`, `session_queue.py`, `clients.py`,
   `apis.py` (line map in `current-state.md`).
5. Start Phase 0.

---

## Phase 0 — Foundations, rename, packaging, parity harness

**Goal:** the codebase is the `relay` package with the new config system,
power abstraction, storage, and tooling — but behaviorally identical for a
single-server/single-endpoint deployment. The ported test suite is green.

Tasks:

1. **Rename** repo artifacts in-repo to `relay` (package dir, logger names,
   monitor title, Docker CMD, `cliff.toml` repo URL placeholder). GitHub
   repo rename is the maintainer's job — coordinate before pushing.
2. **Packaging skeleton:** `pyproject.toml` + `uv.lock` (see
   `packaging-ci.md`); create `relay/` package; move `main.py`, `clients.py`,
   `session_queue.py`, `apis.py`, `monitor.py` in as
   `relay/{main,clients,queue,apis→see below,monitor}.py`; add `__init__.py`
   (`__version__` via setuptools-scm), `__main__.py`, `relay --config` CLI
   (argparse) + `create_app(config_path)` factory. `apis.py` dissolves: the
   two profiles become the sample config's endpoint session-body-field
   entries (OpenAI `user`, Anthropic `metadata.user_id`); keep
   `extract_body_field`-style dotted-path extraction in `config`/`endpoints`.
3. **`config.py`:** YAML load, `${ENV}` interpolation, full validation list
   from `configuration.md`; dataclasses in `models.py`
   (`ProxyConfig`, `ServerConfig`, `PowerConfig` per type, `EndpointConfig`,
   `ReadinessConfig`, `SessionConfig`, `ClientConfig`, `MatchConfig`,
   `StatusConfig`, `ChildrenConfig`).
4. **Sample files:** `config.yaml.sample` (the full annotated example from
   `configuration.md`), `.env.sample`, `secrets.env.sample`; update
   `.gitignore` (`config.yaml`, `secrets.env`, `*.db`, `.ruff_cache/`,
   `.mypy_cache/`, `.pytest_cache/`, `coverage.xml`); delete `.env.example`
   (superseded).
5. **Power package:** `power/base.py` (`PowerBackend` ABC, `PowerState`
   enum), `power/ipmi.py` (`IpmiBackend` + `BmcClient`: host/user/password/
   `verify_ssl`/timeout, retry/backoff, graceful-off helper, state
   normalization), `power/redfish.py` (migrate `main.py:165–251`;
   `system_path` from config), `power/noop.py` (scriptable state, action
   log), `power/__init__.py` (type → class registry; `aws-ec2` entry added
   in Phase 2).
6. **`servers.py` — `ServerRuntime`:** state machine
   (`off/powering_on/on/powering_off/unknown`), power-on cooldown
   (default 30s), **ownership** (`owned`, `adopt_on_traffic`, cleared on
   proxy-off), per-cycle `shutdown_override` (reset on proxy-initiated
   power-on), power-state sync loop, `power_events` logging.
7. **Split the signals (D8):** `check_health` becomes per-endpoint readiness
   probing (`EndpointRuntime`, `endpoints.py`) with `readiness.*` config;
   power state comes only from `PowerBackend`.
8. **`queue.py`:** `session_queue.py` generalized to be instantiable per
   endpoint (same semantics — see `parity.md` §queue). `UnknownTracker`
   becomes the passthrough/catch-all activity tracker.
9. **`store.py`:** schema + WAL + retention + **startup reconciliation**
   (restore `owned`/`shutdown_override`) per `storage.md`; wire
   `server_runtime` writes into `ServerRuntime`.
10. **`transport/http.py`:** move `forward_request` (SSE streaming, host
    strip, read timeout, exactly-once slot release) out of `main.py`.
11. **`main.py`:** build registries from config; longest-prefix router;
    `GET /healthz`; keep monitor endpoints working (they become the v1
    monitor until Phase 1's v2).
12. **Tooling:** ruff config + cleanup pass until green; mypy baseline
    (annotate queue/scheduler/store/servers; record leftovers); `ci.yml`
    (lint/test/container per `packaging-ci.md`); `publish.yml` gated +
    renamed + version build-arg.
13. **Docker:** new Dockerfile (3.12-slim, uv, non-root, healthcheck),
    `.dockerignore`, updated `compose.yaml` (mounts), container smoke job.
14. **Tests:** port `scripts/mock_target.py` → `tests/mocks/mock_target.py`,
    `scripts/mock_opencode_status.py` → `tests/mocks/mock_opencode.py`, add
    `tests/mocks/mock_bmc.py` (scriptable Redfish mock); port
    `scripts/test_queue.py` **faithfully** into `tests/e2e/` (subprocess
    proxy, env shielding, free ports, `wait_until` helpers) with the
    config-file + env-file setup replacing `BASE_ENV`; write the Phase-0
    unit tests listed in `packaging-ci.md` (config, queue, session id,
    ownership incl. the manual-on scenario, store, redfish mapping).
15. **README.md** rewrite: what relay is, 3×`cp` setup, config reference
    (link to `configuration.md`), dev guide (uv/ruff/mypy/pytest), CI/CD
    notes, parking lot. Delete `scripts/` (superseded by `tests/`).

**Gate (feature-parity checkpoint 1):**
- `uv run ruff check . && uv run ruff format --check . && uv run mypy` green.
- `uv run pytest -m unit -m e2e` **fully green** — every ported scenario,
  including: no-503 boot wait, FIFO/spot semantics, opencode status
  behavior, sub-agent shared spot, **a manually-on (unowned) server is never
  shut down**, ownership restored across a proxy restart, unknown-path
  allow/block.
- Container smoke passes: image boots with the noop sample config,
  `/healthz` 200, `/monitor/data` parses.

**Done-when:** a single-server deployment configured via `config.yaml`
behaves exactly like today's `.env` deployment (the maintainer verifies on
the real workstation before Phase 1 starts).

---

## Phase 1 — Multi-server + transports

**Goal:** N servers, N endpoints, per-endpoint queues, WebSocket transport,
per-server shutdown engine (idle + cron), monitor v2.

Tasks:

1. Multi-server registry: every `ServerRuntime`/`EndpointRuntime` running in
   parallel; per-server power sync; per-endpoint readiness polling (normal
   cadence + fast cadence while its queue is non-empty).
2. Routing table: longest-prefix across **all** endpoints; `catch_all` +
   `passthrough` semantics; `unknown_path_policy` allow/block (403 shape
   preserved).
3. Per-endpoint queue wiring: `wait_policy` enforcement (`wait` hold vs
   `error` 503 + retry hint), `queue_timeout` 504, slot accounting per
   endpoint.
4. **WebSocket transport** (`transport/ws.py`, D18): catch-all
   `@app.websocket("/{path:path}")`; prefix routing; accept-then-ping
   keep-alive while waiting (`wait`); close 1013 (`error`); bidirectional
   frame pump; slot held for connection lifetime (released in `finally`);
   session id from headers **and** query params.
5. **`scheduler.py` + shutdown engine (per server, D11/D12/D14):** 60s tick;
   idle off (all endpoints idle for `idle_timeout`, owned, override on,
   verify actual state, skip unknown); cron off via croniter with **deferral
   while active**; `power_events` reasons (`idle_off`, `schedule_off`).
6. **Monitor v2** (`monitor.py`): servers table (power state, owned, next
   scheduled off, idle countdown, per-cycle toggle, manual on/off),
   endpoints table (ready, queue depth, free slots, policies), sessions
   (today's table, per endpoint), passthrough/unknown activity, recent
   `power_events`; `POST /monitor/power` (manual on/off ⇒ ownership update
   per D12).
7. `requests` logging wired into the transport layer (wait_s, active_s,
   status, bytes) via the store flusher.
8. E2E: `test_multi_server.py` (two servers — mock Redfish + noop — with
   isolated queues and independent shutdowns), `test_ws.py`, `test_shutdown.py`
   (cron deferral, idle off, manual-on untouched, per-cycle toggle,
   restart restore), `test_unknown_paths.py`.

**Gate:** all Phase-0 tests still green + new e2e suites green; multi-server
monitor renders correctly (visual check by maintainer).

---

## Phase 2 — AWS EC2 backend

**Goal:** `type: aws-ec2` servers work end-to-end (via fakes in CI; real
instance is a manual smoke).

Tasks:

1. `power/aws_ec2.py`: boto3 client (keys or profile), `start_instances` /
   `stop_instances` (or `terminate_instances` per `off_action`),
   `describe_instances` → state map (`pending→booting-on`, `running→on`,
   `stopping→off`, `shut-down→off`, `terminated→off`+flag,
   `expired/stopped→off`); boto3 calls via `asyncio.to_thread`; retry on
   throttling.
2. Register `aws-ec2` in `power/__init__.py`; validation for the power block
   (region + instance_id + (keys | profile)).
3. `tests/mocks/fake_ec2.py` (in-process fake boto3 client or stub) +
   `test_power_ec2.py` unit tests (state map, action selection incl.
   terminate, error handling on unreachable API → `unknown`).
4. E2E: `test_multi_server.py` gains an EC2-fake server case (boot latency:
   readiness stays not-ready after "start" until the mock target flips on —
   exercises the wait-and-poll path with a slow "boot").
5. Document the manual real-instance smoke test (out of CI) in the README.

**Gate:** unit + e2e green with the fake; maintainer's real-instance smoke
(documented steps) passes once.

---

## Phase 3 — White-glove generalization (opencode parity)

**Goal:** clients are config-driven; `OpencodeStatusSource` is a plugin;
opencode behavior is byte-for-byte preserved (D20, D21).

Tasks:

1. `clients.py` — `ClientRegistry`: matching (plain vs UA-gated headers),
   session-id resolution per D22 (incl. WS query params), generic fallback.
2. `StatusSource` ABC (see `architecture.md`); `status_opencode.py`
   `OpencodeStatusSource`: per-directory context resolution
   (`GET /session/{id}` → directory + parentID, cached), `GET
   /session/status?directory=…` (absence = idle), pending
   `/permission`+`/question` tie-breaker, 30s unreachable grace, basic-auth
   (user `opencode`), per-client poll interval.
3. Child rule `parent-chain` (config depth, cycle-safe) → shared slot
   (generalized `resolve_shared_spot`); `none` for generic clients.
4. Remove all opencode-specific code from `queue.py`/`main.py`/`endpoints.py`
   (they depend only on the `StatusSource` interface + the client registry).
5. Config: `clients:` entries validated; `kind: none` = no status source
   (busy-window only).
6. E2E: `test_opencode.py` covers **every** behavior in `parity.md`
   §client-status (most ported in Phase 0 — this phase proves they pass
   against the generalized implementation with config-driven client
   entries, including a second client entry with `kind: none` alongside
   opencode).

**Gate (feature parity complete):** full suite green; maintainer points a
real opencode at the relay deployment and the monitor shows busy/idle/retry,
shared sub-agent spots, and waiting-for-input exactly as before.

---

## Phase 4 — Parking lot (scope only when explicitly requested)

- Prometheus `/metrics` (queue wait, active sessions, power transitions,
  uptime, per-endpoint request counts) — data already in SQLite.
- Cost accounting: optional per-server rate in config → `$` totals from
  `power_events`/`requests`.
- Admin login/token on `/monitor` + power routes (D7).
- Config hot-reload (mtime watch; additive apply, destructive reject).
- Classic-IPMI `IpmitoolBackend` (structure already exists).
- mypy strict ratchet; coverage threshold.

---

## Cross-phase rules

- **Never break a ported test.** If a new feature requires changing a
  ported test's *setup* (config shape), that's fine; changing its
  *asserted behavior* requires maintainer sign-off (it encodes parity).
- **Conventional commits** per logical step (`feat:`, `refactor:`,
  `test:`, `fix:`, `chore:`) — the changelog is generated from them.
- **One phase per PR or a small stack of PRs** — the gate must be green on
  every PR (CI enforces it).
- Progress notes: keep a `docs/relay/progress.md` (created by the
  implementer) tracking completed tasks, deviations, and open questions.
