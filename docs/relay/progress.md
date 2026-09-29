# Implementation Progress

Phase 0 status: **complete** — gate green (ruff + mypy, 80 unit tests, 26
e2e tests, container smoke). This file tracks completed tasks, deviations
from the design docs, and open questions, per `phases.md`.

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

## Open / deferred

- `requests`/`sessions` table writes + flusher batching — Phase 1 (task 7).
- Scheduler (cron off + deferral) + shutdown-engine e2e — Phase 1.
- WebSocket transport, monitor v2 — Phase 1.
- `aws-ec2` backend, fake, e2e case — Phase 2.
- Client generalization (`StatusSource` plugin, config-driven entries) —
  Phase 3.
- E2E suite runtime: ~4.5 min locally (real subprocesses, by design).

## Verification

- Gate: `uv run ruff check . && uv run ruff format --check . && uv run mypy`
  green; `uv run pytest -m unit` 80 passed; `uv run pytest -m e2e` 26 passed
  (multiple consecutive full runs after the poll-cadence fix).
- Parity: faithful port of every legacy harness scenario (T1–T21, T23, T24)
  plus boot-wait and adoption (unknown-path) suites; the old harness runs
  green on the pre-rework tree for comparison.
- Container: image builds (uv, non-root, aws extra), boots with the noop
  smoke config, `/healthz` 200, `/monitor/data` parses.
