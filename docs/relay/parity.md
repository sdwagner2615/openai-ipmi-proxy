# Parity Checklist

Every behavior the current system has that must survive the rework. Each
item: the behavior, where it lives today, and how the rework verifies it.
**Changing an asserted behavior without maintainer sign-off is a parity
violation** (see `phases.md` cross-phase rules).

## Power — boot & wake

| # | Behavior | Today | Verified by |
|---|----------|-------|-------------|
| P1 | **No 503s during boot.** If the target is off, the first waiting request powers it on; ALL waiting requests simply wait until it is healthy. No "model loading" 503 is ever returned. | `main.py:809–822` (first waiter powers on), `queue_manager` step 3 (`main.py:584–599`) | `test_boot_wait.py` |
| P2 | **Power-on cooldown dedupe:** power-on is re-issued only every `30s` (`power_on_cooldown`) by whoever is next due (request path or manager) — concurrent waiters never stampede the BMC. | `main.py:134–135, 813–817, 594–597` | `test_boot_wait.py` (assert ≤ N BMC calls during a boot) |
| P3 | **Fresh health check on an empty queue:** when the queue is empty the manager's bookkeeping may be stale, so the request path does its own readiness check before enqueueing (healthy ⇒ promote immediately, same latency as pre-queue; dead ⇒ initial power-on). | `main.py:810–817` | `test_boot_wait.py`, `test_queue.py` |
| P4 | Startup state sync logs ONLINE / POWERED-ON-NOT-HEALTHY / POWERED-OFF / UNKNOWN and seeds readiness + power state. | `sync_state` `main.py:275–292` | e2e startup assertions + logs |

## Power — shutdown & ownership (the critical section)

| # | Behavior | Today | Verified by |
|---|----------|-------|-------------|
| P5 | **Ownership invariant:** the proxy only powers OFF a server if `manage_power_with_proxy` is true. A server turned on manually and never used through the proxy is **never** shut down. | `main.py:456–460`, state set at `230, 753, 250` | `test_shutdown.py` (manual-on scenario) + `test_ownership.py` |
| P6 | **Adoption on traffic:** ANY request routed through the proxy (including unknown-API passthrough) sets ownership true. | `main.py:751–753` | `test_ownership.py`, `test_unknown_paths.py` |
| P7 | **Ownership cleared on proxy-initiated off**; taken on proxy-initiated on. | `main.py:246–251, 227–232` | `test_ownership.py` |
| P8 | **Per-cycle auto-off switch:** seeded from config default; toggleable from the monitor for the current cycle; **reset to the config default on every proxy-initiated power-on**. | `main.py:43–44, 127–130, 231–232, 704` | `test_shutdown.py`, `test_ownership.py` (incl. restart restore via SQLite) |
| P9 | **Never down while active:** the idle monitor restarts the idle timer while the queue has work (queued requests OR held spots). | `idle_monitor` `main.py:448–450` | `test_shutdown.py` |
| P10 | **Verify actual state before off:** idle off checks the BMC/CSP power state first; proceeds only on `True`; `False` ⇒ skip (already off); `None`/unknown ⇒ skip **to be safe**. | `main.py:453–466` | `test_shutdown.py` (incl. unknown-state case) |
| P11 | **Monotonic idle clock:** idle measured with `time.monotonic()` (sleep-safe, NTP-immune); never wall-clock in idle/queue paths. | `main.py:113–116, 123` | code review + `test_shutdown.py` (no wall-clock usage) |
| P12 | 60s shutdown tick cadence; idle timer reset after a shutdown attempt (or re-poll) to prevent flapping. | `main.py:432, 462` | `test_shutdown.py` |

## Queue & spots

| # | Behavior | Today | Verified by |
|---|----------|-------|-------------|
| Q1 | **FIFO global wait** per endpoint; a request is promoted when its session may run (free slot or slot already held; per-session cap not hit) AND the endpoint is ready. | `session_queue.py:197–242` | `test_queue.py` |
| Q2 | **Spots, not requests:** a session acquires one of `concurrency` slots when its first request is promoted and **keeps it until genuinely idle** — protecting the model's K/V cache across long tool calls. | `session_queue.py:209–242, 366–375` | `test_queue.py` |
| Q3 | **Per-session in-flight cap:** `per_session_requests` (−1 unlimited / N cap / 0 serialized). | `session_queue.py:124–126, 197–207` | `test_queue.py` |
| Q4 | **Atomic mode:** `request_mode=atomic` ⇒ at most ONE in-flight request **within the endpoint** at any moment; sessions (and slots) still overlap, requests alternate FIFO. | `session_queue.py:128–131, 198–200` | `test_queue.py` |
| Q5 | **Slot release deadlines:** known client — fresh `waiting` report ⇒ release **immediately** (no cache to protect while blocked on a human); fresh `idle` + `immediate_idle_release` ⇒ immediately; else `idle_since + expiry`. Unknown client — `last_request_at + max(busy_window, expiry)`. | `session_queue.py:330–356` | `test_queue.py`, `test_opencode.py` |
| Q6 | **Expiry semantics:** when a spot is surrendered the session is forgotten; future requests with the same id queue at the **back as new**. | `session_queue.py:20, 366–375` | `test_queue.py` |
| Q7 | **Revival:** a new request for a session in its idle countdown restarts the countdown and keeps the spot. | `session_queue.py:165–172` | `test_queue.py` |
| Q8 | **Sub-agent shared spot:** a session whose parent-id chain leads to a tracked session runs on the closest tracked ancestor's slot (depth cap 10, cycle-safe); recomputed every status poll; if the ancestor holds no slot the child queues normally; sharing never takes a slot from another session. | `main.py:327–358`, `session_queue.py:184–196, 234–239` | `test_queue.py`, `test_opencode.py` |
| Q9 | **Disconnected client in queue:** a held request whose client hangs up (misconfigured timeout) is detected (`is_disconnected()` polled ~1s) and dropped from the queue. | `session_queue.py:285–307`, `main.py:832–833` | `test_queue.py` |
| Q10 | **Queue timeout:** `queue_timeout > 0` ⇒ 504 with the held duration after N seconds; `0` ⇒ unbounded hold. | `main.py:822–845` | `test_queue.py` |
| Q11 | **Promote/disconnect race:** if `wait_for_slot` returns non-"ok" but the entry was already promoted (`done`), the slot is **released, not abandoned**. | `main.py:823–833` | `test_queue.py` (unit-level race test) |
| Q12 | **Slot released exactly once** — in the streaming generator's `finally`, for every outcome (success, mid-stream error, read timeout). | `main.py:399–418` | `test_queue.py`, `test_ws.py` |
| Q13 | **Manager tick discipline:** 1s tick; a transient error never kills the queue manager (spots would never be surrendered otherwise). | `main.py:469–491` | fault-injection unit test |

## Session identification

| # | Behavior | Today | Verified by |
|---|----------|-------|-------------|
| S1 | Precedence: (1) known/white-glove client's own headers, (2) generic configured headers, (3) API body field, (4) `ua:<UA>|ip:<IP>`. (Rework inserts WS query params as step 4 per D22.) | `main.py:293–324` | `test_session_id.py` |
| S2 | **UA-gated headers:** opencode's `X-Session-Id`/`x-session-affinity` count only with an `opencode/` User-Agent (so other tools sending the same headers aren't misattributed); `x-opencode-session` counts alone. | `clients.py:84–97, 102–118` | `test_session_id.py`, `test_clients.py` |
| S3 | Body-field extraction walks dotted JSON paths (`metadata.user_id`), tolerates malformed bodies/missing fields, accepts numeric ids. | `apis.py:56–85` | `test_session_id.py` |
| S4 | Session identity is `(client, session_id)` **scoped to the endpoint** (rework: per-endpoint queues; same id on two endpoints = two sessions). | `session_queue.py:147–173` | `test_multi_server.py` |

## Client status (opencode white-glove)

| # | Behavior | Today | Verified by |
|---|----------|-------|-------------|
| C1 | **Per-directory status maps:** a session's working directory is resolved once via `GET /session/{sid}` (cached until the session is gone) and statuses are polled with `GET /session/status?directory=<dir>`; one poll per distinct (client machine, directory) per interval. | `clients.py:131–219`, `main.py:498–524` | `test_opencode.py` (two directories, one client) |
| C2 | **Absence = idle:** a fresh status map that lacks the session IS the client's idle report (opencode deletes idle entries). | `main.py:560–565` | `test_opencode.py` |
| C3 | **Pending input = waiting:** sessions blocked on a pending permission/question stay `busy` in the map, so the pending endpoints (`GET /permission`, `GET /question`, per-directory) are the tie-breaker; a confirmed pending request forces status `waiting` no matter what the map says; a failed pending poll never clears the flag. | `main.py:537–552`, `clients.py:221–250` | `test_opencode.py` |
| C4 | **30s unreachable grace:** while the status API is unreachable, sessions keep their last-known status for `STATUS_UNREACHABLE_GRACE` (30s), then fall back to idle; partial/garbage answers are ignored (None). | `clients.py:57–60`, `session_queue.py:311–327` | `test_opencode.py` (unreachable window) |
| C5 | **Poll grouping:** one status base per client machine (`http://<client-ip>:<port>`), polled at `poll_interval`; directory + parent caches are pruned when sessions vanish. | `main.py:498–505, 574–576` | `test_opencode.py` |
| C6 | **Client requirement (documented, not enforced):** the client must run its status server on a reachable interface (e.g. `opencode serve --hostname 0.0.0.0 --port 4096`). | `clients.py:40–44` | README |
| C7 | **Waiting-for-input release:** a fresh `waiting` report releases the spot immediately regardless of `immediate_idle_release`. | `session_queue.py:337–344` | `test_opencode.py` |
| C8 | **Retry status:** `{"type":"retry"}` is surfaced (monitor detail shows attempt count); retry sessions keep their spot. | `main.py:566–571` | `test_opencode.py` |

## Proxying

| # | Behavior | Today | Verified by |
|---|----------|-------|-------------|
| X1 | **Path-transparent:** every path forwarded verbatim to `service_url` (no API-specific rewriting); the only target-specific assumptions are the readiness path and the session-id body fields. | `main.py:361–374` | `test_queue.py`, `test_multi_server.py` |
| X2 | **Host header stripped** on forward (target rejects mismatched Host). | `main.py:372–373` | unit test on the forwarder |
| X3 | **SSE-safe streaming:** `stream=True` + `aiter_raw()`; SSE token streams pass through unbuffered; mid-stream errors yield an error marker then close (today's behavior). | `main.py:389–424` | `test_queue.py` (stream scenario) |
| X4 | **Per-chunk target read timeout:** `target_read_timeout` bounds per-chunk silence (SSE) or whole-body wait; `0` = no timeout; live-tunable from the monitor (applies to new requests only). | `main.py:48–50, 382–389, 712–731` | `test_queue.py` |
| X5 | **502 with error JSON** when the send to the target fails; slot released on that path too. | `main.py:393–397` | `test_queue.py` |
| X6 | **Unknown paths:** `allow` ⇒ forward unqueued + tracked (ip, ua, method, path, target url, count) and listed on the monitor; `block` ⇒ 403 with the OpenAI-style error body (`code: unknown_api_blocked`). | `main.py:756–780, 758–770` | `test_unknown_paths.py` |
| X7 | Methods supported: GET/POST/PUT/DELETE/PATCH on the HTTP catch-all (rework adds the WS catch-all). | `main.py:734` | e2e |

## Monitor & ops surface

| # | Behavior | Today | Verified by |
|---|----------|-------|-------------|
| M1 | `GET /monitor` self-contained page (no external assets) polling `GET /monitor/data` every 2s. | `monitor.py` | e2e (fetch both) |
| M2 | Session table ordered: spot holders (acquisition order) → waiting (earliest queue position) → shared-spot sub-agents; columns incl. status, client status + age, spot (held/shared/none), releases-in, in-flight, waiting, queue position, last path, client ip. | `session_queue.py:412–478` | `test_queue.py` (data shape) |
| M3 | `POST /monitor/release` manually surrenders a spot (404 when none held). | `main.py:663–685` | `test_queue.py` |
| M4 | `POST /monitor/shutdown` per-cycle toggle (400 on bad body). | `main.py:688–709` | `test_shutdown.py` |
| M5 | `POST /monitor/timeout` live read-timeout set (400 on bad body; 0 = none). | `main.py:712–731` | `test_queue.py` |
| M6 | No authentication on any of the above (trusted network — documented). | all | documented |

## Deployment & ops behavior

| # | Behavior | Today | Verified by |
|---|----------|-------|-------------|
| O1 | `verify=False` toward the BMC (self-signed certs) — becomes `power.verify_ssl` (default false), kept for redfish. | `main.py:614` | `test_power_redfish.py` |
| O2 | Single shared `httpx.AsyncClient` (pooling) for all outbound traffic. | `main.py:106–109` | code review |
| O3 | Idle timer reset on any proxied request (the "last activity" anchor). | `main.py:750` | `test_shutdown.py` |
| O4 | Graceful shutdown of the proxy cancels the background tasks and closes the client. | `main.py:632–636` | e2e (SIGTERM) |

## Things that are NOT parity (intentional changes)

- Single global queue + single `healthy` flag → **per-endpoint** queues +
  split power/readiness signals (D8) — same *semantics* for the
  single-endpoint case, different plumbing.
- Env-only config → YAML + env files (D4).
- Hardcoded `/redfish/v1/Systems/Self` → `power.system_path` (same default).
- `openai-ipmi-proxy` → `relay` naming; `uvicorn main:app` → `relay` entry.
- New: WebSockets, catch_all/passthrough, schedules, SQLite, /healthz.
