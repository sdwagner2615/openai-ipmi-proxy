# Storage (SQLite)

Rule (D5): **config owns *what exists and the policy*; SQLite owns *what is
happening and what has happened*.** The app never reads policy from the DB
and never writes policy to the DB.

Library: `aiosqlite`, **WAL mode** (concurrent readers during writes), single
async writer (the app's asyncio loop). File: `proxy.store.path`.

## Schema

```sql
-- One row per server: current runtime state, reconciled with config at startup.
CREATE TABLE IF NOT EXISTS server_runtime (
    name              TEXT PRIMARY KEY,
    power_state       TEXT,              -- on | off | unknown  (coarse; transient
                                         --   powering_on/off lives in memory)
    owned             INTEGER NOT NULL,  -- 1 = proxy owns the power lifecycle (D12)
    shutdown_override INTEGER,           -- NULL = use config default; else 0/1 for
                                         --   the current power cycle (monitor toggle)
    cycle_id          TEXT,              -- uuid per proxy-initiated power-on
    updated_at        REAL               -- monotonic-ish epoch (time.time() fine here)
);

-- One row per endpoint: latest readiness bookkeeping.
CREATE TABLE IF NOT EXISTS endpoint_runtime (
    name          TEXT PRIMARY KEY,
    ready         INTEGER,               -- 0/1
    ready_since   REAL,                  -- when it last transitioned to ready
    last_check_at REAL
);

-- Append-only audit of every power action. Feeds metrics + future cost.
CREATE TABLE IF NOT EXISTS power_events (
    id           INTEGER PRIMARY KEY,
    ts           REAL,
    server       TEXT,
    action       TEXT,    -- power_on | power_off | schedule_off | idle_off
                    -- | state_sync | external_change
    reason       TEXT,    -- request_wake | idle_timeout | schedule | manual
                    -- | adopted | startup_sync | ...
    initiated_by TEXT,    -- proxy | monitor-ui | startup
    success      INTEGER
);

-- Append-only activity log: metrics + future cost basis (D13: never gates shutdowns).
CREATE TABLE IF NOT EXISTS requests (
    id             INTEGER PRIMARY KEY,
    ts             REAL,
    server         TEXT,
    endpoint       TEXT,
    client         TEXT,        -- white-glove client name, or "ip:<ip> (<ua>)"
    session_id     TEXT,
    wait_seconds   REAL,        -- time held in the queue before promotion
    active_seconds REAL,        -- time in flight (forward start → response done)
    status_code    INTEGER,
    request_bytes  INTEGER,
    response_bytes INTEGER
);

-- History per (client, session_id, endpoint).
CREATE TABLE IF NOT EXISTS sessions (
    client        TEXT,
    session_id    TEXT,
    endpoint      TEXT,
    first_seen    REAL,
    last_seen     REAL,
    request_count INTEGER,
    PRIMARY KEY (client, session_id, endpoint)
);

CREATE INDEX IF NOT EXISTS idx_requests_ts          ON requests(ts);
CREATE INDEX IF NOT EXISTS idx_requests_endpoint    ON requests(endpoint, ts);
CREATE INDEX IF NOT EXISTS idx_power_events_server  ON power_events(server, ts);
```

## Per-table contract

| Table | Written by | When | Read by |
|---|---|---|---|
| `server_runtime` | server runtime, startup reconciliation | every power transition, ownership change, shutdown-override change | startup (restore), monitor, shutdown engine |
| `endpoint_runtime` | endpoint readiness poller | every readiness transition | monitor, startup (warm readiness view) |
| `power_events` | power backends / shutdown engine | every action attempt (success **and** failure) | monitor (recent events), metrics, future cost |
| `requests` | transport layer, via the store flusher (batched) | every completed proxied request (incl. 502/504 outcomes) | metrics, future cost |
| `sessions` | queue (get_or_create / release) | session creation, each request, expiry | monitor, future analytics |

## Startup reconciliation

At boot, after loading config:

1. For each configured server: row missing ⇒ insert
   (`owned=0`, `shutdown_override=NULL`, `power_state=unknown`) and then run
   the normal power-state sync (which may adopt per `adopt_on_traffic` only
   on **traffic**, not on startup sync).
2. Row exists ⇒ **restore `owned` and `shutdown_override`** — this is what
   makes restarts safe (a server the proxy brought up before a restart is
   still owned after it; a manually-on server stays unowned).
3. Configured servers whose rows disappeared (renamed) are treated as new
   (unowned). Removed servers' rows are left (harmless history) or pruned —
   implementer's choice, document it.
4. **Never restore queue/in-flight state** — the queue is ephemeral by
   design (D6). No-timeout clients re-issue; that is the recovery path.

## Retention

- Hourly: delete `requests`, `sessions`, `power_events` rows older than
  `proxy.store.retention_days` (batched deletes, e.g. 10k rows at a time,
  in the store flusher loop).
- `server_runtime` / `endpoint_runtime`: never pruned (one row per entity).
- The DB file can grow; retention is the only bound. Document expected size
  (requests rows ≈ 150 bytes; 100 req/min ≈ ~40 MB/year).

## What is NOT persisted (and why)

- **Queue contents / in-flight requests** — ephemeral; clients re-issue.
- **Readiness cache between polls** — re-probed at startup (first probe is
  part of startup sync).
- **Client status poller caches** (session→directory, parent ids) — rebuilt
  from the clients' APIs; the 30s unreachable grace re-applies naturally.
- **Power-on cooldown timers** — a restart interrupts a boot attempt; the
  next waiter re-triggers power-on (idempotent at the BMC/CSP level:
  powering an already-on box is a no-op/accepted).

## Test hooks

- `store.py` must accept an in-memory (`":memory:"`) or temp-file path —
  unit tests use temp files (WAL + multiple connections), never the real
  `relay.db`.
- Expose a `Store.close()` used by the app lifespan and test fixtures.
