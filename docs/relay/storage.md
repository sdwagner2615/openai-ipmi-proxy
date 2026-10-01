# Storage (SQLite)

Rule (D5): **config owns *what exists and the policy*; SQLite owns *what is
happening and what has happened*.** The app never reads policy from the DB
and never writes policy to the DB.

Library: SQLAlchemy 2.0 (async, `aiosqlite` driver), **WAL mode**
(concurrent readers during writes), single async writer (all writes are
serialized on a store lock). File: `proxy.store.path` (container default:
`/data/relay.db`).

## Schema

The SQLAlchemy models in `relay/db.py` are the single source of truth —
five tables:

- `server_runtime` — one row per server: coarse power state
  (`on | off | unknown`; transient powering_on/off lives in memory),
  `owned` (D12), `shutdown_override` (NULL = config default; else the
  per-cycle monitor toggle), `cycle_id` (uuid per proxy-initiated power-on),
  `updated_at`.
- `endpoint_runtime` — one row per endpoint: `ready`, `ready_since`,
  `last_check_at`.
- `power_events` — append-only audit of every power action (success AND
  failure); feeds metrics + future cost. Index `(server, ts)`.
- `requests` — append-only activity log: metrics + future cost basis
  (D13: never gates shutdowns). Indexes `(ts)`, `(endpoint, ts)`.
- `sessions` — history per `(client, session_id, endpoint)` composite key.

### Migrations (Alembic, D31)

- Migrations live in `relay/migrations/versions/` (shipped in the wheel and
  the Docker image). They are generated from `Base.metadata` —
  `uv run alembic revision --autogenerate -m "..."` (dev `alembic.ini` at
  the repo root) — and applied **automatically on store open**: a fresh
  path is created at head, an existing file is upgraded in place (the
  `alembic_version` table tracks the applied head).
- Cutover note: DBs created by the pre-Alembic store have no
  `alembic_version` row and are **not** auto-upgraded — delete the old
  `.db` when upgrading (runtime state only; history is lost).
- A schema change = a new migration + a unit test; never edit an applied
  migration.

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
  unit tests use temp files (WAL + multiple connections) or `:memory:`
  (schema built straight from the metadata, no Alembic), never the real
  `relay.db`.
- Expose a `Store.close()` used by the app lifespan and test fixtures.
