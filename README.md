# relay

**relay** (formerly `openai-ipmi-proxy`) is a generic **resource-management
proxy**: it fronts one or more AI servers (an LLM workstation, an EC2
instance, ...) and manages their resources — power, concurrency, cost — with
the goal of scaling to zero when no traffic is present. Like an autoscaler,
but for power.

The canonical deployment: a high-power GPU workstation that stays powered
off until a coding agent (e.g. [opencode](https://opencode.ai)) sends a
request; the proxy wakes the box, queues the request while it boots, and
later puts the box to sleep when the work is genuinely done.

## Features

- **Wake-on-request, no 503s.** If the server is off, the first request
  powers it on; every waiting request is simply held until the service is
  ready (clients are expected to run with no timeout). Boot time — model
  loading included — is just wait time.
- **Session-queued concurrency ("spots").** Requests are grouped into
  sessions; `concurrency` sessions run at once and the rest wait FIFO. A
  session keeps its spot until it is *genuinely* idle, so the model's K/V
  cache survives between its requests.
- **Client-aware idleness.** Known clients (opencode) report their real
  per-session state (`busy` / `idle` / `retry`, and *waiting for user
  input*) over their own status API; a session blocked on a permission
  prompt releases its spot immediately. Unknown clients fall back to a
  busy window after their last request.
- **Power ownership.** The proxy only ever powers off a server it brought up
  or adopted (routed traffic, `adopt_on_traffic`). A box you turned on
  manually and never use through the proxy is never touched.
- **Scale to zero.** Idle auto-shutdown per server (`idle_timeout`), with a
  per-power-cycle toggle and (Phase 1) cron off-times that defer while
  active. The monotonic idle clock survives host sleep and NTP jumps.
- **Path-transparent streaming.** Method, path, headers, and body are
  forwarded verbatim (SSE-safe, unbuffered); the target read timeout is
  per-chunk and live-tunable from the monitor.
- **Multi-endpoint, multi-server.** Longest-prefix routing across endpoints
  (OpenAI-style, Anthropic Messages, anything else via the `catch_all`
  endpoint); unmatched paths pass through or are 403'd.
- **Monitor.** `GET /monitor` (HTML) and `GET /monitor/data` (JSON):
  configuration, sessions in queue order with spot state, manual spot
  release, per-cycle auto-off switch, live target read timeout.
- **Server types.** `redfish` (MegaRAC-style BMCs), `noop` (dev/tests), and
  `aws-ec2` (Phase 2, via the `relay[aws]` extra — included in the image).

## Quick start (Docker)

```bash
cp config.yaml.sample config.yaml     # topology + policy (gitignored)
cp .env.sample .env                   # non-secret operational values
cp secrets.env.sample secrets.env     # secrets (gitignored; never commit)
# edit config.yaml + .env + secrets.env
docker compose up -d
```

The proxy listens on `:8000`; `config.yaml` and `secrets.env` are mounted
read-only at `/config/`.

## Running from source

[uv](https://docs.astral.sh/uv/) is the package manager:

```bash
uv venv && uv sync          # or: make setup
uv run relay --config ./config.yaml
```

`relay --config PATH` (or `$RELAY_CONFIG`, default `./config.yaml`) starts
uvicorn on the configured `proxy.host:port`.

## Configuration

Three inputs, one rule: **`config.yaml` owns topology + policy**,
**`.env` owns non-secret operational values**, **`secrets.env` owns secrets**
(referenced from config via `${VAR}`). The full annotated reference,
validation rules, and the old-env-var → new-location migration map live in
[docs/relay/configuration.md](docs/relay/configuration.md).

## Pointing opencode at relay

```jsonc
// opencode.jsonc
"provider": {
  "llama.cpp": {
    "npm": "@ai-sdk/openai-compatible",
    "name": "AI Workstation",
    "options": { "baseURL": "http://<your-proxy-ip>:8000/v1" },
    "models": { "unsloth/gemma-4-31B-it-GGUF:UD-Q4_K_XL": { "name": "Gemma 4 (31b)", "tools": true } }
  }
}
```

**Client requirement:** opencode must run its server on a reachable
interface with a fixed port, e.g.
`opencode serve --hostname 0.0.0.0 --port 4096` (the TUI default of
`127.0.0.1` on a random port is not reachable from the proxy). The proxy
polls that status API for the client's real session state and uses it to
keep spots warm (and to release them the moment the session goes idle or
waits on user input).

Point clients at the proxy with **no timeout** (or a very large one):
waiting requests are held until their turn. A client that hangs up
mid-queue is dropped automatically; `queue_timeout` can cap the wait with a
504 if you prefer.

## Development

```bash
make setup       # uv venv + sync
make lint        # ruff check + format --check (CI)
make typecheck   # mypy
make test-unit   # fast unit suite (seconds)
make test-e2e    # parity suite: real proxy subprocesses + mock BMC/opencode/target (minutes)
make test        # full local gate = lint + typecheck + unit + e2e
make coverage
make run         # relay --config ./config.yaml
make build       # docker image
make smoke       # build + boot image + /healthz + /monitor/data checks
```

The e2e suite is a faithful port of the legacy integration harness: it boots
proxy variants (queued/concurrent/serial/atomic/immediate-idle/cooldown/
read-timeout) against mock LLM, opencode-status, and Redfish-BMC servers,
and asserts the queue/spot/power semantics end-to-end. Unit tests cover the
config loader, queue lifecycle, session-id resolution, client matching,
power mapping, ownership, and the store.

## CI/CD

- `.github/workflows/ci.yml` — ruff + mypy; unit + e2e on a
  3.11/3.12/3.13 matrix; coverage artifact; container smoke job.
- `.github/workflows/publish.yml` — on push to `main`: lint + test gate
  (nothing ships untested), semver bump from conventional commits
  (patch / minor for `feat` / major for `BREAKING CHANGE`), git-cliff
  changelog, GitHub release, and `ghcr.io/<owner>/relay:{tag,latest}`.

The image is non-root, carries the `aws` extra by default, and pins the
release version via `SETUPTOOLS_SCM_PRETEND_VERSION`.

## Design documentation

The full rework design (decisions, architecture, configuration reference,
storage, packaging/CI, phased plan, behavior-parity spec) is in
[`docs/relay/`](docs/relay/README.md). Implementation progress and
recorded deviations: [`docs/relay/progress.md`](docs/relay/progress.md).
