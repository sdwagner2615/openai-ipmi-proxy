# Packaging, Tooling, Testing, CI/CD

## Packaging (D23, D24, D28)

`pyproject.toml` replaces `requirements.txt`. Reference:

```toml
[build-system]
requires = ["setuptools>=68", "setuptools-scm>=8"]
build-backend = "setuptools.build_meta"

[project]
name = "relay"
description = "Generic resource-management proxy: power-aware, session-queued traffic proxy with scale-to-zero."
readme = "README.md"
requires-python = ">=3.11"
dynamic = ["version"]            # setuptools-scm: git tag is the source of truth
dependencies = [
  "fastapi>=0.111",
  "uvicorn[standard]>=0.30",     # [standard] brings websockets, uvloop, httptools
  "httpx>=0.27",
  "python-dotenv>=1.0",
  "pyyaml>=6.0",
  "aiosqlite>=0.20",
  "croniter>=2.0",
  "websockets>=12",
]

[project.optional-dependencies]
aws = ["boto3>=1.34"]            # relay[aws] — EC2 backend (D10)

[project.scripts]
relay = "relay.main:main"        # relay --config /config/config.yaml

[dependency-groups]
dev = [
  "pytest>=8",
  "pytest-asyncio>=0.23",
  "pytest-cov>=5",
  "ruff>=0.5",
  "mypy>=1.10",
]

[tool.setuptools_scm]
fallback_version = "0.0.0"       # untagged checkouts (CI PRs, local dev)

[tool.setuptools.packages.find]
include = ["relay*"]

[tool.ruff]
line-length = 100
target-version = "py311"

[tool.ruff.lint]
select = ["E", "W", "F", "I", "B", "UP", "SIM", "C4", "RUF"]

[tool.mypy]
files = ["relay"]
python_version = "3.11"
strict = false                   # non-strict baseline (D25); ratchet up per phase
ignore_missing_imports = true

[tool.pytest.ini_options]
markers = ["unit", "e2e"]
asyncio_mode = "auto"
```

**uv workflow:** `uv sync` (dev), `uv run pytest`, `uv run ruff check .`,
`uv run ruff format .`, `uv run mypy`. Commit `uv.lock`. CI and Docker use
`uv sync --frozen` / `uv pip install --frozen` so the lockfile is the
reproducibility guarantee.

**Versioning:** setuptools-scm derives the version from the latest git tag
(the publish workflow's existing semver logic). Docker builds pass the tag
via build-arg (see Dockerfile). No manual version edits anywhere.

**Entry points:**
- `relay --config PATH` (argparse: `--config` default `$RELAY_CONFIG` or
  `./config.yaml`) — runs uvicorn on the configured host/port.
- `python -m relay` — same.
- `create_app(config_path)` — FastAPI app factory for embedding/testing.

## Testing (D26)

Layout:

```
tests/
  conftest.py            # shared fixtures: free ports, temp dirs, env shielding
  mocks/
    mock_target.py       # ported from scripts/ (mock LLM: /health, chat+SSE, echo)
    mock_opencode.py     # ported from scripts/ (per-dir status, parentID, pending)
    mock_bmc.py          # NEW: small FastAPI Redfish mock (GET system → PowerState,
                         #   POST Reset; records actions; scriptable state)
    fake_ec2.py          # NEW: in-process fake for AwsEc2Backend (boto3 client stub
                         #   or NoopBackend-with-assertions)
    mock_ws_target.py    # NEW: WebSocket echo/delay target for tunnel tests
  unit/
    test_config.py       # YAML load, ${ENV} interpolation, every validation rule
    test_queue.py        # spots, FIFO, per-session caps, atomic, shared spots,
                         #   release deadlines (idle/waiting/immediate), tick,
                         #   promote/disconnect race
    test_session_id.py   # D22 precedence incl. WS query params
    test_clients.py      # matching (plain vs UA-gated headers)
    test_scheduler.py    # cron evaluation, deferral while active
    test_ownership.py    # D12: adopt, release on off, restore on restart,
                         #   never-off-unowned (the operator's manual-on scenario)
    test_store.py        # schema, reconciliation, retention, in-memory safety
    test_power_redfish.py  # state mapping + action payloads via httpx.MockTransport
    test_power_ec2.py      # state map + start/stop/terminate via fake client
  e2e/
    conftest.py          # fixtures: spawn mocks + proxy SUBPROCESS on free ports
    test_boot_wait.py    # no 503s, power-on triggered, all waiters served after ready
    test_queue.py        # the ported scenarios (FIFO, spots, release, expiry,
                         #   atomic, sub-agent shared spot, disconnected client,
                         #   queue timeout 504)
    test_opencode.py     # status polling, per-dir, pending=waiting, absent=idle,
                         #   immediate idle release, grace period
    test_shutdown.py     # idle off, cron off + deferral, ownership (manual-on
                         #   server untouched), per-cycle toggle, restart restore
    test_unknown_paths.py# allow→catch_all passthrough + adoption, block→403
    test_ws.py           # tunnel, keep-alive while waiting, 1013 on error policy,
                         #   slot held for connection lifetime, query-param session id
    test_multi_server.py # two servers (mock Redfish + noop), routing isolation
```

**E2E discipline (critical):**

- The proxy runs as a **real subprocess** (`relay --config …` or uvicorn) —
  true config-file + env + process-boundary coverage.
- **Env shielding:** every e2e env var the test depends on is set explicitly
  in the subprocess env (today's `BASE_ENV` trick — `load_dotenv` does not
  override existing environment, so a live `.env` in the repo cannot leak in
  and change behavior).
- Fake BMC by pointing redfish `host` at the mock BMC server (127.0.0.1) —
  never at real hardware. EC2 via the fake, never real AWS.
- Free ports via socket-bind-then-close (or `socket.getaddrinfo` helper in
  `conftest`); no hardcoded ports.
- Timeouts on every e2e assertion path (the platform holds requests by
  design — tests must bound their own waits, e.g. `wait_until(predicate, timeout)`).
- `pytest -m unit` must run in seconds (no subprocesses); `-m e2e` may take
  minutes. CI runs both.

**Coverage:** `uv run pytest --cov=relay --cov-report=xml`; XML uploaded as a
CI artifact; **no threshold** until the suite matures (revisit in Phase 4).

## Lint & types (D25)

- `uv run ruff check .` and `uv run ruff format --check .` in CI;
  `uv run ruff check --fix . && uv run ruff format .` locally.
- `uv run mypy` in CI (non-strict; annotate the queue/scheduler/store/core
  paths in Phase 0; ratchet `strict` per-module later — record any module
  you leave unannotated in `phases.md` progress notes).
- Optional `.pre-commit-config.yaml` (ruff + ruff-format) for local DX.

## CI/CD (D27)

### `ci.yml` (new) — on PR + push to `main`

```
jobs:
  lint:        # python 3.12, uv sync --frozen
               #   ruff check . ; ruff format --check . ; mypy
  test:        # matrix: 3.11, 3.12, 3.13 ; uv sync --frozen
               #   pytest -m unit
               #   pytest -m e2e
               #   (3.12 only: --cov + upload coverage artifact)
  container:   # needs: [test]
               #   docker build --build-arg SETUPTOOLS_SCM_PRETEND_VERSION=0.0.0-ci .
               #   docker run: sample config (noop server + mock target in a sidecar
               #               or a single container running both), assert
               #               GET /healthz == 200 and GET /monitor/data parses
```

Keep the container job's smoke minimal — its job is "the image boots and
serves", not re-running the suite.

### `publish.yml` (update existing, keep the semver/tag/changelog/release logic)

1. Add `lint` + `test` jobs (same as `ci.yml`); `publish: needs: [lint, test]`
   — **nothing ships untested**.
2. Image rename: `ghcr.io/<owner>/relay:{latest,tag}`.
3. Build-arg: `SETUPTOOLS_SCM_PRETEND_VERSION=<new tag>` (setuptools-scm
   has no `.git` in the build context).
4. Keep: git-cliff changelog, GitHub release, `fetch-depth: 0`.
5. Keep conventional commits — the version bump greps for `^feat` /
   `BREAKING CHANGE`.

### `.github` hygiene

- The `container` smoke config must use `type: noop` (no secrets needed in CI).
- No secrets required by `ci.yml`.

## Docker (D28)

```dockerfile
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
RUN pip install --no-cache-dir uv

WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
COPY relay ./relay
RUN uv pip install --system --no-cache . && \
    uv pip install --system --no-cache '.[aws]'

# non-root (D28)
RUN useradd -m relay
USER relay

ENV RELAY_CONFIG=/config/config.yaml
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD python -c "import urllib.request;urllib.request.urlopen('http://127.0.0.0:8000/healthz', timeout=3)"

ENTRYPOINT ["relay"]
CMD ["--config", "/config/config.yaml"]
```

- Build-arg `SETUPTOOLS_SCM_PRETEND_VERSION` is consumed via
  `ARG SETUPTOOLS_SCM_PRETEND_VERSION` + `ENV` in the install step (or pass
  through `docker/build-push-action` `build-args`).
- `.dockerignore`: `.git`, `.venv`, `venv`, `__pycache__`, `tests`, `docs`,
  `*.db`, `.env`, `secrets.env`, `config.yaml`, `.github`, `scripts`,
  `.ruff_cache`, `.mypy_cache`, `.pytest_cache`, `coverage.xml`.
- `compose.yaml`: mount `./config.yaml:/config/config.yaml:ro` and
  `./secrets.env:/config/secrets.env:ro`, keep port map + restart policy,
  add the `RELAY_CONFIG` env. (The app loads `.env`/`secrets.env` from its
  working dir **or** next to the config file — implement
  "also load `<config-dir>/secrets.env`" in `config.py` so the mounted file
  is picked up inside the container.)
- The image includes the `aws` extra by default (D10).

## Release process (unchanged except gating + rename)

1. Conventional commits land on `main`.
2. Push to `main` ⇒ `ci.yml` runs; `publish.yml` runs lint+test, bumps semver
   (patch / minor for `^feat` / major for `BREAKING CHANGE`), tags, pushes
   `relay:{tag,latest}` to GHCR, cuts a GitHub release with the git-cliff
   changelog.
3. Operators pull `ghcr.io/<owner>/relay:latest` (or pin a tag), `cp` the
   three sample files, edit, `docker compose up -d`.
