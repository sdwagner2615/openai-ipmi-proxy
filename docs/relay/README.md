# Relay — Rework Proposal & Implementation Plan

**Relay** (formerly `openai-ipmi-proxy`) is a generic **resource-management proxy
platform**. It proxies traffic to a fleet of upstream servers and manages their
resources (power, cost, concurrency) with the goal of scaling to zero when
traffic is absent — like an autoscaler, but for power.

This directory is the **complete, decided proposal** for reworking the project.
Every design decision here was made with the maintainer. Do **not** re-litigate
decisions in `decisions.md` during implementation; if something is genuinely
impossible as specified, stop and document the conflict rather than silently
deviating.

## Read order (do all of this before writing code)

| # | Doc | Contents |
|---|-----|----------|
| 1 | [`current-state.md`](current-state.md) | What exists today, file by file, with line references |
| 2 | [`decisions.md`](decisions.md) | Every locked decision (single source of truth) |
| 3 | [`architecture.md`](architecture.md) | Target architecture: entities, signals, flows, loops, layout |
| 4 | [`configuration.md`](configuration.md) | `config.yaml` schema, env files, validation rules, migration map |
| 5 | [`storage.md`](storage.md) | SQLite schema, reconciliation, retention |
| 6 | [`packaging-ci.md`](packaging-ci.md) | Packaging, tooling (uv/ruff/mypy), tests, CI/CD, Docker |
| 7 | [`phases.md`](phases.md) | Phase 0–4 task lists with explicit gates |
| 8 | [`parity.md`](parity.md) | Exhaustive list of current behaviors that must survive the rework |

## Ground rules

1. **Branch policy:** all work happens on the `relay` branch until **feature
   parity** is reached. Do not merge to `main` before the Phase gates pass.
2. **Definition of feature parity:** the ported e2e suite (see
   `packaging-ci.md`) passes fully, covering: wait-and-poll boot (no 503s),
   queue/spot semantics, opencode white-glove behavior, the power-ownership
   invariant (a server the proxy did not start is never shut down), and
   unknown-path policy.
3. **Conventional commits** (`feat:`, `fix:`, `refactor:`, `test:`,
   `chore:`, ...). The publish workflow derives semver bumps and the
   git-cliff changelog from commit messages — non-conventional commits are
   filtered out of the changelog.
4. **No secrets in the repo.** The real `.env` in the repo root is a live
   deployment config — never read its values into docs, code, or tests, and
   never commit `secrets.env`, `config.yaml`, or `*.db`. Tests must set every
   env var explicitly (see the env-shielding note in `packaging-ci.md`).
5. **Single instance.** The queue is in-memory; the platform is designed to
   run as exactly one process. Do not add multi-replica support.
6. **Start at Phase 0** in `phases.md`. Each phase has a gate; a phase is not
   done until its gate is green.

## Context the new agent must internalize

- The current app is **flat modules + env config + in-memory state**. The
  rework replaces singletons with **registries** (servers, endpoints,
  clients) and env config with a **YAML topology file**. The hard parts are
  not the new features — they are preserving the subtle existing behaviors
  listed in `parity.md` while generalizing them.
- The two most subtle concepts in the current code: **spots** (a session
  holds a concurrency slot until it is *genuinely* idle — client-reported for
  known clients, busy-window inferred for unknown ones) and **power
  ownership** (the proxy only ever shuts down a server it brought up or
  adopted). Both must survive generalization unchanged in semantics.
- Today's single `healthy` flag conflates "is the box on" with "is the
  service answering". The rework **splits** these into power state (from the
  BMC/CSP) and readiness (from the endpoint's HTTP readiness path). This is
  the biggest correctness change in the whole project.
- The existing integration harness `scripts/test_queue.py` (727 lines,
  ~8 scenarios) is the behavioral spec. Porting it to pytest **is** the
  parity gate — port it faithfully in Phase 0 before changing behavior.
