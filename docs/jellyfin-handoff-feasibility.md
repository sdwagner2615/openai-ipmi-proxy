# Feasibility Research: Power-On-Demand Jellyfin with Proxy-Off-Hot-Path Handoff

Status: research / proposal (no implementation)
Date: 2026-09-16
Scope: extending this proxy from its current single-target, always-in-the-hot-path LLM paradigm to a general "power-on-demand service" framework, with Jellyfin as the first service. Phase 1 = wake-on-request + direct handoff only; automatic power-off is explicitly out of scope.

## 1. Conclusion (TL;DR)

**Feasible: yes.** The wake/wait/power machinery already exists in this codebase and is proven. The novel part — taking the proxy *out of the data path* — requires a **front-IP handoff**: the proxy owns the service's LAN IP while the machine is off, and the real Jellyfin server takes over that same IP after boot, so every client (including TV boxes) keeps working against one fixed address and never needs to follow a redirect.

Plain HTTP-redirect handoff is **not sufficient** for the "all clients incl. TV" requirement: Jellyfin's own documentation states that native clients do not handle redirects implicitly (the Android TV app fails to connect in a redirect scenario). The main open risk is TV-client cold-start UX (client timeouts shorter than boot time), which the PoC must validate; mitigations exist (web-first wake, explicit wake endpoint, 503 + Retry-After mode).

## 2. Requirements

| # | Requirement | Source |
|---|---|---|
| R1 | Machine stays powered off until someone calls the Jellyfin API | user |
| R2 | First call triggers Redfish power-on; proxy waits for readiness (no 503 spam) | current app paradigm |
| R3 | Proxy **not** in media data path — streams go Jellyfin → client directly | user |
| R4 | Works with **all** clients: web, desktop (Jellyfin Media Player), mobile, TV/streaming (Roku, Fire Stick, Apple TV, Android TV) | user |
| R5 | Phase 1 = wake + handoff only; auto power-off later | user |
| R6 | One proxy process managing many services (LLM today, Jellyfin next, more later) | user |

Deployment constraints (confirmed):

- Static IP per machine; clients are configured with **raw IPs** (no DNS name).
- Proxy, Jellyfin machine, and clients are on the same L2 segment.
- Plain HTTP on the LAN (Jellyfin's built-in HTTPS is off).
- Proxy runs on an always-on homelab box, **not** the router.
- Jellyfin machine: Debian/Ubuntu, Jellyfin in Docker.

## 3. What the current app already provides (reuse)

- Redfish power-on / graceful shutdown / power-state query per BMC (`main.py`, `redfish_request` / `get_power_state` / `power_on` / `power_off`) — needs generalizing from one hardcoded `discovered_system_path` / target URL to **per-service** BMC + target.
- Health-poll loop + "hold requests, no 503s" pattern (`queue_manager` and the proxy handler in `main.py`) — directly reusable for the boot-wait.
- Power-ownership semantics, monotonic idle clock, startup state sync (`sync_state`) — reusable; ownership / `SHUTDOWN_ENABLED` stay off for the Jellyfin service in phase 1.
- `/monitor` page — generalize to a per-service state table.
- **Not** reusable for Jellyfin: session queueing, spots, opencode status polling (LLM-specific), and the hot-path `forward_request` (handoff-mode services never forward media; only a one-shot first-request relay, see §5).

## 4. The core problem

Clients hold a **raw IP** (`S:8096`). For the first API call to trigger a wake, that request must reach something that is powered on — so **the proxy must own `S` while the machine is off**. The whole design then reduces to one question:

> After boot, how does `S` start serving the real server, without media bytes passing through the proxy?

## 5. Handoff mechanism options

### Option A — HTTP redirect (302/307) to the real server. ❌ Insufficient

Proxy wakes the machine, waits, answers `302 → http://R:8096/path`.

- **Browsers: perfect.** The SPA moves to the real origin; everything after is direct.
- **Native/TV: broken or degraded.** Jellyfin's official docs (Networking → Base URL) state that client applications "generally do not, for now, handle the Base URL redirects implicitly", citing the Android TV app failing to connect unless the host setting already includes the base path. In other words, native clients will not reliably follow a cross-host 302 during their handshake. Even for clients whose HTTP stack does follow it, the configured host stays `S` (the proxy), so every request — including every HLS segment — would 302 through the proxy. Media bytes would be direct, but control chatter doubles, and non-following TV clients fail outright.

Verdict: useful only as a web-client nicety / fallback. Rejected as the mechanism for R4.

### Option B — Front-IP handoff (stable address). ✅ Recommended

`S` is the front IP the clients hold. **The proxy owns `S` while the machine is off; the real server takes over `S` after boot.** No redirect is ever required — the address simply changes owner behind the clients' backs.

Mechanism (no router control needed):

- The server boots with its normal static IP `R` (Jellyfin in Docker, unchanged).
- A small new systemd unit on the Debian host (`jellyfin-front-ip.service`): wait until `curl R:8096/health` returns 200 → sleep `HANDOFF_DELAY` (default 8 s) → `ip addr add S/24` → send a **gratuitous ARP**.
- The proxy binds `S` **only while the BMC reports the machine Off** (the BMC is authoritative, so there is no accidental dual-claim). On health, it unbinds `S` after a short configured delay (default 5 s) — i.e. a ~3 s gap where nobody owns `S`, then the server does.

Cold-start sequence:

1. Client (any type) → `S:8096` → hits the proxy.
2. Proxy issues Redfish `PowerOn` (deduped by the existing 30 s cooldown) and **holds** the request (unbounded by default, same contract as today's queue: clients are expected to have no/small timeouts and retry).
3. Server boots at `R`; proxy polls `R:8096/health` every 2 s.
4. Healthy ⇒ proxy unbinds `S` (t+5 s); server unit adds `S` + gARP (t+8 s). The gARP updates every neighbor's ARP cache, including the waiting client's.
5. Proxy answers the held request by **one-shot-relaying it to the real server** (same method/path, `Host: S:8096` set, response bytes returned verbatim). This is one tiny API response through the proxy — an accepted cold-start exception; no media.
   - Why relay instead of 302: a relayed response is byte-identical to a direct one, so it works with *every* client regardless of redirect handling.
6. Every subsequent request — API and all streams — goes `S` = real server, directly. Proxy traffic afterward: **zero**.

Why relaying (not just waiting) for the first response: a native client whose timeout is shorter than the boot time will drop the held request anyway; if it retries, the retry is relayed once healthy. And during the handoff overlap window (while the proxy still owns `S` and the server is healthy), *any* request is relayed immediately — so no request is ever dropped, no matter when it lands.

Per-service state machine:

```
OFF (proxy owns S)
  │ request arrives → Redfish PowerOn (deduped), hold request
  ▼
BOOTING (power-on issued, S still proxy-owned, health polling at 2 s)
  │ /health == 200 at R
  ▼
UP (server owns S; proxy idle, periodic BMC/health monitoring only)
  │ BMC reports Off (e.g. manual power-off)
  ▼
OFF (re-arm: rebind S)
```

A proxy restart re-syncs from BMC power state + health (the existing `sync_state` pattern), rebinding `S` only when the BMC says Off.

Edge cases handled by construction:

- Multiple simultaneous cold-start clients → one power-on, all held, all relayed.
- Boot never becomes healthy → configurable `HOLD_TIMEOUT` (e.g. 10 min) → 504; proxy logs loudly and stays armed on `S`.
- `R` ≠ `S` is required and documented (two static IPs per machine, or a DHCP reservation for `R`).
- No ARP flapping: zero-overlap ordering (proxy unbinds before the server binds). Worst case, one new connection is delayed a few seconds until the gARP lands.

### Option C — Router-managed virtual IP (NAT toggle). ❌ Not for phase 1

The router DNATs `S:8096` → proxy (off) / → `R:8096` (on); the proxy flips the rule on health. Cleaner IP lifecycle, but requires router API control and is only compelling if the proxy ran *on* the router — it doesn't.

### Option D — Stay in the hot path (proxy/splice after wake). ❌ Violates R3

This is what all prior art does (Wakezilla, `tvup/idle-less`, `Matthi383/wake-proxy`: WOL → wait → forward). Useful to us as validation that the *wake-and-wait* pattern and its pitfalls (client timeouts during boot, idle detection with no proxy traffic) are well-trodden — but forwarding media through the proxy is exactly what R3 forbids.

## 6. Verified Jellyfin facts

| Fact | Detail | Use |
|---|---|---|
| `GET /health` | 200 OK once HTTP **and DB** are connected; stays non-200 during DB migrations (docs warn monitoring/watchdog programs can misfire) | Readiness probe — same shape as the current `HEALTH_PATH`. We simply wait longer. |
| Ports | 8096 HTTP (default), 8920 HTTPS (disabled by default), **7359/UDP** client discovery (LAN-only) | Proxy listens on `S:8096` so clients keep the same host:port shape. |
| Client discovery | On boot the server announces name, IP, and ID over UDP 7359 | Free assist: local TV/mobile clients that re-scan will find the real server at `S` after handoff. Optional phase-2 trick: the proxy answers discovery for the sleeping server (announcing `S`) so discovery-based setup also triggers a wake (faking the server ID is a risk — keep optional). |
| Auth | `api_key` query param or `X-Emby-Token` header (not `Authorization`) | No cookie / auth-header-stripping issues on any relay or redirect. |
| Streams | `/Videos/{id}/stream.*`, `/Videos/{id}/hls/*`, `/audio/*`, … — plain HTTP GETs against the client's configured host | Irrelevant to design detail: handoff is at the **address** level, so no per-endpoint routing is ever needed. |
| `GET /api/sessions` | Admin-key; lists sessions with `NowPlayingItem` | Foundation for phase-2 idle shutdown (out of scope now). |
| `GET /System/Info` | Server self-describes (version, `LocalAddress`, …) | Post-handoff identity check: the proxy can verify the server at `S` is really ours. (Exact fields to confirm in the PoC.) |
| Base URL | Must stay empty (`/`); docs warn clients don't handle base-URL redirects | Keep the server at `/` so raw-IP clients work as-is. |

Sources: jellyfin.org official docs — Post-Install Setup → Networking (ports, discovery, Base URL caveats) and Advanced Networking → Monitoring (`/health` semantics).

## 7. Design sketch (phase 1)

### 7.1 Config — one proxy, many services

Replace the single-target env model with a services file (YAML or JSON, pointed to by `SERVICES_FILE`); the env file keeps shared values and secrets. Per service:

```yaml
services:
  - name: llm                      # today's target, unchanged behavior
    mode: proxy                    # hot-path proxy (current paradigm)
    listen_port: 8000
    target_url: http://192.168.1.101:8080
    health_path: /health
    bmc:
      host: 192.168.1.100
      user: admin
      pass: ...
      system_path: /redfish/v1/Systems/Self
    idle_shutdown: { enabled: true, timeout: 3600 }

  - name: jellyfin
    mode: handoff                  # wake + hand off, never forward media
    listen_port: 8096
    front_ip: 192.168.1.200        # S — what clients hold; proxy owns it while off
    target_url: http://192.168.1.105:8096   # R — boot IP, also the relay target
    health_path: /health
    bmc:
      host: ...
      user: ...
      pass: ...
      system_path: ...
    hold_timeout: 600
    handoff: { unbind_delay: 5 }   # server-side unit adds S at +8 s
    idle_shutdown: { enabled: false }      # phase 2
```

### 7.2 Per-service runtime

- `ServiceState` per service: OFF/BOOTING/UP, owner of `S`, BMC power state, pending-request count, last health result, timestamps (monotonic clock, as today).
- One monitor task per service: 2 s cadence while BOOTING, ~30–60 s while UP (BMC power state + health).
- One listener per service port. Handoff listener behavior:
  - **OFF:** any request triggers the (deduped) power-on, then holds — or returns `503 + Retry-After`, a configurable knob, since some clients retry 503s more gracefully than hung requests.
  - **Healthy (overlap window):** any request is one-shot relayed immediately (no wake needed).
  - **UP:** listener closed — the proxy has released `S`.
- The management port (8000) keeps `/monitor` (generalized to a per-service table: state, `S` owner, BMC state, pending requests, last health) plus an explicit `POST /services/{name}/wake` so a user can pre-wake from a browser before pointing the TV app at `S` — the pragmatic mitigation for client-timeout UX.

### 7.3 Server side (Jellyfin box)

Unchanged Docker setup, plus `jellyfin-front-ip.service` (systemd unit on the Debian host):

1. Wait for `curl -f R:8096/health` to succeed.
2. Sleep `HANDOFF_DELAY` (default 8 s; must exceed the proxy's unbind delay).
3. `ip addr add S/24 dev <iface>` (idempotent — skip if present).
4. Send a gratuitous ARP for `S`.

### 7.4 Deployment

Front-IP binding needs the proxy container on **host networking + `NET_ADMIN`** (today's compose uses a bridge with `8000:8000`; clients point at the box's LAN IP either way, so there is no client-visible change).

### 7.5 What the proxy never does (handoff mode)

- Never stream media bytes.
- Never own `S` while the BMC says On.
- Never power a service off in phase 1.

## 8. Risks & open questions (PoC validation list)

1. **[highest] TV-client cold-start behavior:** request timeout and auto-retry on failure for Roku / Fire Stick / Apple TV / Android TV. Determines whether cold start is seamless (the retry succeeds) or needs the web-first-wake workaround / `503 + Retry-After` mode.
2. One-shot relayed handshake response accepted by all clients (expected: yes — identical bytes to a direct answer).
3. IP handoff on the real LAN: unbind/add/gARP ordering, no lost connections (tcpdump-verified).
4. Docker host-network + `NET_ADMIN` binding of `S`; proxy-restart re-sync.
5. Boot-to-healthy time on the target machine → `hold_timeout` default + optional fast-boot tuning.
6. Concurrent cold-start clients; boot-failure path (504 + re-arm).
7. BMC "Off" detection reliability for re-arming (existing code path).
8. Future questions: multiple front IPs when one box hosts several services (Jellyfin + Sonarr?); HTTPS (cert changes on handoff — revisit if ever needed); discovery-answering trick; phase-2 idle shutdown via `/api/sessions` + a service admin key.

## 9. Verification plan (PoC before any real implementation)

- **Stage 0 — manual IP-handoff test (no app code):** bind `S` on the proxy box, add `S` + gARP on the Debian box per the unit's logic, watch with tcpdump; confirm a phone/TV client sees a clean switchover and that the ordering produces no ARP flapping.
- **Stage 1 — mocks:** extend `scripts/mock_target.py` into a mock Jellyfin (`/health`, `/System/Info`, `/`, fake stream endpoint) with a simulated off→boot→on cycle and a stubbed BMC; validate wake-on-request, hold, one-shot relay, handoff, and re-arm.
- **Stage 2 — real cold start:** real Jellyfin + real BMC; web client end-to-end; **tcpdump on the proxy NIC during playback to prove zero media bytes traverse the proxy.**
- **Stage 3 — TV matrix:** Roku, Fire Stick, Apple TV, Android TV cold starts; record timeout/retry behavior; pick the hold-vs-503 default.

Success criteria:

- (a) A client configured with only `S` watches a full movie after a cold boot.
- (b) tcpdump proves the media path is server → client.
- (c) Proxy CPU/bandwidth ≈ 0 during playback.
- (d) Re-arm after a manual power-off works.

## 10. Effort estimate

- Service config generalization (services file, per-service state/tasks): medium.
- Handoff state machine + front-IP binding (proxy side): medium.
- Server-side systemd unit: small.
- **PoC + TV-matrix validation: the largest chunk, and the real gate** — it resolves risk #1 before any further design or implementation.
