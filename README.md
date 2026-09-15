# OpenAI IPMI Proxy

An energy-efficient AI Gateway that manages the power state of a Gigabyte workstation using MegaRAC SP-X IPMI (Redfish API).

This proxy allows high-power AI workstations to remain powered off when not in use, automatically waking them up the moment a request is received. It is designed specifically for use with coding agents (like opencode) that may have limited retry attempts.

## Features

- **Automatic Wake-on-Request**: Triggers a Redfish `PowerOn` command if the target server is offline.
- **Session Queuing**: Requests are identified as sessions (per client + API) and queued FIFO. A configurable number of sessions may run at once (`CONCURRENT_SESSIONS`); the rest wait their turn. Clients are expected to run with no timeout — a request is simply held until it can run.
- **Status-Aware Spots**: A running session keeps its "spot" (and the model's K/V cache warm) until the client is genuinely done — known clients like OpenCode report their real session state (`busy` / `idle` / `retry`, including long tool-call phases) over their own API. Unknown clients are assumed busy for `CLIENT_BUSY_WINDOW` seconds after their last request.
- **Boot Waiting, No 503s**: If the target is off, the first waiting request powers it on and all waiting requests simply wait until it is healthy. No "model loading" 503s are returned (the old `BOOT_WAIT_TIMEOUT` is deprecated).
- **Full Streaming Support**: Transparently proxies Server-Sent Events (SSE) for real-time token streaming.
- **Auto-Shutdown**: Shuts down the workstation via `GracefulShutdown` after a period of inactivity (`IDLE_TIMEOUT`) — but only if the proxy manages its power (see *Power Ownership* below) and only when no session is active.
- **Sleep-Safe Idle Timer**: Idle time is measured with a monotonic clock, so the proxy host going to sleep (or an NTP jump) never triggers a false shutdown on wake.
- **Monitoring Page**: `http://<proxy>:8000/monitor` shows the proxy configuration, all known sessions in queue order (id, client, API, status, spot state, queue position), and unknown-API sessions with the URLs they target.
- **API Agnostic**: Understands OpenAI (`/v1/*`) and Anthropic (`/v1/messages`) routes for session tracking; anything else passes through (or is blocked, see below).

## Design Decisions

- **Wait-and-Poll Logic**: Agent harnesses have a limited number of retries, so instead of failing fast the proxy holds the request: it powers the server on and polls `/health` until the server is up. With the queue in place, *all* waiting requests simply wait for this — no 503s at all (clients run with no timeout).
- **Session Identification**: The proxy is API-agnostic but the queue needs to know who is talking to it. Session ids are resolved in order: a known client's own header (OpenCode sends `x-opencode-session` on every request), generic configured headers (`SESSION_ID_HEADERS`), the API-native body field (OpenAI `user`, Anthropic `metadata.user_id`), and finally `User-Agent + client IP`.
- **Spots, Not Just Requests**: Concurrency is bounded by *sessions* (`CONCURRENT_SESSIONS`), because one session's K/V cache should not be evicted while that client is still working. A session holds its spot until it is idle — and "idle" is asked of the client, not inferred from the last HTTP response: OpenCode's local server exposes `GET /session/status` reporting `busy` / `idle` / `retry` per session, and `busy` is held for the entire turn including tool execution. Unknown clients fall back to a time heuristic (`CLIENT_BUSY_WINDOW`). Within a session, in-flight requests are separately bounded by `CONCURRENT_SESSION_REQUESTS` (`-1` unlimited, `0` serialized).
- **FIFO Queue**: Waiting requests form a global FIFO. A request is promoted when its session may run (free spot or spot already held, per-session cap not hit) and the target is healthy. A client that hangs up while waiting (misconfigured timeout) is detected and removed from the queue.
- **Unknown APIs**: Requests that match no known API are either passed through unqueued (`UNKNOWN_API_POLICY=allow`, the default) or rejected with 403 (`block`). Allowed unknown traffic is listed on the monitor page with its target URL.
- **Redfish Protocol**: Uses the Redfish API instead of traditional IPMI-tool for better compatibility with modern BMCs and support for graceful OS shutdowns.
- **Global Async Client**: Uses a single shared `httpx.AsyncClient` to enable connection pooling, reducing latency and avoiding socket exhaustion.
- **Streaming Architecture**: Implemented using `StreamingResponse` and `aiter_raw` to ensure that low-latency token streaming from `llama.cpp` is preserved.
- **Power Ownership**: The proxy only powers the server *off* if it owns the power lifecycle. It takes ownership when it powers the server on, or when any request is routed through it (adopting an already-running server). Ownership is cleared when the proxy shuts the server down. This means a workstation you turned on manually — and never use through the proxy — is never shut down by it. The idle monitor also never takes the machine down while any session holds a spot or the queue is non-empty.
- **Monotonic Idle Clock**: Idle time is tracked with `time.monotonic()` rather than wall-clock time. On Linux this clock freezes while the host is asleep and ignores NTP steps, so a long laptop sleep can't make the proxy believe the server has been idle and power it off on wake.
- **Path-Transparent Proxying**: The proxy forwards every path to the target verbatim with no API-specific logic, so it works with whatever API the target serves (OpenAI chat completions, Anthropic Messages, etc.). The only target-specific assumptions are the liveness route (`HEALTH_PATH`) and the API profile table in `apis.py` used for session tracking.

## Setup

1. Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

2. Configure the `.env` file (see `.env.example`):
    - `IPMI_HOST`: IP address of the IPMI interface.
    - `IPMI_USER`: IPMI username.
    - `IPMI_PASS`: IPMI password.
     - `TARGET_SERVER_URL`: The URL of the llama.cpp server on the workstation.
     - `HEALTH_PATH`: The path of the target's health endpoint (default: `/health`; e.g. `/health/liveliness` for LiteLLM).
      - `IDLE_TIMEOUT`: Seconds of inactivity before shutdown (default: 3600).
     - `SHUTDOWN_ENABLED`: Set to `false` to disable idle auto-shutdown entirely (power-on still works) (default: `true`).

   Queuing (all optional — defaults shown):
    - `CONCURRENT_SESSIONS`: How many sessions may run at once (default: `1`).
    - `CONCURRENT_SESSION_REQUESTS`: Max in-flight requests per session — `-1` unlimited (default), `N` a cap, `0` serialized.
    - `UNKNOWN_API_POLICY`: `allow` (default) passes non-OpenAI/Anthropic traffic through unqueued; `block` rejects it with 403.
    - `SESSION_EXPIRY`: Seconds a session may stay idle before its spot is released and the session is forgotten (default: `300`).
    - `CLIENT_BUSY_WINDOW`: Seconds after their last request that unknown clients count as busy (default: `120`).
    - `CLIENT_STATUS_POLL`: Seconds between polls of known clients' status APIs (default: `5`).
    - `QUEUE_TIMEOUT`: Max seconds a request may wait in the queue before a 504; `0` = no timeout (default).
    - `OPENCODE_STATUS_PORT`: Port probed on each client's IP for OpenCode's status API (default: `4096`).
    - `OPENCODE_SERVER_PASSWORD`: Optional basic-auth password for password-protected opencode servers (user `opencode`).
    - `SESSION_ID_HEADERS`: Comma-separated fallback session headers (default: `x-session-id`).
    - `BOOT_WAIT_TIMEOUT` is **deprecated**: waiting for boot is now unbounded and no 503 "model loading" is returned.

## Queuing & Monitoring

- **Queueing**: Known-API requests are grouped into sessions and queued FIFO. A session gets a "spot" (one of `CONCURRENT_SESSIONS`) when its first request runs, and keeps it until it goes idle, so another session is only admitted once the current one is truly done — protecting the model's K/V cache. When a spot is released the session is removed from the queue; a later request with the same id joins the back as a new session.
- **OpenCode clients**: The proxy identifies OpenCode sessions via the `x-opencode-session` header OpenCode sends on every request, and polls the client's opencode server (`GET /session/status` on `OPENCODE_STATUS_PORT` of the client's IP) for real `busy`/`idle`/`retry` state. **Client requirement:** opencode must listen on a reachable interface with a fixed port, e.g. `opencode serve --hostname 0.0.0.0 --port 4096` (the TUI default of `127.0.0.1` on a random port is not reachable from the proxy).
- **Other clients**: Use a header from `SESSION_ID_HEADERS` (settable in e.g. OpenCode's provider `options.headers`), the API body field (`user` / `metadata.user_id`), or fall back to `User-Agent + IP`. Without a status API they are treated as busy for `CLIENT_BUSY_WINDOW` seconds after their last request and never report `retry`.
- **Monitor**: Open `http://<proxy>:8000/monitor` — configuration on top, then known sessions in queue order (id, client, API, status, spot state, in-flight/waiting counts, queue position) and a second list of unknown-API sessions with their target URLs. Machine-readable: `GET /monitor/data`. The page has no authentication, like the proxy itself — keep it on a trusted network.
- **Client timeouts**: Point clients at the proxy with **no timeout** (or a very large one); waiting requests are held until their turn. A client that hangs up mid-queue is dropped automatically. `QUEUE_TIMEOUT` can additionally cap the wait with a 504 if you prefer.

## Running

Start the proxy using uvicorn:
```bash
uvicorn main:app --host 0.0.0.0 --port 8000
```

## Integration Example (opencode)

To point your `opencode` configuration to this proxy, update your `opencode.jsonc` as follows:

```jsonc
"provider": {
  "llama.cpp": {
    "npm": "@ai-sdk/openai-compatible",
    "name": "AI Workstation",
    "options": {
      "baseURL": "http://<your-proxy-ip>:8000/v1"
    },
    "models": {
      "unsloth/gemma-4-31B-it-GGUF:UD-Q4_K_XL": {
        "name": "Gemma 4 (31b)",
        "tools": true
      }
    }
  }
}
```
