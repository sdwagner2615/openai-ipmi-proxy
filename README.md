# OpenAI IPMI Proxy

An energy-efficient AI Gateway that manages the power state of a Gigabyte workstation using MegaRAC SP-X IPMI (Redfish API).

This proxy allows high-power AI workstations to remain powered off when not in use, automatically waking them up the moment a request is received. It is designed specifically for use with coding agents (like opencode) that may have limited retry attempts.

## Features

- **Automatic Wake-on-Request**: Triggers a Redfish `PowerOn` command if the target server is offline.
- **Smart Boot Polling**: Instead of failing immediately, the proxy polls the server's health endpoint for a configurable window (`BOOT_WAIT_TIMEOUT`) to allow the server to boot before returning a response.
- **Full Streaming Support**: Transparently proxies Server-Sent Events (SSE) for real-time token streaming.
- **Auto-Shutdown**: Shuts down the workstation via `GracefulShutdown` after a period of inactivity (`IDLE_TIMEOUT`).
- **OpenAI Compatible**: Implements the standard OpenAI API interface.

## Design Decisions

- **Wait-and-Poll Logic**: Most agent harnesses have a limited number of retries. By blocking the initial request for a short window while polling `/health`, we significantly increase the success rate of the first request.
- **Redfish Protocol**: Uses the Redfish API instead of traditional IPMI-tool for better compatibility with modern BMCs and support for graceful OS shutdowns.
- **Global Async Client**: Uses a single shared `httpx.AsyncClient` to enable connection pooling, reducing latency and avoiding socket exhaustion.
- **Streaming Architecture**: Implemented using `StreamingResponse` and `aiter_raw` to ensure that low-latency token streaming from `llama.cpp` is preserved.

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
   - `IDLE_TIMEOUT`: Seconds of inactivity before shutdown (default: 3600).
   - `BOOT_WAIT_TIMEOUT`: Seconds to poll the health endpoint before returning a 503 (default: 300).

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
