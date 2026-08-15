# OpenAI IPMI Proxy

A lightweight proxy that manages the power state of a Gigabyte workstation with MegaRAC SP-X IPMI.

## Setup

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Configure the `.env` file:
   - `IPMI_HOST`: IP address of the IPMI interface.
   - `IPMI_USER`: IPMI username.
   - `IPMI_PASS`: IPMI password.
   - `TARGET_SERVER_URL`: The URL of the llama.cpp server on the workstation.
   - `IDLE_TIMEOUT`: Seconds of inactivity before shutdown (default: 3600).

## Running

Start the proxy using uvicorn:
```bash
uvicorn main:app --host 0.0.0.0 --port 8000
```

## How it works

- **Requests**: Every request updates the idle timer.
- **Health Check**: The proxy checks `/health` on the target server.
- **Power On**: If the server is down, it sends a Redfish `PowerOn` command and returns a 503 "model still loading" error.
- **Proxy**: Once the server is healthy, requests are transparently forwarded to the target server.
- **Auto-Shutdown**: A background task shuts down the server via IPMI if no requests are received for the configured `IDLE_TIMEOUT`.
