"""relay — app factory, request routing, and the console entry point.

``create_app(config_path)`` builds the FastAPI app (embedding/testing);
``main`` (the ``relay`` console script) loads the config and runs uvicorn.
"""

import argparse
import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import uvicorn
from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import HTMLResponse, JSONResponse

from relay.clients import ClientRegistry, StatusPoller, provider_from_config
from relay.config import ConfigError, load_config
from relay.endpoints import EndpointRuntime, route
from relay.models import RelayConfig
from relay.monitor import HTML_PAGE, build_data
from relay.power import create_power_backend
from relay.queue import UnknownTracker
from relay.servers import ServerRuntime
from relay.store import Store

logger = logging.getLogger("relay")

__all__ = ["RelayApp", "create_app", "main"]


class RelayApp:
    """The assembled relay: config, store, servers, endpoints, clients.

    Construction is pure (no I/O); ``setup`` wires the runtime once the
    shared HTTP client exists, and the lifespan runs startup sync + loops.
    """

    def __init__(self, config: RelayConfig):
        self.config = config
        self.store = Store(config.proxy.store.path)
        self.servers: dict[str, ServerRuntime] = {}
        for server_config in config.servers:
            self.servers[server_config.name] = ServerRuntime(
                server_config,
                create_power_backend(server_config),
                self.store,
                shutdown_tick=config.proxy.shutdown_tick,
            )
        self.registry = ClientRegistry(config.clients, config.proxy.session_id_headers)
        self.pollers: dict[str, StatusPoller] = {}
        self.endpoints: dict[str, EndpointRuntime] = {}
        self.catch_all: EndpointRuntime | None = None
        self.unknown_tracker = UnknownTracker()
        self.http_client: httpx.AsyncClient | None = None
        # Live proxy-to-target read timeout (X4; monitor-tunable).
        self.target_read_timeout: int = config.proxy.target_read_timeout
        self.started_at = time.monotonic()

    def setup(self) -> None:
        """Builds the status pollers and the endpoint runtimes (needs http)."""
        http_client = self.http_client
        if http_client is None:
            raise RuntimeError("setup() must run after the shared HTTP client exists")
        for client_config in self.config.clients:
            if client_config.status.kind != "opencode":
                continue  # kind: none -> busy-window inference only
            self.pollers[client_config.name] = StatusPoller(
                http_client,
                provider_from_config(client_config),
                port=client_config.status.port,
                password=client_config.status.password,
                poll_interval=client_config.status.poll_interval,
            )
        for endpoint_config in self.config.endpoints:
            server = self.servers[endpoint_config.server]
            endpoint = EndpointRuntime(
                endpoint_config,
                server,
                http_client=http_client,
                store=self.store,
                pollers=self.pollers,
                registry=self.registry,
                unknown_tracker=self.unknown_tracker,
                read_timeout=self.target_read_timeout,
            )
            server.endpoints.append(endpoint)
            self.endpoints[endpoint_config.name] = endpoint
            if endpoint_config.catch_all:
                self.catch_all = endpoint

    def set_read_timeout(self, value: int) -> None:
        """Applies a live read-timeout change to every endpoint (X4)."""
        self.target_read_timeout = value
        for endpoint in self.endpoints.values():
            endpoint.read_timeout = value

    async def startup(self) -> None:
        """Startup sync (P4): store reconciliation, readiness probes, power sync."""
        await self.store.open()
        rows = await self.store.reconcile_servers([s.name for s in self.config.servers])
        for server in self.servers.values():
            await server.restore(rows[server.name])
        # The first readiness probe is part of the startup sync (storage.md).
        for endpoint in self.endpoints.values():
            await endpoint.check_readiness()
        for server in self.servers.values():
            await server.sync_state()
        for server in self.servers.values():
            server.start()
        for endpoint in self.endpoints.values():
            endpoint.start()
        self._retention_task = asyncio.create_task(
            self.store.retention_loop(self.config.proxy.store.retention_days),
            name="store-retention",
        )
        # Batched request writer (Phase 1 task 7): staged rows flushed ~1/s.
        self._flush_task = asyncio.create_task(
            self.store.request_flush_loop(1.0), name="store-request-flush"
        )
        logger.info(
            "relay started: %d server(s) [%s], %d endpoint(s) [%s].",
            len(self.servers),
            ", ".join(self.servers),
            len(self.endpoints),
            ", ".join(self.endpoints),
        )

    async def shutdown(self) -> None:
        """Clean teardown (O4): loops first, then client and store."""
        if getattr(self, "_retention_task", None) is not None:
            self._retention_task.cancel()
        if getattr(self, "_flush_task", None) is not None:
            self._flush_task.cancel()
        for endpoint in self.endpoints.values():
            await endpoint.stop()
        for server in self.servers.values():
            await server.stop()
        if self.http_client is not None:
            await self.http_client.aclose()
        # Drain the request buffer so no completed request is lost on shutdown.
        await self.store.flush_requests()
        await self.store.close()
        logger.info("relay stopped.")


@asynccontextmanager
async def lifespan(app: FastAPI):
    relay = app.state.relay
    # A single shared AsyncClient (O2) enables connection pooling for all
    # outbound traffic. verify=False: most BMCs use self-signed certs (O1).
    relay.http_client = httpx.AsyncClient(verify=False)
    relay.setup()
    await relay.startup()
    try:
        yield
    finally:
        await relay.shutdown()


def create_app(config_path: str | Path) -> FastAPI:
    """Builds the FastAPI app for the relay config at ``config_path``."""
    config = load_config(config_path)
    app = FastAPI(title="relay", lifespan=lifespan)
    app.state.relay = RelayApp(config)

    @app.get("/healthz")
    async def healthz(request: Request):
        """200 while the app and its background loops are alive (D28)."""
        relay: RelayApp = request.app.state.relay
        now = time.monotonic()
        for endpoint in relay.endpoints.values():
            if endpoint.last_manager_tick is None or now - endpoint.last_manager_tick > 5.0:
                return JSONResponse(
                    status_code=503,
                    content={
                        "status": "degraded",
                        "detail": f"queue manager stale for endpoint '{endpoint.name}'",
                    },
                )
        for server in relay.servers.values():
            if server.last_sync_at is None or now - server.last_sync_at > 15.0:
                return JSONResponse(
                    status_code=503,
                    content={
                        "status": "degraded",
                        "detail": f"power sync stale for server '{server.name}'",
                    },
                )
        return {"status": "ok", "uptime_seconds": round(now - relay.started_at, 1)}

    @app.get("/monitor")
    async def monitor_page():
        """Simple self-contained monitoring page (polls /monitor/data)."""
        return HTMLResponse(HTML_PAGE)

    @app.get("/monitor/data")
    async def monitor_data(request: Request):
        """JSON snapshot: config, servers, endpoints, sessions, events (monitor v2)."""
        return JSONResponse(await build_data(request.app.state.relay))

    @app.post("/monitor/release")
    async def monitor_release(request: Request):
        """Manually releases a session's spot (M3; searched on all endpoints)."""
        relay: RelayApp = request.app.state.relay
        try:
            data = await request.json()
        except Exception:
            return JSONResponse(
                status_code=400, content={"error": "Expected a JSON body {client, session}."}
            )
        if not isinstance(data, dict):
            return JSONResponse(
                status_code=400, content={"error": "Expected a JSON body {client, session}."}
            )
        client = data.get("client")
        session_id = data.get("session")
        if not client or not session_id:
            return JSONResponse(
                status_code=400, content={"error": "Both 'client' and 'session' are required."}
            )
        for endpoint in relay.endpoints.values():
            if endpoint.queue.release_session(str(client), str(session_id)):
                logger.info("Spot manually released for session %s (%s).", session_id, client)
                return JSONResponse({"released": True})
        return JSONResponse(
            status_code=404,
            content={"error": f"Session '{session_id}' ({client}) holds no spot."},
        )

    @app.post("/monitor/shutdown")
    async def monitor_shutdown(request: Request):
        """Toggles the per-power-cycle auto power-off switch (M4, P8).

        ``server`` targets a specific server; it defaults to the first one
        (the v1 monitor is single-server shaped; v2 gains per-server rows).
        """
        relay: RelayApp = request.app.state.relay
        try:
            data = await request.json()
        except Exception:
            return JSONResponse(
                status_code=400, content={"error": "Expected a JSON body {enabled}."}
            )
        if not isinstance(data, dict) or not isinstance(data.get("enabled"), bool):
            return JSONResponse(
                status_code=400,
                content={"error": "Expected a JSON body {enabled: true|false}."},
            )
        server_name = data.get("server") or next(iter(relay.servers))
        server = relay.servers.get(server_name)
        if server is None:
            return JSONResponse(
                status_code=404, content={"error": f"Unknown server '{server_name}'."}
            )
        await server.set_shutdown_override(data["enabled"])
        return JSONResponse({"shutdown_enabled": data["enabled"]})

    @app.post("/monitor/power")
    async def monitor_power(request: Request):
        """Manually powers a server on or off (monitor v2).

        A manual power-on through the proxy counts as proxy-initiated: it takes
        ownership and starts a new power cycle (D12). A manual power-off releases
        ownership.
        """
        relay: RelayApp = request.app.state.relay
        try:
            data = await request.json()
        except Exception:
            return JSONResponse(
                status_code=400, content={"error": "Expected a JSON body {server, action}."}
            )
        if not isinstance(data, dict):
            return JSONResponse(
                status_code=400, content={"error": "Expected a JSON body {server, action}."}
            )
        server_name = data.get("server")
        action = data.get("action")
        if not server_name or action not in ("on", "off"):
            return JSONResponse(
                status_code=400,
                content={"error": "Expected a JSON body {server, action: on|off}."},
            )
        server = relay.servers.get(server_name)
        if server is None:
            return JSONResponse(
                status_code=404, content={"error": f"Unknown server '{server_name}'."}
            )
        if action == "on":
            ok = await server.power_on(reason="manual", initiated_by="monitor-ui")
        else:
            ok = await server.power_off(reason="manual", initiated_by="monitor-ui")
        logger.info(
            "Manual power %s for server %s from monitor: %s.",
            action,
            server_name,
            "accepted" if ok else "failed",
        )
        return JSONResponse({"ok": ok, "server": server_name, "action": action})

    @app.post("/monitor/timeout")
    async def monitor_timeout(request: Request):
        """Sets the proxy-to-target read timeout at runtime (M5, X4)."""
        relay: RelayApp = request.app.state.relay
        try:
            data = await request.json()
        except Exception:
            return JSONResponse(
                status_code=400, content={"error": "Expected a JSON body {read_timeout}."}
            )
        value = data.get("read_timeout") if isinstance(data, dict) else None
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            return JSONResponse(
                status_code=400,
                content={
                    "error": (
                        "Expected a JSON body {read_timeout: N} with N a non-negative "
                        "integer (0 = no timeout)."
                    )
                },
            )
        relay.set_read_timeout(value)
        logger.info("Target read timeout set to %s from monitor.", value or "none")
        return JSONResponse({"target_read_timeout": value})

    @app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
    async def proxy(request: Request, path: str):
        """The queueing proxy (X1, X6, X7; architecture.md request flow).

        Routes by longest path prefix; unmatched paths go to the catch_all
        endpoint (passthrough, tracked) under ``unknown_path_policy: allow``
        or get a 403 under ``block``. Routed requests adopt power ownership
        of their server (P6); a 403-blocked request does not.
        """
        relay: RelayApp = request.app.state.relay
        full_path = "/" + path
        endpoint = route(full_path, relay.endpoints)
        if endpoint is None:
            if relay.config.proxy.unknown_path_policy != "allow":
                return JSONResponse(
                    status_code=403,
                    content={
                        "error": {
                            "message": (
                                f"Unknown API path '{full_path}' is blocked "
                                "(unknown_path_policy=block)"
                            ),
                            "type": "policy_error",
                            "param": None,
                            "code": "unknown_api_blocked",
                        }
                    },
                )
            endpoint = relay.catch_all
            if endpoint is None:
                return JSONResponse(
                    status_code=404,
                    content={"error": "Unknown path and no catch-all endpoint configured."},
                )
            lowered = {k.lower(): v for k, v in request.headers.items()}
            client_ip = request.client.host if request.client else "unknown"
            relay.unknown_tracker.record(
                client_ip,
                lowered.get("user-agent", ""),
                request.method,
                full_path,
                f"{endpoint.server.config.service_url}{full_path}",
            )
            # D17: unmatched paths use passthrough semantics (no queue, no
            # session), whatever the catch_all endpoint's routing mode is.
            await endpoint.server.on_routed_traffic()
            return await endpoint.admit_catch_all(request, full_path)
        await endpoint.server.on_routed_traffic()
        return await endpoint.admit(request, full_path)

    @app.websocket("/{path:path}")
    async def ws_proxy(websocket: WebSocket, path: str):
        """WebSocket catch-all (D18): mirrors the HTTP proxy's routing.

        Unmatched paths under ``allow`` go to the catch_all endpoint with
        passthrough semantics (no queue/session); under ``block`` (or with no
        catch_all) the connection is accepted then closed. Routed connections
        adopt power ownership of their server (P6).
        """
        relay: RelayApp = websocket.app.state.relay
        full_path = "/" + path
        endpoint = route(full_path, relay.endpoints)
        if endpoint is None:
            if relay.config.proxy.unknown_path_policy != "allow" or relay.catch_all is None:
                await websocket.accept()
                await websocket.close(code=1008)
                return
            endpoint = relay.catch_all
            await endpoint.server.on_routed_traffic()
            await endpoint.admit_ws(websocket, full_path, passthrough=True)
            return
        await endpoint.server.on_routed_traffic()
        await endpoint.admit_ws(websocket, full_path)

    return app


def main() -> None:
    """Console entry point: ``relay --config PATH``."""
    parser = argparse.ArgumentParser(
        prog="relay", description="Run the relay resource-management proxy."
    )
    parser.add_argument(
        "--config",
        default=os.environ.get("RELAY_CONFIG", "./config.yaml"),
        help="Path to config.yaml (default: $RELAY_CONFIG or ./config.yaml)",
    )
    parser.add_argument("--host", default=None, help="Override the bind host from the config")
    parser.add_argument(
        "--port", type=int, default=None, help="Override the bind port from the config"
    )
    args = parser.parse_args()

    try:
        config = load_config(args.config)
    except ConfigError as e:
        raise SystemExit(f"relay: config error: {e}") from e
    # After load_config: .env / secrets.env values are in os.environ.
    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    app = create_app(args.config)
    uvicorn.run(app, host=args.host or config.proxy.host, port=args.port or config.proxy.port)
