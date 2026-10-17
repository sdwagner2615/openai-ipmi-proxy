"""WebSocket tunnel (D18).

Bidirectional frame pump between an inbound (client) WebSocket and the target
service's WebSocket. The proxy:

- derives the target WS URL from the endpoint's HTTP base + the verbatim path;
- runs two pump tasks (client->target, target->client) and closes both when
  either side ends;
- releases the caller's slot exactly once, for every outcome (Q12 analogue);
- while holding a connection for a ``queued`` endpoint, the slot lives for the
  connection's lifetime (released here on close).

Keep-alive pings while waiting and the 1013 (Try Again Later) close for the
``error`` wait policy are handled by the endpoint admission (relay/endpoints.py);
this module owns the tunnel itself.
"""

import asyncio
import logging
from urllib.parse import urlsplit, urlunsplit

from fastapi import WebSocket
from starlette.websockets import WebSocketDisconnect
from websockets.asyncio.client import connect

logger = logging.getLogger("relay.transport.ws")

__all__ = ["CLIENT_SENTINEL", "read_client", "to_ws_url", "tunnel"]

#: Queue sentinel the client reader pushes when the inbound connection ends.
CLIENT_SENTINEL = object()


def to_ws_url(base_url: str, path: str) -> str:
    """Derives the target WebSocket URL from the HTTP base + verbatim path."""
    parts = urlsplit(base_url)
    scheme = "wss" if parts.scheme == "https" else "ws"
    return urlunsplit((scheme, parts.netloc, path, "", ""))


async def read_client(
    websocket: WebSocket, client_q: asyncio.Queue, disconnected: asyncio.Event
) -> None:
    """Drains inbound client frames into ``client_q`` until the connection ends.

    On any close (client disconnect or protocol error) it sets ``disconnected``
    and pushes the sentinel so a waiting consumer (the tunnel pump) terminates.
    """
    try:
        while True:
            message = await websocket.receive()
            if message["type"] == "websocket.disconnect":
                break
            await client_q.put(message)
    except WebSocketDisconnect:
        pass
    finally:
        disconnected.set()
        await client_q.put(CLIENT_SENTINEL)


async def tunnel(
    websocket: WebSocket,
    target_url: str,
    client_q: asyncio.Queue,
    disconnected: asyncio.Event,
    release=None,
) -> None:
    """Tunnels frames between the inbound WS and the outbound target WS.

    ``release`` (when given) is called exactly once when the tunnel ends, for
    every outcome (either side closed, a pump error, or a failed connect).
    """
    try:
        async with await connect(target_url) as target:

            async def client_to_target() -> None:
                while True:
                    message = await client_q.get()
                    if message is CLIENT_SENTINEL or disconnected.is_set():
                        break
                    if message.get("bytes") is not None:
                        await target.send(message["bytes"])
                    elif message.get("text") is not None:
                        await target.send(message["text"])

            async def target_to_client() -> None:
                async for frame in target:
                    if isinstance(frame, (bytes, bytearray)):
                        await websocket.send_bytes(bytes(frame))
                    else:
                        await websocket.send_text(str(frame))
                    if disconnected.is_set():
                        break

            pumps = [
                asyncio.create_task(client_to_target()),
                asyncio.create_task(target_to_client()),
            ]
            done, pending = await asyncio.wait(pumps, return_when=asyncio.FIRST_COMPLETED)
            for task in pending:
                task.cancel()
            for task in done:
                if task.cancelled():
                    continue
                task.result()
    except Exception as e:
        logger.error("WS tunnel to %s failed: %s", target_url, e)
    finally:
        if release is not None:
            release()
