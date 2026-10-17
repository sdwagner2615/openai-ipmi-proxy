"""Mock WebSocket target for tunnel tests (no hardware required).

  GET  /health              -> 200 ok (readiness)
  WS   /ws/{path}           -> echoes frames back (text + bytes), optional delay

Env: MOCK_WS_PORT (default 8200), MOCK_WS_DELAY (seconds per frame, default 0)
"""

import asyncio
import os

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

PORT = int(os.getenv("MOCK_WS_PORT", "8200"))
DELAY = float(os.getenv("MOCK_WS_DELAY", "0"))

app = FastAPI()


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.websocket("/ws/{path:path}")
async def ws_echo(websocket: WebSocket, path: str):
    await websocket.accept()
    try:
        while True:
            message = await websocket.receive()
            if message["type"] == "websocket.disconnect":
                break
            if DELAY:
                await asyncio.sleep(DELAY)
            if message.get("text") is not None:
                await websocket.send_text(message["text"])
            elif message.get("bytes") is not None:
                await websocket.send_bytes(message["bytes"])
    except WebSocketDisconnect:
        pass


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=PORT, log_level="warning")
