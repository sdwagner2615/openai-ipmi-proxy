"""
Mock LLM target server for local proxy tests (no hardware required).

  GET  /health                 -> 200 ok
  POST /v1/chat/completions    -> delayed JSON, or SSE stream when "stream": true
  anything else                -> 200 JSON echo (unknown-API pass-through)

Env: MOCK_TARGET_PORT (default 8100), MOCK_DELAY (seconds, default 2)
"""

import asyncio
import os

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse

PORT = int(os.getenv("MOCK_TARGET_PORT", "8100"))
DELAY = float(os.getenv("MOCK_DELAY", "2"))

app = FastAPI()


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/v1/chat/completions")
async def chat(request: Request):
    body = await request.json()
    if body.get("stream"):
        async def gen():
            for i in range(5):
                await asyncio.sleep(DELAY / 5)
                yield f'data: {{"id":"mock","choices":[{{"delta":{{"content":"tok{i} "}}}}]}}\n\n'
            yield "data: [DONE]\n\n"

        return StreamingResponse(gen(), media_type="text/event-stream")
    await asyncio.sleep(DELAY)
    return {
        "id": "mock",
        "choices": [
            {"message": {"role": "assistant", "content": "done"}, "finish_reason": "stop"}
        ],
    }


@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def catchall(path: str):
    return {"echo": True, "path": "/" + path}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=PORT, log_level="warning")
