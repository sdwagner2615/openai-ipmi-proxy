"""HTTP/SSE streaming forwarder (X1-X5).

Path-transparent: the method, path, headers, and body are forwarded
verbatim to the endpoint's server (only the host header is stripped), and
the response is streamed back unbuffered (SSE-safe). The caller's slot is
released exactly once for every outcome (Q12) - success, mid-stream error,
read timeout, or a failed send (502, X5).
"""

import logging
from collections.abc import Callable

import httpx
from fastapi import Request
from fastapi.responses import JSONResponse, StreamingResponse
from starlette.responses import Response

logger = logging.getLogger("relay.transport.http")

__all__ = ["forward_request"]


async def forward_request(
    http_client: httpx.AsyncClient,
    base_url: str,
    request: Request,
    path: str,
    body: bytes | None = None,
    *,
    read_timeout: float | None = None,
    release: Callable[[int | None], None] | None = None,
) -> Response:
    """Forwards a request to ``base_url`` + path and streams the response back.

    ``read_timeout`` bounds the silence from the target: per-chunk for SSE
    streams, the whole body for non-streaming responses; None/0 = no timeout
    (X4). ``release`` (when given) is called exactly once when the response
    is finished (with the target's status code) or the send failed (None).
    """
    if body is None:
        body = await request.body()
    headers = dict(request.headers)
    # Remove the host header to prevent the target from rejecting the
    # request due to a host mismatch (X2).
    headers.pop("host", None)
    url = f"{base_url}{path}"

    try:
        # Connect/write/pool are unlimited; the read timeout is the only cap.
        req = http_client.build_request(
            method=request.method,
            url=url,
            headers=headers,
            content=body,
            timeout=httpx.Timeout(None, read=read_timeout or None),
        )
        response = await http_client.send(req, stream=True)
    except Exception as e:
        if release is not None:
            release(None)
        logger.error("Proxy error: %s", e)
        return JSONResponse(status_code=502, content={"error": f"Proxy error: {e!s}"})

    async def stream_generator():
        """Forwards raw bytes from the target (SSE-safe, X3)."""
        try:
            async for chunk in response.aiter_raw():
                yield chunk
        except httpx.ReadTimeout:
            logger.error("Read timeout occurred during streaming from the target")
            yield b" [Error: Read Timeout] "
        except Exception as e:
            logger.error("Unexpected error during streaming: %s", e)
            yield f" [Error: {e!s}] ".encode()
        finally:
            # Ensure the connection is closed and the slot is released
            # exactly once (Q12), for every outcome.
            await response.aclose()
            if release is not None:
                release(response.status_code)
            logger.debug("Request for %s finished.", path)

    return StreamingResponse(
        stream_generator(), status_code=response.status_code, headers=dict(response.headers)
    )
