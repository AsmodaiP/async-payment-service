from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from typing import Any

from fastapi import FastAPI, Request, Response, status

app = FastAPI(title="E2E webhook sink")
_lock = asyncio.Lock()
_events: list[dict[str, Any]] = []
_attempts: defaultdict[str, int] = defaultdict(int)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/events")
async def events() -> dict[str, Any]:
    async with _lock:
        return {"count": len(_events), "events": list(_events)}


@app.post("/hooks/fail-twice")
async def fail_twice(request: Request, response: Response) -> dict[str, bool]:
    event = await _record(request)
    event_id = str(event["body"]["event_id"])
    async with _lock:
        _attempts[event_id] += 1
        attempt = _attempts[event_id]
    if attempt <= 2:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {"accepted": False}
    return {"accepted": True}


@app.post("/hooks/always-fail", status_code=status.HTTP_503_SERVICE_UNAVAILABLE)
async def always_fail(request: Request) -> dict[str, bool]:
    await _record(request)
    return {"accepted": False}


@app.post("/hooks/succeed")
async def succeed(request: Request) -> dict[str, bool]:
    await _record(request)
    return {"accepted": True}


@app.post("/hooks/block-first")
async def block_first(request: Request) -> dict[str, bool]:
    event = await _record(request)
    event_id = str(event["body"]["event_id"])
    async with _lock:
        _attempts[event_id] += 1
        attempt = _attempts[event_id]
    if attempt == 1:
        # Give the e2e runner time to SIGKILL a consumer while it holds a DB lease.
        await asyncio.sleep(5)
    return {"accepted": True}


async def _record(request: Request) -> dict[str, Any]:
    event = {
        "received_at": time.monotonic(),
        "body": await request.json(),
        "attempt": request.headers.get("X-Webhook-Attempt"),
        "event_id": request.headers.get("X-Webhook-Event-Id"),
    }
    async with _lock:
        _events.append(event)
    return event
