from __future__ import annotations

import asyncio
import os
import time
from collections import defaultdict
from typing import Any

from fastapi import FastAPI, HTTPException, Request, Response, status

from payment_service.consumer.webhook import (
    SIGNATURE_HEADER,
    TIMESTAMP_HEADER,
    verify_webhook_signature,
)

app = FastAPI(title="E2E webhook sink")
# When set, the sink behaves like a real merchant: unsigned or forged webhooks are refused.
_signing_secret = os.environ.get("WEBHOOK_SIGNING_SECRET")
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
    raw_body = await request.body()
    if _signing_secret is not None and not verify_webhook_signature(
        _signing_secret,
        raw_body,
        timestamp=request.headers.get(TIMESTAMP_HEADER),
        signature=request.headers.get(SIGNATURE_HEADER),
    ):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid webhook signature")
    event = {
        "received_at": time.monotonic(),
        "body": await request.json(),
        "attempt": request.headers.get("X-Webhook-Attempt"),
        "event_id": request.headers.get("X-Webhook-Event-Id"),
        "timestamp": request.headers.get(TIMESTAMP_HEADER),
        "signature": request.headers.get(SIGNATURE_HEADER),
    }
    async with _lock:
        _events.append(event)
    return event
