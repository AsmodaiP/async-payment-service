from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pytest
import respx
from httpx import Response

import payment_service.consumer.webhook as webhook_module
from payment_service.config import Settings
from payment_service.consumer.webhook import (
    SIGNATURE_HEADER,
    TIMESTAMP_HEADER,
    UnsafeWebhookURLError,
    WebhookClient,
    WebhookDeliveryError,
    WebhookPayment,
    sign_webhook,
    validate_webhook_target,
    verify_webhook_signature,
)
from payment_service.domain import Currency, PaymentStatus


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1/hook",
        "http://[::1]/hook",
        "http://169.254.169.254/latest/meta-data",
    ],
)
async def test_private_webhook_targets_are_rejected(url: str) -> None:
    with pytest.raises(UnsafeWebhookURLError):
        await validate_webhook_target(url, allow_private=False)


@pytest.mark.asyncio
async def test_webhook_credentials_are_always_rejected() -> None:
    with pytest.raises(UnsafeWebhookURLError, match="credentials"):
        await validate_webhook_target("https://user:pass@example.com/hook", allow_private=True)


@pytest.mark.asyncio
async def test_plain_http_webhook_is_rejected_by_default() -> None:
    with pytest.raises(UnsafeWebhookURLError, match="must use https"):
        await validate_webhook_target("http://example.com/hook", allow_private=True)


def payment() -> WebhookPayment:
    return WebhookPayment(
        id=UUID("00000000-0000-4000-8000-000000000001"),
        amount=Decimal("10.25"),
        currency=Currency.RUB,
        status=PaymentStatus.SUCCEEDED,
        description="Order #A-1042",
        metadata={"order_id": "A-1042"},
        webhook_url="https://example.com/hook",
        processed_at=datetime(2026, 8, 1, tzinfo=UTC),
    )


@pytest.mark.asyncio
@respx.mock
async def test_webhook_accepts_only_2xx_and_sends_stable_event_id() -> None:
    event_id = UUID("10000000-0000-4000-8000-000000000001")
    route = respx.post("https://example.com/hook").mock(return_value=Response(204))
    client = WebhookClient(Settings(allow_private_webhooks=True))
    try:
        await client.deliver(payment(), event_id=event_id, attempt=2)
    finally:
        await client.close()

    request = route.calls.last.request
    assert request.headers["X-Webhook-Event-Id"] == str(event_id)
    assert request.headers["X-Webhook-Attempt"] == "2"
    assert request.headers["Content-Type"] == "application/json"
    assert SIGNATURE_HEADER not in request.headers
    assert b'"amount":"10.25"' in request.content
    assert b'"metadata":{"order_id":"A-1042"}' in request.content
    assert b'"description":"Order #A-1042"' in request.content


@pytest.mark.asyncio
@respx.mock
async def test_webhook_is_signed_over_timestamp_and_raw_body() -> None:
    secret = "s" * 32
    route = respx.post("https://example.com/hook").mock(return_value=Response(200))
    client = WebhookClient(
        Settings(allow_private_webhooks=True, webhook_signing_secret=secret),
        clock=lambda: 1_700_000_000.7,
    )
    try:
        await client.deliver(
            payment(),
            event_id=UUID("10000000-0000-4000-8000-000000000001"),
            attempt=1,
        )
    finally:
        await client.close()

    request = route.calls.last.request
    assert request.headers[TIMESTAMP_HEADER] == "1700000000"
    assert request.headers[SIGNATURE_HEADER] == sign_webhook(secret, 1_700_000_000, request.content)
    assert verify_webhook_signature(
        secret,
        request.content,
        timestamp=request.headers[TIMESTAMP_HEADER],
        signature=request.headers[SIGNATURE_HEADER],
        now=1_700_000_010,
    )


def test_signature_verification_rejects_tampering_and_replays() -> None:
    secret = "s" * 32
    body = b'{"amount":"10.25"}'
    signature = sign_webhook(secret, 1_700_000_000, body)

    assert verify_webhook_signature(
        secret, body, timestamp="1700000000", signature=signature, now=1_700_000_000
    )
    assert not verify_webhook_signature(
        secret,
        b'{"amount":"99.00"}',
        timestamp="1700000000",
        signature=signature,
        now=1_700_000_000,
    )
    assert not verify_webhook_signature(
        "other-secret", body, timestamp="1700000000", signature=signature, now=1_700_000_000
    )
    assert not verify_webhook_signature(
        secret, body, timestamp="1700000000", signature=signature, now=1_700_000_000 + 301
    )
    assert not verify_webhook_signature(secret, body, timestamp=None, signature=signature)
    assert not verify_webhook_signature(secret, body, timestamp="soon", signature=signature)


@pytest.mark.asyncio
@respx.mock
async def test_non_2xx_webhook_is_retryable_failure() -> None:
    respx.post("https://example.com/hook").mock(return_value=Response(503))
    client = WebhookClient(Settings(allow_private_webhooks=True))
    try:
        with pytest.raises(WebhookDeliveryError, match="HTTP 503"):
            await client.deliver(
                payment(),
                event_id=UUID("10000000-0000-4000-8000-000000000001"),
                attempt=1,
            )
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_delivery_deadline_includes_target_validation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def slow_validation(*_: object, **__: object) -> None:
        await asyncio.sleep(1)

    monkeypatch.setattr(webhook_module, "validate_webhook_target", slow_validation)
    client = WebhookClient(
        Settings(
            allow_private_webhooks=True,
            webhook_timeout_seconds=0.01,
        )
    )
    try:
        with pytest.raises(WebhookDeliveryError, match="timed out"):
            await client.deliver(
                payment(),
                event_id=UUID("10000000-0000-4000-8000-000000000001"),
                attempt=1,
            )
    finally:
        await client.close()
