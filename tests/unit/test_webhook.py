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
    UnsafeWebhookURLError,
    WebhookClient,
    WebhookDeliveryError,
    WebhookPayment,
    validate_webhook_target,
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
    assert b'"amount":"10.25"' in request.content


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
