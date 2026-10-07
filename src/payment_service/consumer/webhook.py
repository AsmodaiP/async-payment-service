from __future__ import annotations

import asyncio
import hashlib
import hmac
import ipaddress
import json
import socket
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any
from urllib.parse import urlsplit
from uuid import UUID

import httpx
from pydantic import JsonValue

from payment_service.config import Settings
from payment_service.domain import Currency, PaymentStatus


class UnsafeWebhookURLError(ValueError):
    """The target violates the service's outbound network policy."""


class WebhookDeliveryError(RuntimeError):
    """The target did not accept the notification."""


SIGNATURE_HEADER = "X-Webhook-Signature"
TIMESTAMP_HEADER = "X-Webhook-Timestamp"
EVENT_ID_HEADER = "X-Webhook-Event-Id"
ATTEMPT_HEADER = "X-Webhook-Attempt"


@dataclass(frozen=True, slots=True)
class WebhookPayment:
    id: UUID
    amount: Decimal
    currency: Currency
    status: PaymentStatus
    description: str | None
    metadata: dict[str, JsonValue]
    webhook_url: str
    processed_at: datetime


def encode_webhook_body(payload: dict[str, Any]) -> bytes:
    """Canonical UTF-8 JSON so the receiver can verify the signature over the raw body."""

    return json.dumps(
        payload,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def sign_webhook(secret: str, timestamp: int, body: bytes) -> str:
    """Return ``sha256=<hex>`` over ``"{timestamp}." + body`` (Stripe-style scheme)."""

    digest = hmac.new(
        secret.encode("utf-8"),
        f"{timestamp}.".encode() + body,
        hashlib.sha256,
    ).hexdigest()
    return f"sha256={digest}"


def verify_webhook_signature(
    secret: str,
    body: bytes,
    *,
    timestamp: str | None,
    signature: str | None,
    tolerance_seconds: int = 300,
    now: float | None = None,
) -> bool:
    """Constant-time check that receivers can copy; rejects replays outside the tolerance."""

    if timestamp is None or signature is None:
        return False
    try:
        sent_at = int(timestamp)
    except ValueError:
        return False
    current = time.time() if now is None else now
    if abs(current - sent_at) > tolerance_seconds:
        return False
    return hmac.compare_digest(sign_webhook(secret, sent_at, body), signature)


async def validate_webhook_target(
    url: str,
    *,
    allow_private: bool,
    require_https: bool = True,
) -> None:
    """Fail closed for non-global targets; production should also enforce network egress."""

    parsed = urlsplit(url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise UnsafeWebhookURLError("webhook URL must use http or https")
    if require_https and parsed.scheme != "https":
        raise UnsafeWebhookURLError("webhook URL must use https")
    if parsed.username is not None or parsed.password is not None:
        raise UnsafeWebhookURLError("webhook URL must not contain credentials")
    if allow_private:
        return

    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    try:
        literal = ipaddress.ip_address(parsed.hostname)
        addresses = {literal}
    except ValueError:
        loop = asyncio.get_running_loop()
        try:
            resolved = await loop.getaddrinfo(
                parsed.hostname,
                port,
                type=socket.SOCK_STREAM,
            )
        except OSError as exc:
            raise WebhookDeliveryError("webhook hostname could not be resolved") from exc
        addresses = {ipaddress.ip_address(item[4][0]) for item in resolved}

    if not addresses or any(not address.is_global for address in addresses):
        raise UnsafeWebhookURLError("webhook target resolves to a non-public address")


class WebhookClient:
    def __init__(
        self,
        settings: Settings,
        *,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._settings = settings
        self._clock = clock
        secret = settings.webhook_signing_secret
        self._signing_secret = secret.get_secret_value() if secret is not None else None
        timeout = httpx.Timeout(settings.webhook_timeout_seconds)
        self._client = httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=False,
            trust_env=False,
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def deliver(
        self,
        payment: WebhookPayment,
        *,
        event_id: UUID,
        attempt: int,
    ) -> None:
        payload: dict[str, Any] = {
            "event_id": str(event_id),
            "event_type": f"payment.{payment.status.value}",
            "payment_id": str(payment.id),
            "status": payment.status.value,
            "amount": str(payment.amount),
            "currency": payment.currency.value,
            "description": payment.description,
            "metadata": payment.metadata,
            "processed_at": payment.processed_at.isoformat(),
        }
        body = encode_webhook_body(payload)
        headers = {
            "Content-Type": "application/json",
            EVENT_ID_HEADER: str(event_id),
            ATTEMPT_HEADER: str(attempt),
        }
        if self._signing_secret is not None:
            timestamp = int(self._clock())
            headers[TIMESTAMP_HEADER] = str(timestamp)
            headers[SIGNATURE_HEADER] = sign_webhook(self._signing_secret, timestamp, body)
        try:
            async with asyncio.timeout(self._settings.webhook_timeout_seconds):
                await validate_webhook_target(
                    payment.webhook_url,
                    allow_private=self._settings.allow_private_webhooks,
                    require_https=self._settings.require_https_webhooks,
                )
                async with self._client.stream(
                    "POST",
                    payment.webhook_url,
                    content=body,
                    headers=headers,
                ) as response:
                    if response.status_code < 200 or response.status_code >= 300:
                        raise WebhookDeliveryError(f"webhook returned HTTP {response.status_code}")
        except TimeoutError as exc:
            raise WebhookDeliveryError("webhook delivery timed out") from exc
        except httpx.HTTPError as exc:
            raise WebhookDeliveryError("webhook request failed") from exc
