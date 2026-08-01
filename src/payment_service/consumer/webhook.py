from __future__ import annotations

import asyncio
import ipaddress
import socket
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any
from urllib.parse import urlsplit
from uuid import UUID

import httpx

from payment_service.config import Settings
from payment_service.domain import Currency, PaymentStatus


class UnsafeWebhookURLError(ValueError):
    """The target violates the service's outbound network policy."""


class WebhookDeliveryError(RuntimeError):
    """The target did not accept the notification."""


@dataclass(frozen=True, slots=True)
class WebhookPayment:
    id: UUID
    amount: Decimal
    currency: Currency
    status: PaymentStatus
    webhook_url: str
    processed_at: datetime


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
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
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
            "processed_at": payment.processed_at.isoformat(),
        }
        headers = {
            "X-Webhook-Event-Id": str(event_id),
            "X-Webhook-Attempt": str(attempt),
        }
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
                    json=payload,
                    headers=headers,
                ) as response:
                    if response.status_code < 200 or response.status_code >= 300:
                        raise WebhookDeliveryError(f"webhook returned HTTP {response.status_code}")
        except TimeoutError as exc:
            raise WebhookDeliveryError("webhook delivery timed out") from exc
        except httpx.HTTPError as exc:
            raise WebhookDeliveryError("webhook request failed") from exc
