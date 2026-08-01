from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Awaitable, Callable
from uuid import UUID

from payment_service.config import Settings
from payment_service.domain import PaymentStatus


class GatewaySimulator:
    """Deterministic 90/10 emulator so a redelivery cannot change a payment outcome."""

    def __init__(
        self,
        settings: Settings,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._min_delay = settings.payment_min_delay_seconds
        self._max_delay = settings.payment_max_delay_seconds
        self._success_rate = settings.payment_success_rate
        self._sleep = sleep

    async def process(self, payment_id: UUID) -> PaymentStatus:
        digest = hashlib.sha256(payment_id.bytes).digest()
        delay_ratio = int.from_bytes(digest[:8]) / ((1 << 64) - 1)
        delay = self._min_delay + (self._max_delay - self._min_delay) * delay_ratio
        await self._sleep(delay)

        outcome_ratio = int.from_bytes(digest[8:16]) / ((1 << 64) - 1)
        if outcome_ratio < self._success_rate:
            return PaymentStatus.SUCCEEDED
        return PaymentStatus.FAILED
