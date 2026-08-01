from __future__ import annotations

from uuid import UUID

import pytest

from payment_service.config import Settings
from payment_service.consumer.gateway import GatewaySimulator
from payment_service.domain import PaymentStatus


@pytest.mark.asyncio
async def test_gateway_is_deterministic_and_non_blocking() -> None:
    delays: list[float] = []

    async def record_sleep(delay: float) -> None:
        delays.append(delay)

    settings = Settings(
        payment_min_delay_seconds=2,
        payment_max_delay_seconds=5,
        payment_success_rate=1,
    )
    gateway = GatewaySimulator(settings, sleep=record_sleep)
    payment_id = UUID("00000000-0000-4000-8000-000000000001")

    first = await gateway.process(payment_id)
    second = await gateway.process(payment_id)

    assert first is PaymentStatus.SUCCEEDED
    assert second is first
    assert len(delays) == 2
    assert delays[0] == delays[1]
    assert 2 <= delays[0] <= 5


@pytest.mark.asyncio
async def test_gateway_can_produce_business_failure() -> None:
    async def no_sleep(_: float) -> None:
        return None

    gateway = GatewaySimulator(
        Settings(
            payment_min_delay_seconds=0,
            payment_max_delay_seconds=0,
            payment_success_rate=0,
        ),
        sleep=no_sleep,
    )
    result = await gateway.process(UUID("00000000-0000-4000-8000-000000000001"))
    assert result is PaymentStatus.FAILED
