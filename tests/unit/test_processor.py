from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, cast
from uuid import UUID

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.config import Settings
from payment_service.consumer.processor import (
    ExhaustedDeliveryError,
    PaymentProcessor,
    PaymentSnapshot,
    RetryDeliveryError,
    WebhookClaim,
    WebhookClaimBusyError,
)
from payment_service.consumer.webhook import (
    UnsafeWebhookURLError,
    WebhookDeliveryError,
    WebhookPayment,
)
from payment_service.db.models import Payment
from payment_service.domain import (
    Currency,
    PaymentNotFoundError,
    PaymentStatus,
    PermanentProcessingError,
)
from payment_service.messaging.events import PaymentCreatedEvent

PAYMENT_ID = UUID("00000000-0000-4000-8000-000000000001")
EVENT_ID = UUID("10000000-0000-4000-8000-000000000001")
PROCESSED_AT = datetime(2026, 8, 1, tzinfo=UTC)
CLAIM_TOKEN = UUID("20000000-0000-4000-8000-000000000001")


def snapshot(
    *,
    status: PaymentStatus = PaymentStatus.PENDING,
    processed_at: datetime | None = None,
    delivered_at: datetime | None = None,
    attempts: int = 0,
) -> PaymentSnapshot:
    return PaymentSnapshot(
        id=PAYMENT_ID,
        amount=Decimal("10.25"),
        currency=Currency.RUB,
        status=status,
        description=None,
        metadata={},
        webhook_url="https://merchant.example/hook",
        processed_at=processed_at,
        webhook_delivered_at=delivered_at,
        webhook_attempts=attempts,
    )


def event() -> PaymentCreatedEvent:
    return PaymentCreatedEvent(
        event_id=EVENT_ID,
        payment_id=PAYMENT_ID,
        occurred_at=PROCESSED_AT,
    )


class Gateway:
    def __init__(self) -> None:
        self.calls = 0

    async def process(self, _: UUID) -> PaymentStatus:
        self.calls += 1
        return PaymentStatus.SUCCEEDED


class Webhook:
    def __init__(self, error: BaseException | None = None) -> None:
        self.error = error
        self.calls: list[tuple[WebhookPayment, UUID, int]] = []

    async def deliver(self, payment: WebhookPayment, *, event_id: UUID, attempt: int) -> None:
        self.calls.append((payment, event_id, attempt))
        if self.error is not None:
            raise self.error


class ProcessorHarness(PaymentProcessor):
    def __init__(
        self,
        initial: PaymentSnapshot,
        claimed: PaymentSnapshot | None,
        *,
        webhook_error: BaseException | None = None,
    ) -> None:
        self.gateway = Gateway()
        self.webhook = Webhook(webhook_error)
        unused_factory = cast(async_sessionmaker[AsyncSession], None)
        super().__init__(Settings(), unused_factory, self.gateway, self.webhook)
        self.initial = initial
        self.claimed = (
            WebhookClaim(claimed, CLAIM_TOKEN, claimed.webhook_attempts)
            if claimed is not None
            else None
        )
        self.saved_result: PaymentStatus | None = None
        self.recorded_error: str | None = None
        self.marked = False
        self.released = False

    async def _load(self, _: UUID) -> PaymentSnapshot:
        return self.initial

    async def _save_terminal_result(self, _: UUID, result: PaymentStatus) -> PaymentSnapshot:
        self.saved_result = result
        return snapshot(status=result, processed_at=PROCESSED_AT)

    async def _claim_webhook_attempt(self, _: UUID) -> WebhookClaim | None:
        return self.claimed

    async def _finish_webhook_failure(self, _: UUID, __: UUID, error: str) -> int | None:
        self.recorded_error = error
        return self.claimed.attempt if self.claimed is not None else None

    async def _release_webhook_claim(self, _: UUID, __: UUID) -> None:
        self.released = True

    async def _mark_webhook_delivered(self, _: UUID, __: UUID) -> bool:
        self.marked = True
        return True


class FakeTransaction:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *_: Any) -> None:
        return None


class FakeSession:
    def __init__(self, scalar_results: list[Any]) -> None:
        self.scalar_results = scalar_results
        self.execute_calls = 0

    async def __aenter__(self) -> FakeSession:
        return self

    async def __aexit__(self, *_: Any) -> None:
        return None

    def begin(self) -> FakeTransaction:
        return FakeTransaction()

    async def scalar(self, _: Any) -> Any:
        return self.scalar_results.pop(0)

    async def execute(self, _: Any) -> None:
        self.execute_calls += 1


def model_payment(*, attempts: int = 0, delivered: bool = False) -> Payment:
    return Payment(
        id=PAYMENT_ID,
        idempotency_key="order-42",
        request_fingerprint="a" * 64,
        amount=Decimal("10.25"),
        currency=Currency.RUB.value,
        status=PaymentStatus.SUCCEEDED.value,
        description=None,
        payment_metadata={},
        webhook_url="https://merchant.example/hook",
        created_at=PROCESSED_AT,
        processed_at=PROCESSED_AT,
        webhook_delivered_at=PROCESSED_AT if delivered else None,
        webhook_attempts=attempts,
        webhook_lock_token=None,
        webhook_locked_until=None,
        webhook_last_error=None,
    )


def processor_with_session(session: FakeSession) -> PaymentProcessor:
    factory = cast(async_sessionmaker[AsyncSession], lambda: session)
    return PaymentProcessor(Settings(), factory, Gateway(), Webhook())


@pytest.mark.asyncio
async def test_pending_payment_is_processed_and_webhook_is_delivered() -> None:
    claimed = snapshot(status=PaymentStatus.SUCCEEDED, processed_at=PROCESSED_AT, attempts=1)
    processor = ProcessorHarness(snapshot(), claimed)

    await processor.process(event())

    assert processor.gateway.calls == 1
    assert processor.saved_result is PaymentStatus.SUCCEEDED
    assert processor.webhook.calls[0][1:] == (EVENT_ID, 1)
    assert processor.marked is True


@pytest.mark.asyncio
async def test_terminal_redelivery_does_not_repeat_gateway_or_delivered_webhook() -> None:
    processor = ProcessorHarness(
        snapshot(
            status=PaymentStatus.SUCCEEDED,
            processed_at=PROCESSED_AT,
            delivered_at=PROCESSED_AT,
        ),
        None,
    )
    await processor.process(event())
    assert processor.gateway.calls == 0
    assert processor.webhook.calls == []


@pytest.mark.asyncio
async def test_retryable_webhook_failure_contains_persisted_attempt() -> None:
    processor = ProcessorHarness(
        snapshot(status=PaymentStatus.FAILED, processed_at=PROCESSED_AT),
        snapshot(status=PaymentStatus.FAILED, processed_at=PROCESSED_AT, attempts=2),
        webhook_error=WebhookDeliveryError("temporary"),
    )
    with pytest.raises(RetryDeliveryError) as captured:
        await processor.process(event())
    assert captured.value.attempt == 2
    assert processor.recorded_error == "temporary"


@pytest.mark.asyncio
async def test_third_webhook_failure_is_exhausted() -> None:
    processor = ProcessorHarness(
        snapshot(status=PaymentStatus.SUCCEEDED, processed_at=PROCESSED_AT),
        snapshot(status=PaymentStatus.SUCCEEDED, processed_at=PROCESSED_AT, attempts=3),
        webhook_error=WebhookDeliveryError("still unavailable"),
    )
    with pytest.raises(ExhaustedDeliveryError):
        await processor.process(event())


@pytest.mark.asyncio
async def test_unsafe_webhook_is_permanent_failure() -> None:
    processor = ProcessorHarness(
        snapshot(status=PaymentStatus.SUCCEEDED, processed_at=PROCESSED_AT),
        snapshot(status=PaymentStatus.SUCCEEDED, processed_at=PROCESSED_AT, attempts=1),
        webhook_error=UnsafeWebhookURLError("private target"),
    )
    with pytest.raises(PermanentProcessingError):
        await processor.process(event())
    assert processor.recorded_error == "private target"


@pytest.mark.asyncio
async def test_missing_processed_at_is_rejected_before_claim() -> None:
    processor = ProcessorHarness(snapshot(status=PaymentStatus.SUCCEEDED), None)
    with pytest.raises(PermanentProcessingError, match="no processed_at"):
        await processor.process(event())


@pytest.mark.asyncio
async def test_claim_race_that_was_already_completed_is_a_noop() -> None:
    processor = ProcessorHarness(
        snapshot(status=PaymentStatus.SUCCEEDED, processed_at=PROCESSED_AT),
        None,
    )
    await processor.process(event())
    assert processor.webhook.calls == []


@pytest.mark.asyncio
async def test_invalid_claimed_snapshot_is_rejected() -> None:
    processor = ProcessorHarness(
        snapshot(status=PaymentStatus.SUCCEEDED, processed_at=PROCESSED_AT),
        snapshot(status=PaymentStatus.SUCCEEDED, attempts=1),
    )
    with pytest.raises(PermanentProcessingError, match="claimed terminal"):
        await processor.process(event())


@pytest.mark.asyncio
async def test_cancellation_releases_claim_without_recording_an_attempt() -> None:
    processor = ProcessorHarness(
        snapshot(status=PaymentStatus.SUCCEEDED, processed_at=PROCESSED_AT),
        snapshot(status=PaymentStatus.SUCCEEDED, processed_at=PROCESSED_AT, attempts=1),
        webhook_error=asyncio.CancelledError(),
    )

    with pytest.raises(asyncio.CancelledError):
        await processor.process(event())

    assert processor.released is True
    assert processor.recorded_error is None


@pytest.mark.asyncio
async def test_unexpected_webhook_error_releases_claim() -> None:
    processor = ProcessorHarness(
        snapshot(status=PaymentStatus.SUCCEEDED, processed_at=PROCESSED_AT),
        snapshot(status=PaymentStatus.SUCCEEDED, processed_at=PROCESSED_AT, attempts=1),
        webhook_error=RuntimeError("unexpected"),
    )

    with pytest.raises(RuntimeError, match="unexpected"):
        await processor.process(event())

    assert processor.released is True


@pytest.mark.asyncio
async def test_database_adapter_loads_and_updates_delivery_state() -> None:
    payment = model_payment()
    session = FakeSession([payment, payment, 2, PAYMENT_ID])
    processor = processor_with_session(session)

    loaded = await processor._load(PAYMENT_ID)
    saved = await processor._save_terminal_result(PAYMENT_ID, PaymentStatus.SUCCEEDED)
    recorded = await processor._finish_webhook_failure(
        PAYMENT_ID,
        CLAIM_TOKEN,
        "temporary",
    )
    await processor._release_webhook_claim(PAYMENT_ID, CLAIM_TOKEN)
    delivered = await processor._mark_webhook_delivered(PAYMENT_ID, CLAIM_TOKEN)

    assert loaded.id == PAYMENT_ID
    assert saved.status is PaymentStatus.SUCCEEDED
    assert recorded == 2
    assert delivered is True
    assert session.execute_calls == 1


@pytest.mark.asyncio
async def test_database_adapter_recovers_terminal_state_after_cas_race() -> None:
    payment = model_payment()
    processor = processor_with_session(FakeSession([None, payment]))

    saved = await processor._save_terminal_result(PAYMENT_ID, PaymentStatus.SUCCEEDED)

    assert saved.id == PAYMENT_ID


@pytest.mark.asyncio
async def test_webhook_claim_uses_lease_and_serializes_duplicates() -> None:
    payment = model_payment(attempts=1)
    claimed = await processor_with_session(FakeSession([payment]))._claim_webhook_attempt(
        PAYMENT_ID
    )

    assert claimed is not None
    assert claimed.attempt == 2

    with pytest.raises(WebhookClaimBusyError):
        await processor_with_session(
            FakeSession([None, model_payment(attempts=1)])
        )._claim_webhook_attempt(PAYMENT_ID)


@pytest.mark.asyncio
async def test_webhook_claim_distinguishes_delivery_and_exhaustion() -> None:
    delivered = await processor_with_session(
        FakeSession([None, model_payment(delivered=True)])
    )._claim_webhook_attempt(PAYMENT_ID)
    assert delivered is None

    with pytest.raises(ExhaustedDeliveryError):
        await processor_with_session(
            FakeSession([None, model_payment(attempts=3)])
        )._claim_webhook_attempt(PAYMENT_ID)

    with pytest.raises(PaymentNotFoundError):
        await processor_with_session(FakeSession([None, None]))._claim_webhook_attempt(PAYMENT_ID)


@pytest.mark.asyncio
async def test_webhook_success_requires_current_owner_token() -> None:
    processor = processor_with_session(FakeSession([None]))

    assert await processor._mark_webhook_delivered(PAYMENT_ID, CLAIM_TOKEN) is False
