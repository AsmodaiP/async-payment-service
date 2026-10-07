from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Protocol
from uuid import UUID, uuid4

from pydantic import JsonValue
from sqlalchemy import func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.config import Settings
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


class PaymentGateway(Protocol):
    async def process(self, payment_id: UUID) -> PaymentStatus: ...


class WebhookSender(Protocol):
    async def deliver(
        self,
        payment: WebhookPayment,
        *,
        event_id: UUID,
        attempt: int,
    ) -> None: ...


@dataclass(frozen=True, slots=True)
class RetryDeliveryError(Exception):
    phase: str
    attempt: int
    reason: str


@dataclass(frozen=True, slots=True)
class ExhaustedDeliveryError(Exception):
    phase: str
    reason: str


@dataclass(frozen=True, slots=True)
class WebhookClaimBusyError(Exception):
    payment_id: UUID


@dataclass(frozen=True, slots=True)
class PaymentSnapshot:
    id: UUID
    amount: Decimal
    currency: Currency
    status: PaymentStatus
    description: str | None
    metadata: dict[str, JsonValue]
    webhook_url: str
    processed_at: datetime | None
    webhook_delivered_at: datetime | None
    webhook_attempts: int


@dataclass(frozen=True, slots=True)
class WebhookClaim:
    payment: PaymentSnapshot
    token: UUID
    attempt: int


class PaymentProcessor:
    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
        gateway: PaymentGateway,
        webhook: WebhookSender,
    ) -> None:
        self._settings = settings
        self._session_factory = session_factory
        self._gateway = gateway
        self._webhook = webhook

    async def process(self, event: PaymentCreatedEvent) -> None:
        payment = await self._load(event.payment_id)
        if payment.status is PaymentStatus.PENDING:
            result = await self._gateway.process(payment.id)
            payment = await self._save_terminal_result(payment.id, result)

        if payment.webhook_delivered_at is not None:
            return
        if payment.processed_at is None:
            raise PermanentProcessingError("terminal payment has no processed_at timestamp")

        claim = await self._claim_webhook_attempt(payment.id)
        if claim is None:
            return
        claimed = claim.payment
        if claimed.processed_at is None:
            raise PermanentProcessingError("claimed terminal payment has no processed_at timestamp")
        webhook_payment = WebhookPayment(
            id=claimed.id,
            amount=claimed.amount,
            currency=claimed.currency,
            status=claimed.status,
            description=claimed.description,
            metadata=claimed.metadata,
            webhook_url=claimed.webhook_url,
            processed_at=claimed.processed_at,
        )
        try:
            await self._webhook.deliver(
                webhook_payment,
                event_id=event.event_id,
                attempt=claim.attempt,
            )
        except UnsafeWebhookURLError as exc:
            recorded_attempt = await self._finish_webhook_failure(
                payment.id,
                claim.token,
                str(exc),
            )
            if recorded_attempt is None:
                return
            raise PermanentProcessingError(str(exc)) from exc
        except WebhookDeliveryError as exc:
            recorded_attempt = await self._finish_webhook_failure(
                payment.id,
                claim.token,
                str(exc),
            )
            if recorded_attempt is None:
                return
            if recorded_attempt >= self._settings.max_delivery_attempts:
                raise ExhaustedDeliveryError("webhook", str(exc)) from exc
            raise RetryDeliveryError(
                "webhook",
                recorded_attempt,
                str(exc),
            ) from exc
        except asyncio.CancelledError:
            await self._release_webhook_claim(payment.id, claim.token)
            raise
        except Exception:
            await self._release_webhook_claim(payment.id, claim.token)
            raise

        await self._mark_webhook_delivered(payment.id, claim.token)

    async def _load(self, payment_id: UUID) -> PaymentSnapshot:
        async with self._session_factory() as session:
            payment = await session.scalar(select(Payment).where(Payment.id == payment_id))
            if payment is None:
                raise PaymentNotFoundError(str(payment_id))
            return self._snapshot(payment)

    async def _save_terminal_result(
        self,
        payment_id: UUID,
        result: PaymentStatus,
    ) -> PaymentSnapshot:
        async with self._session_factory() as session, session.begin():
            payment = await session.scalar(
                update(Payment)
                .where(Payment.id == payment_id, Payment.status == PaymentStatus.PENDING.value)
                .values(status=result.value, processed_at=func.clock_timestamp())
                .returning(Payment)
            )
            if payment is None:
                payment = await session.scalar(select(Payment).where(Payment.id == payment_id))
            if payment is None:
                raise PaymentNotFoundError(str(payment_id))
            return self._snapshot(payment)

    async def _claim_webhook_attempt(self, payment_id: UUID) -> WebhookClaim | None:
        token = uuid4()
        async with self._session_factory() as session, session.begin():
            payment = await session.scalar(
                update(Payment)
                .where(
                    Payment.id == payment_id,
                    Payment.webhook_delivered_at.is_(None),
                    Payment.webhook_attempts < self._settings.max_delivery_attempts,
                    or_(
                        Payment.webhook_locked_until.is_(None),
                        Payment.webhook_locked_until < func.clock_timestamp(),
                    ),
                )
                .values(
                    webhook_lock_token=token,
                    webhook_locked_until=(
                        func.clock_timestamp()
                        + timedelta(seconds=self._settings.webhook_lease_seconds)
                    ),
                )
                .returning(Payment)
            )
            if payment is not None:
                snapshot = self._snapshot(payment)
                return WebhookClaim(
                    payment=snapshot,
                    token=token,
                    attempt=snapshot.webhook_attempts + 1,
                )
            current = await session.scalar(select(Payment).where(Payment.id == payment_id))
            if current is None:
                raise PaymentNotFoundError(str(payment_id))
            if current.webhook_delivered_at is not None:
                return None
            if current.webhook_attempts >= self._settings.max_delivery_attempts:
                raise ExhaustedDeliveryError("webhook", "webhook attempts already exhausted")
            # The only broker message may be a redelivery after the previous owner crashed.
            # It must remain a recovery trigger until the lease expires.
            raise WebhookClaimBusyError(payment_id)

    async def _finish_webhook_failure(
        self,
        payment_id: UUID,
        token: UUID,
        error: str,
    ) -> int | None:
        async with self._session_factory() as session, session.begin():
            return await session.scalar(
                update(Payment)
                .where(
                    Payment.id == payment_id,
                    Payment.webhook_delivered_at.is_(None),
                    Payment.webhook_lock_token == token,
                )
                .values(
                    webhook_attempts=Payment.webhook_attempts + 1,
                    webhook_last_error=error[:1_000],
                    webhook_lock_token=None,
                    webhook_locked_until=None,
                )
                .returning(Payment.webhook_attempts)
            )

    async def _release_webhook_claim(self, payment_id: UUID, token: UUID) -> None:
        async with self._session_factory() as session, session.begin():
            await session.execute(
                update(Payment)
                .where(
                    Payment.id == payment_id,
                    Payment.webhook_delivered_at.is_(None),
                    Payment.webhook_lock_token == token,
                )
                .values(
                    webhook_lock_token=None,
                    webhook_locked_until=None,
                )
            )

    async def _mark_webhook_delivered(self, payment_id: UUID, token: UUID) -> bool:
        async with self._session_factory() as session, session.begin():
            completed = await session.scalar(
                update(Payment)
                .where(
                    Payment.id == payment_id,
                    Payment.webhook_delivered_at.is_(None),
                    Payment.webhook_lock_token == token,
                )
                .values(
                    webhook_delivered_at=func.clock_timestamp(),
                    webhook_attempts=func.least(
                        Payment.webhook_attempts + 1,
                        self._settings.max_delivery_attempts,
                    ),
                    webhook_last_error=None,
                    webhook_lock_token=None,
                    webhook_locked_until=None,
                )
                .returning(Payment.id)
            )
            return completed is not None

    @staticmethod
    def _snapshot(payment: Payment) -> PaymentSnapshot:
        return PaymentSnapshot(
            id=payment.id,
            amount=payment.amount,
            currency=Currency(payment.currency),
            status=PaymentStatus(payment.status),
            description=payment.description,
            metadata=dict(payment.payment_metadata),
            webhook_url=payment.webhook_url,
            processed_at=payment.processed_at,
            webhook_delivered_at=payment.webhook_delivered_at,
            webhook_attempts=payment.webhook_attempts,
        )
