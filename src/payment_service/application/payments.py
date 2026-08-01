from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol
from uuid import UUID, uuid4

from pydantic import JsonValue
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from payment_service.db.models import OutboxEvent, Payment
from payment_service.domain import (
    Currency,
    IdempotencyConflictError,
    PaymentNotFoundError,
    PaymentStatus,
)

PAYMENT_CREATED_EVENT_TYPE = "payment.created.v1"
PAYMENT_CREATED_SCHEMA_VERSION = 1


@dataclass(frozen=True, slots=True)
class CreatePaymentResult:
    payment: Payment
    replayed: bool


class PaymentRequest(Protocol):
    @property
    def amount(self) -> Decimal: ...

    @property
    def currency(self) -> Currency: ...

    @property
    def description(self) -> str | None: ...

    @property
    def metadata_(self) -> dict[str, JsonValue]: ...

    @property
    def webhook_url(self) -> object: ...


def _canonical_decimal(value: Decimal) -> str:
    normalized = value.normalize()
    if normalized == normalized.to_integral_value():
        return format(normalized.quantize(Decimal(1)), "f")
    return format(normalized, "f")


def payment_request_fingerprint(request: PaymentRequest) -> str:
    """Hash the validated semantic request using stable canonical JSON."""

    canonical_request = {
        "amount": _canonical_decimal(request.amount),
        "currency": request.currency.value,
        "description": request.description,
        "metadata": request.metadata_,
        "webhook_url": str(request.webhook_url),
    }
    canonical_json = json.dumps(
        canonical_request,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


async def create_payment(
    session: AsyncSession,
    request: PaymentRequest,
    idempotency_key: str,
) -> CreatePaymentResult:
    """Create a payment and its outbox event atomically, or replay an earlier result."""

    fingerprint = payment_request_fingerprint(request)
    payment_id = uuid4()

    async with session.begin():
        statement = (
            insert(Payment)
            .values(
                id=payment_id,
                idempotency_key=idempotency_key,
                request_fingerprint=fingerprint,
                amount=request.amount,
                currency=request.currency.value,
                status=PaymentStatus.PENDING.value,
                description=request.description,
                payment_metadata=request.metadata_,
                webhook_url=str(request.webhook_url),
                webhook_attempts=0,
            )
            .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
            .returning(Payment)
        )
        payment = (await session.execute(statement)).scalar_one_or_none()

        if payment is None:
            payment = (
                await session.execute(
                    select(Payment).where(Payment.idempotency_key == idempotency_key)
                )
            ).scalar_one()
            if payment.request_fingerprint != fingerprint:
                raise IdempotencyConflictError(
                    "Idempotency-Key was already used for a different payment request"
                )
            return CreatePaymentResult(payment=payment, replayed=True)

        event_id = uuid4()
        session.add(
            OutboxEvent(
                id=event_id,
                aggregate_id=payment.id,
                event_type=PAYMENT_CREATED_EVENT_TYPE,
                schema_version=PAYMENT_CREATED_SCHEMA_VERSION,
                payload={
                    "event_id": str(event_id),
                    "event_type": PAYMENT_CREATED_EVENT_TYPE,
                    "schema_version": PAYMENT_CREATED_SCHEMA_VERSION,
                    "payment_id": str(payment.id),
                    "occurred_at": payment.created_at.isoformat(),
                },
            )
        )

    return CreatePaymentResult(payment=payment, replayed=False)


async def get_payment(session: AsyncSession, payment_id: UUID) -> Payment:
    payment = await session.scalar(select(Payment).where(Payment.id == payment_id))
    if payment is None:
        raise PaymentNotFoundError(f"Payment {payment_id} was not found")
    return payment
