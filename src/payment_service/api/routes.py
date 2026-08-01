from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from payment_service.api.dependencies import require_api_key, require_idempotency_key
from payment_service.api.schemas import PaymentAccepted, PaymentCreate, PaymentDetail
from payment_service.application.payments import create_payment, get_payment
from payment_service.db.models import Payment
from payment_service.db.session import get_db_session
from payment_service.domain import Currency, PaymentStatus

router = APIRouter(
    prefix="/api/v1/payments",
    tags=["payments"],
    dependencies=[Depends(require_api_key)],
)

SessionDependency = Annotated[AsyncSession, Depends(get_db_session)]
IdempotencyKeyDependency = Annotated[str, Depends(require_idempotency_key)]


def _accepted_response(payment: Payment) -> PaymentAccepted:
    return PaymentAccepted(
        payment_id=payment.id,
        status=PaymentStatus(payment.status),
        created_at=payment.created_at,
    )


def _detail_response(payment: Payment) -> PaymentDetail:
    return PaymentDetail(
        payment_id=payment.id,
        idempotency_key=payment.idempotency_key,
        amount=payment.amount,
        currency=Currency(payment.currency),
        description=payment.description,
        metadata_=payment.payment_metadata,
        webhook_url=payment.webhook_url,
        status=PaymentStatus(payment.status),
        created_at=payment.created_at,
        processed_at=payment.processed_at,
        webhook_delivered_at=payment.webhook_delivered_at,
        webhook_attempts=payment.webhook_attempts,
        webhook_last_error=payment.webhook_last_error,
    )


@router.post(
    "",
    response_model=PaymentAccepted,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Accept a payment for asynchronous processing",
    responses={
        status.HTTP_202_ACCEPTED: {
            "description": "Accepted; replayed responses include Idempotent-Replayed: true",
            "headers": {
                "Idempotent-Replayed": {
                    "description": "True when the idempotency key replayed an existing payment",
                    "schema": {"type": "string", "enum": ["true"]},
                }
            },
        },
        status.HTTP_401_UNAUTHORIZED: {"description": "Missing or invalid API key"},
        status.HTTP_409_CONFLICT: {"description": "Idempotency key reused with another body"},
        status.HTTP_422_UNPROCESSABLE_CONTENT: {"description": "Invalid request or headers"},
    },
)
async def post_payment(
    request: PaymentCreate,
    response: Response,
    idempotency_key: IdempotencyKeyDependency,
    session: SessionDependency,
) -> PaymentAccepted:
    result = await create_payment(session, request, idempotency_key)
    if result.replayed:
        response.headers["Idempotent-Replayed"] = "true"
    return _accepted_response(result.payment)


@router.get(
    "/{payment_id}",
    response_model=PaymentDetail,
    summary="Get the current payment state",
    responses={
        status.HTTP_401_UNAUTHORIZED: {"description": "Missing or invalid API key"},
        status.HTTP_404_NOT_FOUND: {"description": "Payment not found"},
    },
)
async def get_payment_detail(
    payment_id: UUID,
    session: SessionDependency,
) -> PaymentDetail:
    payment = await get_payment(session, payment_id)
    return _detail_response(payment)
