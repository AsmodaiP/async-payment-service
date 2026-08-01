from __future__ import annotations

import json
import math
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any
from uuid import UUID

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    HttpUrl,
    JsonValue,
    field_validator,
)

from payment_service.domain import Currency, PaymentStatus


def _reject_non_decimal_number(value: Any) -> Any:
    """Reject lossy JSON floats before Pydantic coerces them to ``Decimal``."""

    if isinstance(value, (float, bool)):
        raise ValueError("amount must be a JSON string or integer, not a floating-point value")
    return value


Amount = Annotated[
    Decimal,
    BeforeValidator(_reject_non_decimal_number),
    Field(gt=0, max_digits=18, decimal_places=2),
]
WebhookUrl = Annotated[HttpUrl, Field(max_length=2_048)]

MAX_METADATA_BYTES = 16 * 1024
MAX_METADATA_DEPTH = 8
MAX_METADATA_ITEMS = 100
MAX_METADATA_STRING_LENGTH = 2_000


def _validate_json_budget(value: JsonValue, *, depth: int = 0) -> JsonValue:
    if depth > MAX_METADATA_DEPTH:
        raise ValueError(f"metadata nesting must not exceed {MAX_METADATA_DEPTH} levels")
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("metadata must not contain NaN or Infinity")
    if isinstance(value, str) and len(value) > MAX_METADATA_STRING_LENGTH:
        raise ValueError(f"metadata strings must not exceed {MAX_METADATA_STRING_LENGTH} chars")
    if isinstance(value, list):
        if len(value) > MAX_METADATA_ITEMS:
            raise ValueError(f"metadata arrays must not exceed {MAX_METADATA_ITEMS} items")
        for item in value:
            _validate_json_budget(item, depth=depth + 1)
    elif isinstance(value, dict):
        if len(value) > MAX_METADATA_ITEMS:
            raise ValueError(f"metadata objects must not exceed {MAX_METADATA_ITEMS} keys")
        for key, item in value.items():
            if len(key) > MAX_METADATA_STRING_LENGTH:
                raise ValueError(
                    f"metadata keys must not exceed {MAX_METADATA_STRING_LENGTH} chars"
                )
            _validate_json_budget(item, depth=depth + 1)
    return value


class PaymentCreate(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "examples": [
                {
                    "amount": "1499.90",
                    "currency": "RUB",
                    "description": "Order #A-1042",
                    "metadata": {"order_id": "A-1042", "customer_id": 321},
                    "webhook_url": "https://merchant.example/webhooks/payments",
                }
            ]
        },
    )

    amount: Amount
    currency: Currency
    description: Annotated[str | None, Field(max_length=500)] = None
    metadata_: dict[str, JsonValue] = Field(default_factory=dict, alias="metadata")
    webhook_url: WebhookUrl

    @field_validator("metadata_")
    @classmethod
    def validate_metadata(cls, value: dict[str, JsonValue]) -> dict[str, JsonValue]:
        _validate_json_budget(value)
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode()
        if len(encoded) > MAX_METADATA_BYTES:
            raise ValueError(f"metadata must not exceed {MAX_METADATA_BYTES} UTF-8 bytes")
        return value


class PaymentAccepted(BaseModel):
    model_config = ConfigDict(
        from_attributes=True,
        json_schema_extra={
            "examples": [
                {
                    "payment_id": "98ce20cc-e407-4881-87ea-c247218668d8",
                    "status": "pending",
                    "created_at": "2026-08-01T12:00:00Z",
                }
            ]
        },
    )

    payment_id: UUID
    status: PaymentStatus
    created_at: datetime


class PaymentDetail(BaseModel):
    model_config = ConfigDict(
        from_attributes=True,
        populate_by_name=True,
        json_schema_extra={
            "examples": [
                {
                    "payment_id": "98ce20cc-e407-4881-87ea-c247218668d8",
                    "idempotency_key": "merchant-order-A-1042",
                    "amount": "1499.90",
                    "currency": "RUB",
                    "description": "Order #A-1042",
                    "metadata": {"order_id": "A-1042"},
                    "webhook_url": "https://merchant.example/webhooks/payments",
                    "status": "succeeded",
                    "created_at": "2026-08-01T12:00:00Z",
                    "processed_at": "2026-08-01T12:00:03Z",
                    "webhook_delivered_at": "2026-08-01T12:00:04Z",
                    "webhook_attempts": 1,
                    "webhook_last_error": None,
                }
            ]
        },
    )

    payment_id: UUID
    idempotency_key: str
    amount: Decimal
    currency: Currency
    description: str | None
    metadata_: dict[str, JsonValue] = Field(alias="metadata")
    webhook_url: HttpUrl
    status: PaymentStatus
    created_at: datetime
    processed_at: datetime | None
    webhook_delivered_at: datetime | None
    webhook_attempts: int
    webhook_last_error: str | None


class HealthResponse(BaseModel):
    status: str
