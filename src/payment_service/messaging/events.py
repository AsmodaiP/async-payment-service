from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class PaymentCreatedEvent(BaseModel):
    """Small, versioned event envelope; payment data remains canonical in PostgreSQL."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    event_id: UUID
    event_type: Literal["payment.created.v1"] = "payment.created.v1"
    schema_version: Literal[1] = 1
    payment_id: UUID
    occurred_at: datetime
