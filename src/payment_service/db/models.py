from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    CHAR,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PostgreSQLUUID
from sqlalchemy.orm import Mapped, mapped_column

from payment_service.domain import Currency, PaymentStatus

from .base import Base

WEBHOOK_ERROR_MAX_LENGTH = 2_000
OUTBOX_ERROR_MAX_LENGTH = 2_000


def _sql_values(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{value}'" for value in values)


class Payment(Base):
    """A payment request and its asynchronous processing/delivery state."""

    __tablename__ = "payments"
    __table_args__ = (
        CheckConstraint("amount > 0", name="amount_positive"),
        CheckConstraint(
            f"currency IN ({_sql_values(tuple(currency.value for currency in Currency))})",
            name="currency_valid",
        ),
        CheckConstraint(
            f"status IN ({_sql_values(tuple(status.value for status in PaymentStatus))})",
            name="status_valid",
        ),
        CheckConstraint(
            "webhook_attempts BETWEEN 0 AND 3",
            name="webhook_attempts_range",
        ),
        UniqueConstraint("idempotency_key", name="uq_payments_idempotency_key"),
    )

    id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    idempotency_key: Mapped[str] = mapped_column(String(255), nullable=False)
    request_fingerprint: Mapped[str] = mapped_column(CHAR(64), nullable=False)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    status: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        default=PaymentStatus.PENDING.value,
        server_default=text(f"'{PaymentStatus.PENDING.value}'"),
    )
    description: Mapped[str | None] = mapped_column(String(500), nullable=True)
    payment_metadata: Mapped[dict[str, Any]] = mapped_column(
        "metadata",
        JSONB,
        nullable=False,
        default=dict,
        server_default=text("'{}'::jsonb"),
    )
    webhook_url: Mapped[str] = mapped_column(String(2048), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    processed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    webhook_delivered_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    webhook_attempts: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default=text("0"),
    )
    webhook_lock_token: Mapped[UUID | None] = mapped_column(
        PostgreSQLUUID(as_uuid=True),
        nullable=True,
    )
    webhook_locked_until: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    webhook_last_error: Mapped[str | None] = mapped_column(
        String(WEBHOOK_ERROR_MAX_LENGTH),
        nullable=True,
    )


class OutboxEvent(Base):
    """An event atomically persisted with its aggregate update."""

    __tablename__ = "outbox"
    __table_args__ = (
        CheckConstraint("schema_version > 0", name="schema_version_positive"),
        CheckConstraint("publish_attempts >= 0", name="publish_attempts_nonnegative"),
        UniqueConstraint(
            "aggregate_id",
            "event_type",
            name="uq_outbox_aggregate_id_event_type",
        ),
        Index(
            "ix_outbox_unpublished_polling",
            "available_at",
            "created_at",
            postgresql_where=text("published_at IS NULL"),
        ),
    )

    id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    aggregate_id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True),
        ForeignKey(
            "payments.id",
            name="fk_outbox_aggregate_id_payments",
            ondelete="CASCADE",
        ),
        nullable=False,
    )
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    schema_version: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=1,
        server_default=text("1"),
    )
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    published_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    publish_attempts: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default=text("0"),
    )
    last_error: Mapped[str | None] = mapped_column(
        String(OUTBOX_ERROR_MAX_LENGTH),
        nullable=True,
    )
    locked_by: Mapped[str | None] = mapped_column(String(255), nullable=True)
    locked_until: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
