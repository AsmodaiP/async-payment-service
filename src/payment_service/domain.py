from __future__ import annotations

from enum import StrEnum


class Currency(StrEnum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentStatus(StrEnum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class IdempotencyConflictError(Exception):
    """An idempotency key was reused with a different normalized request."""


class PaymentNotFoundError(Exception):
    """A payment does not exist."""


class RetryableProcessingError(Exception):
    """An infrastructure failure may succeed on a later delivery."""


class PermanentProcessingError(Exception):
    """An invalid event must not be retried."""
