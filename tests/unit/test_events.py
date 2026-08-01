from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError

from payment_service.messaging.events import PaymentCreatedEvent


def test_payment_event_is_versioned_and_forbids_unknown_fields() -> None:
    event = {
        "event_id": str(uuid4()),
        "event_type": "payment.created.v1",
        "schema_version": 1,
        "payment_id": str(uuid4()),
        "occurred_at": datetime.now(UTC).isoformat(),
        "unexpected": True,
    }
    with pytest.raises(ValidationError):
        PaymentCreatedEvent.model_validate(event)
