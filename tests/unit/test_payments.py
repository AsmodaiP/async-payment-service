from __future__ import annotations

from decimal import Decimal

import pytest
from pydantic import ValidationError

from payment_service.api.schemas import PaymentCreate
from payment_service.application.payments import payment_request_fingerprint


def request(amount: str, *, metadata: dict[str, object] | None = None) -> PaymentCreate:
    return PaymentCreate.model_validate(
        {
            "amount": amount,
            "currency": "RUB",
            "description": "Заказ №42",
            "metadata": metadata or {"customer": 7, "tags": ["a", "b"]},
            "webhook_url": "https://merchant.example/hook",
        }
    )


def test_fingerprint_is_semantic_and_decimal_format_independent() -> None:
    assert payment_request_fingerprint(request("10.20")) == payment_request_fingerprint(
        request("10.2")
    )
    assert payment_request_fingerprint(request("10.20")) != payment_request_fingerprint(
        request("10.21")
    )


def test_integer_decimal_is_canonicalized() -> None:
    model = request("10")
    assert model.amount == Decimal("10")
    assert payment_request_fingerprint(model) == payment_request_fingerprint(request("10.00"))


def test_metadata_size_and_depth_are_bounded() -> None:
    with pytest.raises(ValidationError, match="metadata must not exceed"):
        request(
            "10.00",
            metadata={f"payload_{index}": "x" * 1_800 for index in range(10)},
        )

    nested: dict[str, object] = {"value": "ok"}
    for _ in range(10):
        nested = {"child": nested}
    with pytest.raises(ValidationError, match="nesting"):
        request("10.00", metadata=nested)
