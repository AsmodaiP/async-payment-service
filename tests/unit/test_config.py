from __future__ import annotations

import pytest
from pydantic import ValidationError

from payment_service.config import Settings


def test_payment_delay_range_is_validated() -> None:
    with pytest.raises(ValidationError, match="must not be less"):
        Settings(payment_min_delay_seconds=5, payment_max_delay_seconds=2)


def test_three_delivery_attempts_are_required() -> None:
    settings = Settings(max_delivery_attempts=3)
    assert settings.max_delivery_attempts == 3

    with pytest.raises(ValidationError):
        Settings(max_delivery_attempts=4)


def test_webhook_lease_must_outlive_http_timeout() -> None:
    with pytest.raises(ValidationError, match="must be greater"):
        Settings(webhook_timeout_seconds=10, webhook_lease_seconds=10)


def test_production_rejects_local_security_defaults() -> None:
    with pytest.raises(ValidationError, match="API_KEY must be replaced"):
        Settings(environment="production")

    with pytest.raises(ValidationError, match="HTTPS webhooks are required"):
        Settings(
            environment="production",
            api_key="a-production-key-with-sufficient-entropy",
            require_https_webhooks=False,
        )
