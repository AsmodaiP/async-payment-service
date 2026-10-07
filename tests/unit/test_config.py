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
    with pytest.raises(ValidationError, match="must exceed"):
        Settings(webhook_timeout_seconds=10, webhook_lease_seconds=10)


def test_outbox_lease_must_outlive_publish_timeout() -> None:
    with pytest.raises(ValidationError, match="OUTBOX_LEASE_SECONDS must exceed"):
        Settings(rabbit_publish_timeout_seconds=10, outbox_lease_seconds=10)


def test_webhook_busy_retry_must_be_shorter_than_lease() -> None:
    with pytest.raises(ValidationError, match="must be shorter"):
        Settings(webhook_busy_retry_delay_seconds=30, webhook_lease_seconds=30)


def test_production_rejects_local_security_defaults() -> None:
    with pytest.raises(ValidationError, match="API_KEY must be a non-default"):
        Settings(environment="production")

    with pytest.raises(ValidationError, match="HTTPS webhooks are required"):
        Settings(
            environment="production",
            api_key="a-production-key-with-sufficient-entropy",
            require_https_webhooks=False,
        )


def test_production_rejects_default_infrastructure_credentials() -> None:
    safe_key = "a-production-key-with-sufficient-entropy"
    with pytest.raises(ValidationError, match="default database credentials"):
        Settings(environment="production", api_key=safe_key)

    with pytest.raises(ValidationError, match="default RabbitMQ credentials"):
        Settings(
            environment="production",
            api_key=safe_key,
            database_url="postgresql+asyncpg://service:secret@postgres:5432/payments",
        )


def test_environment_is_an_explicit_enum() -> None:
    with pytest.raises(ValidationError):
        Settings(environment="prod")


def test_production_requires_webhook_signing_secret() -> None:
    def production(secret: str | None) -> Settings:
        return Settings(
            environment="production",
            api_key="a-production-key-with-sufficient-entropy",
            database_url="postgresql+asyncpg://service:secret@postgres:5432/payments",
            rabbitmq_url="amqp://service:secret@rabbitmq:5672/",
            webhook_signing_secret=secret,
        )

    with pytest.raises(ValidationError, match="WEBHOOK_SIGNING_SECRET"):
        production(None)
    with pytest.raises(ValidationError, match="WEBHOOK_SIGNING_SECRET"):
        production("short")
    assert production("w" * 32).webhook_signing_secret is not None
