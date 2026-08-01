from __future__ import annotations

from functools import lru_cache

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration shared by the API, outbox relay, and consumer."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    service_name: str = "payment-service"
    environment: str = "development"
    log_level: str = "INFO"

    api_key: SecretStr = SecretStr("local-development-key")
    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    rabbitmq_url: SecretStr = SecretStr("amqp://payments:payments@localhost:5672/")

    outbox_poll_interval_seconds: float = Field(default=0.5, gt=0)
    outbox_batch_size: int = Field(default=50, ge=1, le=1_000)
    outbox_lease_seconds: int = Field(default=30, ge=5, le=300)
    rabbit_publish_timeout_seconds: float = Field(default=10.0, gt=0, le=60)

    payment_min_delay_seconds: float = Field(default=2.0, ge=0)
    payment_max_delay_seconds: float = Field(default=5.0, ge=0)
    payment_success_rate: float = Field(default=0.9, ge=0, le=1)

    max_delivery_attempts: int = Field(default=3, ge=3, le=3)
    retry_base_delay_seconds: int = Field(default=2, ge=1, le=3_600)
    webhook_timeout_seconds: float = Field(default=5.0, gt=0, le=60)
    webhook_lease_seconds: int = Field(default=30, ge=5, le=300)
    require_https_webhooks: bool = True
    allow_private_webhooks: bool = False

    @model_validator(mode="after")
    def validate_delay_range(self) -> Settings:
        if self.payment_max_delay_seconds < self.payment_min_delay_seconds:
            raise ValueError("PAYMENT_MAX_DELAY_SECONDS must not be less than the minimum")
        if self.webhook_lease_seconds <= self.webhook_timeout_seconds:
            raise ValueError("WEBHOOK_LEASE_SECONDS must be greater than WEBHOOK_TIMEOUT_SECONDS")
        if self.environment.lower() in {"prod", "production"}:
            if self.api_key.get_secret_value() in {"change-me", "local-development-key"}:
                raise ValueError("API_KEY must be replaced in production")
            if not self.require_https_webhooks:
                raise ValueError("HTTPS webhooks are required in production")
            if self.allow_private_webhooks:
                raise ValueError("private webhook targets must stay disabled in production")
        return self


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
