from __future__ import annotations

import asyncio
from typing import Any

import structlog
from faststream import AckPolicy, FastStream
from faststream.rabbit import Channel, RabbitBroker, RabbitMessage
from pamqp.commands import Basic
from pydantic import ValidationError

from payment_service.config import get_settings
from payment_service.consumer.gateway import GatewaySimulator
from payment_service.consumer.processor import (
    ExhaustedDeliveryError,
    PaymentProcessor,
    RetryDeliveryError,
    WebhookClaimBusyError,
)
from payment_service.consumer.webhook import WebhookClient
from payment_service.db.session import AsyncSessionFactory
from payment_service.domain import PaymentNotFoundError, PermanentProcessingError
from payment_service.logging import configure_logging
from payment_service.messaging.events import PaymentCreatedEvent
from payment_service.messaging.topology import build_topology, declare_topology

settings = get_settings()
configure_logging(settings.log_level)
logger = structlog.get_logger(__name__)
topology = build_topology(settings)
broker = RabbitBroker(
    settings.rabbitmq_url.get_secret_value(),
    fail_fast=True,
    reconnect_interval=2.0,
    graceful_timeout=30.0,
    default_channel=Channel(
        prefetch_count=1,
        publisher_confirms=True,
        on_return_raises=True,
    ),
)
app = FastStream(broker)
webhook_client = WebhookClient(settings)
processor = PaymentProcessor(
    settings,
    AsyncSessionFactory,
    GatewaySimulator(settings),
    webhook_client,
)


@app.after_startup
async def setup_topology() -> None:
    await declare_topology(broker, topology)
    logger.info("consumer_started")


@app.after_shutdown
async def close_http_client() -> None:
    await webhook_client.close()
    logger.info("consumer_stopped")


def raw_event_body(message: RabbitMessage) -> bytes:
    # Decode inside the handler: malformed JSON must reach its reject/DLQ path
    # instead of failing in FastStream before manual acknowledgement is possible.
    return bytes(message.body)


@broker.subscriber(
    topology.new_payments_queue,
    topology.events_exchange,
    ack_policy=AckPolicy.MANUAL,
    decoder=raw_event_body,
)
async def handle_payment(raw_event: bytes, message: RabbitMessage) -> None:
    try:
        event = PaymentCreatedEvent.model_validate_json(raw_event)
    except ValidationError as exc:
        logger.warning(
            "poison_event_rejected",
            message_id=message.message_id,
            validation_errors=[
                {"location": list(error["loc"]), "type": error["type"]}
                for error in exc.errors(include_input=False)
            ],
        )
        await message.reject(requeue=False)
        return

    headers = message.headers or {}
    processing_attempt = _header_attempt(headers)
    try:
        await processor.process(event)
    except (PaymentNotFoundError, PermanentProcessingError) as exc:
        logger.warning(
            "permanent_processing_failure",
            event_id=str(event.event_id),
            payment_id=str(event.payment_id),
            reason=str(exc),
        )
        await message.reject(requeue=False)
        return
    except ExhaustedDeliveryError as exc:
        logger.warning(
            "delivery_attempts_exhausted",
            event_id=str(event.event_id),
            payment_id=str(event.payment_id),
            phase=exc.phase,
            reason=exc.reason,
        )
        await message.reject(requeue=False)
        return
    except WebhookClaimBusyError:
        await _defer_busy_webhook_claim(event, message)
        return
    except RetryDeliveryError as exc:
        await _retry_or_reject(event, message, exc.phase, exc.attempt)
        return
    except asyncio.CancelledError:
        await message.nack(requeue=True)
        raise
    except Exception as exc:
        logger.exception(
            "unexpected_processing_failure",
            event_id=str(event.event_id),
            payment_id=str(event.payment_id),
            attempt=processing_attempt,
        )
        await _retry_or_reject(event, message, "processing", processing_attempt, exc)
        return

    await message.ack()
    logger.info(
        "payment_event_processed",
        event_id=str(event.event_id),
        payment_id=str(event.payment_id),
    )


def _header_attempt(headers: dict[str, Any]) -> int:
    try:
        attempt = int(headers.get("x-attempt", 1))
    except (TypeError, ValueError):
        return 1
    return max(1, attempt)


async def _defer_busy_webhook_claim(
    event: PaymentCreatedEvent,
    message: RabbitMessage,
) -> None:
    logger.info(
        "webhook_claim_busy_deferred",
        event_id=str(event.event_id),
        payment_id=str(event.payment_id),
        delay_seconds=settings.webhook_busy_retry_delay_seconds,
    )
    try:
        await asyncio.sleep(settings.webhook_busy_retry_delay_seconds)
    except asyncio.CancelledError:
        await message.nack(requeue=True)
        raise
    await message.nack(requeue=True)


async def _retry_or_reject(
    event: PaymentCreatedEvent,
    message: RabbitMessage,
    phase: str,
    attempt: int,
    cause: Exception | None = None,
) -> None:
    if attempt >= settings.max_delivery_attempts:
        logger.warning(
            "delivery_rejected_to_dlq",
            event_id=str(event.event_id),
            payment_id=str(event.payment_id),
            phase=phase,
            attempt=attempt,
            reason=str(cause) if cause else None,
        )
        await message.reject(requeue=False)
        return

    next_attempt = attempt + 1
    retry_queue = topology.retry_queue_for_attempt(next_attempt)
    try:
        confirmation = await asyncio.wait_for(
            broker.publish(
                event.model_dump(mode="json"),
                exchange=topology.retry_exchange,
                routing_key=retry_queue.routing_key,
                mandatory=True,
                persist=True,
                message_id=str(event.event_id),
                correlation_id=str(event.payment_id),
                message_type=event.event_type,
                headers={"x-attempt": next_attempt, "x-phase": phase},
            ),
            timeout=settings.rabbit_publish_timeout_seconds,
        )
        if not isinstance(confirmation, Basic.Ack):
            raise RuntimeError("RabbitMQ did not confirm retry publish")
    except asyncio.CancelledError:
        await message.nack(requeue=True)
        raise
    except Exception:
        logger.exception(
            "retry_publish_failed",
            event_id=str(event.event_id),
            payment_id=str(event.payment_id),
            phase=phase,
            attempt=attempt,
        )
        await message.nack(requeue=True)
        return

    await message.ack()
    logger.info(
        "delivery_retry_scheduled",
        event_id=str(event.event_id),
        payment_id=str(event.payment_id),
        phase=phase,
        next_attempt=next_attempt,
        queue=retry_queue.name,
    )


def main() -> None:
    asyncio.run(app.run())


if __name__ == "__main__":
    main()
