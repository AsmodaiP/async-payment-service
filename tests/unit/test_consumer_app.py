from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any, cast
from uuid import UUID

import pytest
from faststream.rabbit import RabbitMessage
from faststream.rabbit import TestRabbitBroker as RabbitTestBroker
from faststream.rabbit.message import RabbitMessage as StreamRabbitMessage

import payment_service.consumer.app as consumer_app
from payment_service.messaging.events import PaymentCreatedEvent


class Message:
    def __init__(self) -> None:
        self.acked = False
        self.nacked: list[bool] = []
        self.rejected: list[bool] = []

    async def ack(self) -> None:
        self.acked = True

    async def nack(self, *, requeue: bool) -> None:
        self.nacked.append(requeue)

    async def reject(self, *, requeue: bool) -> None:
        self.rejected.append(requeue)


def event() -> PaymentCreatedEvent:
    return PaymentCreatedEvent(
        event_id=UUID("10000000-0000-4000-8000-000000000001"),
        payment_id=UUID("00000000-0000-4000-8000-000000000001"),
        occurred_at=datetime(2026, 8, 1, tzinfo=UTC),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [b"{", b"[]", b"null", b"\xff", b'{"schema_version":99}'])
async def test_poison_messages_are_rejected_after_raw_decoding(
    payload: bytes,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rejections: list[bool] = []

    async def reject(_: Any, requeue: bool = False) -> None:
        rejections.append(requeue)

    monkeypatch.setattr(StreamRabbitMessage, "reject", reject)
    async with RabbitTestBroker(consumer_app.broker) as test_broker:
        await test_broker.publish(
            payload,
            exchange=consumer_app.topology.events_exchange,
            routing_key="payments.new",
            content_type="application/json",
        )

    assert rejections == [False]


@pytest.mark.asyncio
async def test_valid_raw_event_is_processed_before_ack(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    actions: list[str] = []

    async def process(parsed: PaymentCreatedEvent) -> None:
        assert parsed == event()
        actions.append("process")

    async def ack(_: Any, multiple: bool = False) -> None:
        actions.append("ack")

    monkeypatch.setattr(consumer_app.processor, "process", process)
    monkeypatch.setattr(StreamRabbitMessage, "ack", ack)
    async with RabbitTestBroker(consumer_app.broker) as test_broker:
        await test_broker.publish(
            event().model_dump_json().encode(),
            exchange=consumer_app.topology.events_exchange,
            routing_key="payments.new",
            content_type="application/json",
        )

    assert actions == ["process", "ack"]


@pytest.mark.asyncio
async def test_busy_claim_keeps_message_as_recovery_trigger(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    message = Message()
    monkeypatch.setattr(consumer_app.settings, "webhook_busy_retry_delay_seconds", 0.001)

    await consumer_app._defer_busy_webhook_claim(
        event(),
        cast(RabbitMessage, message),
    )

    assert message.nacked == [True]
    assert message.acked is False
    assert message.rejected == []


@pytest.mark.asyncio
async def test_retry_publish_timeout_requeues_original_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    message = Message()

    async def blocked_publish(*_: Any, **__: Any) -> None:
        await asyncio.Event().wait()

    monkeypatch.setattr(consumer_app.broker, "publish", blocked_publish)
    monkeypatch.setattr(consumer_app.settings, "rabbit_publish_timeout_seconds", 0.01)

    await consumer_app._retry_or_reject(
        event(),
        cast(RabbitMessage, message),
        "webhook",
        1,
    )

    assert message.nacked == [True]
    assert message.acked is False
    assert message.rejected == []
