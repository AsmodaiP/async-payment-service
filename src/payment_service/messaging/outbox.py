from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass
from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

import structlog
from faststream.rabbit import Channel, RabbitBroker
from pamqp.commands import Basic
from sqlalchemy import func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.config import Settings
from payment_service.db.models import OutboxEvent
from payment_service.messaging.topology import (
    NEW_PAYMENT_ROUTING_KEY,
    RabbitTopology,
    declare_topology,
)

logger = structlog.get_logger(__name__)


@dataclass(frozen=True, slots=True)
class ClaimedEvent:
    id: UUID
    aggregate_id: UUID
    event_type: str
    schema_version: int
    payload: dict[str, Any]
    publish_attempts: int


class OutboxRelay:
    """Lease and publish outbox rows without holding a DB transaction over the network."""

    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
        topology: RabbitTopology,
    ) -> None:
        self._settings = settings
        self._session_factory = session_factory
        self._topology = topology
        self._worker_id = uuid4().hex
        self._stop = asyncio.Event()
        self._broker = RabbitBroker(
            settings.rabbitmq_url.get_secret_value(),
            fail_fast=False,
            reconnect_interval=2.0,
            default_channel=Channel(publisher_confirms=True, on_return_raises=True),
        )

    async def run(self) -> None:
        """Reconnect forever; RabbitMQ availability must not gate HTTP acceptance."""

        while not self._stop.is_set():
            try:
                async with self._broker:
                    await declare_topology(self._broker, self._topology)
                    logger.info("outbox_relay_connected", worker_id=self._worker_id)
                    await self._publish_loop()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("outbox_relay_connection_failed", worker_id=self._worker_id)
                await self._wait(2.0)

    async def stop(self) -> None:
        self._stop.set()

    async def _publish_loop(self) -> None:
        while not self._stop.is_set():
            claimed_count = 0
            for _ in range(self._settings.outbox_batch_size):
                event = await self._claim_next()
                if event is None:
                    break
                claimed_count += 1
                if self._stop.is_set():
                    return
                await self._publish_one(event)
            if claimed_count == 0:
                await self._wait(self._settings.outbox_poll_interval_seconds)

    async def _claim_next(self) -> ClaimedEvent | None:
        async with self._session_factory() as session, session.begin():
            row = await session.scalar(
                select(OutboxEvent)
                .where(
                    OutboxEvent.published_at.is_(None),
                    OutboxEvent.available_at <= func.clock_timestamp(),
                    or_(
                        OutboxEvent.locked_until.is_(None),
                        OutboxEvent.locked_until < func.clock_timestamp(),
                    ),
                )
                .order_by(OutboxEvent.created_at)
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            if row is None:
                return None
            publish_attempts = row.publish_attempts + 1
            await session.execute(
                update(OutboxEvent)
                .where(OutboxEvent.id == row.id)
                .values(
                    locked_by=self._worker_id,
                    locked_until=(
                        func.clock_timestamp()
                        + timedelta(seconds=self._settings.outbox_lease_seconds)
                    ),
                    publish_attempts=publish_attempts,
                )
            )
            return ClaimedEvent(
                id=row.id,
                aggregate_id=row.aggregate_id,
                event_type=row.event_type,
                schema_version=row.schema_version,
                payload=dict(row.payload),
                publish_attempts=publish_attempts,
            )

    async def _publish_one(self, event: ClaimedEvent) -> None:
        try:
            confirmation = await asyncio.wait_for(
                self._broker.publish(
                    event.payload,
                    exchange=self._topology.events_exchange,
                    routing_key=NEW_PAYMENT_ROUTING_KEY,
                    mandatory=True,
                    persist=True,
                    message_id=str(event.id),
                    correlation_id=str(event.aggregate_id),
                    message_type=event.event_type,
                    headers={"schema-version": event.schema_version},
                ),
                timeout=self._settings.rabbit_publish_timeout_seconds,
            )
            if not isinstance(confirmation, Basic.Ack):
                raise RuntimeError("RabbitMQ did not confirm the outbox publish")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self._release_failed(event, exc)
            logger.exception(
                "outbox_publish_failed",
                event_id=str(event.id),
                payment_id=str(event.aggregate_id),
                attempt=event.publish_attempts,
            )
            return

        async with self._session_factory() as session, session.begin():
            completed = await session.scalar(
                update(OutboxEvent)
                .where(
                    OutboxEvent.id == event.id,
                    OutboxEvent.locked_by == self._worker_id,
                    OutboxEvent.published_at.is_(None),
                )
                .values(
                    published_at=func.clock_timestamp(),
                    locked_by=None,
                    locked_until=None,
                    last_error=None,
                )
                .returning(OutboxEvent.id)
            )
        if completed is None:
            logger.warning(
                "outbox_lease_lost_after_publish",
                event_id=str(event.id),
                payment_id=str(event.aggregate_id),
                attempt=event.publish_attempts,
            )
            return
        logger.info(
            "outbox_published",
            event_id=str(event.id),
            payment_id=str(event.aggregate_id),
            attempt=event.publish_attempts,
        )

    async def _release_failed(self, event: ClaimedEvent, exc: Exception) -> None:
        ceiling = min(60.0, float(2 ** min(event.publish_attempts - 1, 6)))
        delay = ceiling + random.uniform(0, ceiling * 0.2)
        async with self._session_factory() as session, session.begin():
            await session.execute(
                update(OutboxEvent)
                .where(
                    OutboxEvent.id == event.id,
                    OutboxEvent.locked_by == self._worker_id,
                    OutboxEvent.published_at.is_(None),
                )
                .values(
                    available_at=func.clock_timestamp() + timedelta(seconds=delay),
                    locked_by=None,
                    locked_until=None,
                    last_error=str(exc)[:1_000],
                )
            )

    async def _wait(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)
        except TimeoutError:
            pass
