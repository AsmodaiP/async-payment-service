from __future__ import annotations

from dataclasses import dataclass

from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from payment_service.config import Settings

EVENTS_EXCHANGE_NAME = "payments.events"
RETRY_EXCHANGE_NAME = "payments.retry"
DEAD_LETTER_EXCHANGE_NAME = "payments.dlx"
NEW_PAYMENT_ROUTING_KEY = "payments.new"
DEAD_PAYMENT_ROUTING_KEY = "payments.dead"


@dataclass(frozen=True, slots=True)
class RabbitTopology:
    events_exchange: RabbitExchange
    retry_exchange: RabbitExchange
    dead_letter_exchange: RabbitExchange
    new_payments_queue: RabbitQueue
    retry_queues: tuple[RabbitQueue, ...]
    dead_letter_queue: RabbitQueue

    def retry_queue_for_attempt(self, next_attempt: int) -> RabbitQueue:
        """Return the delay queue that precedes ``next_attempt`` (attempts start at one)."""

        index = next_attempt - 2
        if index < 0 or index >= len(self.retry_queues):
            raise ValueError(f"no retry queue configured for attempt {next_attempt}")
        return self.retry_queues[index]


def build_topology(settings: Settings) -> RabbitTopology:
    events = RabbitExchange(EVENTS_EXCHANGE_NAME, type=ExchangeType.DIRECT, durable=True)
    retry = RabbitExchange(RETRY_EXCHANGE_NAME, type=ExchangeType.DIRECT, durable=True)
    dead = RabbitExchange(DEAD_LETTER_EXCHANGE_NAME, type=ExchangeType.DIRECT, durable=True)

    main_queue = RabbitQueue(
        "payments.new",
        durable=True,
        routing_key=NEW_PAYMENT_ROUTING_KEY,
        arguments={
            "x-dead-letter-exchange": DEAD_LETTER_EXCHANGE_NAME,
            "x-dead-letter-routing-key": DEAD_PAYMENT_ROUTING_KEY,
        },
    )
    retry_queues = tuple(
        RabbitQueue(
            f"payments.retry.{attempt}",
            durable=True,
            routing_key=f"payments.retry.{attempt}",
            arguments={
                "x-message-ttl": settings.retry_base_delay_seconds * (2 ** (attempt - 2)) * 1_000,
                "x-dead-letter-exchange": EVENTS_EXCHANGE_NAME,
                "x-dead-letter-routing-key": NEW_PAYMENT_ROUTING_KEY,
            },
        )
        for attempt in range(2, settings.max_delivery_attempts + 1)
    )
    dead_queue = RabbitQueue(
        "payments.dlq",
        durable=True,
        routing_key=DEAD_PAYMENT_ROUTING_KEY,
    )
    return RabbitTopology(events, retry, dead, main_queue, retry_queues, dead_queue)


async def declare_topology(broker: RabbitBroker, topology: RabbitTopology) -> None:
    """Idempotently declare and bind every durable exchange and queue."""

    events = await broker.declare_exchange(topology.events_exchange)
    retry = await broker.declare_exchange(topology.retry_exchange)
    dead = await broker.declare_exchange(topology.dead_letter_exchange)

    main_queue = await broker.declare_queue(topology.new_payments_queue)
    await main_queue.bind(events, routing_key=topology.new_payments_queue.routing_key)

    for queue_schema in topology.retry_queues:
        queue = await broker.declare_queue(queue_schema)
        await queue.bind(retry, routing_key=queue_schema.routing_key)

    dead_queue = await broker.declare_queue(topology.dead_letter_queue)
    await dead_queue.bind(dead, routing_key=topology.dead_letter_queue.routing_key)
