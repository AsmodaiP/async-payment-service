from __future__ import annotations

import pytest

from payment_service.config import Settings
from payment_service.messaging.topology import build_topology


def test_retry_topology_has_exponential_ttl_and_final_dlx() -> None:
    topology = build_topology(Settings(retry_base_delay_seconds=3, max_delivery_attempts=3))

    assert topology.new_payments_queue.name == "payments.new"
    assert topology.new_payments_queue.arguments["x-dead-letter-exchange"] == "payments.dlx"
    assert [queue.arguments["x-message-ttl"] for queue in topology.retry_queues] == [
        3_000,
        6_000,
    ]
    assert topology.dead_letter_queue.name == "payments.dlq"


def test_retry_queue_maps_to_the_next_attempt() -> None:
    topology = build_topology(Settings())
    assert topology.retry_queue_for_attempt(2).name == "payments.retry.2"
    assert topology.retry_queue_for_attempt(3).name == "payments.retry.3"

    with pytest.raises(ValueError, match="no retry queue"):
        topology.retry_queue_for_attempt(1)


def test_dead_letter_transfers_wait_for_confirmations() -> None:
    topology = build_topology(Settings())
    for queue in (topology.new_payments_queue, *topology.retry_queues):
        assert queue.arguments["x-queue-type"] == "quorum"
        assert queue.arguments["x-dead-letter-strategy"] == "at-least-once"
        assert queue.arguments["x-overflow"] == "reject-publish"
        # A busy webhook lease must not exhaust RabbitMQ's separate delivery limit.
        assert queue.arguments["x-delivery-limit"] == -1
    assert topology.dead_letter_queue.arguments["x-queue-type"] == "quorum"
