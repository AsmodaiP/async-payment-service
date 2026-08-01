from __future__ import annotations

import asyncio
import base64
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import UUID, uuid4

import asyncpg  # type: ignore[import-untyped]

API = os.environ.get("E2E_API_URL", "http://127.0.0.1:8000")
SINK = os.environ.get("E2E_SINK_URL", "http://127.0.0.1:18080")
RABBIT = os.environ.get("E2E_RABBIT_URL", "http://127.0.0.1:15672")
DATABASE = os.environ.get(
    "E2E_DATABASE_URL",
    "postgresql://payments:payments@127.0.0.1:15432/payments",
)
API_KEY = os.environ["API_KEY"]


def request_json(
    url: str,
    *,
    method: str = "GET",
    body: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    expected: int = 200,
) -> tuple[dict[str, Any], dict[str, str]]:
    data = json.dumps(body).encode() if body is not None else None
    request = Request(url, data=data, method=method, headers=headers or {})
    if data is not None:
        request.add_header("Content-Type", "application/json")
    try:
        with urlopen(request, timeout=5) as response:
            payload = json.loads(response.read())
            status = response.status
            response_headers = {key.lower(): value for key, value in response.headers.items()}
    except HTTPError as exc:
        payload = json.loads(exc.read())
        status = exc.code
        response_headers = {key.lower(): value for key, value in exc.headers.items()}
    assert status == expected, (status, payload)
    return payload, response_headers


def wait_for_payment(payment_id: str, predicate: Any, timeout: float = 20) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        payment, _ = request_json(
            f"{API}/api/v1/payments/{payment_id}",
            headers={"X-API-Key": API_KEY},
        )
        if predicate(payment):
            return payment
        time.sleep(0.2)
    raise TimeoutError(f"payment {payment_id} did not reach the expected state")


def create(
    key: str, webhook_path: str, amount: str = "10.25"
) -> tuple[dict[str, Any], dict[str, str]]:
    return request_json(
        f"{API}/api/v1/payments",
        method="POST",
        headers={"X-API-Key": API_KEY, "Idempotency-Key": key},
        body={
            "amount": amount,
            "currency": "RUB",
            "description": "Docker e2e payment",
            "metadata": {"scenario": webhook_path},
            "webhook_url": f"http://webhook-sink:8080{webhook_path}",
        },
        expected=202,
    )


def rabbit_queue(name: str) -> dict[str, Any]:
    credentials = base64.b64encode(b"payments:payments").decode()
    payload, _ = request_json(
        f"{RABBIT}/api/queues/%2F/{name}",
        headers={"Authorization": f"Basic {credentials}"},
    )
    return payload


async def insert_payment_with_live_webhook_claim() -> tuple[UUID, UUID, datetime]:
    payment_id = uuid4()
    event_id = uuid4()
    processed_at = datetime.now(UTC)
    connection = await asyncpg.connect(DATABASE)
    try:
        await connection.execute(
            """
            INSERT INTO payments (
                id,
                idempotency_key,
                request_fingerprint,
                amount,
                currency,
                status,
                description,
                metadata,
                webhook_url,
                processed_at,
                webhook_attempts,
                webhook_lock_token,
                webhook_locked_until
            )
            VALUES (
                $1, $2, $3, $4, 'RUB', 'succeeded', $5, $6::jsonb, $7, $8, 0, $9,
                clock_timestamp() + INTERVAL '2 seconds'
            )
            """,
            payment_id,
            f"e2e-live-claim-{payment_id}",
            "b" * 64,
            Decimal("11.50"),
            "Simulated crash after webhook claim",
            json.dumps({"scenario": "live-claim-redelivery"}),
            "http://webhook-sink:8080/hooks/succeed",
            processed_at,
            uuid4(),
        )
    finally:
        await connection.close()
    return payment_id, event_id, processed_at


def publish_payment_event(event_id: UUID, payment_id: UUID, occurred_at: datetime) -> None:
    credentials = base64.b64encode(b"payments:payments").decode()
    result, _ = request_json(
        f"{RABBIT}/api/exchanges/%2F/payments.events/publish",
        method="POST",
        headers={"Authorization": f"Basic {credentials}"},
        body={
            "properties": {
                "delivery_mode": 2,
                "message_id": str(event_id),
                "correlation_id": str(payment_id),
                "type": "payment.created.v1",
                "headers": {"schema-version": 1, "x-attempt": 1},
            },
            "routing_key": "payments.new",
            "payload": json.dumps(
                {
                    "event_id": str(event_id),
                    "event_type": "payment.created.v1",
                    "schema_version": 1,
                    "payment_id": str(payment_id),
                    "occurred_at": occurred_at.isoformat(),
                }
            ),
            "payload_encoding": "string",
        },
    )
    assert result["routed"] is True


def main() -> None:
    accepted, _ = create("e2e-eventual-success", "/hooks/fail-twice")
    payment_id = str(accepted["payment_id"])
    delivered = wait_for_payment(payment_id, lambda item: item["webhook_delivered_at"] is not None)
    assert delivered["status"] in {"succeeded", "failed"}
    assert delivered["webhook_attempts"] == 3

    replay, replay_headers = create("e2e-eventual-success", "/hooks/fail-twice")
    assert replay["payment_id"] == payment_id
    assert replay_headers.get("idempotent-replayed") == "true"

    conflict_body = {
        "amount": "99.00",
        "currency": "RUB",
        "description": "Docker e2e payment",
        "metadata": {"scenario": "/hooks/fail-twice"},
        "webhook_url": "http://webhook-sink:8080/hooks/fail-twice",
    }
    request_json(
        f"{API}/api/v1/payments",
        method="POST",
        headers={"X-API-Key": API_KEY, "Idempotency-Key": "e2e-eventual-success"},
        body=conflict_body,
        expected=409,
    )

    with ThreadPoolExecutor(max_workers=8) as executor:
        concurrent = list(
            executor.map(
                lambda _: create("e2e-concurrent-key", "/hooks/succeed"),
                range(8),
            )
        )
    concurrent_ids = {item[0]["payment_id"] for item in concurrent}
    assert len(concurrent_ids) == 1
    concurrent_id = str(concurrent[0][0]["payment_id"])
    wait_for_payment(concurrent_id, lambda item: item["webhook_delivered_at"] is not None)

    locked_id, locked_event_id, locked_processed_at = asyncio.run(
        insert_payment_with_live_webhook_claim()
    )
    publish_payment_event(locked_event_id, locked_id, locked_processed_at)
    recovered = wait_for_payment(
        str(locked_id),
        lambda item: item["webhook_delivered_at"] is not None,
    )
    assert recovered["webhook_attempts"] == 1

    doomed, _ = create("e2e-dead-letter", "/hooks/always-fail")
    doomed_id = str(doomed["payment_id"])
    exhausted = wait_for_payment(
        doomed_id,
        lambda item: item["webhook_attempts"] == 3 and item["webhook_last_error"] is not None,
    )
    assert exhausted["webhook_delivered_at"] is None

    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if rabbit_queue("payments.dlq")["messages"] >= 1:
            break
        time.sleep(0.25)
    else:
        raise TimeoutError("exhausted event was not routed to payments.dlq")

    sink, _ = request_json(f"{SINK}/events")
    successful_attempts = [
        event for event in sink["events"] if event["body"]["payment_id"] == payment_id
    ]
    assert [event["attempt"] for event in successful_attempts] == ["1", "2", "3"]
    concurrent_deliveries = [
        event for event in sink["events"] if event["body"]["payment_id"] == concurrent_id
    ]
    assert len(concurrent_deliveries) == 1
    recovered_deliveries = [
        event for event in sink["events"] if event["body"]["payment_id"] == str(locked_id)
    ]
    assert len(recovered_deliveries) == 1
    print("E2E passed: outbox, idempotency, retries, webhook recovery, delivery, and DLQ")


if __name__ == "__main__":
    main()
