from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, cast
from uuid import UUID

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.api import routes
from payment_service.api.app import create_app
from payment_service.application.payments import CreatePaymentResult
from payment_service.config import Settings
from payment_service.db.models import Payment
from payment_service.db.session import get_db_session
from payment_service.domain import IdempotencyConflictError, PaymentNotFoundError

API_KEY = "test-api-key"
PAYMENT_ID = UUID("00000000-0000-4000-8000-000000000001")


def payment(*, status: str = "pending") -> Payment:
    return Payment(
        id=PAYMENT_ID,
        idempotency_key="order-42",
        request_fingerprint="a" * 64,
        amount=Decimal("10.25"),
        currency="RUB",
        status=status,
        description="Order 42",
        payment_metadata={"order_id": 42},
        webhook_url="https://merchant.example/hook",
        created_at=datetime(2026, 8, 1, tzinfo=UTC),
        processed_at=None,
        webhook_delivered_at=None,
        webhook_attempts=0,
        webhook_lock_token=None,
        webhook_locked_until=None,
        webhook_last_error=None,
    )


async def client() -> AsyncIterator[AsyncClient]:
    async def ready() -> bool:
        return True

    app = create_app(
        Settings(api_key=API_KEY),
        readiness_check=ready,
        start_outbox_relay=False,
    )

    async def fake_session() -> AsyncIterator[Any]:
        yield object()

    app.dependency_overrides[get_db_session] = fake_session
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as http:
        yield http


@pytest.fixture
async def http_client() -> AsyncIterator[AsyncClient]:
    async for value in client():
        yield value


@pytest.mark.asyncio
async def test_payment_endpoints_require_api_key(http_client: AsyncClient) -> None:
    response = await http_client.get(f"/api/v1/payments/{PAYMENT_ID}")
    assert response.status_code == 401
    assert response.json() == {"detail": "Invalid or missing API key"}


@pytest.mark.asyncio
async def test_create_payment_normalizes_key_and_marks_replay(
    http_client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_key: str | None = None

    async def create(_: Any, __: Any, key: str) -> CreatePaymentResult:
        nonlocal captured_key
        captured_key = key
        return CreatePaymentResult(payment=payment(), replayed=True)

    monkeypatch.setattr(routes, "create_payment", create)
    response = await http_client.post(
        "/api/v1/payments",
        headers={"X-API-Key": API_KEY, "Idempotency-Key": "  order-42  "},
        json={
            "amount": "10.25",
            "currency": "RUB",
            "description": "Order 42",
            "metadata": {"order_id": 42},
            "webhook_url": "https://merchant.example/hook",
        },
    )

    assert response.status_code == 202
    assert response.headers["Idempotent-Replayed"] == "true"
    assert response.json()["payment_id"] == str(PAYMENT_ID)
    assert captured_key == "order-42"


@pytest.mark.asyncio
async def test_openapi_documents_idempotent_replay_header(http_client: AsyncClient) -> None:
    schema = (await http_client.get("/openapi.json", headers={"X-API-Key": API_KEY})).json()
    response = schema["paths"]["/api/v1/payments"]["post"]["responses"]["202"]
    assert "Idempotent-Replayed" in response["headers"]


@pytest.mark.asyncio
async def test_create_rejects_float_amount_and_blank_key(http_client: AsyncClient) -> None:
    headers = {"X-API-Key": API_KEY, "Idempotency-Key": "order-42"}
    payload = {
        "amount": 10.25,
        "currency": "RUB",
        "webhook_url": "https://merchant.example/hook",
    }
    assert (
        await http_client.post("/api/v1/payments", headers=headers, json=payload)
    ).status_code == 422

    payload["amount"] = "10.25"
    headers["Idempotency-Key"] = "   "
    assert (
        await http_client.post("/api/v1/payments", headers=headers, json=payload)
    ).status_code == 422


@pytest.mark.asyncio
async def test_idempotency_conflict_is_mapped_to_409(
    http_client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def create(_: Any, __: Any, ___: str) -> CreatePaymentResult:
        raise IdempotencyConflictError("key belongs to another request")

    monkeypatch.setattr(routes, "create_payment", create)
    response = await http_client.post(
        "/api/v1/payments",
        headers={"X-API-Key": API_KEY, "Idempotency-Key": "order-42"},
        json={
            "amount": "10.25",
            "currency": "RUB",
            "webhook_url": "https://merchant.example/hook",
        },
    )
    assert response.status_code == 409
    assert response.json() == {"detail": "key belongs to another request"}


@pytest.mark.asyncio
async def test_get_payment_serializes_metadata_alias(
    http_client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def get(_: Any, __: UUID) -> Payment:
        return payment(status="succeeded")

    monkeypatch.setattr(routes, "get_payment", get)
    response = await http_client.get(
        f"/api/v1/payments/{PAYMENT_ID}",
        headers={"X-API-Key": API_KEY},
    )

    assert response.status_code == 200
    assert response.json()["metadata"] == {"order_id": 42}
    assert "metadata_" not in response.json()


@pytest.mark.asyncio
async def test_get_payment_maps_not_found(
    http_client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def get(_: Any, payment_id: UUID) -> Payment:
        raise PaymentNotFoundError(f"Payment {payment_id} was not found")

    monkeypatch.setattr(routes, "get_payment", get)
    response = await http_client.get(
        f"/api/v1/payments/{PAYMENT_ID}",
        headers={"X-API-Key": API_KEY},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["/health/live", "/health/ready"])
async def test_health_endpoints_require_api_key(http_client: AsyncClient, path: str) -> None:
    assert (await http_client.get(path)).status_code == 401
    assert (await http_client.get(path, headers={"X-API-Key": "wrong-key"})).status_code == 401
    assert (await http_client.get(path, headers={"X-API-Key": API_KEY})).status_code == 200


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["/docs", "/openapi.json"])
async def test_docs_are_public_by_default_and_protectable(path: str) -> None:
    async def ready() -> bool:
        return True

    public = create_app(Settings(api_key=API_KEY), ready, start_outbox_relay=False)
    async with AsyncClient(transport=ASGITransport(app=public), base_url="http://test") as http:
        assert (await http.get(path)).status_code == 200

    locked = create_app(
        Settings(api_key=API_KEY, public_docs=False), ready, start_outbox_relay=False
    )
    async with AsyncClient(transport=ASGITransport(app=locked), base_url="http://test") as http:
        assert (await http.get(path)).status_code == 401
        assert (await http.get(path, headers={"X-API-Key": "wrong-key"})).status_code == 401
        assert (await http.get(path, headers={"X-API-Key": API_KEY})).status_code == 200


@pytest.mark.asyncio
async def test_authenticated_health_endpoints(http_client: AsyncClient) -> None:
    headers = {"X-API-Key": API_KEY}
    assert (await http_client.get("/health/live", headers=headers)).json() == {"status": "ok"}
    assert (await http_client.get("/health/ready", headers=headers)).json() == {"status": "ready"}


@pytest.mark.asyncio
async def test_not_ready_returns_503() -> None:
    async def not_ready() -> bool:
        return False

    app = create_app(Settings(api_key=API_KEY), not_ready, start_outbox_relay=False)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as http:
        response = await http.get("/health/ready", headers={"X-API-Key": API_KEY})
    assert response.status_code == 503
    assert response.json() == {"status": "not_ready"}


@pytest.mark.asyncio
async def test_lifespan_starts_and_stops_outbox_relay(monkeypatch: pytest.MonkeyPatch) -> None:
    import payment_service.api.app as app_module

    lifecycle: list[str] = []

    class Relay:
        def __init__(self, *_: Any) -> None:
            self.finished = asyncio.Event()

        async def run(self) -> None:
            lifecycle.append("run")
            await self.finished.wait()

        async def stop(self) -> None:
            lifecycle.append("stop")
            self.finished.set()

    class Engine:
        async def dispose(self) -> None:
            lifecycle.append("dispose")

    module = cast(Any, app_module)
    unused_factory = cast(async_sessionmaker[AsyncSession], None)
    monkeypatch.setattr(module, "OutboxRelay", Relay)
    monkeypatch.setattr(module, "build_async_engine", lambda _: Engine())
    monkeypatch.setattr(module, "build_session_factory", lambda _: unused_factory)
    app = create_app(Settings(api_key=API_KEY))

    async with app.router.lifespan_context(app):
        await asyncio.sleep(0)
        assert lifecycle == ["run"]

    assert lifecycle == ["run", "stop", "dispose"]


def test_create_app_builds_database_from_explicit_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import payment_service.api.app as app_module

    captured_url: str | None = None

    class Engine:
        async def dispose(self) -> None:
            pass

    def build_engine(settings: Settings) -> Engine:
        nonlocal captured_url
        captured_url = settings.database_url
        return Engine()

    module = cast(Any, app_module)
    unused_factory = cast(async_sessionmaker[AsyncSession], None)
    monkeypatch.setattr(module, "build_async_engine", build_engine)
    monkeypatch.setattr(module, "build_session_factory", lambda _: unused_factory)

    custom_url = "postgresql+asyncpg://custom:custom@db.example/custom"
    create_app(
        Settings(api_key=API_KEY, database_url=custom_url),
        start_outbox_relay=False,
    )

    assert captured_url == custom_url
