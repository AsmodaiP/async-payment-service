from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager, suppress

import structlog
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from payment_service.api.routes import router as payments_router
from payment_service.api.schemas import HealthResponse
from payment_service.config import Settings, get_settings
from payment_service.db.session import (
    build_async_engine,
    build_session_factory,
    database_ready,
    get_db_session,
)
from payment_service.domain import IdempotencyConflictError, PaymentNotFoundError
from payment_service.logging import configure_logging
from payment_service.messaging.outbox import OutboxRelay
from payment_service.messaging.topology import build_topology

ReadinessCheck = Callable[[], Awaitable[bool]]


def create_app(
    settings: Settings | None = None,
    readiness_check: ReadinessCheck | None = None,
    *,
    start_outbox_relay: bool = True,
) -> FastAPI:
    app_settings = settings or get_settings()
    db_engine = build_async_engine(app_settings)
    session_factory = build_session_factory(db_engine)

    async def app_session() -> AsyncIterator[AsyncSession]:
        session = session_factory()
        try:
            yield session
        except BaseException:
            await session.rollback()
            raise
        finally:
            await session.close()

    async def app_readiness() -> bool:
        if readiness_check is not None:
            return await readiness_check()
        return await database_ready(db_engine)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        configure_logging(app_settings.log_level)
        logger = structlog.get_logger()
        relay: OutboxRelay | None = None
        relay_task: asyncio.Task[None] | None = None
        if start_outbox_relay:
            relay = OutboxRelay(
                app_settings,
                session_factory,
                build_topology(app_settings),
            )
            relay_task = asyncio.create_task(relay.run(), name="outbox-relay")
        logger.info(
            "service_started",
            service=app_settings.service_name,
            environment=app_settings.environment,
        )
        try:
            yield
        finally:
            if relay is not None and relay_task is not None:
                await relay.stop()
                try:
                    await asyncio.wait_for(relay_task, timeout=5)
                except TimeoutError:
                    relay_task.cancel()
                    with suppress(asyncio.CancelledError):
                        await relay_task
            await db_engine.dispose()
            logger.info("service_stopped", service=app_settings.service_name)

    app = FastAPI(
        title=app_settings.service_name,
        version="1.0.0",
        lifespan=lifespan,
    )
    app.state.settings = app_settings
    app.state.db_engine = db_engine
    app.state.session_factory = session_factory
    app.dependency_overrides[get_db_session] = app_session

    if settings is not None:

        def settings_override() -> Settings:
            return app_settings

        app.dependency_overrides[get_settings] = settings_override

    @app.exception_handler(IdempotencyConflictError)
    async def idempotency_conflict_handler(
        _: Request,
        exc: IdempotencyConflictError,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={"detail": str(exc)},
        )

    @app.exception_handler(PaymentNotFoundError)
    async def payment_not_found_handler(
        _: Request,
        exc: PaymentNotFoundError,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={"detail": str(exc)},
        )

    @app.get(
        "/health/live",
        response_model=HealthResponse,
        tags=["health"],
        summary="Process liveness",
    )
    async def health_live() -> HealthResponse:
        return HealthResponse(status="ok")

    @app.get(
        "/health/ready",
        response_model=HealthResponse,
        tags=["health"],
        summary="Database readiness",
        responses={status.HTTP_503_SERVICE_UNAVAILABLE: {"description": "Database unavailable"}},
    )
    async def health_ready() -> HealthResponse | JSONResponse:
        if await app_readiness():
            return HealthResponse(status="ready")
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"status": "not_ready"},
        )

    app.include_router(payments_router)
    return app


app = create_app()
