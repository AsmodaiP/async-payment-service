from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from payment_service.config import Settings, get_settings


def build_async_engine(settings: Settings | None = None) -> AsyncEngine:
    """Build an engine without opening a connection eagerly."""

    resolved_settings = settings or get_settings()
    return create_async_engine(
        resolved_settings.database_url,
        pool_pre_ping=True,
        hide_parameters=True,
    )


def build_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        autoflush=False,
        expire_on_commit=False,
    )


engine = build_async_engine()
AsyncSessionFactory = build_session_factory(engine)

# Compatibility names kept intentionally small for framework/worker call sites.
SessionFactory = AsyncSessionFactory
async_session_maker = AsyncSessionFactory


async def get_db_session() -> AsyncIterator[AsyncSession]:
    """Yield a request-scoped session; transaction ownership stays with the caller."""

    session = AsyncSessionFactory()
    try:
        yield session
    except BaseException:
        await session.rollback()
        raise
    finally:
        await session.close()


get_session = get_db_session


@asynccontextmanager
async def session_scope(
    session_factory: async_sessionmaker[AsyncSession] = AsyncSessionFactory,
) -> AsyncIterator[AsyncSession]:
    """Provide a short transaction that commits or rolls back as one unit."""

    session = session_factory()
    try:
        async with session.begin():
            yield session
    except BaseException:
        if session.in_transaction():
            await session.rollback()
        raise
    finally:
        await session.close()


async def check_database_readiness(db_engine: AsyncEngine = engine) -> None:
    """Raise when PostgreSQL cannot accept a trivial query."""

    async with db_engine.connect() as connection:
        await connection.execute(text("SELECT 1"))


async def database_ready(
    db_engine: AsyncEngine = engine,
    *,
    timeout_seconds: float | None = None,
) -> bool:
    """Return whether PostgreSQL can accept a trivial query."""

    resolved_timeout = timeout_seconds or get_settings().database_readiness_timeout_seconds
    try:
        async with asyncio.timeout(resolved_timeout):
            await check_database_readiness(db_engine)
    except (SQLAlchemyError, OSError, TimeoutError):
        return False
    return True


is_database_ready = database_ready
