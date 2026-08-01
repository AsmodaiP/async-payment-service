from __future__ import annotations

import asyncio
from typing import Any, cast

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

import payment_service.db.session as session_module
from payment_service.config import Settings


def test_engine_hides_query_parameters() -> None:
    engine = session_module.build_async_engine(Settings())
    try:
        assert cast(Any, engine.sync_engine).hide_parameters is True
    finally:
        asyncio.run(engine.dispose())


@pytest.mark.asyncio
async def test_readiness_query_has_a_deadline(monkeypatch: pytest.MonkeyPatch) -> None:
    async def never_ready(_: AsyncEngine) -> None:
        await asyncio.Event().wait()

    monkeypatch.setattr(session_module, "check_database_readiness", never_ready)

    assert (
        await session_module.database_ready(
            cast(Any, object()),
            timeout_seconds=0.01,
        )
        is False
    )
