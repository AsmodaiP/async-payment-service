from __future__ import annotations

import secrets
from typing import Annotated

from fastapi import Depends, Header, HTTPException, status
from fastapi.security import APIKeyHeader

from payment_service.config import Settings, get_settings

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def require_api_key(
    supplied_api_key: Annotated[str | None, Depends(api_key_header)],
    settings: Annotated[Settings, Depends(get_settings)],
) -> None:
    expected_api_key = settings.api_key.get_secret_value().encode("utf-8")
    if supplied_api_key is None or not secrets.compare_digest(
        supplied_api_key.encode("utf-8"), expected_api_key
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
        )


async def require_idempotency_key(
    idempotency_key: Annotated[
        str,
        Header(
            alias="Idempotency-Key",
            min_length=1,
            max_length=255,
            description="Unique key for safely retrying this payment request",
        ),
    ],
) -> str:
    normalized_key = idempotency_key.strip()
    if not normalized_key:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="Idempotency-Key must not be blank",
        )
    return normalized_key
