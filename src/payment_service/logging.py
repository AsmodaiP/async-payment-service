from __future__ import annotations

import logging
import sys

import structlog


def configure_logging(level: str) -> None:
    """Configure deterministic JSON logs for every process."""

    logging.basicConfig(
        format="%(message)s",
        level=level.upper(),
        stream=sys.stdout,
        force=True,
    )
    # Avoid leaking webhook query strings from httpx's default INFO request logs.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelName(level.upper())
            if isinstance(logging.getLevelName(level.upper()), int)
            else logging.INFO
        ),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )
