from __future__ import annotations

import re

import structlog

_SECRET_PATTERN = re.compile(
    r"(private_key|api_key|api_secret|^password$|^secret$)", re.IGNORECASE
)


def _sanitize_processor(
    _logger: object, _method: str, event_dict: dict
) -> dict:
    for key in list(event_dict):
        if _SECRET_PATTERN.search(key):
            event_dict[key] = "***REDACTED***"
    return event_dict


def setup_logging(*, json_output: bool = False) -> None:
    processors: list = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        _sanitize_processor,
    ]
    if json_output:
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer())

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(0),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str = "") -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name)
