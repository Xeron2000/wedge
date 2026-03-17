from __future__ import annotations

import logging
import logging.handlers
import re
import sys
from pathlib import Path

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


def setup_logging(
    *,
    json_output: bool = False,
    log_file: Path | str | None = None,
) -> None:
    """Configure structlog with optional file output.

    Args:
        json_output: Render console output as JSON (default: pretty console).
        log_file: If given, also write JSON-formatted logs to this file with
                  daily rotation (7-day retention). Parent directory is
                  created automatically.
    """
    shared_processors: list = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        _sanitize_processor,
    ]

    if log_file is not None:
        log_file = Path(log_file)
        log_file.parent.mkdir(parents=True, exist_ok=True)

        # Remove any existing TimedRotatingFileHandler to avoid duplicates
        root_logger = logging.getLogger()
        for h in root_logger.handlers[:]:
            if isinstance(h, logging.handlers.TimedRotatingFileHandler):
                root_logger.removeHandler(h)
                h.close()

        # File handler: JSON, rotate daily, keep 7 days
        file_handler = logging.handlers.TimedRotatingFileHandler(
            filename=str(log_file),
            when="midnight",
            interval=1,
            backupCount=7,
            encoding="utf-8",
            utc=True,
        )
        file_handler.setLevel(logging.DEBUG)
        root_logger.addHandler(file_handler)
        root_logger.setLevel(logging.DEBUG)

        # Processors for file output (always JSON)
        file_processors = shared_processors + [
            structlog.processors.JSONRenderer(),
        ]

        # Processors for console output
        console_processors = shared_processors + [
            structlog.processors.JSONRenderer() if json_output else structlog.dev.ConsoleRenderer(),
        ]

        # Use stdlib integration so structlog routes to both stdout and file
        structlog.configure(
            processors=shared_processors + [
                structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
            ],
            wrapper_class=structlog.make_filtering_bound_logger(0),
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
            cache_logger_on_first_use=True,
        )

        # Console handler via stdlib
        console_handler = logging.StreamHandler(sys.stdout)
        console_formatter = structlog.stdlib.ProcessorFormatter(
            processors=[
                structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                structlog.processors.JSONRenderer() if json_output else structlog.dev.ConsoleRenderer(),
            ],
            foreign_pre_chain=shared_processors,
        )
        console_handler.setFormatter(console_formatter)
        console_handler.setLevel(logging.DEBUG)

        file_formatter = structlog.stdlib.ProcessorFormatter(
            processors=[
                structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                structlog.processors.JSONRenderer(),
            ],
            foreign_pre_chain=shared_processors,
        )
        file_handler.setFormatter(file_formatter)

        root_logger.addHandler(console_handler)

    else:
        # Console-only mode (original behaviour)
        processors = shared_processors + [
            structlog.processors.JSONRenderer() if json_output else structlog.dev.ConsoleRenderer(),
        ]
        structlog.configure(
            processors=processors,
            wrapper_class=structlog.make_filtering_bound_logger(0),
            context_class=dict,
            logger_factory=structlog.PrintLoggerFactory(),
            cache_logger_on_first_use=True,
        )


def get_logger(name: str = "") -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name)
