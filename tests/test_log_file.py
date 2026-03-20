from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest
import structlog

from wedge.log import get_logger, setup_logging


@pytest.fixture(autouse=True)
def reset_structlog_and_logging():
    """Reset structlog + stdlib logging between tests to avoid state bleed."""
    yield
    structlog.reset_defaults()
    root = logging.getLogger()
    for h in root.handlers[:]:
        root.removeHandler(h)
        h.close()


class TestSetupLoggingFileOutput:
    def test_log_file_created(self, tmp_path: Path):
        log_file = tmp_path / "logs" / "wedge-test.log"
        setup_logging(log_file=log_file)
        assert log_file.exists(), "log file should be created on setup"

    def test_log_file_parent_created(self, tmp_path: Path):
        log_file = tmp_path / "deep" / "nested" / "wedge.log"
        setup_logging(log_file=log_file)
        assert log_file.parent.exists()

    def test_log_file_writes_json(self, tmp_path: Path):
        log_file = tmp_path / "wedge.log"
        setup_logging(log_file=log_file)

        logger = get_logger("test_file")
        logger.info("hello_world", city="Tokyo", value=42)

        # Flush handlers
        for h in logging.getLogger().handlers:
            h.flush()

        content = log_file.read_text(encoding="utf-8").strip()
        assert content, "log file should not be empty"

        line = json.loads(content.splitlines()[-1])
        assert line["event"] == "hello_world"
        assert line["city"] == "Tokyo"
        assert line["value"] == 42
        assert line["level"] == "info"
        assert "timestamp" in line

    def test_log_file_redacts_secrets(self, tmp_path: Path):
        log_file = tmp_path / "wedge.log"
        setup_logging(log_file=log_file)

        logger = get_logger("test_secrets")
        logger.info("auth_attempt", api_key="super-secret-123", city="NYC")

        for h in logging.getLogger().handlers:
            h.flush()

        content = log_file.read_text(encoding="utf-8").strip()
        line = json.loads(content.splitlines()[-1])
        assert line["api_key"] == "***REDACTED***"
        assert line["city"] == "NYC"  # non-secret untouched

    def test_log_file_contains_multiple_levels(self, tmp_path: Path):
        log_file = tmp_path / "wedge.log"
        setup_logging(log_file=log_file)

        logger = get_logger("test_levels")
        logger.debug("debug_event")
        logger.info("info_event")
        logger.warning("warn_event")
        logger.error("error_event")

        for h in logging.getLogger().handlers:
            h.flush()

        lines = [
            json.loads(line_text)
            for line_text in log_file.read_text().strip().splitlines()
            if line_text.strip()
        ]
        levels = {line["level"] for line in lines}
        assert {"debug", "info", "warning", "error"}.issubset(levels)

    def test_no_log_file_console_only(self, tmp_path: Path, capsys):
        """Without log_file, should not create any file and not crash."""
        setup_logging()
        logger = get_logger("test_console")
        logger.info("console_only_event")
        # No file should exist in tmp_path
        assert list(tmp_path.iterdir()) == []

    def test_no_duplicate_file_handlers(self, tmp_path: Path):
        """Calling setup_logging twice should not add duplicate handlers."""
        log_file = tmp_path / "wedge.log"
        setup_logging(log_file=log_file)
        setup_logging(log_file=log_file)

        file_handlers = [
            h
            for h in logging.getLogger().handlers
            if isinstance(h, logging.handlers.TimedRotatingFileHandler)
        ]
        assert len(file_handlers) == 1
