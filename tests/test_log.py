from __future__ import annotations

import structlog

from wedge.log import _sanitize_processor, get_logger, setup_logging


class TestSanitizeProcessor:
    def _run(self, event_dict: dict) -> dict:
        return _sanitize_processor(None, None, dict(event_dict))

    def test_private_key_redacted(self):
        result = self._run({"private_key": "abc123", "event": "x"})
        assert result["private_key"] == "***REDACTED***"
        assert result["event"] == "x"

    def test_api_key_redacted(self):
        result = self._run({"api_key": "secret"})
        assert result["api_key"] == "***REDACTED***"

    def test_api_secret_redacted(self):
        result = self._run({"api_secret": "shh"})
        assert result["api_secret"] == "***REDACTED***"

    def test_password_redacted(self):
        result = self._run({"password": "hunter2"})
        assert result["password"] == "***REDACTED***"

    def test_secret_redacted(self):
        result = self._run({"secret": "topsecret"})
        assert result["secret"] == "***REDACTED***"

    def test_non_secret_key_untouched(self):
        result = self._run({"username": "alice", "city": "NYC"})
        assert result["username"] == "alice"
        assert result["city"] == "NYC"

    def test_multiple_keys_mixed(self):
        result = self._run({"api_key": "x", "city": "NYC", "secret": "y"})
        assert result["api_key"] == "***REDACTED***"
        assert result["secret"] == "***REDACTED***"
        assert result["city"] == "NYC"

    def test_case_insensitive(self):
        result = self._run({"API_KEY": "upper", "Private_Key": "mixed"})
        assert result["API_KEY"] == "***REDACTED***"
        assert result["Private_Key"] == "***REDACTED***"


class TestSetupLogging:
    def test_console_renderer(self):
        # Should not raise
        setup_logging(json_output=False)
        logger = structlog.get_logger("test")
        assert logger is not None

    def test_json_renderer(self):
        # Should not raise
        setup_logging(json_output=True)
        logger = structlog.get_logger("test")
        assert logger is not None


class TestGetLogger:
    def test_returns_logger(self):
        logger = get_logger("mymodule")
        assert logger is not None

    def test_empty_name(self):
        logger = get_logger()
        assert logger is not None
