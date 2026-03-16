from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from wedge.monitoring.notify import (
    StdoutNotifier,
    TelegramNotifier,
    create_notifier,
    format_alert,
    format_pipeline_summary,
    format_stats,
)


class TestStdoutNotifier:
    @pytest.mark.asyncio
    async def test_send(self):
        notifier = StdoutNotifier()
        # Should not raise; log.info is a side effect we don't assert on
        await notifier.send("hello world")


class TestTelegramNotifier:
    @pytest.mark.asyncio
    async def test_send_success(self):
        mock_bot = AsyncMock()
        mock_bot.send_message = AsyncMock()

        with patch("telegram.Bot", return_value=mock_bot):
            notifier = TelegramNotifier(token="tok", chat_id="123")
            await notifier.send("test message")

        mock_bot.send_message.assert_awaited_once_with(chat_id="123", text="test message")

    @pytest.mark.asyncio
    async def test_send_failure_logs_error(self):
        mock_bot = AsyncMock()
        mock_bot.send_message = AsyncMock(side_effect=Exception("network error"))

        with patch("telegram.Bot", return_value=mock_bot):
            notifier = TelegramNotifier(token="tok", chat_id="123")
            # Must not raise
            await notifier.send("test message")

    @pytest.mark.asyncio
    async def test_ensure_bot_idempotent(self):
        mock_bot_instance = AsyncMock()
        mock_bot_class = MagicMock(return_value=mock_bot_instance)

        with patch("telegram.Bot", mock_bot_class):
            notifier = TelegramNotifier(token="tok", chat_id="123")
            await notifier._ensure_bot()
            await notifier._ensure_bot()

        # Bot constructor called only once
        assert mock_bot_class.call_count == 1


class TestCreateNotifier:
    def test_with_token_and_chat_id(self):
        result = create_notifier(token="tok", chat_id="123")
        assert isinstance(result, TelegramNotifier)

    def test_empty_token_returns_stdout(self):
        result = create_notifier(token="", chat_id="123")
        assert isinstance(result, StdoutNotifier)

    def test_empty_chat_id_returns_stdout(self):
        result = create_notifier(token="tok", chat_id="")
        assert isinstance(result, StdoutNotifier)

    def test_both_empty_returns_stdout(self):
        result = create_notifier(token="", chat_id="")
        assert isinstance(result, StdoutNotifier)


class TestFormatPipelineSummary:
    def test_without_signals(self):
        text = format_pipeline_summary(
            mode="dry_run",
            cities=["NYC", "LA"],
            edges_found=3,
            orders_placed=2,
            balance=950.50,
        )
        assert "[Pipeline Complete] (dry_run)" in text
        assert "NYC, LA" in text
        assert "Edges found: 3" in text
        assert "Orders placed: 2" in text
        assert "Balance: $950.50" in text
        assert "Top signals" not in text

    def test_with_signals(self):
        signals = [
            {"city": "NYC", "temp_value": 85, "temp_unit": "F", "edge": 0.12, "size": 20.0},
            {"city": "LA", "temp_value": 72, "temp_unit": "F", "edge": 0.08, "size": 15.0},
        ]
        text = format_pipeline_summary(
            mode="live",
            cities=["NYC", "LA"],
            edges_found=5,
            orders_placed=2,
            balance=1000.0,
            signals=signals,
        )
        assert "Top signals:" in text
        assert "NYC" in text
        assert "85°F" in text

    def test_signals_capped_at_five(self):
        signals = [
            {"city": f"C{i}", "temp_value": 70 + i, "temp_unit": "F", "edge": 0.05 + i * 0.01, "size": 10.0}
            for i in range(8)
        ]
        text = format_pipeline_summary(
            mode="dry_run",
            cities=["NYC"],
            edges_found=8,
            orders_placed=5,
            balance=500.0,
            signals=signals,
        )
        # Only first 5 signals shown
        assert text.count("°F") == 5


class TestFormatAlert:
    def test_without_details(self):
        text = format_alert("low balance")
        assert "[ALERT] low balance" in text
        assert text == "[ALERT] low balance"

    def test_with_details(self):
        text = format_alert("low balance", details="only $10 left")
        assert "[ALERT] low balance" in text
        assert "only $10 left" in text


class TestFormatStats:
    def test_brier_none(self):
        text = format_stats(
            days=30,
            total_trades=5,
            wins=3,
            win_rate=0.6,
            total_pnl=25.0,
            brier=None,
        )
        assert "Brier: N/A" in text
        assert "[Stats] (30 days)" in text
        assert "Wins: 3 (60.0%)" in text

    def test_brier_good(self):
        text = format_stats(
            days=30,
            total_trades=10,
            wins=7,
            win_rate=0.7,
            total_pnl=50.0,
            brier=0.15,
        )
        assert "(GOOD)" in text
        assert "0.1500" in text

    def test_brier_ok(self):
        text = format_stats(
            days=30,
            total_trades=10,
            wins=5,
            win_rate=0.5,
            total_pnl=10.0,
            brier=0.22,
        )
        assert "(OK)" in text

    def test_brier_paused(self):
        text = format_stats(
            days=30,
            total_trades=10,
            wins=3,
            win_rate=0.3,
            total_pnl=-5.0,
            brier=0.30,
        )
        assert "(PAUSED)" in text

    def test_total_trades_zero(self):
        text = format_stats(
            days=7,
            total_trades=0,
            wins=0,
            win_rate=0.0,
            total_pnl=0.0,
            brier=None,
        )
        assert "Wins: N/A" in text
