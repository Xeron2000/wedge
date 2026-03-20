from __future__ import annotations

import pytest

from wedge.monitoring.notify import (
    StdoutNotifier,
    format_alert,
    format_exit_notification,
    format_pipeline_summary,
    format_positions,
    format_stats,
)


class TestStdoutNotifier:
    @pytest.mark.asyncio
    async def test_send(self):
        notifier = StdoutNotifier()
        await notifier.send("hello world")


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
            {
                "city": f"C{i}",
                "temp_value": 70 + i,
                "temp_unit": "F",
                "edge": 0.05 + i * 0.01,
                "size": 10.0,
            }
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
        assert text.count("°F") == 5


class TestFormatAlert:
    def test_without_details(self):
        text = format_alert("low balance")
        assert text == "[ALERT] low balance"

    def test_with_details(self):
        text = format_alert("low balance", details="only $10 left")
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


def test_format_positions_empty():
    text = format_positions([])
    assert "No open positions" in text


def test_format_positions_with_positions():
    text = format_positions(
        [
            {
                "city": "NYC",
                "date": "2026-07-01",
                "temp_value": 80,
                "temp_unit": "F",
                "size": 25.0,
                "entry_price": 0.20,
                "edge": 0.10,
            }
        ]
    )
    assert "[Positions]" in text
    assert "NYC 80°F" in text
    assert "Total invested: $25.00" in text


def test_format_exit_notification_stop_loss():
    msg = format_exit_notification(
        city="Chicago",
        date="2026-07-04",
        temp_f=85,
        exit_reason="stop_loss",
        pnl=-12.50,
        p_model=0.20,
        entry_price=0.45,
    )
    assert "Chicago" in msg
    assert "stop_loss" in msg or "Stop Loss" in msg
    assert "2026-07-04" in msg


def test_format_exit_notification_take_profit():
    msg = format_exit_notification(
        city="Dallas",
        date="2026-08-01",
        temp_f=100,
        exit_reason="take_profit",
        pnl=8.75,
        p_model=0.70,
        entry_price=0.55,
    )
    assert "Dallas" in msg
    assert "take_profit" in msg or "Take Profit" in msg
    assert "8.75" in msg
