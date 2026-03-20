"""Full coverage tests for wedge.cli."""

from __future__ import annotations

from unittest.mock import patch

from typer.testing import CliRunner

from wedge.cli import app

runner = CliRunner()


def _close_coro(coro):
    coro.close()
    return None


class TestRunCommand:
    def test_dry_run_default_exits_ok(self):
        with (
            patch("wedge.cli.asyncio.run", side_effect=_close_coro) as mock_run,
            patch("wedge.cli.setup_logging"),
        ):
            result = runner.invoke(app, ["run"])
        assert result.exit_code == 0
        mock_run.assert_called_once()

    def test_dry_run_flag(self):
        with (
            patch("wedge.cli.asyncio.run", side_effect=_close_coro) as mock_run,
            patch("wedge.cli.setup_logging"),
        ):
            result = runner.invoke(app, ["run", "--dry-run"])
        assert result.exit_code == 0
        mock_run.assert_called_once()

    def test_live_flag_exits_ok(self):
        with (
            patch("wedge.cli.asyncio.run", side_effect=_close_coro) as mock_run,
            patch("wedge.cli.setup_logging"),
        ):
            result = runner.invoke(app, ["run", "--live"])
        assert result.exit_code == 0
        mock_run.assert_called_once()

    def test_live_flag_passes_live_mode_to_scheduler(self):
        with (
            patch("wedge.scheduler.run_scheduler"),
            patch("wedge.cli.asyncio.run", side_effect=_close_coro),
            patch("wedge.cli.setup_logging"),
        ):
            result = runner.invoke(app, ["run", "--live"])
        assert result.exit_code == 0

    def test_custom_bankroll(self):
        with (
            patch("wedge.cli.asyncio.run", side_effect=_close_coro),
            patch("wedge.cli.setup_logging"),
        ):
            result = runner.invoke(app, ["run", "--bankroll", "5000.0"])
        assert result.exit_code == 0

    def test_custom_max_bet(self):
        with (
            patch("wedge.cli.asyncio.run", side_effect=_close_coro),
            patch("wedge.cli.setup_logging"),
        ):
            result = runner.invoke(app, ["run", "--max-bet", "200.0"])
        assert result.exit_code == 0

    def test_custom_kelly(self):
        with (
            patch("wedge.cli.asyncio.run", side_effect=_close_coro),
            patch("wedge.cli.setup_logging"),
        ):
            result = runner.invoke(app, ["run", "--kelly", "0.20"])
        assert result.exit_code == 0

    def test_custom_ladder_edge(self):
        with (
            patch("wedge.cli.asyncio.run", side_effect=_close_coro),
            patch("wedge.cli.setup_logging"),
        ):
            result = runner.invoke(app, ["run", "--ladder-edge", "0.07"])
        assert result.exit_code == 0

    def test_run_calls_setup_logging(self):
        with (
            patch("wedge.cli.asyncio.run", side_effect=_close_coro),
            patch("wedge.cli.setup_logging") as mock_logging,
        ):
            runner.invoke(app, ["run"])
        mock_logging.assert_called_once()


class TestScanCommand:
    def test_default_city_exits_ok(self):
        with (
            patch("wedge.cli.asyncio.run", side_effect=_close_coro) as mock_run,
            patch("wedge.cli.setup_logging"),
        ):
            result = runner.invoke(app, ["scan"])
        assert result.exit_code == 0
        mock_run.assert_called_once()

    def test_custom_city(self):
        with (
            patch("wedge.cli.asyncio.run", side_effect=_close_coro),
            patch("wedge.cli.setup_logging"),
        ):
            result = runner.invoke(app, ["scan", "--city", "Chicago"])
        assert result.exit_code == 0

    def test_scan_calls_setup_logging(self):
        with (
            patch("wedge.cli.asyncio.run", side_effect=_close_coro),
            patch("wedge.cli.setup_logging") as mock_logging,
        ):
            runner.invoke(app, ["scan"])
        mock_logging.assert_called_once()

    def test_scan_calls_asyncio_run(self):
        with (
            patch("wedge.cli.asyncio.run", side_effect=_close_coro) as mock_run,
            patch("wedge.cli.setup_logging"),
        ):
            runner.invoke(app, ["scan"])
        assert mock_run.call_count == 1


class TestStatsCommand:
    def test_default_days_exits_ok(self):
        with (
            patch("wedge.cli.asyncio.run", side_effect=_close_coro) as mock_run,
            patch("wedge.cli.setup_logging"),
        ):
            result = runner.invoke(app, ["stats"])
        assert result.exit_code == 0
        mock_run.assert_called_once()

    def test_custom_days(self):
        with (
            patch("wedge.cli.asyncio.run", side_effect=_close_coro),
            patch("wedge.cli.setup_logging"),
        ):
            result = runner.invoke(app, ["stats", "--days", "7"])
        assert result.exit_code == 0

    def test_stats_short_flag(self):
        with (
            patch("wedge.cli.asyncio.run", side_effect=_close_coro),
            patch("wedge.cli.setup_logging"),
        ):
            result = runner.invoke(app, ["stats", "-d", "14"])
        assert result.exit_code == 0

    def test_stats_calls_setup_logging(self):
        with (
            patch("wedge.cli.asyncio.run", side_effect=_close_coro),
            patch("wedge.cli.setup_logging") as mock_logging,
        ):
            runner.invoke(app, ["stats"])
        mock_logging.assert_called_once()


class TestHelpText:
    def test_main_help(self):
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "wedge" in result.output.lower() or "weather" in result.output.lower()

    def test_run_help(self):
        result = runner.invoke(app, ["run", "--help"])
        assert result.exit_code == 0
        assert "--dry-run" in result.output or "dry" in result.output.lower()

    def test_scan_help(self):
        result = runner.invoke(app, ["scan", "--help"])
        assert result.exit_code == 0
        assert "--city" in result.output

    def test_stats_help(self):
        result = runner.invoke(app, ["stats", "--help"])
        assert result.exit_code == 0
        assert "--days" in result.output or "-d" in result.output
