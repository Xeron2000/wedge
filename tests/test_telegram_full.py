"""Full coverage tests for wedge.telegram.TelegramBotManager."""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from wedge.config import CityConfig, Settings
from wedge.telegram import TelegramBotManager


# ── fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def settings():
    return Settings(
        mode="dry_run",
        bankroll=1000.0,
        brier_threshold=0.25,
        telegram_token="fake:token",
        telegram_chat_id="99999",
        cities=[CityConfig(name="NYC", lat=40.77, lon=-73.87, timezone="America/New_York")],
    )


@pytest.fixture
def db():
    mock = AsyncMock()
    mock.get_brier_score.return_value = 0.10
    mock.get_pnl_summary.return_value = {
        "total_trades": 10,
        "wins": 7,
        "win_rate": 0.70,
        "total_pnl": 55.0,
    }
    mock.get_last_balance.return_value = 1050.0
    return mock


@pytest.fixture
def manager(settings, db):
    return TelegramBotManager(settings, db)


def _make_update(chat_id: str = "99999"):
    update = MagicMock()
    update.effective_chat.id = int(chat_id)
    update.message.reply_text = AsyncMock()
    return update


def _make_context(args=None):
    ctx = MagicMock()
    ctx.args = args or []
    return ctx


# ── _check_auth ────────────────────────────────────────────────────────────────

class TestCheckAuth:
    def test_authorized_chat_returns_true(self, manager):
        update = _make_update("99999")
        assert manager._check_auth(update) is True

    def test_unauthorized_chat_returns_false(self, manager):
        update = _make_update("11111")
        assert manager._check_auth(update) is False

    def test_no_effective_chat_with_allowed_returns_false(self, manager):
        update = MagicMock()
        update.effective_chat = None
        # allowed is set, chat_id becomes "" → != allowed → False
        assert manager._check_auth(update) is False

    def test_no_allowed_chat_id_always_true(self, settings, db):
        s = settings.model_copy(update={"telegram_chat_id": ""})
        mgr = TelegramBotManager(s, db)
        update = _make_update("12345")
        assert mgr._check_auth(update) is True

    def test_no_effective_chat_no_allowed_returns_true(self, settings, db):
        s = settings.model_copy(update={"telegram_chat_id": ""})
        mgr = TelegramBotManager(s, db)
        update = MagicMock()
        update.effective_chat = None
        assert mgr._check_auth(update) is True


# ── _handle_scan ───────────────────────────────────────────────────────────────

class TestHandleScan:
    @pytest.mark.asyncio
    async def test_unauthorized_does_nothing(self, manager):
        update = _make_update("00000")
        ctx = _make_context()
        await manager._handle_scan(update, ctx)
        update.message.reply_text.assert_not_called()

    @pytest.mark.asyncio
    async def test_default_city_is_nyc(self, manager):
        update = _make_update("99999")
        ctx = _make_context(args=[])
        with patch("wedge.pipeline.run_single_scan", new_callable=AsyncMock) as mock_scan:
            await manager._handle_scan(update, ctx)
        mock_scan.assert_awaited_once_with(manager._settings, "NYC")

    @pytest.mark.asyncio
    async def test_custom_city_arg(self, manager):
        update = _make_update("99999")
        ctx = _make_context(args=["Chicago"])
        with patch("wedge.pipeline.run_single_scan", new_callable=AsyncMock) as mock_scan:
            await manager._handle_scan(update, ctx)
        mock_scan.assert_awaited_once_with(manager._settings, "Chicago")

    @pytest.mark.asyncio
    async def test_sends_scanning_message(self, manager):
        update = _make_update("99999")
        ctx = _make_context(args=["Miami"])
        with patch("wedge.pipeline.run_single_scan", new_callable=AsyncMock):
            await manager._handle_scan(update, ctx)
        first_call_args = update.message.reply_text.call_args_list[0][0][0]
        assert "Miami" in first_call_args

    @pytest.mark.asyncio
    async def test_sends_complete_message_on_success(self, manager):
        update = _make_update("99999")
        ctx = _make_context(args=["Dallas"])
        with patch("wedge.pipeline.run_single_scan", new_callable=AsyncMock):
            await manager._handle_scan(update, ctx)
        calls = [c[0][0] for c in update.message.reply_text.call_args_list]
        assert any("complete" in c.lower() for c in calls)

    @pytest.mark.asyncio
    async def test_scan_exception_replies_with_error(self, manager):
        update = _make_update("99999")
        ctx = _make_context(args=["NYC"])
        with patch(
            "wedge.pipeline.run_single_scan",
            new_callable=AsyncMock,
            side_effect=RuntimeError("boom"),
        ):
            await manager._handle_scan(update, ctx)
        calls = [c[0][0] for c in update.message.reply_text.call_args_list]
        assert any("failed" in c.lower() or "boom" in c for c in calls)


# ── _handle_stats ──────────────────────────────────────────────────────────────

class TestHandleStats:
    @pytest.mark.asyncio
    async def test_unauthorized_does_nothing(self, manager):
        update = _make_update("00000")
        ctx = _make_context()
        await manager._handle_stats(update, ctx)
        update.message.reply_text.assert_not_called()

    @pytest.mark.asyncio
    async def test_default_days_is_30(self, manager, db):
        update = _make_update("99999")
        ctx = _make_context(args=[])
        await manager._handle_stats(update, ctx)
        db.get_brier_score.assert_awaited_with(30)
        db.get_pnl_summary.assert_awaited_with(30)

    @pytest.mark.asyncio
    async def test_custom_days_arg(self, manager, db):
        update = _make_update("99999")
        ctx = _make_context(args=["7"])
        await manager._handle_stats(update, ctx)
        db.get_brier_score.assert_awaited_with(7)
        db.get_pnl_summary.assert_awaited_with(7)

    @pytest.mark.asyncio
    async def test_sends_stats_message(self, manager):
        update = _make_update("99999")
        ctx = _make_context()
        await manager._handle_stats(update, ctx)
        update.message.reply_text.assert_awaited_once()
        msg = update.message.reply_text.call_args[0][0]
        assert "Stats" in msg or "Trade" in msg or "P&L" in msg


# ── _handle_status ─────────────────────────────────────────────────────────────

class TestHandleStatus:
    @pytest.mark.asyncio
    async def test_unauthorized_does_nothing(self, manager):
        update = _make_update("00000")
        ctx = _make_context()
        await manager._handle_status(update, ctx)
        update.message.reply_text.assert_not_called()

    @pytest.mark.asyncio
    async def test_active_status_when_brier_ok(self, manager, db):
        db.get_brier_score.return_value = 0.10  # below 0.25 threshold
        update = _make_update("99999")
        ctx = _make_context()
        await manager._handle_status(update, ctx)
        msg = update.message.reply_text.call_args[0][0]
        assert "ACTIVE" in msg

    @pytest.mark.asyncio
    async def test_paused_status_when_brier_high(self, manager, db):
        db.get_brier_score.return_value = 0.40  # above 0.25
        update = _make_update("99999")
        ctx = _make_context()
        await manager._handle_status(update, ctx)
        msg = update.message.reply_text.call_args[0][0]
        assert "PAUSED" in msg

    @pytest.mark.asyncio
    async def test_na_when_no_brier(self, manager, db):
        db.get_brier_score.return_value = None
        update = _make_update("99999")
        ctx = _make_context()
        await manager._handle_status(update, ctx)
        msg = update.message.reply_text.call_args[0][0]
        assert "N/A" in msg

    @pytest.mark.asyncio
    async def test_shows_balance_and_mode(self, manager):
        update = _make_update("99999")
        ctx = _make_context()
        await manager._handle_status(update, ctx)
        msg = update.message.reply_text.call_args[0][0]
        assert "dry_run" in msg or "Mode" in msg
        assert "$" in msg or "Balance" in msg


# ── _handle_stop ───────────────────────────────────────────────────────────────

class TestHandleStop:
    @pytest.mark.asyncio
    async def test_unauthorized_does_nothing(self, manager):
        update = _make_update("00000")
        ctx = _make_context()
        await manager._handle_stop(update, ctx)
        update.message.reply_text.assert_not_called()

    @pytest.mark.asyncio
    async def test_sets_stop_event(self, manager):
        stop_event = asyncio.Event()
        manager.set_stop_event(stop_event)
        update = _make_update("99999")
        ctx = _make_context()
        await manager._handle_stop(update, ctx)
        assert stop_event.is_set()

    @pytest.mark.asyncio
    async def test_no_stop_event_no_error(self, manager):
        manager._stop_event = None
        update = _make_update("99999")
        ctx = _make_context()
        # Should not raise
        await manager._handle_stop(update, ctx)
        update.message.reply_text.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_sends_shutdown_message(self, manager):
        stop_event = asyncio.Event()
        manager.set_stop_event(stop_event)
        update = _make_update("99999")
        ctx = _make_context()
        await manager._handle_stop(update, ctx)
        msg = update.message.reply_text.call_args[0][0]
        assert "shut" in msg.lower() or "down" in msg.lower()


# ── _handle_help ───────────────────────────────────────────────────────────────

class TestHandleHelp:
    @pytest.mark.asyncio
    async def test_unauthorized_does_nothing(self, manager):
        update = _make_update("00000")
        ctx = _make_context()
        await manager._handle_help(update, ctx)
        update.message.reply_text.assert_not_called()

    @pytest.mark.asyncio
    async def test_sends_help_text(self, manager):
        update = _make_update("99999")
        ctx = _make_context()
        await manager._handle_help(update, ctx)
        msg = update.message.reply_text.call_args[0][0]
        assert "/scan" in msg
        assert "/stats" in msg
        assert "/status" in msg
        assert "/stop" in msg
        assert "/help" in msg


# ── start / stop ──────────────────────────────────────────────────────────────

class TestStartStop:
    @pytest.mark.asyncio
    async def test_start_without_token_skips(self, db):
        settings = Settings(telegram_token="", telegram_chat_id="")
        mgr = TelegramBotManager(settings, db)
        await mgr.start()
        assert mgr._app is None

    @pytest.mark.asyncio
    async def test_start_with_token_builds_app(self, settings, db):
        mgr = TelegramBotManager(settings, db)

        mock_app = AsyncMock()
        mock_app.updater = AsyncMock()
        mock_builder = MagicMock()
        mock_builder.token.return_value = mock_builder
        mock_builder.build.return_value = mock_app

        with patch("wedge.telegram.Application") as MockApp:
            MockApp.builder.return_value = mock_builder
            await mgr.start()

        mock_app.initialize.assert_awaited_once()
        mock_app.start.assert_awaited_once()
        mock_app.updater.start_polling.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_stop_with_no_app_does_nothing(self, manager):
        manager._app = None
        # Should not raise
        await manager.stop()

    @pytest.mark.asyncio
    async def test_stop_with_running_updater(self, manager):
        mock_app = AsyncMock()
        mock_app.updater.running = True
        manager._app = mock_app

        await manager.stop()

        mock_app.updater.stop.assert_awaited_once()
        mock_app.stop.assert_awaited_once()
        mock_app.shutdown.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_stop_with_non_running_updater_does_nothing(self, manager):
        mock_app = MagicMock()
        mock_app.updater.running = False
        manager._app = mock_app

        await manager.stop()

        mock_app.updater.stop.assert_not_called()

    def test_set_stop_event(self, manager):
        ev = asyncio.Event()
        manager.set_stop_event(ev)
        assert manager._stop_event is ev
