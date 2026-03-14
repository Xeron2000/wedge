from __future__ import annotations

import asyncio

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

from wedge.config import Settings
from wedge.db import Database
from wedge.log import get_logger
from wedge.monitoring.notify import format_stats

log = get_logger("telegram")


class TelegramBotManager:
    """Manages the Telegram bot alongside the trading scheduler."""

    def __init__(self, settings: Settings, db: Database) -> None:
        self._settings = settings
        self._db = db
        self._app: Application | None = None
        self._stop_event: asyncio.Event | None = None

    def set_stop_event(self, event: asyncio.Event) -> None:
        self._stop_event = event

    async def start(self) -> None:
        if not self._settings.telegram_token:
            log.warning("telegram_disabled", reason="no token")
            return

        self._app = (
            Application.builder()
            .token(self._settings.telegram_token)
            .build()
        )

        self._app.add_handler(CommandHandler("scan", self._handle_scan))
        self._app.add_handler(CommandHandler("stats", self._handle_stats))
        self._app.add_handler(CommandHandler("status", self._handle_status))
        self._app.add_handler(CommandHandler("stop", self._handle_stop))
        self._app.add_handler(CommandHandler("help", self._handle_help))

        await self._app.initialize()
        await self._app.start()
        await self._app.updater.start_polling(drop_pending_updates=True)
        log.info("telegram_bot_started")

    async def stop(self) -> None:
        if self._app and self._app.updater.running:
            await self._app.updater.stop()
            await self._app.stop()
            await self._app.shutdown()
            log.info("telegram_bot_stopped")

    def _check_auth(self, update: Update) -> bool:
        chat_id = str(update.effective_chat.id) if update.effective_chat else ""
        allowed = self._settings.telegram_chat_id
        if allowed and chat_id != allowed:
            log.warning("telegram_unauthorized", chat_id=chat_id)
            return False
        return True

    async def _handle_scan(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._check_auth(update):
            return

        args = context.args or []
        city = args[0] if args else "NYC"

        await update.message.reply_text(f"Scanning {city}...")

        from wedge.pipeline import run_single_scan
        try:
            await run_single_scan(self._settings, city)
            await update.message.reply_text(f"Scan complete for {city}. Check logs for details.")
        except Exception as e:
            await update.message.reply_text(f"Scan failed: {e}")

    async def _handle_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._check_auth(update):
            return

        args = context.args or []
        days = int(args[0]) if args else 30

        brier = await self._db.get_brier_score(days)
        pnl = await self._db.get_pnl_summary(days)

        msg = format_stats(
            days=days,
            total_trades=pnl["total_trades"],
            wins=pnl["wins"],
            win_rate=pnl["win_rate"],
            total_pnl=pnl["total_pnl"],
            brier=brier,
        )
        await update.message.reply_text(msg, parse_mode="Markdown")

    async def _handle_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._check_auth(update):
            return

        brier = await self._db.get_brier_score(7)
        brier_str = f"{brier:.4f}" if brier is not None else "N/A"
        status = "PAUSED" if brier and brier > self._settings.brier_threshold else "ACTIVE"

        msg = "\n".join([
            f"*Status*: {status}",
            f"Mode: {self._settings.mode}",
            f"Bankroll: ${self._settings.bankroll:.2f}",
            f"Brier (7d): {brier_str}",
            f"Cities: {', '.join(c.name for c in self._settings.cities)}",
        ])
        await update.message.reply_text(msg, parse_mode="Markdown")

    async def _handle_stop(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._check_auth(update):
            return
        await update.message.reply_text("Shutting down...")
        if self._stop_event:
            self._stop_event.set()

    async def _handle_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._check_auth(update):
            return
        msg = "\n".join([
            "*Weather Edge Bot*",
            "/scan <city> — Run single scan",
            "/stats [days] — Show P&L and Brier",
            "/status — Current bot status",
            "/stop — Graceful shutdown",
            "/help — This message",
        ])
        await update.message.reply_text(msg, parse_mode="Markdown")
