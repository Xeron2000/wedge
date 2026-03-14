from __future__ import annotations

import asyncio
import signal
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

_UTC = ZoneInfo("UTC")

from wedge.config import Settings
from wedge.db import Database
from wedge.log import get_logger
from wedge.monitoring.notify import create_notifier, format_alert
from wedge.pipeline import run_pipeline

log = get_logger("scheduler")


async def run_scheduler(settings: Settings, *, enable_telegram: bool = False) -> None:
    """Main entry: start APScheduler and run until SIGINT/SIGTERM."""
    db = Database(settings.db_path)
    await db.connect()

    notifier = create_notifier(settings.telegram_token, settings.telegram_chat_id)

    scheduler = AsyncIOScheduler()
    _running_lock = asyncio.Lock()

    async def _guarded_pipeline() -> None:
        if _running_lock.locked():
            log.warning("pipeline_skipped_already_running")
            return
        async with _running_lock:
            try:
                brier = await db.get_brier_score(days=30)
                if brier is not None and brier > settings.brier_threshold:
                    log.warning("circuit_breaker_active", brier_score=f"{brier:.3f}")
                    await notifier.send(
                        format_alert("Circuit breaker active", f"Brier score: {brier:.4f}")
                    )
                    return
                await run_pipeline(settings, db, notifier=notifier)
            except Exception as e:
                log.error("pipeline_error", error=str(e))
                await notifier.send(format_alert("Pipeline error", str(e)))

    for offset in settings.offsets_utc:
        hour, minute = offset.split(":")
        scheduler.add_job(
            _guarded_pipeline,
            trigger=CronTrigger(hour=int(hour), minute=int(minute), timezone=_UTC),
            coalesce=True,
            misfire_grace_time=600,
            id=f"pipeline_{offset}",
        )

    scheduler.start()
    log.info(
        "scheduler_started",
        mode=settings.mode,
        windows=settings.offsets_utc,
        bankroll=settings.bankroll,
        telegram=enable_telegram,
    )

    # Run one immediate cycle
    await _guarded_pipeline()

    # Wait for shutdown signal
    stop_event = asyncio.Event()

    def _handle_signal() -> None:
        log.info("shutdown_signal_received")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)

    # Optionally start Telegram bot
    tg_manager = None
    if enable_telegram and settings.telegram_token:
        from wedge.telegram import TelegramBotManager

        tg_manager = TelegramBotManager(settings, db)
        tg_manager.set_stop_event(stop_event)
        await tg_manager.start()
        await notifier.send("Weather Edge Bot started")

    await stop_event.wait()

    log.info("shutting_down")
    if tg_manager:
        await tg_manager.stop()
    scheduler.shutdown(wait=True)
    await db.close()
    log.info("shutdown_complete")
