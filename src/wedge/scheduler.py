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
from wedge.market.arb_scanner import ArbScanner
from wedge.monitoring.notify import create_notifier, format_alert
from wedge.pipeline import run_pipeline, run_settlement

log = get_logger("scheduler")


async def run_scheduler(settings: Settings, *, enable_telegram: bool = False) -> None:
    """Main entry: start APScheduler and run until SIGINT/SIGTERM."""
    db = Database(settings.db_path)
    await db.connect()

    notifier = create_notifier(settings.telegram_token, settings.telegram_chat_id)

    scheduler = AsyncIOScheduler()
    _running_lock = asyncio.Lock()

    async def _guarded_pipeline() -> None:
        if _running_lock.locked():  # pragma: no cover — concurrency guard
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

    async def _run_settlement() -> None:
        try:
            await run_settlement(settings, db, notifier=notifier)
        except Exception as e:
            log.error("settlement_error", error=str(e))
            await notifier.send(format_alert("Settlement error", str(e)))

    # Daily settlement at 23:45 UTC (after all daily highs are recorded)
    scheduler.add_job(
        _run_settlement,
        trigger=CronTrigger(hour=23, minute=45, timezone=_UTC),
        coalesce=True,
        misfire_grace_time=3600,
        id="settlement_daily",
    )

    for offset in settings.offsets_utc:
        hour, minute = offset.split(":")
        scheduler.add_job(
            _guarded_pipeline,
            trigger=CronTrigger(hour=int(hour), minute=int(minute), timezone=_UTC),
            coalesce=True,
            misfire_grace_time=600,
            id=f"pipeline_{offset}",
        )

    # Arbitrage scanner: init with top-6 markets, then scan every minute
    _CITY_SLUGS = {
        "NYC": "nyc", "Miami": "miami", "Seoul": "seoul",
        "London": "london", "Shanghai": "shanghai", "Wellington": "wellington",
    }
    city_slugs = {c.name: _CITY_SLUGS[c.name] for c in settings.cities if c.name in _CITY_SLUGS}
    arb_scanner = ArbScanner(top_n=6, min_gap=0.05)
    _arb_lock = asyncio.Lock()
    _arb_discovered = False

    async def _arb_scan() -> None:
        """Lightweight every-minute arbitrage scan on cached top markets."""
        nonlocal _arb_discovered
        if _arb_lock.locked():
            return
        async with _arb_lock:
            try:
                import httpx
                async with httpx.AsyncClient() as http_client:
                    # First run: discover top-N markets
                    if not _arb_discovered:
                        n = await arb_scanner.discover(http_client, city_slugs)
                        _arb_discovered = n > 0
                        if not _arb_discovered:
                            log.warning("arb_discovery_no_markets")
                            return
                    import json as _json
                    signals = await arb_scanner.fast_scan(http_client)
                    for sig in signals:
                        log.info(
                            "arb_scanner_opportunity",
                            city=sig.city,
                            date=str(sig.date),
                            gap=round(sig.gap, 4),
                            price_sum=round(sig.price_sum, 4),
                            bucket_count=sig.bucket_count,
                        )
                        await notifier.send(
                            f"🎯 [Arbitrage] {sig.city} {sig.date}\n"
                            f"Buckets: {sig.bucket_count} | Sum: {sig.price_sum:.3f} | Gap: {sig.gap*100:.1f}%"
                        )
                        await db.record_arbitrage(
                            run_id="arb_scanner",
                            city=sig.city,
                            date=str(sig.date),
                            bucket_count=sig.bucket_count,
                            price_sum=sig.price_sum,
                            gap=sig.gap,
                            token_ids=_json.dumps(sig.token_ids),
                            acted_on=0,
                        )
            except Exception as e:
                log.warning("arb_scan_error", error=str(e))

    scheduler.add_job(
        _arb_scan,
        trigger="interval",
        seconds=60,
        coalesce=True,
        misfire_grace_time=30,
        id="arb_scan_1m",
    )

    scheduler.start()
    log.info(
        "scheduler_started",
        mode=settings.mode,
        windows=settings.offsets_utc,
        bankroll=settings.bankroll,
        telegram=enable_telegram,
        arb_scan_markets=arb_scanner.top_n,
    )

    # Run one immediate cycle
    await _guarded_pipeline()

    # Wait for shutdown signal
    stop_event = asyncio.Event()

    def _handle_signal() -> None:  # pragma: no cover — OS signal handler
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
