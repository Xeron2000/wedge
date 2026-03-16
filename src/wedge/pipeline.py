from __future__ import annotations

import uuid
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

import httpx
import structlog

from wedge.config import CityConfig, Settings
from wedge.db import Database
from wedge.execution.models import OrderRequest
from wedge.log import get_logger
from wedge.market.models import MarketBucket
from wedge.market.polymarket import PolymarketClient
from wedge.market.scanner import scan_weather_markets
from wedge.strategy.edge import detect_edges
from wedge.strategy.ladder import evaluate_ladder
from wedge.strategy.portfolio import allocate
from wedge.strategy.tail import evaluate_tail
from wedge.weather.client import fetch_actual_temperature, fetch_ensemble
from wedge.weather.ensemble import parse_distribution

if TYPE_CHECKING:
    from wedge.execution.dry_run import DryRunExecutor
    from wedge.execution.live import LiveExecutor
else:
    from wedge.execution.dry_run import DryRunExecutor
    from wedge.execution.live import LiveExecutor

log = get_logger("pipeline")


async def run_pipeline(
    settings: Settings, db: Database, *, notifier: object | None = None
) -> None:
    """Execute one full trading pipeline cycle across all cities."""
    run_id = uuid.uuid4().hex[:16]
    now = datetime.now(UTC)
    structlog.contextvars.bind_contextvars(run_id=run_id)

    await db.insert_run(run_id, now.isoformat())
    log.info("pipeline_start", mode=settings.mode, bankroll=settings.bankroll)

    # Restore balance from last snapshot (persists across pipeline runs)
    current_balance = await db.get_last_balance(default=settings.bankroll)

    # Set up executor and shared Polymarket client
    # IMPORTANT: Both modes need Polymarket client for real market data
    poly_client: PolymarketClient | None = None
    if settings.polymarket_api_key and settings.polymarket_api_secret:
        poly_client = PolymarketClient(
            settings.polymarket_private_key,
            settings.polymarket_api_key,
            settings.polymarket_api_secret,
        )
        await poly_client.connect()

    if settings.mode == "live":
        if not poly_client:
            raise ValueError("Live mode requires Polymarket API credentials")
        executor = LiveExecutor(db, poly_client, current_balance, settings.max_bet)
    else:
        executor = DryRunExecutor(db, current_balance, settings.max_bet)

    # Budget allocation based on current balance, not initial bankroll
    ladder_budget, tail_budget, _ = allocate(
        current_balance,
        settings.ladder_alloc,
        settings.tail_alloc,
    )

    total_orders = 0

    async with httpx.AsyncClient() as http_client:
        for city_cfg in settings.cities:
            try:
                # Compute target date per city timezone (contract settlement is local)
                city_tz = ZoneInfo(city_cfg.timezone)
                local_today = datetime.now(city_tz).date()
                target_date = local_today + timedelta(days=3)

                orders = await _process_city(
                    http_client=http_client,
                    settings=settings,
                    db=db,
                    executor=executor,
                    city_cfg=city_cfg,
                    target_date=target_date,
                    run_id=run_id,
                    ladder_budget=ladder_budget,
                    tail_budget=tail_budget,
                    poly_client=poly_client,
                )
                total_orders += orders

                # Update position prices for dry-run mode
                if settings.mode == "dry_run" and poly_client:
                    markets = await scan_weather_markets(
                        poly_client, city_cfg.name, target_date
                    )
                    await executor.update_position_prices(markets)

            except Exception as e:
                log.error("city_failed", city=city_cfg.name, error=str(e))

    status = "completed"
    await db.complete_run(run_id, datetime.now(UTC).isoformat(), status)

    # Calculate unrealized P&L for dry-run mode
    unrealized_pnl = 0.0
    if settings.mode == "dry_run":
        unrealized_pnl = await executor.get_unrealized_pnl()

    await db.insert_bankroll_snapshot(
        await executor.get_balance(), unrealized_pnl, datetime.now(UTC).isoformat()
    )
    log.info(
        "pipeline_complete",
        total_orders=total_orders,
        balance=await executor.get_balance(),
        unrealized_pnl=unrealized_pnl,
    )

    # Send notification if notifier is available
    if notifier and hasattr(notifier, "send"):
        from wedge.monitoring.notify import format_pipeline_summary

        summary = format_pipeline_summary(
            mode=settings.mode,
            cities=[c.name for c in settings.cities],
            edges_found=total_orders,  # approximate
            orders_placed=total_orders,
            balance=await executor.get_balance(),
        )
        await notifier.send(summary)

    structlog.contextvars.unbind_contextvars("run_id")


async def _process_city(
    *,
    http_client: httpx.AsyncClient,
    settings: Settings,
    db: Database,
    executor: DryRunExecutor | LiveExecutor,
    city_cfg: CityConfig,
    target_date: date,
    run_id: str,
    ladder_budget: float,
    tail_budget: float,
    poly_client: PolymarketClient | None = None,
) -> int:
    """Process a single city. Returns number of orders placed."""
    log.info("processing_city", city=city_cfg.name, date=str(target_date))

    # 1. Fetch weather data
    raw = await fetch_ensemble(http_client, city_cfg)
    if not raw:
        log.warning("no_weather_data", city=city_cfg.name)
        return 0

    # 2. Parse distribution
    forecast = parse_distribution(raw, city_cfg.name, target_date)
    if not forecast:
        log.warning("no_distribution", city=city_cfg.name)
        return 0

    # 3. Store forecasts
    for temp_f, prob in forecast.buckets.items():
        await db.insert_forecast(
            run_id=run_id,
            city=city_cfg.name,
            date=target_date.isoformat(),
            temp_f=temp_f,
            p_model=prob,
            created_at=datetime.now(UTC).isoformat(),
        )

    # 4. Scan market (prefer real market data when available)
    if poly_client:
        markets = await scan_weather_markets(poly_client, city_cfg.name, target_date)
    elif settings.mode == "dry_run":
        # Fallback to synthetic for dry-run without API credentials
        log.warning("no_polymarket_client_using_synthetic", city=city_cfg.name)
        markets = _generate_synthetic_markets(forecast, city_cfg.name, target_date)
    else:
        # Live mode requires real market data
        markets = []

    if not markets:
        log.warning("no_markets", city=city_cfg.name)
        return 0

    # 5. Detect edges
    signals = detect_edges(
        forecast,
        markets,
        ladder_threshold=settings.ladder_edge,
        tail_threshold=settings.tail_edge,
    )
    if not signals:
        log.info("no_edges", city=city_cfg.name)
        return 0

    log.info("edges_found", city=city_cfg.name, count=len(signals))

    # 6. Generate positions
    ladder_positions = evaluate_ladder(
        signals, ladder_budget,
        edge_threshold=settings.ladder_edge,
        kelly_fraction=settings.kelly_fraction,
        max_bet=settings.max_bet,
        max_bet_pct=settings.max_bet_pct,
    )
    tail_positions = evaluate_tail(
        signals, tail_budget,
        edge_threshold=settings.tail_edge,
        min_odds=settings.tail_odds,
        kelly_fraction=settings.kelly_fraction,
        max_bet=settings.max_bet,
        max_bet_pct=settings.max_bet_pct,
    )

    # 7. Execute orders
    orders = 0
    for pos in ladder_positions + tail_positions:
        request = OrderRequest(
            run_id=run_id,
            token_id=pos.bucket.token_id,
            city=pos.bucket.city,
            date=pos.bucket.date,
            temp_f=pos.bucket.temp_f,
            strategy=pos.strategy,
            limit_price=pos.entry_price,
            size=pos.size,
            p_model=pos.p_model,
            p_market=pos.entry_price,
            edge=pos.edge,
        )
        result = await executor.place_order(request)
        if result.success:
            orders += 1

    return orders


def _generate_synthetic_markets(
    forecast, city: str, target_date: date
) -> list[MarketBucket]:
    """Generate synthetic market buckets for dry-run testing.
    Simulates market inefficiency by adding noise to model probabilities.
    Seeded by city+date for reproducibility."""
    import random

    rng = random.Random(f"{city}_{target_date}")
    markets = []
    for temp_f, p_model in forecast.buckets.items():
        noise = rng.uniform(-0.10, 0.10)
        market_price = max(0.01, min(0.99, p_model + noise))
        markets.append(
            MarketBucket(
                token_id=f"syn_{city}_{target_date}_{temp_f}",
                city=city,
                date=target_date,
                temp_f=temp_f,
                market_price=round(market_price, 2),
                implied_prob=round(market_price, 2),
            )
        )
    return markets


async def run_settlement(
    settings: Settings, db: Database, *, notifier: object | None = None
) -> int:
    """Settle all trades whose target date has passed.

    Fetches actual observed temperatures and updates forecasts + trades.
    Returns total number of trades settled.
    """
    unsettled = await db.get_unsettled_dates()
    if not unsettled:
        log.info("settlement_no_pending")
        return 0

    log.info("settlement_start", pending_pairs=len(unsettled))

    city_map = {c.name: c for c in settings.cities}
    total_settled = 0

    async with httpx.AsyncClient() as http_client:
        for city_name, trade_date in unsettled:
            city_cfg = city_map.get(city_name)
            if not city_cfg:
                log.warning("settlement_unknown_city", city=city_name)
                continue

            actual_temp = await fetch_actual_temperature(
                http_client, city_cfg, trade_date
            )
            if actual_temp is None:
                log.warning("settlement_no_actual", city=city_name, date=trade_date)
                continue

            await db.update_forecast_actual(city_name, trade_date, actual_temp)
            count = await db.settle_trades(city_name, trade_date, actual_temp)
            total_settled += count
            log.info(
                "settlement_settled",
                city=city_name,
                date=trade_date,
                actual_temp=actual_temp,
                trades_settled=count,
            )

    if total_settled > 0 and notifier and hasattr(notifier, "send"):
        await notifier.send(
            f"[Settlement] Settled {total_settled} trade(s) across {len(unsettled)} date(s)"
        )

    log.info("settlement_complete", total_settled=total_settled)
    return total_settled


async def run_single_scan(settings: Settings, city_name: str) -> None:
    """Run a single scan for one city (CLI scan command)."""
    from wedge.log import setup_logging
    setup_logging()

    city_cfg = next(
        (c for c in settings.cities if c.name.lower() == city_name.lower()), None
    )
    if not city_cfg:
        log.error("city_not_found", city=city_name)
        return

    city_tz = ZoneInfo(city_cfg.timezone)
    target_date = (datetime.now(city_tz) + timedelta(days=3)).date()

    async with httpx.AsyncClient() as http_client:

        raw = await fetch_ensemble(http_client, city_cfg)
        if not raw:
            log.error("no_weather_data", city=city_name)
            return

        forecast = parse_distribution(raw, city_cfg.name, target_date)
        if not forecast:
            log.error("no_distribution", city=city_name)
            return

        log.info(
            "forecast_distribution",
            city=city_name,
            date=str(target_date),
            members=forecast.member_count,
            spread=f"{forecast.ensemble_spread:.1f}°F",
        )

        for temp_f in sorted(forecast.buckets):
            prob = forecast.buckets[temp_f]
            log.info("bucket", temp_f=temp_f, probability=f"{prob:.1%}")
