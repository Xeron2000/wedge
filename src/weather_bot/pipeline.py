from __future__ import annotations

import uuid
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import httpx
import structlog

from weather_bot.config import CityConfig, Settings
from weather_bot.db import Database
from weather_bot.execution.dry_run import DryRunExecutor
from weather_bot.execution.live import LiveExecutor
from weather_bot.execution.models import OrderRequest
from weather_bot.log import get_logger
from weather_bot.market.models import MarketBucket
from weather_bot.market.polymarket import PolymarketClient
from weather_bot.market.scanner import scan_weather_markets
from weather_bot.strategy.edge import detect_edges
from weather_bot.strategy.ladder import evaluate_ladder
from weather_bot.strategy.portfolio import allocate
from weather_bot.strategy.tail import evaluate_tail
from weather_bot.weather.client import fetch_ensemble
from weather_bot.weather.ensemble import parse_distribution

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

    # Set up executor and shared Polymarket client
    poly_client: PolymarketClient | None = None
    if settings.mode == "live":
        poly_client = PolymarketClient(
            settings.polymarket_private_key,
            settings.polymarket_api_key,
            settings.polymarket_api_secret,
        )
        await poly_client.connect()
        executor = LiveExecutor(db, poly_client, settings.bankroll, settings.max_bet)
    else:
        executor = DryRunExecutor(db, settings.bankroll, settings.max_bet)

    # Budget allocation
    ladder_budget, tail_budget, _ = allocate(
        settings.bankroll,
        settings.strategy.ladder.allocation,
        settings.strategy.tail.allocation,
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
            except Exception as e:
                log.error("city_failed", city=city_cfg.name, error=str(e))

    status = "completed"
    await db.complete_run(run_id, datetime.now(UTC).isoformat(), status)
    await db.insert_bankroll_snapshot(
        await executor.get_balance(), 0, datetime.now(UTC).isoformat()
    )
    log.info("pipeline_complete", total_orders=total_orders)

    # Send notification if notifier is available
    if notifier and hasattr(notifier, "send"):
        from weather_bot.monitoring.notify import format_pipeline_summary

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

    # 4. Scan market (dry-run uses synthetic data if no real market)
    if settings.mode == "dry_run":
        markets = _generate_synthetic_markets(forecast, city_cfg.name, target_date)
    elif poly_client:
        markets = await scan_weather_markets(poly_client, city_cfg.name, target_date)
    else:
        markets = []

    if not markets:
        log.warning("no_markets", city=city_cfg.name)
        return 0

    # 5. Detect edges
    signals = detect_edges(
        forecast,
        markets,
        ladder_threshold=settings.strategy.ladder.edge_threshold,
        tail_threshold=settings.strategy.tail.edge_threshold,
    )
    if not signals:
        log.info("no_edges", city=city_cfg.name)
        return 0

    log.info("edges_found", city=city_cfg.name, count=len(signals))

    # 6. Generate positions
    ladder_positions = evaluate_ladder(
        signals, ladder_budget,
        edge_threshold=settings.strategy.ladder.edge_threshold,
        kelly_fraction=settings.kelly_fraction,
        max_bet=settings.max_bet,
        max_bet_pct=settings.max_bet_pct,
    )
    tail_positions = evaluate_tail(
        signals, tail_budget,
        edge_threshold=settings.strategy.tail.edge_threshold,
        min_odds=settings.strategy.tail.min_odds,
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
        noise = rng.uniform(-0.05, 0.03)
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


async def run_single_scan(settings: Settings, city_name: str) -> None:
    """Run a single scan for one city (CLI scan command)."""
    from weather_bot.log import setup_logging
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
