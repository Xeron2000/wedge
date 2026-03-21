from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from wedge.config import CityConfig, Settings


@pytest.fixture
def settings(tmp_path):
    return Settings(
        mode="dry_run",
        bankroll=1000.0,
        brier_threshold=0.25,
        scheduler_brier_days=30,
        readiness_mode="active",
        readiness_probe_start_offset_minutes=200,
        readiness_probe_fast_poll_seconds=30,
        readiness_probe_fast_until_minutes=250,
        readiness_probe_slow_poll_seconds=10,
        readiness_probe_timeout_minutes=270,
        readiness_probe_max_attempts=180,
        readiness_fetch_concurrency=16,
        readiness_error_rate_threshold=0.05,
        enable_parallel_noaa_fetch=True,
        offsets_utc=["04:30"],
        cities=[CityConfig(name="NYC", lat=40.77, lon=-73.87, timezone="America/New_York")],
        db_path=str(tmp_path / "test.db"),
    )


@pytest.mark.asyncio
async def test_scheduler_active_mode_skips_already_claimed_cycle(settings):
    from wedge.weather.client import parse_readiness_probe

    probe = parse_readiness_probe(
        run_date=datetime(2026, 3, 20, tzinfo=UTC).date(),
        cycle_hour=0,
        target_date=datetime(2026, 3, 21, tzinfo=UTC).date(),
        forecast_hours=[24],
        prefetched_temperatures={"c00": [80.0], "p01": [79.0]},
        ready=True,
        reason="ready",
        checked_at=datetime.now(UTC),
        attempts=1,
    )

    with (
        patch("wedge.scheduler.probe_cycle_readiness", new_callable=AsyncMock, return_value=probe),
        patch("wedge.scheduler.run_pipeline", new_callable=AsyncMock) as mock_pipeline,
        patch("wedge.scheduler.run_settlement", new_callable=AsyncMock),
        patch("wedge.scheduler.AsyncIOScheduler") as mock_sched_cls,
        patch("wedge.scheduler.Database") as mock_db_cls,
    ):
        mock_sched = MagicMock()
        mock_sched_cls.return_value = mock_sched
        mock_db = AsyncMock()
        mock_db.get_brier_score.return_value = None
        mock_db.claim_cycle_marker.return_value = False
        mock_db_cls.return_value = mock_db

        stop_event = asyncio.Event()

        async def run_and_stop():
            await asyncio.sleep(0.05)
            stop_event.set()

        from wedge.scheduler import run_scheduler

        with patch("wedge.scheduler.asyncio.Event", return_value=stop_event):
            task = asyncio.create_task(run_and_stop())
            await asyncio.wait_for(run_scheduler(settings), timeout=3.0)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    mock_pipeline.assert_not_awaited()
    mock_db.update_cycle_marker_status.assert_not_awaited()


@pytest.mark.asyncio
async def test_scheduler_shadow_mode_preserves_legacy_pipeline_call(settings):
    from wedge.weather.client import parse_readiness_probe

    shadow_settings = settings.model_copy(update={"readiness_mode": "shadow"})
    probe = parse_readiness_probe(
        run_date=datetime(2026, 3, 20, tzinfo=UTC).date(),
        cycle_hour=0,
        target_date=datetime(2026, 3, 21, tzinfo=UTC).date(),
        forecast_hours=[24],
        prefetched_temperatures={},
        ready=False,
        reason="not_ready_timeout",
        checked_at=datetime.now(UTC),
        attempts=5,
    )

    with (
        patch("wedge.scheduler.probe_cycle_readiness", new_callable=AsyncMock, return_value=probe),
        patch("wedge.scheduler.run_pipeline", new_callable=AsyncMock) as mock_pipeline,
        patch("wedge.scheduler.run_settlement", new_callable=AsyncMock),
        patch("wedge.scheduler.AsyncIOScheduler") as mock_sched_cls,
        patch("wedge.scheduler.Database") as mock_db_cls,
    ):
        mock_sched = MagicMock()
        mock_sched_cls.return_value = mock_sched
        mock_db = AsyncMock()
        mock_db.get_brier_score.return_value = None
        mock_db_cls.return_value = mock_db

        stop_event = asyncio.Event()

        async def run_and_stop():
            await asyncio.sleep(0.05)
            stop_event.set()

        from wedge.scheduler import run_scheduler

        with patch("wedge.scheduler.asyncio.Event", return_value=stop_event):
            task = asyncio.create_task(run_and_stop())
            await asyncio.wait_for(run_scheduler(shadow_settings), timeout=3.0)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    mock_pipeline.assert_awaited()


@pytest.mark.asyncio
async def test_run_pipeline_uses_batch_forecast_insert_and_parallel_fetch_controls(tmp_path):
    from wedge.pipeline import _process_city
    from wedge.weather.models import ForecastDistribution

    settings = Settings(
        mode="dry_run",
        bankroll=1000.0,
        readiness_mode="active",
        readiness_fetch_concurrency=8,
        readiness_error_rate_threshold=0.05,
        enable_parallel_noaa_fetch=True,
        cities=[CityConfig(name="NYC", lat=40.77, lon=-73.87, timezone="America/New_York")],
        db_path=str(tmp_path / "test.db"),
    )
    db = AsyncMock()
    executor = AsyncMock()
    forecast = ForecastDistribution(
        city="NYC",
        date=datetime(2026, 3, 20, tzinfo=UTC).date(),
        buckets={70: 0.2, 71: 0.8},
        ensemble_spread=2.0,
        member_count=30,
        updated_at=datetime.now(UTC),
    )

    async with AsyncMock() as http_client:
        pass

    city_cfg = CityConfig(name="NYC", lat=40.77, lon=-73.87, timezone="America/New_York")

    fetch_result = {
        "source": "noaa_gefs",
        "status": "ready",
        "member_temps_f": {"c00": 80.0},
        "target_date": "2026-03-20",
        "run_time": datetime.now(UTC).isoformat(),
    }

    with (
        patch(
            "wedge.pipeline.fetch_ensemble",
            new_callable=AsyncMock,
            return_value=fetch_result,
        ) as mock_fetch,
        patch("wedge.pipeline.parse_distribution", return_value=forecast),
        patch("wedge.pipeline._generate_synthetic_markets", return_value=[]),
    ):
        import httpx
        async with httpx.AsyncClient() as http_client:
            await _process_city(
                http_client=http_client,
                settings=settings,
                db=db,
                executor=executor,
                city_cfg=city_cfg,
                target_date=datetime(2026, 3, 20, tzinfo=UTC).date(),
                run_id="run-1",
                ladder_budget=100.0,
                poly_client=None,
            )

    mock_fetch.assert_awaited_once()
    kwargs = mock_fetch.await_args.kwargs
    assert kwargs["parallel"] is True
    assert kwargs["max_concurrency"] == 8
    assert kwargs["error_rate_threshold"] == 0.05
    db.insert_forecasts_batch.assert_awaited_once()
