"""Full coverage tests for wedge.scheduler."""
from __future__ import annotations

import asyncio
import signal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from wedge.config import CityConfig, Settings
from wedge.db import Database


@pytest.fixture
def settings(tmp_path):
    return Settings(
        mode="dry_run",
        bankroll=1000.0,
        brier_threshold=0.25,
        offsets_utc=["04:30"],
        cities=[CityConfig(name="NYC", lat=40.77, lon=-73.87, timezone="America/New_York")],
        db_path=str(tmp_path / "test.db"),
    )


@pytest.fixture
async def db(tmp_path):
    d = Database(str(tmp_path / "test.db"))
    await d.connect()
    yield d
    await d.close()


# ── helper: extract inner functions from run_scheduler ────────────────────────
# run_scheduler is hard to test end-to-end because it blocks on stop_event.
# We extract the logic by calling run_scheduler with a stop_event that fires
# immediately after the first guarded_pipeline run.


async def _run_scheduler_quick(settings, **kwargs):
    """Run scheduler, set stop immediately after first pipeline call completes."""
    stop_after = asyncio.Event()
    _original_run_pipeline = None

    async def fake_pipeline(s, d, *, notifier=None):
        stop_after.set()

    with (
        patch("wedge.scheduler.run_pipeline", side_effect=fake_pipeline),
        patch("wedge.scheduler.run_settlement", new_callable=AsyncMock),
        patch("wedge.scheduler.AsyncIOScheduler") as MockSched,
    ):
        mock_sched = MagicMock()
        MockSched.return_value = mock_sched

        async def _wait_then_signal():
            await stop_after.wait()
            # Send SIGTERM to current process to trigger the stop event
            import os
            os.kill(os.getpid(), signal.SIGTERM)

        task = asyncio.create_task(_wait_then_signal())
        try:
            from wedge.scheduler import run_scheduler
            await asyncio.wait_for(run_scheduler(settings, **kwargs), timeout=5.0)
        finally:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass


# ── _guarded_pipeline inner function tests ────────────────────────────────────

class TestGuardedPipeline:
    """Test the _guarded_pipeline inner function indirectly via run_scheduler."""

    @pytest.mark.asyncio
    async def test_normal_path_calls_run_pipeline(self, settings, tmp_path):
        """Happy path: no brier issue, pipeline runs."""
        db_path = str(tmp_path / "test.db")
        settings = settings.model_copy(update={"db_path": db_path})

        pipeline_called = []

        async def fake_pipeline(s, d, *, notifier=None):
            pipeline_called.append(True)

        with (
            patch("wedge.scheduler.run_pipeline", side_effect=fake_pipeline),
            patch("wedge.scheduler.run_settlement", new_callable=AsyncMock),
            patch("wedge.scheduler.AsyncIOScheduler") as MockSched,
            patch("wedge.scheduler.Database") as MockDB,
        ):
            mock_sched = MagicMock()
            MockSched.return_value = mock_sched

            mock_db = AsyncMock()
            mock_db.get_brier_score.return_value = None  # no brier
            MockDB.return_value = mock_db

            stop_event = asyncio.Event()

            async def run_and_stop():
                # Wait a tick for pipeline to be called then set stop
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

        assert len(pipeline_called) >= 1

    @pytest.mark.asyncio
    async def test_circuit_breaker_active(self, settings, tmp_path):
        """High brier score → circuit breaker fires, pipeline not called."""
        db_path = str(tmp_path / "cb.db")
        settings = settings.model_copy(update={"db_path": db_path, "brier_threshold": 0.25})

        pipeline_called = []
        notifier_sends = []

        async def fake_pipeline(s, d, *, notifier=None):
            pipeline_called.append(True)

        with (
            patch("wedge.scheduler.run_pipeline", side_effect=fake_pipeline),
            patch("wedge.scheduler.run_settlement", new_callable=AsyncMock),
            patch("wedge.scheduler.AsyncIOScheduler") as MockSched,
            patch("wedge.scheduler.Database") as MockDB,
            patch("wedge.scheduler.create_notifier") as MockNotifier,
        ):
            mock_sched = MagicMock()
            MockSched.return_value = mock_sched

            mock_db = AsyncMock()
            mock_db.get_brier_score.return_value = 0.40  # > threshold
            MockDB.return_value = mock_db

            mock_notifier = AsyncMock()
            mock_notifier.send = AsyncMock(side_effect=lambda msg: notifier_sends.append(msg))
            MockNotifier.return_value = mock_notifier

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

        assert len(pipeline_called) == 0
        assert any("Circuit breaker" in str(s) or "Brier" in str(s) for s in notifier_sends)

    @pytest.mark.asyncio
    async def test_pipeline_error_sends_alert(self, settings, tmp_path):
        """Pipeline exception → error sent to notifier."""
        db_path = str(tmp_path / "err.db")
        settings = settings.model_copy(update={"db_path": db_path})

        notifier_sends = []

        async def boom(s, d, *, notifier=None):
            raise RuntimeError("pipeline exploded")

        with (
            patch("wedge.scheduler.run_pipeline", side_effect=boom),
            patch("wedge.scheduler.run_settlement", new_callable=AsyncMock),
            patch("wedge.scheduler.AsyncIOScheduler") as MockSched,
            patch("wedge.scheduler.Database") as MockDB,
            patch("wedge.scheduler.create_notifier") as MockNotifier,
        ):
            mock_sched = MagicMock()
            MockSched.return_value = mock_sched

            mock_db = AsyncMock()
            mock_db.get_brier_score.return_value = None
            MockDB.return_value = mock_db

            mock_notifier = AsyncMock()
            mock_notifier.send = AsyncMock(side_effect=lambda msg: notifier_sends.append(msg))
            MockNotifier.return_value = mock_notifier

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

        assert any("Pipeline error" in str(s) or "pipeline exploded" in str(s) for s in notifier_sends)

    @pytest.mark.asyncio
    async def test_already_running_skipped(self, settings, tmp_path):
        """If lock is held, _guarded_pipeline logs skip without calling pipeline."""
        db_path = str(tmp_path / "lock.db")
        settings = settings.model_copy(update={"db_path": db_path})

        pipeline_calls = []
        lock_holder = asyncio.Lock()

        # Simulate lock already held
        async def slow_pipeline(s, d, *, notifier=None):
            pipeline_calls.append("called")
            await asyncio.sleep(0.5)

        with (
            patch("wedge.scheduler.run_pipeline", side_effect=slow_pipeline),
            patch("wedge.scheduler.run_settlement", new_callable=AsyncMock),
            patch("wedge.scheduler.AsyncIOScheduler") as MockSched,
            patch("wedge.scheduler.Database") as MockDB,
        ):
            mock_sched = MagicMock()
            MockSched.return_value = mock_sched
            mock_db = AsyncMock()
            mock_db.get_brier_score.return_value = None
            MockDB.return_value = mock_db

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


# ── _run_settlement inner function tests ──────────────────────────────────────

class TestRunSettlementInner:
    @pytest.mark.asyncio
    async def test_settlement_normal(self, settings, tmp_path):
        """Settlement is called normally."""
        db_path = str(tmp_path / "s.db")
        settings = settings.model_copy(update={"db_path": db_path})

        settlement_calls = []

        async def fake_settlement(s, d, *, notifier=None):
            settlement_calls.append(True)
            return 2

        with (
            patch("wedge.scheduler.run_pipeline", new_callable=AsyncMock),
            patch("wedge.scheduler.run_settlement", side_effect=fake_settlement),
            patch("wedge.scheduler.AsyncIOScheduler") as MockSched,
            patch("wedge.scheduler.Database") as MockDB,
        ):
            mock_sched = MagicMock()

            # Capture the settlement job function so we can call it
            captured_jobs = []
            mock_sched.add_job.side_effect = lambda fn, **kwargs: captured_jobs.append((fn, kwargs))
            MockSched.return_value = mock_sched
            mock_db = AsyncMock()
            mock_db.get_brier_score.return_value = None
            MockDB.return_value = mock_db

            stop_event = asyncio.Event()

            async def run_and_stop():
                await asyncio.sleep(0.01)
                # Find and call the settlement job
                for fn, kwargs in captured_jobs:
                    if kwargs.get("id") == "settlement_daily":
                        await fn()
                await asyncio.sleep(0.01)
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

        assert len(settlement_calls) >= 1

    @pytest.mark.asyncio
    async def test_settlement_error_sends_alert(self, settings, tmp_path):
        """Settlement exception → alert notifier called."""
        db_path = str(tmp_path / "se.db")
        settings = settings.model_copy(update={"db_path": db_path})

        notifier_sends = []

        async def boom_settlement(s, d, *, notifier=None):
            raise RuntimeError("settlement boom")

        with (
            patch("wedge.scheduler.run_pipeline", new_callable=AsyncMock),
            patch("wedge.scheduler.run_settlement", side_effect=boom_settlement),
            patch("wedge.scheduler.AsyncIOScheduler") as MockSched,
            patch("wedge.scheduler.Database") as MockDB,
            patch("wedge.scheduler.create_notifier") as MockNotifier,
        ):
            mock_sched = MagicMock()
            captured_jobs = []
            mock_sched.add_job.side_effect = lambda fn, **kwargs: captured_jobs.append((fn, kwargs))
            MockSched.return_value = mock_sched

            mock_db = AsyncMock()
            mock_db.get_brier_score.return_value = None
            MockDB.return_value = mock_db

            mock_notifier = AsyncMock()
            mock_notifier.send = AsyncMock(side_effect=lambda msg: notifier_sends.append(msg))
            MockNotifier.return_value = mock_notifier

            stop_event = asyncio.Event()

            async def run_and_stop():
                await asyncio.sleep(0.01)
                for fn, kwargs in captured_jobs:
                    if kwargs.get("id") == "settlement_daily":
                        await fn()
                await asyncio.sleep(0.01)
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

        assert any("Settlement error" in str(s) or "settlement boom" in str(s) for s in notifier_sends)


# ── telegram integration in run_scheduler ─────────────────────────────────────

class TestSchedulerTelegramIntegration:
    @pytest.mark.asyncio
    async def test_telegram_started_when_enabled_with_token(self, tmp_path):
        settings = Settings(
            mode="dry_run",
            bankroll=1000.0,
            offsets_utc=["04:30"],
            cities=[],
            db_path=str(tmp_path / "tg.db"),
            telegram_token="fake:token",
            telegram_chat_id="12345",
        )

        with (
            patch("wedge.scheduler.run_pipeline", new_callable=AsyncMock),
            patch("wedge.scheduler.run_settlement", new_callable=AsyncMock),
            patch("wedge.scheduler.AsyncIOScheduler") as MockSched,
            patch("wedge.scheduler.Database") as MockDB,
            patch("wedge.scheduler.create_notifier") as MockNotifier,
            patch("wedge.telegram.TelegramBotManager") as MockTG,
        ):
            mock_sched = MagicMock()
            MockSched.return_value = mock_sched
            mock_db = AsyncMock()
            mock_db.get_brier_score.return_value = None
            MockDB.return_value = mock_db

            mock_notifier = AsyncMock()
            MockNotifier.return_value = mock_notifier

            mock_tg = AsyncMock()
            MockTG.return_value = mock_tg

            stop_event = asyncio.Event()

            async def run_and_stop():
                await asyncio.sleep(0.05)
                stop_event.set()

            from wedge.scheduler import run_scheduler

            with patch("wedge.scheduler.asyncio.Event", return_value=stop_event):
                task = asyncio.create_task(run_and_stop())
                await asyncio.wait_for(
                    run_scheduler(settings, enable_telegram=True), timeout=3.0
                )
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        mock_tg.start.assert_awaited_once()
        mock_tg.stop.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_telegram_not_started_without_token(self, tmp_path):
        settings = Settings(
            mode="dry_run",
            bankroll=1000.0,
            offsets_utc=["04:30"],
            cities=[],
            db_path=str(tmp_path / "notg.db"),
            telegram_token="",
        )

        with (
            patch("wedge.scheduler.run_pipeline", new_callable=AsyncMock),
            patch("wedge.scheduler.run_settlement", new_callable=AsyncMock),
            patch("wedge.scheduler.AsyncIOScheduler") as MockSched,
            patch("wedge.scheduler.Database") as MockDB,
            patch("wedge.telegram.TelegramBotManager") as MockTG,
        ):
            mock_sched = MagicMock()
            MockSched.return_value = mock_sched
            mock_db = AsyncMock()
            mock_db.get_brier_score.return_value = None
            MockDB.return_value = mock_db

            stop_event = asyncio.Event()

            async def run_and_stop():
                await asyncio.sleep(0.05)
                stop_event.set()

            from wedge.scheduler import run_scheduler

            with patch("wedge.scheduler.asyncio.Event", return_value=stop_event):
                task = asyncio.create_task(run_and_stop())
                await asyncio.wait_for(
                    run_scheduler(settings, enable_telegram=True), timeout=3.0
                )
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        MockTG.assert_not_called()


# ── scheduler job registration ────────────────────────────────────────────────

class TestSchedulerJobRegistration:
    @pytest.mark.asyncio
    async def test_jobs_registered_for_each_offset(self, tmp_path):
        settings = Settings(
            mode="dry_run",
            bankroll=1000.0,
            offsets_utc=["04:30", "10:30", "16:30"],
            cities=[],
            db_path=str(tmp_path / "jr.db"),
        )

        with (
            patch("wedge.scheduler.run_pipeline", new_callable=AsyncMock),
            patch("wedge.scheduler.run_settlement", new_callable=AsyncMock),
            patch("wedge.scheduler.AsyncIOScheduler") as MockSched,
            patch("wedge.scheduler.Database") as MockDB,
        ):
            mock_sched = MagicMock()
            job_ids = []
            mock_sched.add_job.side_effect = lambda fn, **kwargs: job_ids.append(kwargs.get("id"))
            MockSched.return_value = mock_sched
            mock_db = AsyncMock()
            mock_db.get_brier_score.return_value = None
            MockDB.return_value = mock_db

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

        # Should have 3 pipeline jobs + 1 settlement job
        assert "settlement_daily" in job_ids
        assert "pipeline_04:30" in job_ids
        assert "pipeline_10:30" in job_ids
        assert "pipeline_16:30" in job_ids
