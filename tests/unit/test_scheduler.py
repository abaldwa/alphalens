"""
tests/unit/test_scheduler.py

Phase: 0.3 (Scheduler & Checkpoint Engine)
Specs: SPEC-SCHED-001 through SPEC-SCHED-011
Owner: Platform / Scheduler
Consumers: CI, pytest

Unit tests for gap_detector.py, checkpoint.py, and pipeline_scheduler.py's
backfill ordering.
"""

from datetime import date

import pytest

from ingestion.scheduler import gap_detector
from ingestion.scheduler.checkpoint import STEP_NAMES, CheckpointManager
from ingestion.scheduler.pipeline_scheduler import run_backfill


# ===== Gap detection =====
class TestGapDetection:
    def test_finds_missed_dates_excluding_holidays(self, monkeypatch):
        """
        SPEC-SCHED-003, SPEC-SCHED-004, SPEC-SCHED-008: a gap spanning a
        full Mon-Fri trading week, with 2 of those 5 days declared NSE
        holidays, must yield exactly the remaining 3 trading dates,
        oldest first.

        Window: last_run_date = Sun 2026-01-04 (last successful run).
        Candidate gap days = Mon 01-05 .. Fri 01-09 (5 weekdays, no
        weekend inside the window). Holidays: Tue 01-06, Thu 01-08.
        Expected missed trading dates: Mon 01-05, Wed 01-07, Fri 01-09.
        """
        holidays = {date(2026, 1, 6), date(2026, 1, 8)}
        monkeypatch.setattr(
            gap_detector, "is_nse_holiday", lambda d: d in holidays
        )

        gaps = gap_detector.detect_gaps(
            last_run_date=date(2026, 1, 4),
            today=date(2026, 1, 10),
        )

        assert gaps == [date(2026, 1, 5), date(2026, 1, 7), date(2026, 1, 9)]

    def test_no_gap_when_last_run_was_yesterday(self):
        """SPEC-SCHED-003: a one-day-old run with no trading days between it and today yields no gap."""
        gaps = gap_detector.detect_gaps(
            last_run_date=date(2026, 1, 8), today=date(2026, 1, 9)
        )
        assert gaps == []

    def test_no_history_returns_empty(self, tmp_path):
        """SPEC-SCHED-003: first-ever run (empty pipeline_runs) has nothing to backfill."""
        from datastore.schema.create_signals import create_pipeline_runs_schema

        db_path = tmp_path / "pipeline_log_empty.db"
        create_pipeline_runs_schema(db_path=db_path)

        gaps = gap_detector.detect_gaps(
            last_run_date=None, today=date(2026, 1, 10), db_path=db_path
        )
        assert gaps == []

    def test_is_trading_day_excludes_weekends(self, monkeypatch):
        """SPEC-SCHED-008: weekends are never trading days, regardless of the holiday calendar."""
        monkeypatch.setattr(gap_detector, "is_nse_holiday", lambda d: False)
        saturday = date(2026, 1, 10)
        sunday = date(2026, 1, 11)
        monday = date(2026, 1, 12)
        assert gap_detector.is_trading_day(saturday) is False
        assert gap_detector.is_trading_day(sunday) is False
        assert gap_detector.is_trading_day(monday) is True


# ===== Checkpoint save / resume =====
class TestCheckpointManager:
    def test_save_and_resume_after_failure_at_compute_features(self):
        """
        SPEC-SCHED-002: simulate a failure at 'compute_features'. The next
        run must resume from that exact step, not restart from step 0.
        """
        ckpt = CheckpointManager(in_memory=True)
        run_date = date(2026, 1, 5)

        for step in ["download_bhavcopy", "download_fno", "download_macro", "adjust_prices"]:
            ckpt.save_checkpoint(run_date, step, status="success")
        ckpt.save_checkpoint(
            run_date, "compute_features", status="failed", error_message="boom"
        )

        assert ckpt.load_checkpoint(run_date) == "adjust_prices"
        assert ckpt.get_resume_step(run_date) == "compute_features"

    def test_resume_step_is_first_step_when_nothing_succeeded(self):
        """SPEC-SCHED-002: with no checkpoints at all, resume starts at step 0."""
        ckpt = CheckpointManager(in_memory=True)
        run_date = date(2026, 1, 6)
        assert ckpt.get_resume_step(run_date) == STEP_NAMES[0]
        assert ckpt.load_checkpoint(run_date) is None

    def test_resume_step_is_none_when_all_steps_succeeded(self):
        """SPEC-SCHED-002: once every step has succeeded, there is nothing left to resume."""
        ckpt = CheckpointManager(in_memory=True)
        run_date = date(2026, 1, 7)
        for step in STEP_NAMES:
            ckpt.save_checkpoint(run_date, step, status="success")

        assert ckpt.get_resume_step(run_date) is None
        assert ckpt.load_checkpoint(run_date) == STEP_NAMES[-1]

    def test_checkpoints_are_per_date(self):
        """Checkpoints for one date must not leak into another date's resume state."""
        ckpt = CheckpointManager(in_memory=True)
        date_a, date_b = date(2026, 1, 5), date(2026, 1, 6)

        ckpt.save_checkpoint(date_a, "download_bhavcopy", status="success")

        assert ckpt.load_checkpoint(date_a) == "download_bhavcopy"
        assert ckpt.load_checkpoint(date_b) is None
        assert ckpt.get_resume_step(date_b) == STEP_NAMES[0]


# ===== Backfill ordering =====
class TestBackfillOrdering:
    def test_processes_dates_oldest_first(self):
        """SPEC-SCHED-004: backfill must process dates chronologically, oldest first, regardless of input order."""
        calls = []

        def fake_step_runner(run_date, step_name):
            calls.append((run_date, step_name))

        ckpt = CheckpointManager(in_memory=True)
        unordered_dates = [date(2026, 1, 7), date(2026, 1, 5), date(2026, 1, 6)]

        processed = run_backfill(unordered_dates, fake_step_runner, ckpt)

        assert processed == sorted(unordered_dates)

        # The first distinct date seen in call order must be the oldest,
        # and dates must never appear out of order once introduced.
        seen_order = []
        for run_date, _ in calls:
            if not seen_order or seen_order[-1] != run_date:
                seen_order.append(run_date)
        assert seen_order == sorted(unordered_dates)

    def test_backfill_never_runs_model_inference_steps(self):
        """SPEC-SCHED-006: run_models and write_signals must never execute during backfill."""
        calls = []

        def fake_step_runner(run_date, step_name):
            calls.append(step_name)

        ckpt = CheckpointManager(in_memory=True)
        run_backfill([date(2026, 1, 5)], fake_step_runner, ckpt)

        assert "run_models" not in calls
        assert "write_signals" not in calls
        assert "download_bhavcopy" in calls
        assert "compute_features" in calls


# ===== Backfill catch-up scheduling (SPEC-SCHED-012) =====
class TestBackfillCatchupScheduling:
    def test_schedule_backfill_catchup_registers_daily_cron_job(self):
        """SPEC-SCHED-012: must register a daily (no day_of_week restriction) cron job at the configured time."""
        from apscheduler.schedulers.background import BackgroundScheduler

        from ingestion.scheduler.pipeline_scheduler import schedule_backfill_catchup

        scheduler = BackgroundScheduler()
        schedule_backfill_catchup(scheduler, schedule_time="20:00")

        jobs = scheduler.get_jobs()
        assert len(jobs) == 1
        job = jobs[0]
        assert job.id == "backfill_catchup"
        trigger_str = str(job.trigger)
        assert "hour='20'" in trigger_str
        assert "minute='0'" in trigger_str
        # No day_of_week restriction -> must fire every day, unlike the Mon-Fri daily pipeline.
        assert "day_of_week" not in trigger_str

    def test_execute_backfill_catchup_skips_safely_with_no_cached_token(self, monkeypatch):
        """
        SPEC-SCHED-012: an unattended scheduled run must never reach FYERS'
        interactive OAuth2 fallback (which blocks forever on input() with
        no connected stdin) -- it must detect a missing/invalid token and
        skip cleanly instead.
        """
        import ingestion.scheduler.pipeline_scheduler as ps
        from ingestion.scrapers.fyers_backfill import FYERSBackfill

        monkeypatch.setattr(FYERSBackfill, "_load_cached_token", lambda self: None)
        # SPEC-SCHED-013: _execute_backfill_catchup now writes a heartbeat on
        # every exit path — mock it out so this test never touches the real
        # PIPELINE_LOG_DB_PATH.
        monkeypatch.setattr(ps, "_record_heartbeat", lambda *a, **k: None)

        run_backfill_calls = []
        monkeypatch.setattr(
            "ingestion.backfill_runner.run_backfill",
            lambda tickers, frm, to: run_backfill_calls.append((tickers, frm, to)),
        )

        ps._execute_backfill_catchup()

        assert run_backfill_calls == []

    def test_execute_backfill_catchup_skips_safely_with_invalid_cached_token(self, monkeypatch):
        """A cached token that fails live validation must also skip, not be used as-is."""
        import ingestion.scheduler.pipeline_scheduler as ps
        from ingestion.scrapers.fyers_backfill import FYERSBackfill

        monkeypatch.setattr(FYERSBackfill, "_load_cached_token", lambda self: "stale-token")
        monkeypatch.setattr(FYERSBackfill, "_validate_token", lambda self, token: False)
        monkeypatch.setattr(ps, "_record_heartbeat", lambda *a, **k: None)

        run_backfill_calls = []
        monkeypatch.setattr(
            "ingestion.backfill_runner.run_backfill",
            lambda tickers, frm, to: run_backfill_calls.append((tickers, frm, to)),
        )

        ps._execute_backfill_catchup()

        assert run_backfill_calls == []

    def test_execute_backfill_catchup_runs_with_valid_cached_token(self, monkeypatch):
        """A valid same-day cached token must let the catch-up proceed and call run_backfill."""
        import ingestion.scheduler.pipeline_scheduler as ps
        from ingestion.scrapers.fyers_backfill import FYERSBackfill

        monkeypatch.setattr(FYERSBackfill, "_load_cached_token", lambda self: "good-token")
        monkeypatch.setattr(FYERSBackfill, "_validate_token", lambda self, token: True)
        monkeypatch.setattr("config.universe.get_tickers", lambda: ["AAA", "BBB"])

        heartbeat_calls = []
        monkeypatch.setattr(ps, "_record_heartbeat", lambda *a, **k: heartbeat_calls.append((a, k)))

        run_backfill_calls = []
        monkeypatch.setattr(
            "ingestion.backfill_runner.run_backfill",
            lambda tickers, frm, to: run_backfill_calls.append((tickers, frm, to)),
        )

        ps._execute_backfill_catchup()

        assert len(run_backfill_calls) == 1
        assert heartbeat_calls == [(("backfill_catchup", "success"), {})]
        assert run_backfill_calls[0][0] == ["AAA", "BBB"]


class TestMFHoldingsScheduling:
    """SPEC-PIPE-003: Groww (P2.2's primary MF-holdings source) exposes no historical archive — only "now"."""

    def test_schedule_mf_holdings_ingestion_registers_twice_monthly_cron_job(self):
        """config.settings.MF_HOLDINGS_SCHEDULE_DAYS = '5,20' -> must fire on both days, not just one."""
        from apscheduler.schedulers.background import BackgroundScheduler

        from ingestion.scheduler.pipeline_scheduler import schedule_mf_holdings_ingestion

        scheduler = BackgroundScheduler()
        schedule_mf_holdings_ingestion(scheduler, days="5,20", schedule_time="08:00")

        jobs = scheduler.get_jobs()
        assert len(jobs) == 1
        job = jobs[0]
        assert job.id == "mf_holdings_ingestion"
        trigger_str = str(job.trigger)
        assert "day='5,20'" in trigger_str
        assert "hour='8'" in trigger_str

    def test_determine_groww_live_snapshot_month_reads_first_available_portfolio_date(self, monkeypatch):
        import ingestion.scheduler.pipeline_scheduler as ps

        monkeypatch.setattr(
            "ingestion.scrapers.groww_mf_holdings._list_scheme_ids", lambda fund_house: ["scheme-a", "scheme-b"]
        )
        monkeypatch.setattr(
            "ingestion.scrapers.groww_mf_holdings._fetch_scheme_detail",
            lambda scheme_id: {"holdings": [{"portfolio_date": "2026-05-30T18:30:00.000Z"}]},
        )

        year, month = ps._determine_groww_live_snapshot_month()

        assert (year, month) == (2026, 5)

    def test_determine_groww_live_snapshot_month_skips_schemes_with_no_holdings(self, monkeypatch):
        """The first scheme sampled might genuinely have no holdings (e.g. a brand-new NFO) — must try the next one."""
        import ingestion.scheduler.pipeline_scheduler as ps

        monkeypatch.setattr(
            "ingestion.scrapers.groww_mf_holdings._list_scheme_ids",
            lambda fund_house: ["empty-scheme", "real-scheme"],
        )

        def fake_detail(scheme_id):
            if scheme_id == "empty-scheme":
                return {"holdings": []}
            return {"holdings": [{"portfolio_date": "2026-06-15T18:30:00.000Z"}]}

        monkeypatch.setattr("ingestion.scrapers.groww_mf_holdings._fetch_scheme_detail", fake_detail)

        year, month = ps._determine_groww_live_snapshot_month()

        assert (year, month) == (2026, 6)

    def test_determine_groww_live_snapshot_month_raises_if_nothing_found(self, monkeypatch):
        import ingestion.scheduler.pipeline_scheduler as ps

        monkeypatch.setattr("ingestion.scrapers.groww_mf_holdings._list_scheme_ids", lambda fund_house: [])

        with pytest.raises(ConnectionError, match="Could not determine"):
            ps._determine_groww_live_snapshot_month()

    def test_execute_mf_holdings_job_runs_ingestion_for_the_determined_month(self, monkeypatch):
        import ingestion.scheduler.pipeline_scheduler as ps

        monkeypatch.setattr(ps, "_determine_groww_live_snapshot_month", lambda: (2026, 5))
        monkeypatch.setattr("ingestion.scrapers.groww_mf_holdings.register_all_amcs", lambda: 49)

        ingestion_calls = []
        monkeypatch.setattr(
            "ingestion.scrapers.amfi_holdings.run_monthly_ingestion",
            lambda year, month: ingestion_calls.append((year, month)),
        )

        heartbeat_calls = []
        monkeypatch.setattr(ps, "_record_heartbeat", lambda *a, **k: heartbeat_calls.append((a, k)))

        ps._execute_mf_holdings_job()

        assert ingestion_calls == [(2026, 5)]
        assert heartbeat_calls == [(("mf_holdings_ingestion", "success"), {})]

    def test_execute_mf_holdings_job_records_failure_heartbeat_on_unexpected_exception(self, monkeypatch):
        import ingestion.scheduler.pipeline_scheduler as ps

        def boom():
            raise ValueError("network broke")

        monkeypatch.setattr("ingestion.scrapers.groww_mf_holdings.register_all_amcs", boom)

        heartbeat_calls = []
        monkeypatch.setattr(ps, "_record_heartbeat", lambda *a, **k: heartbeat_calls.append((a, k)))

        ps._execute_mf_holdings_job()

        assert heartbeat_calls[0][0][0] == "mf_holdings_ingestion"
        assert heartbeat_calls[0][0][1] == "failed"
