"""
tests/unit/test_scheduler.py

Phase: 0.3 (Scheduler & Checkpoint Engine)
Specs: SPEC-SCHED-001 through SPEC-SCHED-011
Owner: Platform / Scheduler
Consumers: CI, pytest

Unit tests for gap_detector.py, checkpoint.py, and pipeline_scheduler.py's
backfill ordering.
"""

import contextlib
from datetime import date

import pytest

from ingestion.scheduler import gap_detector
from ingestion.scheduler.checkpoint import STEP_NAMES, STEPS, CheckpointManager
from ingestion.scheduler.pipeline_scheduler import (
    _STEP_DEPS,
    run_backfill,
    run_morning_catchup_sequence,
    run_startup_sequence,
    run_steps_for_date,
)


@pytest.fixture(autouse=True)
def _isolated_pipeline_run_lock(tmp_path, monkeypatch):
    """
    run_steps_for_date acquires a real cross-process fcntl.flock on
    config.settings.PIPELINE_RUN_LOCK_PATH (see pipeline_run_lock's
    docstring, 2026-07-05 race-condition fix). Without this fixture, any
    test that calls run_steps_for_date/run_backfill/run_morning_catchup_
    sequence races against whatever real process holds the production
    lock file — e.g. the actual alphalens-scheduler.service, if it
    happens to be mid-run when tests execute — and silently gets skipped
    ("another run is already in progress"), which looks like a step-
    ordering bug but is really test/production lock-path collision.
    Point every test at its own tmp_path lock file instead.
    """
    import config.settings as settings_mod

    monkeypatch.setattr(settings_mod, "PIPELINE_RUN_LOCK_PATH", tmp_path / "pipeline_run.lock")


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

        # Every STEP_NAMES step strictly before compute_features must
        # actually be marked success — get_resume_step() walks STEP_NAMES
        # order looking for the first non-success step, so skipping one
        # (e.g. download_corporate_actions, inserted before adjust_prices)
        # would make that skipped step the resume point instead.
        steps_before_compute_features = STEP_NAMES[: STEP_NAMES.index("compute_features")]
        for step in steps_before_compute_features:
            ckpt.save_checkpoint(run_date, step, status="success")
        ckpt.save_checkpoint(
            run_date, "compute_features", status="failed", error_message="boom"
        )

        assert ckpt.load_checkpoint(run_date) == steps_before_compute_features[-1]
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

    def test_backfill_runs_model_inference_but_never_paper_trades(self):
        """2026-07-08 (user decision): run_models/write_signals/sanity_check
        ARE run during backfill — a missed trading day should still get its
        EOD signals computed and persisted, however late. Only paper_trade
        stays excluded from backfill (SPEC-SCHED-006/Gate 7: never auto-trade
        a gap day's signals retroactively)."""
        calls = []

        def fake_step_runner(run_date, step_name):
            calls.append(step_name)

        ckpt = CheckpointManager(in_memory=True)
        run_backfill([date(2026, 1, 5)], fake_step_runner, ckpt)

        assert "run_models" in calls
        assert "write_signals" in calls
        assert "sanity_check" in calls
        assert "paper_trade" not in calls
        assert "download_bhavcopy" in calls
        assert "compute_features" in calls


# ===== Morning catch-up: backward-only gap-backfill (2026-07, backlog #1/#2/#3) =====
class TestMorningCatchupSequence:
    """run_morning_catchup_sequence must retry only gap days strictly
    before `today` and must never attempt today's own pipeline run — the
    bug schedule_morning_catchup had while it reused run_startup_sequence
    (which always 404'd on "today" at 07:30 IST)."""

    def test_never_attempts_todays_own_pipeline_run(self, tmp_path):
        from datastore.schema.create_signals import create_pipeline_runs_schema

        db_path = tmp_path / "pipeline_log.db"
        create_pipeline_runs_schema(db_path=db_path)

        calls = []

        def fake_step_runner(run_date, step_name):
            calls.append(run_date)

        ckpt = CheckpointManager(in_memory=True)
        today = date(2026, 1, 10)

        # No prior successful run recorded -> detect_gaps returns [] (first
        # run, nothing to backfill) -- and today itself must never appear
        # in `calls` regardless.
        ok = run_morning_catchup_sequence(fake_step_runner, ckpt, today=today, db_path=db_path)

        assert ok is True
        assert today not in calls
        assert calls == []

    def test_backfills_gap_days_before_today_only(self, tmp_path, monkeypatch):
        from datastore.schema.create_signals import create_pipeline_runs_schema
        from datastore.api.db import get_sqlite_connection

        db_path = tmp_path / "pipeline_log.db"
        create_pipeline_runs_schema(db_path=db_path)
        # Seed one successful run so detect_gaps has a last_run_date to
        # walk forward from (2026-01-05 was a Monday).
        with get_sqlite_connection(db_path) as conn:
            conn.execute(
                "INSERT INTO pipeline_runs (date, started_at, completed_at, status, "
                "stocks_processed, error_message) VALUES (?, ?, ?, 'success', 0, NULL)",
                ("2026-01-05", "2026-01-05T18:00:00+05:30", "2026-01-05T18:05:00+05:30"),
            )
            conn.commit()

        monkeypatch.setattr(gap_detector, "is_nse_holiday", lambda d: False)

        calls = []

        def fake_step_runner(run_date, step_name):
            calls.append(run_date)

        ckpt = CheckpointManager(in_memory=True)
        today = date(2026, 1, 9)  # Friday

        ok = run_morning_catchup_sequence(fake_step_runner, ckpt, today=today, db_path=db_path)

        assert ok is True
        # Gap days are Tue/Wed/Thu (01-06, 01-07, 01-08) -- strictly between
        # the seeded last-success and today, both ends excluded.
        assert set(calls) == {date(2026, 1, 6), date(2026, 1, 7), date(2026, 1, 8)}
        assert today not in calls

    def test_returns_false_when_a_gap_day_fails(self, tmp_path, monkeypatch):
        from datastore.schema.create_signals import create_pipeline_runs_schema
        from datastore.api.db import get_sqlite_connection

        db_path = tmp_path / "pipeline_log.db"
        create_pipeline_runs_schema(db_path=db_path)
        with get_sqlite_connection(db_path) as conn:
            conn.execute(
                "INSERT INTO pipeline_runs (date, started_at, completed_at, status, "
                "stocks_processed, error_message) VALUES (?, ?, ?, 'success', 0, NULL)",
                ("2026-01-05", "2026-01-05T18:00:00+05:30", "2026-01-05T18:05:00+05:30"),
            )
            conn.commit()
        monkeypatch.setattr(gap_detector, "is_nse_holiday", lambda d: False)

        def failing_step_runner(run_date, step_name):
            if step_name == "download_bhavcopy":
                raise RuntimeError("boom")

        ckpt = CheckpointManager(in_memory=True)
        today = date(2026, 1, 9)

        ok = run_morning_catchup_sequence(failing_step_runner, ckpt, today=today, db_path=db_path)

        assert ok is False


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
        import ingestion.scheduler.scheduler_jobs as _sj
        from ingestion.scrapers.fyers_backfill import FYERSBackfill

        monkeypatch.setattr(FYERSBackfill, "_load_cached_token", lambda self: "good-token")
        monkeypatch.setattr(FYERSBackfill, "_validate_token", lambda self, token: True)
        monkeypatch.setattr("config.universe.get_tickers", lambda: ["AAA", "BBB"])

        heartbeat_calls = []

        def _hb(*a, **k):
            heartbeat_calls.append((a, k))
        # A46: _execute_backfill_catchup lives in scheduler_jobs and calls its
        # own module-local _record_heartbeat binding — patch it there too.
        monkeypatch.setattr(ps, "_record_heartbeat", _hb)
        monkeypatch.setattr(_sj, "_record_heartbeat", _hb)

        run_backfill_calls = []
        monkeypatch.setattr(
            "ingestion.backfill_runner.run_backfill",
            lambda tickers, frm, to, client=None: run_backfill_calls.append((tickers, frm, to, client)),
        )

        ps._execute_backfill_catchup()

        assert len(run_backfill_calls) == 1
        # 2026-07-30: the already-validated, non_interactive=True client
        # must actually be passed through to run_backfill(), not just
        # validated and discarded — see step's docstring for why (a
        # freshly-constructed default client would re-derive its own
        # token and could fall through to the hanging interactive flow).
        assert run_backfill_calls[0][3] is not None
        assert run_backfill_calls[0][3]._non_interactive is True
        assert len(heartbeat_calls) == 1
        args, kwargs = heartbeat_calls[0]
        assert args == ("backfill_catchup", "success")
        # A23: duration_seconds/peak_rss_mb are now measured and passed through.
        assert set(kwargs) == {"duration_seconds", "peak_rss_mb"}
        assert kwargs["duration_seconds"] >= 0
        assert kwargs["peak_rss_mb"] > 0
        assert run_backfill_calls[0][0] == ["AAA", "BBB"]


class TestFnoLateCatchupScheduling:
    """A56 follow-up (2026-07-30): download_fno was failing almost every
    day at 18:00 simply because NSE hadn't published that day's F&O
    bhavcopy yet. schedule_fno_late_catchup makes the one real attempt
    later, and re-triggers compute_features if it already ran off stale
    F&O data."""

    def test_schedule_registers_mon_fri_cron_job(self):
        from apscheduler.schedulers.background import BackgroundScheduler

        from ingestion.scheduler.pipeline_scheduler import schedule_fno_late_catchup

        scheduler = BackgroundScheduler()
        schedule_fno_late_catchup(scheduler, CheckpointManager(in_memory=True), schedule_time="21:00")

        jobs = scheduler.get_jobs()
        assert len(jobs) == 1
        job = jobs[0]
        assert job.id == "fno_late_catchup"
        trigger_str = str(job.trigger)
        assert "hour='21'" in trigger_str
        assert "minute='0'" in trigger_str
        assert "mon-fri" in trigger_str

    def test_skips_on_non_trading_day(self, monkeypatch):
        import ingestion.scheduler.pipeline_scheduler as ps
        import ingestion.scheduler.scheduler_jobs as _sj
        from datetime import datetime as dt_cls

        monkeypatch.setattr(_sj, "now_ist", lambda: dt_cls(2026, 8, 1))  # a Saturday
        monkeypatch.setattr(_sj, "is_trading_day", lambda d: False)

        heartbeat_calls = []

        def _hb(*a, **k):
            heartbeat_calls.append((a, k))
        monkeypatch.setattr(ps, "_record_heartbeat", _hb)
        monkeypatch.setattr(_sj, "_record_heartbeat", _hb)

        cm = CheckpointManager(in_memory=True)
        ps._execute_fno_late_catchup_job(cm)

        assert heartbeat_calls[0][0] == ("fno_late_catchup", "skipped", "not a trading day")

    def test_success_recomputes_features_when_already_ran_off_stale_data(self, monkeypatch):
        import ingestion.scheduler.pipeline_scheduler as ps
        import ingestion.scheduler.scheduler_jobs as _sj
        from datetime import datetime as dt_cls

        today = date(2026, 7, 30)
        monkeypatch.setattr(_sj, "now_ist", lambda: dt_cls(2026, 7, 30, 21, 0))
        monkeypatch.setattr(_sj, "is_trading_day", lambda d: True)

        heartbeat_calls = []

        def _hb(*a, **k):
            heartbeat_calls.append((a, k))
        monkeypatch.setattr(ps, "_record_heartbeat", _hb)
        monkeypatch.setattr(_sj, "_record_heartbeat", _hb)

        recompute_calls = []
        import ingestion.scheduler.daily_pipeline as dp
        monkeypatch.setattr(dp, "step_download_fno", lambda d: None)
        monkeypatch.setattr(dp, "step_compute_features", lambda d: recompute_calls.append(d))

        cm = CheckpointManager(in_memory=True)
        # compute_features already ran (as it always does — no dependency
        # on download_fno) off whatever stale F&O data was available.
        cm.save_checkpoint(today, "compute_features", status="success")

        ps._execute_fno_late_catchup_job(cm)

        assert recompute_calls == [today]
        assert heartbeat_calls[0][0][:2] == ("fno_late_catchup", "success")
        assert "features recomputed: True" in heartbeat_calls[0][0][2]

    def test_does_not_recompute_features_when_compute_features_has_not_run_yet(self, monkeypatch):
        """If compute_features hasn't run for today at all, there's
        nothing stale to fix — it'll naturally pick up today's now-
        available F&O data whenever it does run."""
        import ingestion.scheduler.pipeline_scheduler as ps
        import ingestion.scheduler.scheduler_jobs as _sj
        from datetime import datetime as dt_cls

        monkeypatch.setattr(_sj, "now_ist", lambda: dt_cls(2026, 7, 30, 21, 0))
        monkeypatch.setattr(_sj, "is_trading_day", lambda d: True)

        def _hb(*a, **k):
            return None

        monkeypatch.setattr(ps, "_record_heartbeat", _hb)
        monkeypatch.setattr(_sj, "_record_heartbeat", _hb)

        recompute_calls = []
        import ingestion.scheduler.daily_pipeline as dp
        monkeypatch.setattr(dp, "step_download_fno", lambda d: None)
        monkeypatch.setattr(dp, "step_compute_features", lambda d: recompute_calls.append(d))

        cm = CheckpointManager(in_memory=True)
        ps._execute_fno_late_catchup_job(cm)

        assert recompute_calls == []

    def test_download_fno_failure_records_failed_heartbeat(self, monkeypatch):
        import ingestion.scheduler.pipeline_scheduler as ps
        import ingestion.scheduler.scheduler_jobs as _sj
        from datetime import datetime as dt_cls

        monkeypatch.setattr(_sj, "now_ist", lambda: dt_cls(2026, 7, 30, 21, 0))
        monkeypatch.setattr(_sj, "is_trading_day", lambda d: True)

        heartbeat_calls = []

        def _hb(*a, **k):
            heartbeat_calls.append((a, k))
        monkeypatch.setattr(ps, "_record_heartbeat", _hb)
        monkeypatch.setattr(_sj, "_record_heartbeat", _hb)

        import ingestion.scheduler.daily_pipeline as dp

        def _raise(d):
            raise ConnectionError("NSE still hasn't published it")

        monkeypatch.setattr(dp, "step_download_fno", _raise)

        cm = CheckpointManager(in_memory=True)
        ps._execute_fno_late_catchup_job(cm)

        assert heartbeat_calls[0][0][:2] == ("fno_late_catchup", "failed")


class TestMFHoldingsScheduling:
    """SPEC-PIPE-003: Groww (P2.2's primary MF-holdings source) exposes no historical archive — only "now"."""

    def test_schedule_mf_holdings_ingestion_registers_weekly_cron_job(self):
        """config.settings.MF_HOLDINGS_SCHEDULE_DAY_OF_WEEK = 'sat' -> must fire every Saturday at 13:00 IST."""
        from apscheduler.schedulers.background import BackgroundScheduler

        from ingestion.scheduler.pipeline_scheduler import schedule_mf_holdings_ingestion

        scheduler = BackgroundScheduler()
        schedule_mf_holdings_ingestion(scheduler, day_of_week="sat", schedule_time="13:00")

        jobs = scheduler.get_jobs()
        assert len(jobs) == 1
        job = jobs[0]
        assert job.id == "mf_holdings_ingestion"
        trigger_str = str(job.trigger)
        assert "day_of_week='sat'" in trigger_str
        assert "hour='13'" in trigger_str

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
        import ingestion.scheduler.scheduler_jobs as _sj

        def _det():
            return (2026, 5)
        monkeypatch.setattr(ps, "_determine_groww_live_snapshot_month", _det)
        monkeypatch.setattr(_sj, "_determine_groww_live_snapshot_month", _det)
        monkeypatch.setattr("ingestion.scrapers.groww_mf_holdings.register_all_amcs", lambda: 49)

        ingestion_calls = []
        monkeypatch.setattr(
            "ingestion.scrapers.amfi_holdings.run_monthly_ingestion",
            lambda year, month: ingestion_calls.append((year, month)),
        )

        heartbeat_calls = []

        def _hb(*a, **k):
            heartbeat_calls.append((a, k))
        monkeypatch.setattr(ps, "_record_heartbeat", _hb)
        monkeypatch.setattr(_sj, "_record_heartbeat", _hb)

        ps._execute_mf_holdings_job()

        assert ingestion_calls == [(2026, 5)]
        assert len(heartbeat_calls) == 1
        args, kwargs = heartbeat_calls[0]
        assert args == ("mf_holdings_ingestion", "success")
        # A23: duration_seconds/peak_rss_mb are now measured and passed through.
        assert set(kwargs) == {"duration_seconds", "peak_rss_mb"}
        assert kwargs["duration_seconds"] >= 0
        assert kwargs["peak_rss_mb"] > 0

    def test_execute_mf_holdings_job_records_failure_heartbeat_on_unexpected_exception(self, monkeypatch):
        import ingestion.scheduler.pipeline_scheduler as ps
        import ingestion.scheduler.scheduler_jobs as _sj

        def boom():
            raise ValueError("network broke")

        monkeypatch.setattr("ingestion.scrapers.groww_mf_holdings.register_all_amcs", boom)

        heartbeat_calls = []

        def _hb(*a, **k):
            heartbeat_calls.append((a, k))
        monkeypatch.setattr(ps, "_record_heartbeat", _hb)
        monkeypatch.setattr(_sj, "_record_heartbeat", _hb)

        ps._execute_mf_holdings_job()

        assert heartbeat_calls[0][0][0] == "mf_holdings_ingestion"


# ===== SPEC-SCHED-011: Job dependency + fallback mechanism =====
class TestJobDependency:
    """SPEC-SCHED-011: depends_on graph and fallback when a non-critical step fails."""

    def test_all_steps_have_depends_on_key(self):
        """SPEC-SCHED-011: every step in STEPS declares depends_on."""
        for step in STEPS:
            assert "depends_on" in step, f"Step '{step['name']}' missing depends_on"
            assert isinstance(step["depends_on"], list)

    def test_step_deps_precomputed_matches_steps(self):
        """_STEP_DEPS must mirror STEPS depends_on declarations exactly."""
        for step in STEPS:
            assert _STEP_DEPS[step["name"]] == step["depends_on"]

    def test_independent_downloaders_have_no_deps(self):
        """download_fno/macro/corporate_actions/large_deals must have no hard deps."""
        independent = {"download_fno", "download_macro", "download_corporate_actions", "download_large_deals"}
        for step in STEPS:
            if step["name"] in independent:
                assert step["depends_on"] == [], f"{step['name']} should have no hard deps"

    def test_adjust_prices_depends_on_bhavcopy(self):
        """adjust_prices cannot run without OHLCV rows from download_bhavcopy."""
        adj = next(s for s in STEPS if s["name"] == "adjust_prices")
        assert "download_bhavcopy" in adj["depends_on"]

    def test_inference_chain_depends_on_previous(self):
        """run_models→write_signals→sanity_check→paper_trade form a hard dependency chain.

        AF-2 (#9): paper_trade now hard-depends on sanity_check (not
        write_signals directly) so a day whose signals fail the output
        sanity gate is never traded on; sanity_check itself still
        hard-depends on write_signals, so the full chain is preserved.
        """
        chain = [
            ("run_models", "compute_features"),
            ("write_signals", "run_models"),
            ("sanity_check", "write_signals"),
            ("paper_trade", "sanity_check"),
        ]
        for step_name, expected_dep in chain:
            step = next(s for s in STEPS if s["name"] == step_name)
            assert expected_dep in step["depends_on"], (
                f"{step_name} must depend on {expected_dep}"
            )

    def test_fallback_independent_steps_run_when_bhavcopy_fails(self):
        """
        SPEC-SCHED-011: when download_bhavcopy fails, the 4 independent
        downloaders (fno/macro/corporate_actions/large_deals) still run
        because they have no hard dependencies.  adjust_prices and later
        steps are skipped because their bhavcopy dep is unmet.
        """
        executed = []
        failed_steps = {"download_bhavcopy"}

        def step_runner(run_date, step_name):
            if step_name in failed_steps:
                raise RuntimeError(f"{step_name} simulated failure")
            executed.append(step_name)

        cm = CheckpointManager(in_memory=True)
        run_date = date(2026, 7, 2)

        result = run_steps_for_date(run_date, step_runner, cm, is_backfill=False)

        # Overall result is False (bhavcopy failed)
        assert result is False
        # Independent downloaders ran despite bhavcopy failure
        assert "download_fno" in executed
        assert "download_macro" in executed
        assert "download_corporate_actions" in executed
        assert "download_large_deals" in executed
        # Dependent steps were skipped
        assert "adjust_prices" not in executed
        assert "compute_features" not in executed
        assert "run_models" not in executed

    def test_fallback_pipeline_completes_when_only_noncritical_fails(self):
        """
        SPEC-SCHED-011: if only download_large_deals raises (non-critical),
        all downstream steps that depend only on bhavcopy/adjust/compute
        still execute. Return value is False (a step failed) but the
        inference chain (run_models, write_signals) ran.
        """
        executed = []
        failed_steps = {"download_large_deals"}

        def step_runner(run_date, step_name):
            if step_name in failed_steps:
                raise RuntimeError(f"{step_name} simulated failure")
            executed.append(step_name)

        cm = CheckpointManager(in_memory=True)
        run_date = date(2026, 7, 2)

        result = run_steps_for_date(run_date, step_runner, cm, is_backfill=False)

        # Returns False because download_large_deals failed
        assert result is False
        # The full inference chain still ran
        for step_name in ("adjust_prices", "compute_features", "run_models", "write_signals"):
            assert step_name in executed, f"{step_name} should have run despite large_deals failure"

    def test_checkpoint_get_succeeded_steps(self):
        """CheckpointManager.get_succeeded_steps returns the set of succeeded step names."""
        cm = CheckpointManager(in_memory=True)
        run_date = date(2026, 7, 2)
        cm.save_checkpoint(run_date, "download_bhavcopy", "success")
        cm.save_checkpoint(run_date, "download_fno", "success")
        cm.save_checkpoint(run_date, "adjust_prices", "failed")

        succeeded = cm.get_succeeded_steps(run_date)
        assert succeeded == {"download_bhavcopy", "download_fno"}
        assert "adjust_prices" not in succeeded

    def test_get_step_is_backfill_returns_recorded_flag(self):
        """A43: CheckpointManager.get_step_is_backfill lets the DataStore API
        join is_backfill onto ml_signals rows by (date, step_name) since
        ml_signals (DuckDB) and pipeline_checkpoints (SQLite) can't be
        joined in SQL directly."""
        cm = CheckpointManager(in_memory=True)
        live_date = date(2026, 7, 3)
        backfilled_date = date(2026, 7, 6)
        cm.save_checkpoint(live_date, "write_signals", "success", is_backfill=False)
        cm.save_checkpoint(backfilled_date, "write_signals", "success", is_backfill=True)

        assert cm.get_step_is_backfill(live_date, "write_signals") is False
        assert cm.get_step_is_backfill(backfilled_date, "write_signals") is True

    def test_get_step_is_backfill_returns_none_when_no_checkpoint_row(self):
        """No checkpoint row for (date, step_name) yet -> None, not False,
        so API callers can distinguish 'known live' from 'unknown'."""
        cm = CheckpointManager(in_memory=True)
        assert cm.get_step_is_backfill(date(2026, 7, 3), "write_signals") is None


# ===== Cross-process run lock (2026-07-05 race-condition fix) =====
def _acquire_lock_in_subprocess(lock_path_str, hold_seconds, result_queue):
    """Run in a real child process so fcntl.flock's cross-*process*
    exclusion (not just cross-thread) is actually exercised."""
    import time as _time
    from pathlib import Path

    import config.settings as settings_mod

    settings_mod.PIPELINE_RUN_LOCK_PATH = Path(lock_path_str)
    from ingestion.scheduler.pipeline_scheduler import pipeline_run_lock

    with pipeline_run_lock() as acquired:
        result_queue.put(acquired)
        _time.sleep(hold_seconds)


class TestPipelineRunLockPerStepScope:
    """A56 (2026-07-30): pipeline_run_lock is now re-acquired per-step
    inside run_steps_for_date's loop, instead of held once for the whole
    cascade — so it's released between steps, not just after every step
    for this date has been attempted."""

    def test_lock_is_acquired_once_per_step_not_once_for_the_whole_run(self, tmp_path, monkeypatch):
        """The core A56 guarantee, proven directly: pipeline_run_lock()
        must be called (acquire + release) once per attempted step, not
        once for the entire STEPS cascade — otherwise it's still held
        continuously end-to-end and nothing else could ever interleave."""
        import config.settings as settings_mod
        import ingestion.scheduler.pipeline_scheduler as ps_mod
        import ingestion.scheduler.pipeline_steps as _ps_steps

        monkeypatch.setattr(settings_mod, "PIPELINE_RUN_LOCK_PATH", tmp_path / "pipeline_run.lock")

        real_lock = ps_mod.pipeline_run_lock
        acquisitions = []

        @contextlib.contextmanager
        def counting_lock():
            with real_lock() as acquired:
                acquisitions.append(acquired)
                yield acquired

        # A46: run_steps_for_date lives in pipeline_steps and uses its own
        # binding — patch both the facade and the consumer submodule.
        monkeypatch.setattr(ps_mod, "pipeline_run_lock", counting_lock)
        monkeypatch.setattr(_ps_steps, "pipeline_run_lock", counting_lock)

        executed = []

        def step_runner(run_date, step_name):
            executed.append(step_name)

        cm = CheckpointManager(in_memory=True)
        result = run_steps_for_date(date(2026, 7, 2), step_runner, cm, is_backfill=False)

        assert result is True
        # One acquire/release cycle per step actually attempted (not per
        # STEPS entry — dependency-skipped steps don't touch the lock at
        # all, see run_steps_for_date's docstring) — and NOT a single
        # acquisition covering the whole run.
        assert len(acquisitions) == len(executed) > 1
        assert all(acquisitions)

    def test_losing_lock_after_partial_progress_returns_false_not_true(self, tmp_path, monkeypatch):
        """If this invocation already completed some steps before losing
        the lock to another process, it must report False (incomplete) —
        not True — so callers like run_backfill correctly retry the date
        instead of wrongly marking it done."""
        import config.settings as settings_mod
        import ingestion.scheduler.pipeline_scheduler as ps_mod
        import ingestion.scheduler.pipeline_steps as _ps_steps

        monkeypatch.setattr(settings_mod, "PIPELINE_RUN_LOCK_PATH", tmp_path / "pipeline_run.lock")

        real_lock = ps_mod.pipeline_run_lock
        call_count = {"n": 0}

        @contextlib.contextmanager
        def fake_lock():
            call_count["n"] += 1
            if call_count["n"] <= 2:
                with real_lock() as acquired:
                    yield acquired
            else:
                yield False  # a "competing process" holds it from the 3rd acquisition onward

        monkeypatch.setattr(ps_mod, "pipeline_run_lock", fake_lock)
        monkeypatch.setattr(_ps_steps, "pipeline_run_lock", fake_lock)

        executed = []

        def step_runner(run_date, step_name):
            executed.append(step_name)

        cm = CheckpointManager(in_memory=True)
        result = run_steps_for_date(date(2026, 7, 2), step_runner, cm, is_backfill=False)

        assert result is False
        assert len(executed) >= 1, "at least one step should have run before the lock was lost"
        assert len(executed) < len(STEPS), "the run must have stopped early, not completed everything"

    def test_losing_lock_before_any_progress_returns_true(self, tmp_path, monkeypatch):
        """If the lock is already held by someone else from the very
        first step, this invocation attempted nothing — must return True
        (defer entirely) exactly as before this change, not False."""
        import config.settings as settings_mod
        import ingestion.scheduler.pipeline_scheduler as ps_mod
        import ingestion.scheduler.pipeline_steps as _ps_steps

        monkeypatch.setattr(settings_mod, "PIPELINE_RUN_LOCK_PATH", tmp_path / "pipeline_run.lock")

        @contextlib.contextmanager
        def fake_lock():
            yield False

        monkeypatch.setattr(ps_mod, "pipeline_run_lock", fake_lock)
        monkeypatch.setattr(_ps_steps, "pipeline_run_lock", fake_lock)

        executed = []

        def step_runner(run_date, step_name):
            executed.append(step_name)

        cm = CheckpointManager(in_memory=True)
        result = run_steps_for_date(date(2026, 7, 2), step_runner, cm, is_backfill=False)

        assert result is True
        assert executed == []


class TestPipelineRunLock:
    """Regression test for the 2026-07-05 double-fire bug: two concurrent
    callers of run_steps_for_date (daily_pipeline cron + a misfire-grace
    re-fire, or a separate Ops force_run_step process) both saw steps as
    'running' and raced on pipeline_checkpoints. pipeline_run_lock's
    fcntl.flock must let exactly one concurrent holder proceed."""

    def test_second_concurrent_process_does_not_acquire_lock(self, tmp_path):
        import multiprocessing
        import time

        lock_path = tmp_path / "pipeline_run.lock"
        ctx = multiprocessing.get_context("fork")
        q1, q2 = ctx.Queue(), ctx.Queue()

        p1 = ctx.Process(target=_acquire_lock_in_subprocess, args=(str(lock_path), 2.0, q1))
        p1.start()
        time.sleep(0.3)  # let p1 acquire first

        p2 = ctx.Process(target=_acquire_lock_in_subprocess, args=(str(lock_path), 0.1, q2))
        p2.start()

        p1.join(timeout=5)
        p2.join(timeout=5)

        assert q1.get(timeout=5) is True, "first process should have acquired the lock"
        assert q2.get(timeout=5) is False, "second concurrent process must be turned away, not race"

    def test_lock_is_released_for_next_caller(self, tmp_path, monkeypatch):
        """After the holder exits its `with` block, a fresh call must be
        able to acquire the lock — the fix must not deadlock same-process
        sequential runs (e.g. tomorrow's catch-up after today's success)."""
        import config.settings as settings_mod
        from ingestion.scheduler.pipeline_scheduler import pipeline_run_lock

        monkeypatch.setattr(settings_mod, "PIPELINE_RUN_LOCK_PATH", tmp_path / "pipeline_run.lock")

        with pipeline_run_lock() as first:
            assert first is True
        with pipeline_run_lock() as second:
            assert second is True


class TestPipelineRunsStartedFinishedRecording:
    """Pipeline & Monitoring Remediation Phase 1 (2026-07-10): reproduces
    the real incident where bhavcopy download succeeded but
    compute_features failed, yet nothing on the Ops dashboard clearly
    reflected "this run did not finish cleanly" for an operator glancing
    at it. Root cause: pipeline_runs previously only ever got a row at
    the END of a run (_record_pipeline_run) — a process killed mid-run
    (e.g. OOM) left NO row at all for that date, so GET /api/v1/ops/runs
    kept showing a PRIOR day's success as "most recent". Now a 'running'
    row is written the moment a run starts and updated in place once it
    finishes, so a crash leaves a diagnosable stale 'running' row instead
    of silence."""

    def _seed_db(self, tmp_path):
        from datastore.schema.create_signals import create_pipeline_runs_schema

        db_path = tmp_path / "pipeline_log.db"
        create_pipeline_runs_schema(db_path=db_path)
        return db_path

    def _rows(self, db_path):
        from datastore.api.db import get_sqlite_connection

        with get_sqlite_connection(db_path) as conn:
            return conn.execute(
                "SELECT date, started_at, completed_at, status, error_message "
                "FROM pipeline_runs ORDER BY run_id"
            ).fetchall()

    def test_successful_run_writes_one_row_running_then_success(self, tmp_path, monkeypatch):
        db_path = self._seed_db(tmp_path)
        monkeypatch.setattr(gap_detector, "is_nse_holiday", lambda d: False)

        def always_succeeds(run_date, step_name):
            return None

        ckpt = CheckpointManager(in_memory=True)
        today = date(2026, 6, 10)

        ok = run_startup_sequence(always_succeeds, ckpt, today=today, db_path=db_path)

        assert ok is True
        rows = self._rows(db_path)
        # Exactly one row for the whole invocation, in its FINAL state —
        # not two separate rows (the started-row was UPDATEd, not a
        # second row INSERTed alongside it).
        assert len(rows) == 1
        assert rows[0][3] == "success"
        assert rows[0][2] is not None  # completed_at populated

    def test_partial_failure_run_ends_in_failed_status_not_stale_success(self, tmp_path, monkeypatch):
        """The exact 2026-07-10 incident shape: download_bhavcopy (and
        every step before compute_features) succeeds, compute_features
        raises. The run must end recorded as 'failed', never 'success' —
        this is the direct regression test for the false-completed bug."""
        db_path = self._seed_db(tmp_path)
        monkeypatch.setattr(gap_detector, "is_nse_holiday", lambda d: False)

        def fails_at_compute_features(run_date, step_name):
            if step_name == "compute_features":
                raise RuntimeError("feature engineering crashed")

        ckpt = CheckpointManager(in_memory=True)
        today = date(2026, 6, 10)

        ok = run_startup_sequence(fails_at_compute_features, ckpt, today=today, db_path=db_path)

        assert ok is False
        rows = self._rows(db_path)
        assert len(rows) == 1
        assert rows[0][3] == "failed"

    def test_crash_before_finish_leaves_diagnosable_running_row(self, tmp_path):
        """Simulates a process kill mid-run: only the 'started' half of
        the started/finished pair runs (as if the process died before
        _record_pipeline_run could execute). Must leave a row behind
        (status='running') rather than nothing — this is what makes the
        stale run detectable at all, instead of a prior day's success
        silently remaining "most recent"."""
        from ingestion.scheduler.pipeline_scheduler import _record_pipeline_run_started

        db_path = self._seed_db(tmp_path)
        started_at = date(2026, 7, 10)

        from config.timezone import now_ist

        run_id = _record_pipeline_run_started(started_at, now_ist(), db_path=db_path)

        rows = self._rows(db_path)
        assert len(rows) == 1
        assert rows[0][3] == "running"
        assert rows[0][2] is None  # completed_at still NULL
        assert isinstance(run_id, int)
