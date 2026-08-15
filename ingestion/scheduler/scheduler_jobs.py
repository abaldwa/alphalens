"""
ingestion/scheduler/scheduler_jobs.py — Job execution and registration.

APScheduler job targets and schedule registration functions, extracted
from pipeline_scheduler.py (A46 — per-concern module split).

Contains:
  - create_jobstore / create_scheduler / _determine_groww_live_snapshot_month
  - _MODEL_TRAINING_SCRIPT_MAP / _trigger_model_retrain / trigger_stacking_ensemble_retrain
  - All _execute_*_job() functions (APScheduler picklable job targets)
  - All schedule_*() functions (register jobs with the scheduler)

Consumers: pipeline_scheduler.py (re-export shim), tests/unit/*.py
"""

import json
import logging
import subprocess
import sys
import time
from datetime import date as date_type
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger

from config.timezone import now_ist
from datastore.api.db import get_duckdb_connection
from ingestion.scheduler.checkpoint import CheckpointManager
from ingestion.scheduler.gap_detector import is_trading_day
from ingestion.scheduler.pipeline_run_lock import pipeline_run_lock
from ingestion.scheduler.pipeline_startup import run_morning_catchup_sequence, run_startup_sequence
from ingestion.scheduler.pipeline_steps import StepRunner
from ingestion.scheduler.run_recording import _job_timer_start, _job_timer_stats, _record_heartbeat

logger = logging.getLogger(__name__)

_VALID_MODES = ("linear", "timestamp", "manual")

# ── APScheduler helpers ────────────────────────────────────────────────


def create_jobstore(db_path: Optional[Path] = None) -> SQLAlchemyJobStore:
    """Build the persistent APScheduler job store (SPEC-SCHED-001)."""
    if db_path is None:
        from config.settings import SCHEDULER_DB_PATH
        db_path = SCHEDULER_DB_PATH
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return SQLAlchemyJobStore(url=f"sqlite:///{db_path}")


def create_scheduler(db_path: Optional[Path] = None) -> BackgroundScheduler:
    """Build a BackgroundScheduler backed by SQLAlchemyJobStore."""
    return BackgroundScheduler(jobstores={"default": create_jobstore(db_path)})


def _determine_groww_live_snapshot_month() -> tuple:
    """Sample one scheme to find Groww's live snapshot (year, month)."""
    from ingestion.scrapers.groww_mf_holdings import _fetch_scheme_detail, _list_scheme_ids
    scheme_ids = _list_scheme_ids("SBI Mutual Fund")
    for scheme_id in scheme_ids:
        detail = _fetch_scheme_detail(scheme_id)
        holdings = (detail or {}).get("holdings") or []
        if holdings:
            portfolio_date = holdings[0].get("portfolio_date")
            if portfolio_date:
                snapshot_dt = datetime.fromisoformat(portfolio_date.replace("Z", "+00:00"))
                return snapshot_dt.year, snapshot_dt.month
    raise ConnectionError("Could not determine Groww's live snapshot month")


# ── Model training map & triggers ──────────────────────────────────────

_MODEL_TRAINING_SCRIPT_MAP: Dict[str, Optional[str]] = {
    "hmm_market": "systems.ml_signal_engine.inference.train_all_phase1",
    "pnd_detector": "systems.ml_signal_engine.inference.train_all_phase1",
    "signal_5d": "systems.ml_signal_engine.inference.train_all_phase1",
    "signal_21d": "systems.ml_signal_engine.inference.train_all_phase1",
    "meta_labeler": "systems.ml_signal_engine.inference.train_all_phase1",
    "conformal_signal5d": "systems.ml_signal_engine.inference.train_all_phase1",
    "signal_63d": "systems.ml_signal_engine.inference.retrain_phase2",
    "multibagger": "systems.ml_signal_engine.inference.train_multibagger",
    "tft": "systems.ml_signal_engine.inference.train_deep_models",
    "bilstm": "systems.ml_signal_engine.inference.train_deep_models",
}

_REPO_ROOT = Path(__file__).resolve().parents[2]

_MODEL_TRAINING_GROUPS: Dict[str, Dict[str, Any]] = {
    "phase1": {"day_of_week": "mon", "models": [
        "hmm_market", "pnd_detector", "signal_5d",
        "signal_21d", "meta_labeler", "conformal_signal5d",
    ]},
    "phase2": {"day_of_week": "tue", "models": ["signal_63d"]},
    "multibagger": {"day_of_week": "wed", "models": ["multibagger"]},
    "deep_models": {"day_of_week": "thu", "models": ["tft", "bilstm"]},
}


def _trigger_model_retrain(model_name: str) -> None:
    """Invoke `python -m <module>` subprocess for the given model."""
    import importlib.util
    module = _MODEL_TRAINING_SCRIPT_MAP.get(model_name)
    if module is None:
        logger.warning(f"_trigger_model_retrain: no training module known for '{model_name}'")
        return
    try:
        if importlib.util.find_spec(module) is None:
            logger.error(f"_trigger_model_retrain: module '{module}' does not resolve — skipping")
            return
    except (ImportError, ModuleNotFoundError, ValueError):
        logger.error(f"_trigger_model_retrain: module '{module}' not found — skipping")
        return

    extra_args = []
    if module == "systems.ml_signal_engine.inference.retrain_phase2":
        extra_args = ["--subprocess-per-horizon"]

    try:
        result = subprocess.run(
            [sys.executable, "-m", module, *extra_args],
            cwd=str(_REPO_ROOT), capture_output=False, timeout=3600 * 8,
        )
        if result.returncode != 0:
            logger.error(f"_trigger_model_retrain: '{model_name}' exited with code {result.returncode}")
        else:
            logger.info(f"_trigger_model_retrain: '{model_name}' completed successfully")
    except subprocess.TimeoutExpired:
        logger.error(f"_trigger_model_retrain: '{model_name}' exceeded 8-hour timeout")
    except Exception as exc:
        logger.error(f"_trigger_model_retrain: '{model_name}' failed to start: {exc}")


def trigger_stacking_ensemble_retrain(
    dry_run: bool = True, timeout_seconds: int = 3600 * 8, output_dir: Optional[str] = None,
) -> int:
    """A40 — StackingEnsemble subprocess trigger. Returns exit code."""
    extra_args = ["--dry-run"] if dry_run else []
    if output_dir is not None:
        extra_args += ["--output-dir", output_dir]
    try:
        result = subprocess.run(
            [sys.executable, "-m", "scripts.train_stacking", *extra_args],
            cwd=str(_REPO_ROOT), capture_output=False, timeout=timeout_seconds,
        )
        logger.info(f"trigger_stacking_ensemble_retrain: exit {result.returncode}")
        return result.returncode
    except subprocess.TimeoutExpired:
        logger.error(f"trigger_stacking_ensemble_retrain: exceeded {timeout_seconds}s timeout")
        return -1


# ── Job execution targets (picklable by APScheduler's SQLAlchemyJobStore) ─


def _execute_morning_catchup_job(
    step_runner: StepRunner, checkpoint_manager: CheckpointManager, job_id: str = "morning_catchup"
) -> None:
    """Morning gap-backfill only (never runs 'today's pipeline)."""
    today_ = now_ist().date()
    _t0 = _job_timer_start()
    try:
        ok = run_morning_catchup_sequence(step_runner, checkpoint_manager, today=today_)
        try:
            from ingestion.scheduler.daily_pipeline import step_download_macro_morning
            step_download_macro_morning(today_)
        except Exception as exc:
            logger.warning(f"morning_catchup: step_download_macro_morning failed: {exc}")
        error = None if ok else "one or more gap days still incomplete"
        dur, rss = _job_timer_stats(_t0)
        _record_heartbeat(job_id, "success" if ok else "failed", error, duration_seconds=dur, peak_rss_mb=rss)
    except Exception as exc:
        logger.error(f"{job_id} job raised exception: {exc}", exc_info=True)
        dur, rss = _job_timer_stats(_t0)
        _record_heartbeat(job_id, "failed", str(exc), duration_seconds=dur, peak_rss_mb=rss)


def _execute_fno_late_catchup_job(
    checkpoint_manager: CheckpointManager, job_id: str = "fno_late_catchup"
) -> None:
    """Late-evening F&O download and compute_features re-run."""
    from ingestion.scheduler.daily_pipeline import step_compute_features, step_download_fno
    today_ = now_ist().date()
    _t0 = _job_timer_start()
    try:
        if not is_trading_day(today_):
            dur, rss = _job_timer_stats(_t0)
            _record_heartbeat(job_id, "skipped", "not a trading day", duration_seconds=dur, peak_rss_mb=rss)
            return
        checkpoint_manager.save_checkpoint(today_, "download_fno", status="running", is_backfill=True)
        try:
            step_download_fno(today_)
        except Exception as exc:
            checkpoint_manager.save_checkpoint(today_, "download_fno", status="failed", error_message=str(exc), is_backfill=True)
            dur, rss = _job_timer_stats(_t0)
            _record_heartbeat(job_id, "failed", str(exc), duration_seconds=dur, peak_rss_mb=rss)
            return
        checkpoint_manager.save_checkpoint(today_, "download_fno", status="success", is_backfill=True)
        recomputed = False
        if "compute_features" in checkpoint_manager.get_succeeded_steps(today_):
            try:
                step_compute_features(today_)
                checkpoint_manager.save_checkpoint(today_, "compute_features", status="success", is_backfill=True)
                recomputed = True
            except Exception as exc:
                logger.warning(f"{job_id}: compute_features re-run failed: {exc}")
        dur, rss = _job_timer_stats(_t0)
        _record_heartbeat(job_id, "success", f"features recomputed: {recomputed}", duration_seconds=dur, peak_rss_mb=rss)
    except Exception as exc:
        logger.error(f"{job_id} job raised exception: {exc}", exc_info=True)
        dur, rss = _job_timer_stats(_t0)
        _record_heartbeat(job_id, "failed", str(exc), duration_seconds=dur, peak_rss_mb=rss)


def _execute_daily_job(
    step_runner: StepRunner, checkpoint_manager: CheckpointManager, job_id: str = "daily_pipeline"
) -> None:
    """Daily pipeline: gap backfill + run today's steps."""
    _t0 = _job_timer_start()
    try:
        ok = run_startup_sequence(step_runner, checkpoint_manager, today=now_ist().date())
        error = None if ok else "pipeline run returned False"
        dur, rss = _job_timer_stats(_t0)
        _record_heartbeat(job_id, "success" if ok else "failed", error, duration_seconds=dur, peak_rss_mb=rss)
    except Exception as exc:
        logger.error(f"{job_id} job raised exception: {exc}", exc_info=True)
        dur, rss = _job_timer_stats(_t0)
        _record_heartbeat(job_id, "failed", str(exc), duration_seconds=dur, peak_rss_mb=rss)


def _execute_backfill_catchup() -> None:
    """Daily FYERS backfill catch-up (guarded by token check)."""
    from config.settings import BACKFILL_YEARS
    from config.universe import get_tickers
    from ingestion.backfill_runner import run_backfill
    from ingestion.scrapers.fyers_backfill import FYERSBackfill
    _t0 = _job_timer_start()
    try:
        fb = FYERSBackfill(non_interactive=True)
        cached_token = fb._load_cached_token()
        if not cached_token or not fb._validate_token(cached_token):
            skip = "no valid (same-day) FYERS token cached"
            logger.warning(f"Backfill catch-up skipped: {skip}")
            dur, rss = _job_timer_stats(_t0)
            _record_heartbeat("backfill_catchup", "skipped", skip, duration_seconds=dur, peak_rss_mb=rss)
            return
        to_date = now_ist().date()
        from_date = to_date - timedelta(days=365 * BACKFILL_YEARS)
        tickers = get_tickers()
        logger.info(f"Backfill catch-up: {len(tickers)} tickers, {from_date}..{to_date}")
        run_backfill(tickers, from_date.isoformat(), to_date.isoformat(), client=fb)
        dur, rss = _job_timer_stats(_t0)
        _record_heartbeat("backfill_catchup", "success", duration_seconds=dur, peak_rss_mb=rss)
    except Exception as exc:
        logger.error(f"backfill_catchup job raised exception: {exc}", exc_info=True)
        dur, rss = _job_timer_stats(_t0)
        _record_heartbeat("backfill_catchup", "failed", str(exc), duration_seconds=dur, peak_rss_mb=rss)


def _execute_model_training_job(model_names: Optional[List[str]] = None, job_id: str = "model_training") -> None:
    """Check registry.json and trigger retrains for overdue models."""
    from config.settings import DEFAULT_TRAINING_INTERVAL_DAYS, MODELS_DIR, RETRAIN_OVERDUE_MULTIPLIER
    _t0 = _job_timer_start()
    try:
        registry_path = Path(MODELS_DIR) / "registry.json"
        if not registry_path.exists():
            dur, rss = _job_timer_stats(_t0)
            _record_heartbeat(job_id, "skipped", "registry.json not found", duration_seconds=dur, peak_rss_mb=rss)
            return
        with registry_path.open() as f:
            registry = json.load(f)
        known_models = set(registry.keys()) | {
            name for name, script in _MODEL_TRAINING_SCRIPT_MAP.items() if script is not None
        }
        if model_names is not None:
            known_models &= set(model_names)
        today_ = now_ist().date()
        overdue_models = []
        for model_name in known_models:
            meta = registry.get(model_name, {})
            last_train_str = meta.get("last_trained_date")
            interval_days = meta.get("training_interval_days", DEFAULT_TRAINING_INTERVAL_DAYS)
            if not last_train_str:
                overdue_models.append((model_name, "never trained"))
                continue
            last_train = date_type.fromisoformat(last_train_str)
            days_since = (today_ - last_train).days
            threshold = interval_days * RETRAIN_OVERDUE_MULTIPLIER
            if days_since > threshold:
                overdue_models.append((model_name, f"{days_since}d since last train, threshold {threshold:.0f}d"))
        if not overdue_models:
            dur, rss = _job_timer_stats(_t0)
            _record_heartbeat(job_id, "skipped", "no models overdue", duration_seconds=dur, peak_rss_mb=rss)
            return
        seen_scripts: set = set()
        for model_name, reason in overdue_models:
            script = _MODEL_TRAINING_SCRIPT_MAP.get(model_name)
            if script is not None and script in seen_scripts:
                logger.info(f"  Skipping '{model_name}' — already covered by '{script}'")
                continue
            logger.info(f"  Queuing retrain for '{model_name}' ({reason})")
            _trigger_model_retrain(model_name)
            if script is not None:
                seen_scripts.add(script)
        dur, rss = _job_timer_stats(_t0)
        _record_heartbeat(job_id, "success", duration_seconds=dur, peak_rss_mb=rss)
    except Exception as exc:
        logger.error(f"{job_id} job raised exception: {exc}", exc_info=True)
        dur, rss = _job_timer_stats(_t0)
        _record_heartbeat(job_id, "failed", str(exc), duration_seconds=dur, peak_rss_mb=rss)


def _execute_model_training_job_for_group(group_name: str) -> None:
    """Nightly training wrapper — delegates to _execute_model_training_job."""
    group = _MODEL_TRAINING_GROUPS.get(group_name)
    if group is None:
        logger.error(f"Unknown training group '{group_name}'")
        return
    _execute_model_training_job(model_names=group["models"], job_id=f"model_training_{group_name}")


# ── Weekend / occasional job targets ───────────────────────────────────


def _execute_weekend_feature_backfill_job() -> None:
    """Saturday morning feature Parquet gap scan."""
    _t0 = _job_timer_start()
    try:
        result = subprocess.run(
            [sys.executable, "scripts/feature_backfill_hybrid.py", "--stage2-chunk-size", "150"],
            capture_output=False, timeout=3600 * 6,
        )
        dur, rss = _job_timer_stats(_t0)
        status = "failed" if result.returncode != 0 else "success"
        _record_heartbeat("weekend_feature_backfill", status, f"exit code {result.returncode}" if result.returncode else None,
                          duration_seconds=dur, peak_rss_mb=rss)
    except subprocess.TimeoutExpired:
        dur, rss = _job_timer_stats(_t0)
        _record_heartbeat("weekend_feature_backfill", "failed", "timeout after 6h", duration_seconds=dur, peak_rss_mb=rss)
    except Exception as exc:
        logger.error(f"weekend_feature_backfill job raised exception: {exc}", exc_info=True)
        dur, rss = _job_timer_stats(_t0)
        _record_heartbeat("weekend_feature_backfill", "failed", str(exc), duration_seconds=dur, peak_rss_mb=rss)


def _execute_queued_feature_backfill_job(
    from_date: str, to_date: str, run_id: str, no_hmm: bool = True,
    force: bool = False, chronological: bool = False,
    wait_for_lock_timeout_seconds: int = 21600, poll_interval_seconds: int = 60,
) -> None:
    """Ad-hoc feature backfill, queued through the scheduler."""
    job_id = f"queued_feature_backfill_{run_id}"
    _t0 = _job_timer_start()
    waited = 0.0
    while True:
        with pipeline_run_lock() as acquired:
            if acquired:
                break
        if waited >= wait_for_lock_timeout_seconds:
            dur, rss = _job_timer_stats(_t0)
            _record_heartbeat(job_id, "failed", f"pipeline lock unavailable after {waited:.0f}s",
                              duration_seconds=dur, peak_rss_mb=rss)
            return
        time.sleep(poll_interval_seconds)
        waited += poll_interval_seconds
    cmd = [sys.executable, "scripts/feature_backfill.py", "--from-date", from_date,
           "--to-date", to_date, "--run-id", run_id]
    if no_hmm:
        cmd.append("--no-hmm")
    if force:
        cmd.append("--force")
    if chronological:
        cmd.append("--chronological")
    try:
        result = subprocess.run(cmd, capture_output=False, timeout=3600 * 6)
        dur, rss = _job_timer_stats(_t0)
        status = "failed" if result.returncode != 0 else "success"
        _record_heartbeat(job_id, status, f"exit code {result.returncode}" if result.returncode else None,
                          duration_seconds=dur, peak_rss_mb=rss)
    except subprocess.TimeoutExpired:
        dur, rss = _job_timer_stats(_t0)
        _record_heartbeat(job_id, "failed", "timeout after 6h", duration_seconds=dur, peak_rss_mb=rss)
    except Exception as exc:
        logger.error(f"{job_id} raised exception: {exc}", exc_info=True)
        dur, rss = _job_timer_stats(_t0)
        _record_heartbeat(job_id, "failed", str(exc), duration_seconds=dur, peak_rss_mb=rss)


def _execute_weekend_fundamentals_job() -> None:
    """Saturday fundamentals catch-up (Screener.in / Trendlyne)."""
    _t0 = _job_timer_start()
    try:
        result = subprocess.run(
            [sys.executable, "scripts/backfill_fundamentals_trendlyne.py"],
            capture_output=False, timeout=3600 * 4,
        )
        dur, rss = _job_timer_stats(_t0)
        status = "failed" if result.returncode != 0 else "success"
        _record_heartbeat("weekend_fundamentals", status, f"exit code {result.returncode}" if result.returncode else None,
                          duration_seconds=dur, peak_rss_mb=rss)
    except subprocess.TimeoutExpired:
        dur, rss = _job_timer_stats(_t0)
        _record_heartbeat("weekend_fundamentals", "failed", "timeout after 4h", duration_seconds=dur, peak_rss_mb=rss)
    except Exception as exc:
        logger.error(f"weekend_fundamentals job raised exception: {exc}", exc_info=True)
        dur, rss = _job_timer_stats(_t0)
        _record_heartbeat("weekend_fundamentals", "failed", str(exc), duration_seconds=dur, peak_rss_mb=rss)


def _execute_daily_backup_job() -> None:
    """Daily off-machine backup via rclone to Backblaze B2."""
    from scripts.backup_to_b2 import run_backup
    _t0 = _job_timer_start()
    try:
        results = run_backup()
        dur, rss = _job_timer_stats(_t0)
        if results["failed"]:
            _record_heartbeat("daily_backup", "failed", f"failed dirs: {results['failed']}", duration_seconds=dur, peak_rss_mb=rss)
        else:
            _record_heartbeat("daily_backup", "success", duration_seconds=dur, peak_rss_mb=rss)
    except RuntimeError as exc:
        dur, rss = _job_timer_stats(_t0)
        _record_heartbeat("daily_backup", "skipped", str(exc), duration_seconds=dur, peak_rss_mb=rss)
    except Exception as exc:
        logger.error(f"daily_backup job raised exception: {exc}", exc_info=True)
        dur, rss = _job_timer_stats(_t0)
        _record_heartbeat("daily_backup", "failed", str(exc), duration_seconds=dur, peak_rss_mb=rss)


def _execute_fyers_login_job() -> None:
    """Unattended daily FYERS login attempt."""
    from ingestion.scrapers.fyers_login import FyersLoginError, daily_login
    _t0 = _job_timer_start()
    try:
        daily_login(headless=True)
        dur, rss = _job_timer_stats(_t0)
        _record_heartbeat("fyers_login", "success", duration_seconds=dur, peak_rss_mb=rss)
    except FyersLoginError as exc:
        dur, rss = _job_timer_stats(_t0)
        _record_heartbeat("fyers_login", "failed", str(exc), duration_seconds=dur, peak_rss_mb=rss)
    except Exception as exc:
        logger.error(f"fyers_login job raised exception: {exc}", exc_info=True)
        dur, rss = _job_timer_stats(_t0)
        _record_heartbeat("fyers_login", "failed", str(exc), duration_seconds=dur, peak_rss_mb=rss)


def _execute_job_health_check_job() -> None:
    """A21 — weekly job-completeness audit."""
    from config.settings import DUCKDB_PATH
    from datastore.health.runner import run_job_health_check
    _t0 = _job_timer_start()
    try:
        with get_duckdb_connection(DUCKDB_PATH, persist=False) as conn:
            result = run_job_health_check(conn, now_ist().date())
        logger.info(f"job_health_check: findings={result.findings_by_check} critical={result.critical_count}")
        dur, rss = _job_timer_stats(_t0)
        _record_heartbeat("job_health_check", "success", duration_seconds=dur, peak_rss_mb=rss)
    except Exception as exc:
        logger.error(f"job_health_check job raised exception: {exc}", exc_info=True)
        dur, rss = _job_timer_stats(_t0)
        _record_heartbeat("job_health_check", "failed", str(exc), duration_seconds=dur, peak_rss_mb=rss)


def _execute_promoter_pledge_backfill_job() -> None:
    """Saturday promoter-pledge catch-up."""
    _t0 = _job_timer_start()
    try:
        result = subprocess.run(
            [sys.executable, "scripts/backfill_promoter_pledge_nse.py"],
            capture_output=False, timeout=3600 * 4,
        )
        dur, rss = _job_timer_stats(_t0)
        status = "failed" if result.returncode != 0 else "success"
        _record_heartbeat("promoter_pledge_backfill", status, f"exit {result.returncode}" if result.returncode else None,
                          duration_seconds=dur, peak_rss_mb=rss)
    except subprocess.TimeoutExpired:
        dur, rss = _job_timer_stats(_t0)
        _record_heartbeat("promoter_pledge_backfill", "failed", "timeout after 4h", duration_seconds=dur, peak_rss_mb=rss)
    except Exception as exc:
        logger.error(f"promoter_pledge_backfill job raised exception: {exc}", exc_info=True)
        dur, rss = _job_timer_stats(_t0)
        _record_heartbeat("promoter_pledge_backfill", "failed", str(exc), duration_seconds=dur, peak_rss_mb=rss)


def _execute_balance_sheet_backfill_job() -> None:
    """Saturday balance-sheet catch-up."""
    _t0 = _job_timer_start()
    try:
        result = subprocess.run(
            [sys.executable, "scripts/backfill_balance_sheet_from_screener.py"],
            capture_output=False, timeout=3600 * 2,
        )
        dur, rss = _job_timer_stats(_t0)
        status = "failed" if result.returncode != 0 else "success"
        _record_heartbeat("balance_sheet_backfill", status, f"exit {result.returncode}" if result.returncode else None,
                          duration_seconds=dur, peak_rss_mb=rss)
    except subprocess.TimeoutExpired:
        dur, rss = _job_timer_stats(_t0)
        _record_heartbeat("balance_sheet_backfill", "failed", "timeout after 2h", duration_seconds=dur, peak_rss_mb=rss)
    except Exception as exc:
        logger.error(f"balance_sheet_backfill job raised exception: {exc}", exc_info=True)
        dur, rss = _job_timer_stats(_t0)
        _record_heartbeat("balance_sheet_backfill", "failed", str(exc), duration_seconds=dur, peak_rss_mb=rss)


def _execute_multibagger_scoring_job() -> None:
    """Weekly full-universe multibagger scoring."""
    _t0 = _job_timer_start()
    try:
        result = subprocess.run(
            [sys.executable, "-m", "systems.ml_signal_engine.inference.score_multibagger"],
            capture_output=False, timeout=3600 * 2,
        )
        dur, rss = _job_timer_stats(_t0)
        status = "failed" if result.returncode != 0 else "success"
        _record_heartbeat("multibagger_scoring", status, f"exit {result.returncode}" if result.returncode else None,
                          duration_seconds=dur, peak_rss_mb=rss)
    except subprocess.TimeoutExpired:
        dur, rss = _job_timer_stats(_t0)
        _record_heartbeat("multibagger_scoring", "failed", "timeout after 2h", duration_seconds=dur, peak_rss_mb=rss)
    except Exception as exc:
        logger.error(f"multibagger_scoring job raised exception: {exc}", exc_info=True)
        dur, rss = _job_timer_stats(_t0)
        _record_heartbeat("multibagger_scoring", "failed", str(exc), duration_seconds=dur, peak_rss_mb=rss)


def _execute_forensic_scoring_job() -> None:
    """Weekly full-universe forensic scoring."""
    _t0 = _job_timer_start()
    try:
        result = subprocess.run(
            [sys.executable, "-m", "systems.ml_signal_engine.inference.score_forensic"],
            capture_output=False, timeout=3600 * 2,
        )
        dur, rss = _job_timer_stats(_t0)
        status = "failed" if result.returncode != 0 else "success"
        _record_heartbeat("forensic_scoring", status, f"exit {result.returncode}" if result.returncode else None,
                          duration_seconds=dur, peak_rss_mb=rss)
    except subprocess.TimeoutExpired:
        dur, rss = _job_timer_stats(_t0)
        _record_heartbeat("forensic_scoring", "failed", "timeout after 2h", duration_seconds=dur, peak_rss_mb=rss)
    except Exception as exc:
        logger.error(f"forensic_scoring job raised exception: {exc}", exc_info=True)
        dur, rss = _job_timer_stats(_t0)
        _record_heartbeat("forensic_scoring", "failed", str(exc), duration_seconds=dur, peak_rss_mb=rss)


def _execute_nse_xbrl_fundamentals_job() -> None:
    """Weekly NSE Integrated Filing (IndAS) fundamentals scan."""
    _t0 = _job_timer_start()
    try:
        result = subprocess.run(
            [sys.executable, "scripts/backfill_fundamentals_nse_xbrl.py"],
            capture_output=False, timeout=3600 * 4,
        )
        dur, rss = _job_timer_stats(_t0)
        status = "failed" if result.returncode != 0 else "success"
        _record_heartbeat("nse_xbrl_fundamentals", status, f"exit {result.returncode}" if result.returncode else None,
                          duration_seconds=dur, peak_rss_mb=rss)
    except subprocess.TimeoutExpired:
        dur, rss = _job_timer_stats(_t0)
        _record_heartbeat("nse_xbrl_fundamentals", "failed", "timeout after 4h", duration_seconds=dur, peak_rss_mb=rss)
    except Exception as exc:
        logger.error(f"nse_xbrl_fundamentals job raised exception: {exc}", exc_info=True)
        dur, rss = _job_timer_stats(_t0)
        _record_heartbeat("nse_xbrl_fundamentals", "failed", str(exc), duration_seconds=dur, peak_rss_mb=rss)


def _execute_mf_holdings_job() -> None:
    """Weekly MF-holdings ingestion (Groww)."""
    from config.settings import DUCKDB_PATH
    from ingestion.scrapers.amfi_holdings import run_monthly_ingestion, sync_duckdb_table
    from ingestion.scrapers.groww_mf_holdings import register_all_amcs
    _t0 = _job_timer_start()
    try:
        register_all_amcs()
        year, month = _determine_groww_live_snapshot_month()
        run_monthly_ingestion(year, month)
        with get_duckdb_connection(DUCKDB_PATH, persist=False) as conn:
            sync_duckdb_table(conn, year, month)
        dur, rss = _job_timer_stats(_t0)
        _record_heartbeat("mf_holdings_ingestion", "success", duration_seconds=dur, peak_rss_mb=rss)
    except RuntimeError as exc:
        dur, rss = _job_timer_stats(_t0)
        _record_heartbeat("mf_holdings_ingestion", "skipped", str(exc), duration_seconds=dur, peak_rss_mb=rss)
    except Exception as exc:
        logger.error(f"mf_holdings_ingestion job raised exception: {exc}", exc_info=True)
        dur, rss = _job_timer_stats(_t0)
        _record_heartbeat("mf_holdings_ingestion", "failed", str(exc), duration_seconds=dur, peak_rss_mb=rss)


def _execute_emergency_recompute_job(
    from_date: Optional[str] = None, ticker_batch_size: int = 150,
    start_batch_idx: int = 0, start_stage: str = "stage1",
) -> None:
    """One-off emergency feature-cache recompute + model retrain chain."""
    from config.settings import DUCKDB_PATH
    MODEL_NAMES = ("signal_5d", "signal_21d", "signal_63d", "tft", "bilstm",
                   "multibagger", "hmm_market", "pnd_detector")
    progress_path = Path("datastore/logs/emergency_recompute_progress.json")
    progress_path.parent.mkdir(parents=True, exist_ok=True)

    def _write_progress(**fields) -> None:
        state = {}
        if progress_path.exists():
            try:
                state = json.loads(progress_path.read_text())
            except Exception:
                state = {}
        state.update(fields)
        state["updated_at"] = now_ist().isoformat()
        progress_path.write_text(json.dumps(state, indent=2))

    _t0 = _job_timer_start()
    try:
        if start_stage == "stage1":
            with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
                n_active = conn.execute("""
                    SELECT count(DISTINCT ticker) FROM ohlcv_adjusted
                    WHERE date >= (SELECT CAST(MAX(date) - INTERVAL 30 DAYS AS DATE) FROM ohlcv_adjusted)
                """).fetchone()[0]
            n_batches = (n_active + ticker_batch_size - 1) // ticker_batch_size
            logger.info(f"emergency_recompute: {n_active} active tickers -> {n_batches} batches")
            _write_progress(stage="stage1", stage1_batches_total=n_batches, stage1_batches_done=start_batch_idx,
                            active_tickers=n_active, models_total=len(MODEL_NAMES), models_done=[])
            for batch_idx in range(start_batch_idx, n_batches):
                cmd = [sys.executable, "scripts/feature_backfill_hybrid.py",
                       "--all-db-tickers", "--active-only", "--force",
                       "--ticker-batch-size", str(ticker_batch_size),
                       "--ticker-batch-index", str(batch_idx), "--workers", "3"]
                if from_date:
                    cmd += ["--from-date", from_date]
                logger.info(f"emergency_recompute: Stage 1 batch {batch_idx + 1}/{n_batches}")
                result = subprocess.run(cmd, capture_output=False, timeout=3600 * 2)
                if result.returncode != 0:
                    _write_progress(stage="stage1_failed", stage1_batches_done=batch_idx,
                                    error=f"batch {batch_idx + 1} exit {result.returncode}")
                    dur, rss = _job_timer_stats(_t0)
                    _record_heartbeat("emergency_recompute", "failed", f"stage1 batch {batch_idx + 1} exit {result.returncode}",
                                      duration_seconds=dur, peak_rss_mb=rss)
                    return
                _write_progress(stage="stage1", stage1_batches_done=batch_idx + 1)

        _write_progress(stage="stage2")
        stage2_cmd = [sys.executable, "scripts/feature_backfill_hybrid.py",
                      "--rebuild-daily", "--all-db-tickers", "--active-only", "--force", "--stage2-chunk-size", "150"]
        if from_date:
            stage2_cmd += ["--from-date", from_date]
        result = subprocess.run(stage2_cmd, capture_output=False, timeout=3600 * 8)
        if result.returncode != 0:
            _write_progress(stage="stage2_failed", error=f"stage2 exit {result.returncode}")
            dur, rss = _job_timer_stats(_t0)
            _record_heartbeat("emergency_recompute", "failed", f"stage2 exit {result.returncode}", duration_seconds=dur, peak_rss_mb=rss)
            return
        _write_progress(stage="retrain", stage2_done=True)

        for model_name in MODEL_NAMES:
            _trigger_model_retrain(model_name)
            _write_progress(stage="retrain", models_done=list(MODEL_NAMES))
        dur, rss = _job_timer_stats(_t0)
        _record_heartbeat("emergency_recompute", "success", duration_seconds=dur, peak_rss_mb=rss)
        _write_progress(stage="complete")
    except subprocess.TimeoutExpired:
        dur, rss = _job_timer_stats(_t0)
        _record_heartbeat("emergency_recompute", "failed", "timeout after 8h", duration_seconds=dur, peak_rss_mb=rss)
    except Exception as exc:
        logger.error(f"emergency_recompute job raised exception: {exc}", exc_info=True)
        dur, rss = _job_timer_stats(_t0)
        _record_heartbeat("emergency_recompute", "failed", str(exc), duration_seconds=dur, peak_rss_mb=rss)


# ── Schedule registration functions ────────────────────────────────────


def schedule_fno_late_catchup(scheduler: BackgroundScheduler, checkpoint_manager: CheckpointManager,
                              schedule_time: Optional[str] = None) -> None:
    if schedule_time is None:
        from config.settings import FNO_LATE_CATCHUP_SCHEDULE_TIME
        schedule_time = FNO_LATE_CATCHUP_SCHEDULE_TIME
    h, m = (int(p) for p in schedule_time.split(":"))
    scheduler.add_job(_execute_fno_late_catchup_job, CronTrigger(hour=h, minute=m, day_of_week="mon-fri", timezone="Asia/Kolkata"),
                      args=[checkpoint_manager], id="fno_late_catchup", replace_existing=True, misfire_grace_time=86400, coalesce=True)
    logger.info(f"F&O late catch-up scheduled: {schedule_time} IST")


def schedule_daily_pipeline(scheduler: BackgroundScheduler, step_runner: StepRunner,
                            checkpoint_manager: CheckpointManager, mode: Optional[str] = None, schedule_time: str = "18:00") -> None:
    if mode is None:
        from config.settings import SCHEDULER_MODE
        mode = SCHEDULER_MODE
    if mode not in _VALID_MODES:
        raise ValueError(f"Unknown scheduler mode '{mode}'")
    if mode == "manual":
        logger.info("SCHEDULER_MODE=manual — no recurring job registered")
        return
    h, m = (int(p) for p in schedule_time.split(":"))
    scheduler.add_job(_execute_daily_job, CronTrigger(hour=h, minute=m, day_of_week="mon-fri", timezone="Asia/Kolkata"),
                      args=[step_runner, checkpoint_manager, "daily_pipeline"],
                      id="daily_pipeline", replace_existing=True, misfire_grace_time=86400, coalesce=True)
    logger.info(f"Daily pipeline scheduled: mode={mode}, time={schedule_time} IST")


def schedule_morning_catchup(scheduler: BackgroundScheduler, step_runner: StepRunner,
                             checkpoint_manager: CheckpointManager, schedule_time: str = "07:30") -> None:
    h, m = (int(p) for p in schedule_time.split(":"))
    scheduler.add_job(_execute_morning_catchup_job, CronTrigger(hour=h, minute=m, day_of_week="mon-fri", timezone="Asia/Kolkata"),
                      args=[step_runner, checkpoint_manager, "morning_catchup"],
                      id="morning_catchup", replace_existing=True, misfire_grace_time=86400, coalesce=True)
    logger.info(f"Morning catch-up scheduled: {schedule_time} IST")


def schedule_backfill_catchup(scheduler: BackgroundScheduler, schedule_time: Optional[str] = None) -> None:
    if schedule_time is None:
        from config.settings import BACKFILL_CATCHUP_TIME
        schedule_time = BACKFILL_CATCHUP_TIME
    h, m = (int(p) for p in schedule_time.split(":"))
    scheduler.add_job(_execute_backfill_catchup, CronTrigger(hour=h, minute=m, timezone="Asia/Kolkata"),
                      id="backfill_catchup", replace_existing=True, misfire_grace_time=86400, coalesce=True)
    logger.info(f"Backfill catch-up scheduled: {schedule_time} IST")


def schedule_mf_holdings_ingestion(scheduler: BackgroundScheduler, day_of_week: Optional[str] = None,
                                   schedule_time: Optional[str] = None) -> None:
    if day_of_week is None:
        from config.settings import MF_HOLDINGS_SCHEDULE_DAY_OF_WEEK
        day_of_week = MF_HOLDINGS_SCHEDULE_DAY_OF_WEEK
    if schedule_time is None:
        from config.settings import AMFI_SCHEDULE_TIME
        schedule_time = AMFI_SCHEDULE_TIME
    h, m = (int(p) for p in schedule_time.split(":"))
    scheduler.add_job(_execute_mf_holdings_job, CronTrigger(day_of_week=day_of_week, hour=h, minute=m, timezone="Asia/Kolkata"),
                      id="mf_holdings_ingestion", replace_existing=True, misfire_grace_time=86400, coalesce=True)
    logger.info(f"MF holdings ingestion scheduled: {day_of_week} {schedule_time} IST")


def schedule_model_training(scheduler: BackgroundScheduler, schedule_time: Optional[str] = None) -> None:
    from config.settings import MODEL_TRAINING_DAY_OF_WEEK
    if schedule_time is None:
        from config.settings import MODEL_TRAINING_SCHEDULE_TIME
        schedule_time = MODEL_TRAINING_SCHEDULE_TIME
    h, m = (int(p) for p in schedule_time.split(":"))
    scheduler.add_job(_execute_model_training_job, CronTrigger(hour=h, minute=m, day_of_week=MODEL_TRAINING_DAY_OF_WEEK, timezone="Asia/Kolkata"),
                      id="model_training", replace_existing=True, misfire_grace_time=86400, coalesce=True)
    logger.info(f"Model training check scheduled: {schedule_time} IST ({MODEL_TRAINING_DAY_OF_WEEK})")


def schedule_model_training_nightly(scheduler: BackgroundScheduler, schedule_time: Optional[str] = None) -> None:
    if schedule_time is None:
        from config.settings import MODEL_TRAINING_NIGHTLY_TIME
        schedule_time = MODEL_TRAINING_NIGHTLY_TIME
    h, m = (int(p) for p in schedule_time.split(":"))
    for group_name, group in _MODEL_TRAINING_GROUPS.items():
        scheduler.add_job(_execute_model_training_job_for_group,
                          CronTrigger(hour=h, minute=m, day_of_week=group["day_of_week"], timezone="Asia/Kolkata"),
                          id=f"model_training_{group_name}", args=[group_name],
                          replace_existing=True, misfire_grace_time=86400, coalesce=True)
        logger.info(f"Model training '{group_name}': {schedule_time} IST ({group['day_of_week']})")


def schedule_weekend_feature_backfill(scheduler: BackgroundScheduler, schedule_time: Optional[str] = None) -> None:
    if schedule_time is None:
        from config.settings import WEEKEND_FEATURE_BACKFILL_TIME
        schedule_time = WEEKEND_FEATURE_BACKFILL_TIME
    h, m = (int(p) for p in schedule_time.split(":"))
    scheduler.add_job(_execute_weekend_feature_backfill_job, CronTrigger(hour=h, minute=m, day_of_week="sat", timezone="Asia/Kolkata"),
                      id="weekend_feature_backfill", replace_existing=True, misfire_grace_time=86400, coalesce=True)
    logger.info(f"Weekend feature backfill scheduled: {schedule_time} IST (sat)")


def schedule_feature_backfill_once(scheduler: BackgroundScheduler, from_date: str, to_date: str, run_id: str,
                                   no_hmm: bool = True, force: bool = False, chronological: bool = False) -> None:
    job_id = f"queued_feature_backfill_{run_id}"
    scheduler.add_job(_execute_queued_feature_backfill_job, DateTrigger(),
                      kwargs={"from_date": from_date, "to_date": to_date, "run_id": run_id,
                              "no_hmm": no_hmm, "force": force, "chronological": chronological},
                      id=job_id, replace_existing=True, misfire_grace_time=86400)
    logger.info(f"Queued feature backfill {job_id}: {from_date} -> {to_date}")


def schedule_weekend_fundamentals(scheduler: BackgroundScheduler, schedule_time: Optional[str] = None) -> None:
    if schedule_time is None:
        from config.settings import WEEKEND_FUNDAMENTALS_TIME
        schedule_time = WEEKEND_FUNDAMENTALS_TIME
    h, m = (int(p) for p in schedule_time.split(":"))
    scheduler.add_job(_execute_weekend_fundamentals_job, CronTrigger(hour=h, minute=m, day_of_week="sat", timezone="Asia/Kolkata"),
                      id="weekend_fundamentals", replace_existing=True, misfire_grace_time=86400, coalesce=True)
    logger.info(f"Weekend fundamentals backfill scheduled: {schedule_time} IST (sat)")


def schedule_daily_backup(scheduler: BackgroundScheduler, schedule_time: Optional[str] = None) -> None:
    if schedule_time is None:
        from config.settings import BACKUP_SCHEDULE_TIME
        schedule_time = BACKUP_SCHEDULE_TIME
    h, m = (int(p) for p in schedule_time.split(":"))
    scheduler.add_job(_execute_daily_backup_job, CronTrigger(hour=h, minute=m, timezone="Asia/Kolkata"),
                      id="daily_backup", replace_existing=True, misfire_grace_time=86400, coalesce=True)
    logger.info(f"Daily backup scheduled: {schedule_time} IST")


def schedule_fyers_login(scheduler: BackgroundScheduler, schedule_time: str = "06:30") -> None:
    h, m = (int(p) for p in schedule_time.split(":"))
    scheduler.add_job(_execute_fyers_login_job, CronTrigger(hour=h, minute=m, timezone="Asia/Kolkata"),
                      id="fyers_login", replace_existing=True, misfire_grace_time=3600, coalesce=True)
    logger.info(f"FYERS login attempt scheduled: {schedule_time} IST")


def schedule_job_health_check(scheduler: BackgroundScheduler, day_of_week: Optional[str] = None,
                              schedule_time: Optional[str] = None) -> None:
    if day_of_week is None:
        from config.settings import JOB_HEALTH_CHECK_DAY_OF_WEEK
        day_of_week = JOB_HEALTH_CHECK_DAY_OF_WEEK
    if schedule_time is None:
        from config.settings import JOB_HEALTH_CHECK_SCHEDULE_TIME
        schedule_time = JOB_HEALTH_CHECK_SCHEDULE_TIME
    h, m = (int(p) for p in schedule_time.split(":"))
    scheduler.add_job(_execute_job_health_check_job, CronTrigger(day_of_week=day_of_week, hour=h, minute=m, timezone="Asia/Kolkata"),
                      id="job_health_check", replace_existing=True, misfire_grace_time=86400, coalesce=True)
    logger.info(f"Job health check scheduled: {schedule_time} IST ({day_of_week})")


def schedule_promoter_pledge_backfill(scheduler: BackgroundScheduler, schedule_time: Optional[str] = None) -> None:
    if schedule_time is None:
        from config.settings import PROMOTER_PLEDGE_BACKFILL_SCHEDULE_TIME
        schedule_time = PROMOTER_PLEDGE_BACKFILL_SCHEDULE_TIME
    h, m = (int(p) for p in schedule_time.split(":"))
    scheduler.add_job(_execute_promoter_pledge_backfill_job, CronTrigger(hour=h, minute=m, day_of_week="sat", timezone="Asia/Kolkata"),
                      id="promoter_pledge_backfill", replace_existing=True, misfire_grace_time=86400, coalesce=True)
    logger.info(f"Promoter pledge backfill scheduled: {schedule_time} IST (sat)")


def schedule_balance_sheet_backfill(scheduler: BackgroundScheduler, schedule_time: Optional[str] = None) -> None:
    if schedule_time is None:
        from config.settings import BALANCE_SHEET_BACKFILL_SCHEDULE_TIME
        schedule_time = BALANCE_SHEET_BACKFILL_SCHEDULE_TIME
    h, m = (int(p) for p in schedule_time.split(":"))
    scheduler.add_job(_execute_balance_sheet_backfill_job, CronTrigger(hour=h, minute=m, day_of_week="sat", timezone="Asia/Kolkata"),
                      id="balance_sheet_backfill", replace_existing=True, misfire_grace_time=86400, coalesce=True)
    logger.info(f"Balance sheet backfill scheduled: {schedule_time} IST (sat)")


def schedule_multibagger_scoring(scheduler: BackgroundScheduler, schedule_time: Optional[str] = None) -> None:
    if schedule_time is None:
        from config.settings import MULTIBAGGER_SCORING_SCHEDULE_TIME
        schedule_time = MULTIBAGGER_SCORING_SCHEDULE_TIME
    h, m = (int(p) for p in schedule_time.split(":"))
    scheduler.add_job(_execute_multibagger_scoring_job, CronTrigger(hour=h, minute=m, day_of_week="sun", timezone="Asia/Kolkata"),
                      id="multibagger_scoring", replace_existing=True, misfire_grace_time=86400, coalesce=True)
    logger.info(f"Multibagger scoring scheduled: {schedule_time} IST (sun)")


def schedule_forensic_scoring(scheduler: BackgroundScheduler, schedule_time: Optional[str] = None) -> None:
    if schedule_time is None:
        from config.settings import FORENSIC_SCORING_SCHEDULE_TIME
        schedule_time = FORENSIC_SCORING_SCHEDULE_TIME
    h, m = (int(p) for p in schedule_time.split(":"))
    scheduler.add_job(_execute_forensic_scoring_job, CronTrigger(hour=h, minute=m, day_of_week="sun", timezone="Asia/Kolkata"),
                      id="forensic_scoring", replace_existing=True, misfire_grace_time=86400, coalesce=True)
    logger.info(f"Forensic scoring scheduled: {schedule_time} IST (sun)")


def schedule_nse_xbrl_fundamentals(scheduler: BackgroundScheduler, schedule_time: Optional[str] = None) -> None:
    if schedule_time is None:
        from config.settings import NSE_XBRL_FUNDAMENTALS_SCHEDULE_TIME
        schedule_time = NSE_XBRL_FUNDAMENTALS_SCHEDULE_TIME
    h, m = (int(p) for p in schedule_time.split(":"))
    scheduler.add_job(_execute_nse_xbrl_fundamentals_job, CronTrigger(hour=h, minute=m, day_of_week="sat", timezone="Asia/Kolkata"),
                      id="nse_xbrl_fundamentals", replace_existing=True, misfire_grace_time=86400, coalesce=True)
    logger.info(f"NSE XBRL fundamentals scan scheduled: {schedule_time} IST (sat)")


def schedule_emergency_recompute(scheduler: BackgroundScheduler, run_at=None,
                                 from_date: Optional[str] = None, job_id: Optional[str] = None) -> str:
    if run_at is None:
        run_at = now_ist() + timedelta(seconds=10)
    if job_id is None:
        job_id = f"emergency_recompute_{now_ist().strftime('%Y%m%d_%H%M%S')}"
    scheduler.add_job(_execute_emergency_recompute_job, DateTrigger(run_date=run_at, timezone="Asia/Kolkata"),
                      args=[from_date], id=job_id, replace_existing=True, misfire_grace_time=86400)
    logger.info(f"Emergency recompute scheduled: {job_id} at {run_at}")
    return job_id