"""
datastore/health/catchup.py

Phase: A21 (Pipeline Health Checker)
Specs: FeatureBacklog.md A21
Owner: Ops / Scheduler
Consumers: datastore/health/findings.py::approve_finding

Small executor registry dispatched by approve_finding — the ONLY code
path that actually triggers a catch-up run, and only on an explicit
human approval (never automatic), matching A12/A20/A25's "flag, don't
silently write" discipline.
"""

from __future__ import annotations

import logging
import subprocess
import sys
from datetime import date as date_type
from typing import Any, Dict

logger = logging.getLogger(__name__)


def _force_run_daily_pipeline(job_id: str, missed_date: date_type, params: Dict[str, Any]) -> None:
    """Re-run the daily_pipeline STEPS for the missed date(s), respecting depends_on ordering."""
    from config.timezone import now_ist
    from ingestion.scheduler.force_run import force_run_date_sync

    missed_dates = [date_type.fromisoformat(d) for d in params.get("missed_dates", [missed_date.isoformat()])]
    today = now_ist().date()
    force_run_date_sync("download_bhavcopy", missed_dates, today, cascade=True)


def _rerun_script(job_id: str, missed_date: date_type, params: Dict[str, Any]) -> None:
    """Re-invoke the job's own script, mirroring how the scheduler itself calls it (e.g. _execute_weekend_feature_backfill_job)."""
    script = params["script"]
    args = params.get("args", [])
    result = subprocess.run([sys.executable, script, *args], capture_output=True, text=True, timeout=6 * 3600)
    if result.returncode != 0:
        raise RuntimeError(f"{script} exited {result.returncode}: {result.stderr[-2000:]}")


def _rerun_mf_holdings(job_id: str, missed_date: date_type, params: Dict[str, Any]) -> None:
    """Re-run mf_holdings_ingestion directly — idempotent/merge-not-overwrite per its own docstring, safe to re-run."""
    from ingestion.scheduler.pipeline_scheduler import _execute_mf_holdings_job

    _execute_mf_holdings_job()


_CATCHUP_EXECUTORS = {
    "force_run_daily_pipeline": _force_run_daily_pipeline,
    "rerun_script": _rerun_script,
    "rerun_mf_holdings": _rerun_mf_holdings,
}


def run_catchup(action: str, job_id: str, missed_date: date_type, params: Dict[str, Any]) -> None:
    """
    Dispatch `action` to its executor. Raises KeyError for an unregistered
    action (a Finding should never have one that isn't in _CATCHUP_EXECUTORS
    — see datastore/health/job_registry.py), and propagates whatever the
    executor itself raises (approve_finding does not swallow catch-up
    failures — an approver needs to know the catch-up didn't actually work).
    """
    executor = _CATCHUP_EXECUTORS[action]
    logger.info("run_catchup: dispatching action=%s job_id=%s missed_date=%s", action, job_id, missed_date)
    executor(job_id, missed_date, params)
