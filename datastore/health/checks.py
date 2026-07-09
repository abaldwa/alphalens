"""
datastore/health/checks.py

Phase: A21 (Pipeline Health Checker)
Specs: FeatureBacklog.md A21
Owner: Ops / Scheduler
Consumers: datastore/health/runner.py

check_job_completeness diffs each registered job's expected calendar
dates (datastore/health/job_registry.py) against job_run_log's actual
'success' rows, and emits one Finding per gap.
"""

from __future__ import annotations

from datetime import date as date_type
from datetime import timedelta
from typing import List

from datastore.health.findings import Finding
from datastore.health.job_registry import JOB_REGISTRY, expected_dates


def check_job_completeness(conn, as_of_date: date_type, lookback_days: int = 7) -> List[Finding]:
    """
    For every job in JOB_REGISTRY, find calendar dates in the trailing
    `lookback_days` (inclusive of as_of_date) on which the job was
    expected to fire (per its registered cadence) but job_run_log has no
    status='success' row recorded that date. Consecutive gaps are grouped
    into a single Finding per job (severity='critical' if 2+ missed
    dates, 'warning' if exactly 1 — a single-run flake vs. a systemic
    gap), with the job's registered catch-up action attached.
    """
    # lookback_days=7 means a true 7-calendar-day inclusive window
    # (as_of_date and the 6 days before it) — timedelta(lookback_days - 1),
    # not lookback_days, or the window would span 8 days and could contain
    # two occurrences of as_of_date's own weekday (e.g. two Sundays).
    window_start = as_of_date - timedelta(days=lookback_days - 1)

    findings: List[Finding] = []
    for job_id, meta in JOB_REGISTRY.items():
        expected = expected_dates(job_id, window_start, as_of_date)
        if not expected:
            continue

        successes = conn.execute(
            "SELECT DISTINCT CAST(recorded_at AS DATE) AS d FROM job_run_log "
            "WHERE job_id = ? AND status = 'success' AND CAST(recorded_at AS DATE) BETWEEN ? AND ?",
            [job_id, window_start, as_of_date],
        ).df()
        # pandas .df() surfaces DuckDB DATE columns as pandas Timestamp,
        # not datetime.date — normalize so `d in success_dates` (d from
        # expected_dates(), real date objects) actually matches.
        success_dates = {ts.date() for ts in successes["d"]} if not successes.empty else set()

        missed = [d for d in expected if d not in success_dates]
        if not missed:
            continue

        severity = "critical" if len(missed) >= 2 else "warning"
        missed_str = ", ".join(str(d) for d in missed)
        findings.append(
            Finding(
                job_id=job_id,
                missed_date=missed[-1],
                severity=severity,
                description=(
                    f"{job_id}: no successful run recorded for expected date(s) {missed_str} "
                    f"in the trailing {lookback_days} days"
                ),
                proposed_catchup_action=meta["catchup_action"],
                proposed_catchup_params={**meta["catchup_params"], "missed_dates": [str(d) for d in missed]},
            )
        )

    return findings
