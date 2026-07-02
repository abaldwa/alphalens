"""
ingestion/scheduler/checkpoint.py

Phase: 0.3 (Scheduler & Checkpoint Engine)
Specs: SPEC-SCHED-001, SPEC-SCHED-002, SPEC-SCHED-005, SPEC-SCHED-006, SPEC-SCHED-010
Owner: Platform / Scheduler
Consumers: ingestion/scheduler/pipeline_scheduler, ingestion/scheduler/gap_detector,
    ingestion/scheduler/daily_pipeline

CheckpointManager: per-step checkpoint read/write for the daily pipeline,
against the transactional pipeline_checkpoints SQLite table (SPEC-DS-007:
SQLite for transactional stores, never DuckDB). This is the resume-on-
failure mechanism behind SPEC-SCHED-002 — if a run crashes at step N, the
next run for that date resumes from step N rather than re-executing
steps 1..N-1.

STEPS includes download_macro (FII/DII, India VIX, USD/INR — SPEC-PIPE-006)
as its own checkpointed step, separate from download_bhavcopy/download_fno:
macro indicators are fetched from different NSE/RBI endpoints with
independent failure modes (e.g. FII/DII can fail while bhavcopy succeeds),
so folding them into an existing step would hide which source actually
failed. delivery_qty/delivery_pct are NOT a separate step — bhavcopy.py's
download_bhavcopy() already parses them from the same CSV row set used for
OHLCV, so ingestion/scheduler/daily_pipeline.py's download_bhavcopy
dispatch writes both in one pass; ingestion/scrapers/nse_delivery_loader.py
exists only for the historical-backfill case (FYERS has no delivery data),
not the daily live case.
"""

import logging
from datetime import date as date_type
from pathlib import Path
from typing import Optional

from config.timezone import now_ist
from datastore.api.db import get_sqlite_connection

logger = logging.getLogger(__name__)

# SPEC-SCHED-006: each step declares is_backfillable. Steps after feature
# computation are model inference / signal-writing and must never run
# during backfill — gap days get data + features only, never predictions.
#
# SPEC-SCHED-011: each step declares depends_on (list of prerequisite step
# names). run_steps_for_date evaluates dependencies before executing each
# step: if any required dependency failed or was skipped, this step is
# skipped too. Steps with no hard dependencies (download_fno, download_macro,
# download_corporate_actions, download_large_deals) run unconditionally —
# their failures are isolated and never cascade.
#
# depends_on semantics:
#   [] (empty) — no hard prerequisite; step always attempted (unless already
#       succeeded). Step implementations handle their own soft failures
#       internally (try/except, returning normally even on source outage).
#   ["step_x"] — only runs if step_x completed with status='success' for
#       this date. If step_x failed/was-skipped, this step is skipped with
#       reason logged; no pipeline abort.
#
# This enables the fallback the user requested: a failure in a non-critical
# step (e.g. download_large_deals) never blocks compute_features, because
# compute_features only depends on adjust_prices (which only depends on
# download_bhavcopy). The 4 parallel downloader steps are fully independent.
STEPS = [
    # Foundation: OHLCV is the hard prerequisite for everything downstream.
    {"name": "download_bhavcopy", "is_backfillable": True, "depends_on": []},
    # Independent downloaders: failures are isolated; each handles its own
    # source outages via try/except and returns normally on failure.
    {"name": "download_fno", "is_backfillable": True, "depends_on": []},
    {"name": "download_macro", "is_backfillable": True, "depends_on": []},
    # Corporate actions must land before adjust_prices so the adjuster sees
    # the full ledger when it eventually runs (PRICE_ADJUSTMENT_ENABLED
    # controls whether adjust_prices actually applies factors — see
    # config/settings.py). Soft dep on bhavcopy only at the scheduler level:
    # no OHLCV rows exist yet for today if bhavcopy failed, so the adjuster
    # would be a no-op — but the corporate_actions table is useful for future
    # dates regardless, so we still attempt download even if bhavcopy failed.
    {"name": "download_corporate_actions", "is_backfillable": True, "depends_on": []},
    {"name": "download_large_deals", "is_backfillable": True, "depends_on": []},
    # adjust_prices is the first hard-dependency step: it needs OHLCV rows
    # (written by download_bhavcopy) to be present. If bhavcopy failed there
    # are no rows to adjust — skip.
    {"name": "adjust_prices", "is_backfillable": True, "depends_on": ["download_bhavcopy"]},
    # compute_features needs adjusted OHLCV. macro/fno data is consumed as
    # NaN-tolerant soft inputs — features compute fine without them.
    {"name": "compute_features", "is_backfillable": True, "depends_on": ["adjust_prices"]},
    # check_ta_alerts: evaluates the 42 TA screener templates + user-defined
    # alerts against run_date's own feature Parquet only (systems/
    # technical_analysis/alerts/{daily_alert_checker,alert_store}.py) —
    # deterministic given that day's features, no model inference, so
    # unlike run_models/write_signals it IS safe to backfill.
    {"name": "check_ta_alerts", "is_backfillable": True, "depends_on": ["compute_features"]},
    # Inference chain: each step hard-depends on the previous one.
    {"name": "run_models", "is_backfillable": False, "depends_on": ["compute_features"]},
    {"name": "write_signals", "is_backfillable": False, "depends_on": ["run_models"]},
    # [AS BUILT, P3.x] paper_trade: not backfillable, same reasoning as
    # run_models/write_signals — a gap day's signals were never genuinely
    # live, so trading on them after the fact would let backfill silently
    # inflate Phase 3 Gate 7's forward-time day count.
    {"name": "paper_trade", "is_backfillable": False, "depends_on": ["write_signals"]},
]
STEP_NAMES = [step["name"] for step in STEPS]
_BACKFILLABLE = {step["name"]: step["is_backfillable"] for step in STEPS}

_VALID_STATUSES = {"running", "success", "failed", "skipped"}

_CREATE_PIPELINE_CHECKPOINTS = """
    CREATE TABLE IF NOT EXISTS pipeline_checkpoints (
        date DATE NOT NULL,
        step_name VARCHAR NOT NULL,
        step_index INTEGER NOT NULL,
        status VARCHAR NOT NULL,
        started_at TIMESTAMP,
        completed_at TIMESTAMP,
        error_message TEXT,
        retry_count INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (date, step_name)
    )
"""


def is_backfillable(step_name: str) -> bool:
    """
    Return whether `step_name` is allowed to run during a backfill.

    Parameters
    ----------
    step_name : str
        Must be one of STEP_NAMES.

    Returns
    -------
    bool

    Spec References
    ----------------
    SPEC-SCHED-006: model inference / signal-writing steps are never
    backfillable — gap days get data + features only.

    Raises
    ------
    ValueError
        If step_name is not a recognized step.
    """
    if step_name not in _BACKFILLABLE:
        raise ValueError(f"Unknown step_name '{step_name}'. Must be one of {STEP_NAMES}")
    return _BACKFILLABLE[step_name]


class CheckpointManager:
    """
    Per-step checkpoint read/write for the daily pipeline (SPEC-SCHED-002).

    Each (date, step_name) pair has exactly one row in pipeline_checkpoints,
    upserted on every status transition. SPEC-SCHED-010: every write is a
    single statement inside one commit — never a partially-applied row.
    """

    def __init__(self, db_path: Optional[Path] = None, in_memory: bool = False) -> None:
        """
        Parameters
        ----------
        db_path : Path, optional
            Path to the pipeline log SQLite file. If None and in_memory is
            False, uses config.settings.PIPELINE_LOG_DB_PATH (the same file
            pipeline_runs lives in).
        in_memory : bool
            If True, use an in-memory SQLite database (db_path is ignored).
            Used by tests/unit/test_scheduler.py and
            tests/integration/test_scheduler_resume.py.

        Spec References
        ----------------
        SPEC-SCHED-002, SPEC-SCHED-005, SPEC-DS-007

        Raises
        ------
        None
        """
        if in_memory:
            self._db_path: Optional[Path] = None
        elif db_path is not None:
            self._db_path = db_path
            self._db_path.parent.mkdir(parents=True, exist_ok=True)
        else:
            from config.settings import PIPELINE_LOG_DB_PATH

            self._db_path = PIPELINE_LOG_DB_PATH
            self._db_path.parent.mkdir(parents=True, exist_ok=True)

        self._ensure_schema()

    def _ensure_schema(self) -> None:
        with get_sqlite_connection(self._db_path) as conn:
            conn.execute(_CREATE_PIPELINE_CHECKPOINTS)
            conn.commit()

    def save_checkpoint(
        self,
        run_date: date_type,
        step_name: str,
        status: str,
        error_message: Optional[str] = None,
        retry_count: int = 0,
    ) -> None:
        """
        Upsert the checkpoint row for (run_date, step_name).

        Parameters
        ----------
        run_date : date
            Trading date this step ran for.
        step_name : str
            Must be one of STEP_NAMES.
        status : str
            One of 'running', 'success', 'failed', 'skipped'.
        error_message : str, optional
            Set when status='failed' (SPEC-SCHED-002). Cleared (None) on
            success.
        retry_count : int
            Number of retry attempts made for this step so far.

        Returns
        -------
        None

        Spec References
        ----------------
        SPEC-SCHED-002: on failure, record error_message and status='failed'
            so the next startup resumes from this step.
        SPEC-SCHED-005: pipeline_checkpoints is the per-step source of truth.
        SPEC-SCHED-010: atomic write — one statement, one commit.

        PIT Assumptions
        ----------------
        None — this is operational metadata, not market data.

        Raises
        ------
        ValueError
            If step_name or status is not recognized.
        """
        if step_name not in STEP_NAMES:
            raise ValueError(f"Unknown step_name '{step_name}'. Must be one of {STEP_NAMES}")
        if status not in _VALID_STATUSES:
            raise ValueError(f"Unknown status '{status}'. Must be one of {sorted(_VALID_STATUSES)}")

        step_index = STEP_NAMES.index(step_name)
        now = now_ist().isoformat()
        started_at = now if status == "running" else None
        completed_at = now if status in ("success", "failed", "skipped") else None

        with get_sqlite_connection(self._db_path) as conn:
            conn.execute(
                """
                INSERT INTO pipeline_checkpoints
                    (date, step_name, step_index, status, started_at,
                     completed_at, error_message, retry_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(date, step_name) DO UPDATE SET
                    step_index = excluded.step_index,
                    status = excluded.status,
                    started_at = COALESCE(pipeline_checkpoints.started_at, excluded.started_at),
                    completed_at = excluded.completed_at,
                    error_message = excluded.error_message,
                    retry_count = excluded.retry_count
                """,
                (
                    run_date.isoformat(),
                    step_name,
                    step_index,
                    status,
                    started_at,
                    completed_at,
                    error_message,
                    retry_count,
                ),
            )
            conn.commit()

        logger.info(f"Checkpoint saved: {run_date} / {step_name} -> {status}")

    def get_succeeded_steps(self, run_date: date_type) -> set:
        """
        Return the set of step names that have status='success' for run_date.

        Used by run_steps_for_date to pre-seed the succeeded-steps set so
        that SPEC-SCHED-011 dependency checks work correctly on a resume
        (steps that succeeded in a prior run of this date count as satisfied
        prerequisites for later steps).

        Parameters
        ----------
        run_date : date

        Returns
        -------
        set of str
            Step names with status='success' for this date, possibly empty.

        Spec References
        ----------------
        SPEC-SCHED-011: dependency pre-seeding across run boundaries.

        Raises
        ------
        None
        """
        with get_sqlite_connection(self._db_path) as conn:
            rows = conn.execute(
                "SELECT step_name FROM pipeline_checkpoints "
                "WHERE date = ? AND status = 'success'",
                (run_date.isoformat(),),
            ).fetchall()
        return {row[0] for row in rows}

    def load_checkpoint(self, run_date: date_type) -> Optional[str]:
        """
        Return the name of the last successfully completed step for a date.

        Parameters
        ----------
        run_date : date

        Returns
        -------
        str or None
            The step_name with the highest step_index among rows with
            status='success' for run_date, or None if no step has
            succeeded yet.

        Spec References
        ----------------
        SPEC-SCHED-002, SPEC-SCHED-005

        PIT Assumptions
        ----------------
        None.

        Raises
        ------
        None
        """
        with get_sqlite_connection(self._db_path) as conn:
            row = conn.execute(
                """
                SELECT step_name FROM pipeline_checkpoints
                WHERE date = ? AND status = 'success'
                ORDER BY step_index DESC LIMIT 1
                """,
                (run_date.isoformat(),),
            ).fetchone()

        return row[0] if row else None

    def get_resume_step(self, run_date: date_type) -> Optional[str]:
        """
        Return the step to resume from next for a date.

        The first step in STEPS order that is not yet terminal ('success'
        or 'skipped') for run_date — i.e. steps that previously failed or
        have never been attempted. 'skipped' steps are re-evaluated on
        resume because their unmet dependency may have been fixed (e.g. a
        previously failed prerequisite was force-run via the Ops API).

        Parameters
        ----------
        run_date : date

        Returns
        -------
        str or None
            Step name to resume from, or None if every step is terminal
            ('success' or 'skipped-with-still-unmet-deps' aside, see note
            below). For completeness: if every applicable step has
            status='success', returns None (nothing to do).

        Spec References
        ----------------
        SPEC-SCHED-002: "next run resumes from step N (does not re-execute
        1 to N-1)".
        SPEC-SCHED-011: skipped-due-to-unmet-dep steps are re-evaluated on
        the next startup so they can run once their dependency is fixed.

        PIT Assumptions
        ----------------
        None.

        Raises
        ------
        None
        """
        with get_sqlite_connection(self._db_path) as conn:
            done = {
                row[0]
                for row in conn.execute(
                    "SELECT step_name FROM pipeline_checkpoints "
                    "WHERE date = ? AND status = 'success'",
                    (run_date.isoformat(),),
                ).fetchall()
            }

        # Resume from the first step that hasn't definitively succeeded.
        # Skipped and failed steps are both retried so a fixed prerequisite
        # can unlock its dependents.
        for step_name in STEP_NAMES:
            if step_name not in done:
                return step_name
        return None
