"""
ingestion/scheduler/pipeline_steps.py

Per-date step execution: run_steps_for_date, run_backfill, and StepRunner
type alias. Extracted from pipeline_scheduler.py (A46 — per-concern module split).

Consumers: pipeline_startup.py, scheduler_jobs.py, tests/*.py
           (re-exported via pipeline_scheduler.py for backward compat)
"""

import logging
from datetime import date as date_type
from typing import Callable, List

from ingestion.scheduler.checkpoint import STEP_NAMES, STEPS, CheckpointManager
from ingestion.scheduler.pipeline_run_lock import pipeline_run_lock

logger = logging.getLogger(__name__)

# Pre-compute the depends_on lookup for fast access in run_steps_for_date.
# {step_name: [dep_name, ...]} — empty list means no hard prerequisites.
_STEP_DEPS: dict = {step["name"]: step.get("depends_on", []) for step in STEPS}

# Signature: step_runner(run_date, step_name) -> None. Must raise on failure.
StepRunner = Callable[[date_type, str], None]


def run_steps_for_date(
    run_date: date_type,
    step_runner: StepRunner,
    checkpoint_manager: CheckpointManager,
    is_backfill: bool = False,
) -> bool:
    """
    Execute STEPS for one date with checkpoint-resume, backfill ML-skip,
    and dependency-based fallback (SPEC-SCHED-011).

    See pipeline_scheduler.py for full docstring (moved to pipeline_steps.py
    as part of A46 per-concern module split).

    Parameters
    ----------
    run_date : date
    step_runner : StepRunner
    checkpoint_manager : CheckpointManager
    is_backfill : bool
        If True, steps with is_backfillable=False are skipped entirely.

    Returns
    -------
    bool
        True if every applicable step succeeded or was intentionally skipped.
        False if any attempted step raised an exception.
    """
    resume_step = checkpoint_manager.get_resume_step(run_date)
    if resume_step is None:
        logger.info(f"All steps already succeeded for {run_date} — nothing to do")
        return True

    resume_index = STEP_NAMES.index(resume_step)
    succeeded_this_run: set = checkpoint_manager.get_succeeded_steps(run_date)

    # Fast-path: date whose output is already fully delivered.
    if {"write_signals", "publish_and_snapshot"} <= succeeded_this_run:
        logger.info(
            f"{run_date}: output already fully delivered (signals+snapshot) — "
            f"skipping re-process on restart"
        )
        return True

    any_step_failed = False
    any_step_attempted = False

    for index, step in enumerate(STEPS):
        step_name = step["name"]

        if index < resume_index:
            succeeded_this_run.add(step_name)
            continue

        if is_backfill and not step["is_backfillable"]:
            logger.info(f"Skipping non-backfillable step '{step_name}' for {run_date} (backfill)")
            continue

        # SPEC-SCHED-011: dependency check.
        deps = _STEP_DEPS.get(step_name, [])
        unmet = [d for d in deps if d not in succeeded_this_run]
        if unmet:
            reason = f"dependency not met: {unmet}"
            checkpoint_manager.save_checkpoint(
                run_date, step_name, status="skipped", error_message=reason, is_backfill=is_backfill
            )
            logger.warning(f"Skipping '{step_name}' for {run_date} — {reason}")
            continue

        with pipeline_run_lock() as acquired:
            if not acquired:
                if any_step_attempted:
                    logger.warning(
                        f"run_steps_for_date({run_date}): lock acquired by another "
                        f"run before '{step_name}' could start, after this invocation "
                        "already completed earlier steps — stopping here"
                    )
                    return False
                logger.warning(
                    f"run_steps_for_date({run_date}): another run is already in progress "
                    f"(cross-process lock held) — skipping this call"
                )
                return True

            checkpoint_manager.save_checkpoint(run_date, step_name, status="running", is_backfill=is_backfill)
            any_step_attempted = True
            try:
                step_runner(run_date, step_name)
            except Exception as exc:
                checkpoint_manager.save_checkpoint(
                    run_date, step_name, status="failed", error_message=str(exc), is_backfill=is_backfill
                )
                logger.error(f"Step '{step_name}' failed for {run_date}: {exc}")
                any_step_failed = True
            else:
                checkpoint_manager.save_checkpoint(
                    run_date, step_name, status="success", is_backfill=is_backfill
                )
                succeeded_this_run.add(step_name)

    return not any_step_failed


def run_backfill(
    gap_dates: List[date_type],
    step_runner: StepRunner,
    checkpoint_manager: CheckpointManager,
) -> List[date_type]:
    """
    Process missing dates chronologically, oldest first.

    Returns the subset of gap_dates that completed successfully.
    """
    ordered = sorted(gap_dates)
    succeeded = []
    for gap_date in ordered:
        ok = run_steps_for_date(gap_date, step_runner, checkpoint_manager, is_backfill=True)
        if ok:
            succeeded.append(gap_date)
        else:
            logger.warning(f"Backfill for {gap_date} incomplete — will retry on next startup")
    return succeeded