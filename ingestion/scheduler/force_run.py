"""
ingestion/scheduler/force_run.py

Phase: A21 (Pipeline Health Checker)
Specs: FeatureBacklog.md A20 (approve-before-apply pattern), A21
Owner: Platform / Scheduler
Consumers: datastore/api/routers/ops.py::force_run_step,
    datastore/health/catchup.py

Synchronous core of "force-run STEPS from `step_name` forward for a list
of dates, respecting checkpoint.py's depends_on/STEPS ordering and
stopping at the first failure" — extracted out of
datastore/api/routers/ops.py's force_run_step endpoint (2026-07-09, A21)
so A21's "force_run_daily_pipeline" catch-up action can reuse the EXACT
same dependency-respecting walk instead of reimplementing it. The Ops
endpoint itself becomes a thin async wrapper (asyncio.to_thread) around
this function; no behavior change there.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date as date_type
from typing import List, Optional

from config.settings import PIPELINE_LOG_DB_PATH
from datastore.api.db import get_sqlite_connection
from ingestion.scheduler.checkpoint import STEPS, CheckpointManager
from ingestion.scheduler.daily_pipeline import step_runner


@dataclass
class ForceRunResult:
    step_name: str
    date: str
    status: str  # 'success' | 'failed'
    error_message: Optional[str] = None


def force_run_date_sync(
    step_name: str,
    run_dates: List[date_type],
    today: date_type,
    cascade: bool = True,
    checkpoint_manager: Optional[CheckpointManager] = None,
) -> List[ForceRunResult]:
    """
    Run `step_name` (and, if cascade, every following STEPS entry in
    order) for each date in `run_dates`, stopping a given date's walk at
    the first failure, and never crossing into a non-backfillable step
    (e.g. paper_trade) for a date before `today`.

    Parameters
    ----------
    step_name : str
        Must be a name in checkpoint.STEPS.
    run_dates : List[date]
        Dates to attempt, in order.
    today : date
        Used only to decide whether a backfilled (< today) date may run a
        non-backfillable step (it may not).
    cascade : bool
        If True (default), keep running subsequent STEPS for the same
        date after step_name succeeds. If False, run only step_name.
    checkpoint_manager : CheckpointManager, optional
        Defaults to a fresh CheckpointManager() against the real
        pipeline_checkpoints table — pass an in_memory=True instance in
        tests.

    Returns
    -------
    List[ForceRunResult]
        One entry per step actually attempted, across all run_dates.

    Raises
    ------
    ValueError
        - If `step_name` is not a known STEPS name.
        - If a SINGLE requested date has unmet lower-index prerequisites
          (a multi-date backfill instead just skips that date and tries
          the next, logging a warning — same semantics as the original
          ops.py endpoint).
        - If every candidate date was skipped for unmet prerequisites
          (nothing was ever run).
    """
    step_names = [s["name"] for s in STEPS]
    if step_name not in step_names:
        raise ValueError(f"Unknown step '{step_name}'. Must be one of {step_names}")
    step_index = step_names.index(step_name)

    if checkpoint_manager is None:
        checkpoint_manager = CheckpointManager()

    results: List[ForceRunResult] = []

    for run_date in run_dates:
        resolved_date = run_date.isoformat()

        with get_sqlite_connection(PIPELINE_LOG_DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT step_name FROM pipeline_checkpoints WHERE date = ? AND status = 'success'",
                (resolved_date,),
            )
            succeeded = {r[0] for r in cursor.fetchall()}

        unmet = [name for name in step_names[:step_index] if name not in succeeded]
        if unmet:
            if len(run_dates) > 1:
                continue
            raise ValueError(
                f"Cannot force-run '{step_name}' — prerequisite step(s) not yet successful for {resolved_date}: {unmet}"
            )

        current_index = step_index
        while current_index < len(STEPS):
            current_meta = STEPS[current_index]
            current_name = current_meta["name"]

            if run_date < today and not current_meta["is_backfillable"]:
                break  # never run inference/signal/paper-trade steps on a gap day

            checkpoint_manager.save_checkpoint(run_date, current_name, status="running")
            try:
                step_runner(run_date, current_name)
            except Exception as exc:  # noqa: BLE001
                checkpoint_manager.save_checkpoint(run_date, current_name, status="failed", error_message=str(exc))
                results.append(ForceRunResult(step_name=current_name, date=resolved_date, status="failed", error_message=str(exc)))
                break  # don't cascade past a failure

            checkpoint_manager.save_checkpoint(run_date, current_name, status="success")
            results.append(ForceRunResult(step_name=current_name, date=resolved_date, status="success"))
            succeeded.add(current_name)

            if not cascade:
                break
            current_index += 1

    if not results:
        last_date = run_dates[-1].isoformat() if run_dates else "n/a"
        first_date = run_dates[0].isoformat() if run_dates else "n/a"
        raise ValueError(
            f"Cannot force-run '{step_name}' — no candidate date had its prerequisite steps successful "
            f"(checked {first_date} to {last_date})"
        )

    return results
