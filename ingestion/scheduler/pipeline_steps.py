"""
ingestion/scheduler/pipeline_steps.py

Per-date step execution: run_steps_for_date, run_backfill, and StepRunner
type alias. Extracted from pipeline_scheduler.py (A46 — per-concern module split).

Consumers: pipeline_startup.py, scheduler_jobs.py, tests/*.py
           (re-exported via pipeline_scheduler.py for backward compat)
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date as date_type
from typing import Callable, Dict, List, Optional, Tuple

from ingestion.scheduler.checkpoint import STEP_NAMES, STEPS, CheckpointManager
from ingestion.scheduler.pipeline_run_lock import pipeline_run_lock

logger = logging.getLogger(__name__)

# Pre-compute the depends_on lookup for fast access in run_steps_for_date.
# {step_name: [dep_name, ...]} — empty list means no hard prerequisites.
_STEP_DEPS: dict[str, list[str]] = {step["name"]: step.get("depends_on", []) for step in STEPS}

# Signature: step_runner(run_date, step_name) -> None. Must raise on failure.
StepRunner = Callable[[date_type, str], None]


def _compute_step_dependency_depth(step_name: str, memo: Optional[Dict[str, int]] = None) -> int:
    """
    Compute the dependency depth of a step for wave grouping.

    Depth 0: steps with no dependencies.
    Depth N: steps whose deepest dependency has depth N-1.

    Used to group steps into waves where all steps in a wave can run
    concurrently (no intra-wave dependencies).

    `memo` defaults to None (never a mutable `{}` default — every call
    without an explicit memo gets its own fresh dict) even though caching
    across calls would actually be safe here (_STEP_DEPS is a static
    module-level constant, never mutated at runtime) — an explicit-only
    shared cache is easier to reason about than an implicit one hiding in
    a default argument, and costs nothing since STEPS is small.
    """
    if memo is None:
        memo = {}
    if step_name in memo:
        return memo[step_name]
    deps = _STEP_DEPS.get(step_name, [])
    if not deps:
        depth = 0
    else:
        depth = 1 + max(_compute_step_dependency_depth(d, memo) for d in deps if d in _STEP_DEPS)
    memo[step_name] = depth
    return depth


def _execute_wave(
    wave: List[Tuple[int, str]],
    run_date: date_type,
    step_runner: StepRunner,
    checkpoint_manager: CheckpointManager,
    succeeded_this_run: set[str],
    is_backfill: bool,
) -> bool:
    """
    Execute all steps in a wave concurrently using ThreadPoolExecutor.

    Parameters
    ----------
    wave : List[Tuple[int, str]]
        List of (index, step_name) tuples to execute.
    run_date, step_runner, checkpoint_manager, succeeded_this_run, is_backfill
        Passed to step execution logic.

    Returns
    -------
    bool
        True if all steps in the wave succeeded, False if any failed.
    """
    any_failed = False

    def run_step_thread(step_name: str) -> None:
        """Run one step, update checkpoint, track success/failure."""
        nonlocal any_failed
        checkpoint_manager.save_checkpoint(run_date, step_name, status="running", is_backfill=is_backfill)
        try:
            step_runner(run_date, step_name)
        except Exception as exc:
            checkpoint_manager.save_checkpoint(
                run_date, step_name, status="failed", error_message=str(exc), is_backfill=is_backfill
            )
            logger.error(f"Step '{step_name}' failed for {run_date}: {exc}")
            any_failed = True
        else:
            checkpoint_manager.save_checkpoint(
                run_date, step_name, status="success", is_backfill=is_backfill
            )
            succeeded_this_run.add(step_name)

    # Execute steps concurrently
    if len(wave) == 1:
        # Single step in wave: run directly without thread overhead
        _, step_name = wave[0]
        run_step_thread(step_name)
    else:
        # Multiple steps in wave: use ThreadPoolExecutor for parallelism
        logger.debug(f"Executing wave with {len(wave)} concurrent steps: {[s for _, s in wave]}")
        with ThreadPoolExecutor(max_workers=len(wave)) as executor:
            futures = {
                executor.submit(run_step_thread, step_name): step_name
                for _, step_name in wave
            }
            # Wait for all threads to complete and collect any exceptions
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as exc:
                    logger.error(f"Unexpected exception from thread: {exc}")
                    any_failed = True

    return any_failed


def run_steps_for_date(
    run_date: date_type,
    step_runner: StepRunner,
    checkpoint_manager: CheckpointManager,
    is_backfill: bool = False,
    stop_before_step: Optional[str] = None,
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
    stop_before_step : str, optional
        [2026-09-05] If set, the loop returns as soon as it would attempt
        this step, WITHOUT executing or checkpointing it (not even as
        'skipped') — leaves it untouched for a later call to pick up via
        the normal resume-from-checkpoint path. Used by run_backfill to run
        every date up to (not including) 'compute_features' individually,
        batch that one step across all gap dates via the hybrid backfill
        script, then call this function again per date with no
        stop_before_step to resume from 'compute_features's successor.

    Returns
    -------
    bool
        True if every applicable step succeeded or was intentionally skipped
        (or execution stopped cleanly at stop_before_step).
        False if any attempted step raised an exception.

    Lock Acquisition Strategy (SPEC-SCHED-013, 2026-09-05 parallelization)
    --------
    A56 acquires `pipeline_run_lock()` once per step (not once for the whole
    date) so other *processes* can interleave between steps. This function
    keeps that per-step granularity but at wave level: one lock hold per
    wave, spanning that wave's ENTIRE execution (not released until every
    step in the wave has actually run) — the lock must cover the real work,
    not just gate entry into the loop, or a second process's own lock-check
    could see "available" mid-wave and start running concurrently, which is
    exactly the double-run race this lock exists to prevent.
    - fcntl.flock is held per-process (per open file description), not
      per-thread, so holding it for the whole wave does NOT serialize the
      ThreadPoolExecutor threads inside _execute_wave against each other —
      they never separately acquire this lock, only this one `with` block
      does, so independent steps genuinely run concurrently regardless.
    - DuckDB write-lock contention between those threads' own DB connections
      is a separate, already-handled concern: get_duckdb_connection's
      retry-with-backoff logic (SPEC-SCHED-013's original intent).
    - If another process holds the lock before a wave starts, this function
      bails out with the same semantics as the original per-step version.
    """
    resume_step = checkpoint_manager.get_resume_step(run_date)
    if resume_step is None:
        logger.info(f"All steps already succeeded for {run_date} — nothing to do")
        return True

    resume_index = STEP_NAMES.index(resume_step)
    succeeded_this_run: set[str] = checkpoint_manager.get_succeeded_steps(run_date)

    # Fast-path: date whose output is already fully delivered.
    if {"write_signals", "publish_and_snapshot"} <= succeeded_this_run:
        logger.info(
            f"{run_date}: output already fully delivered (signals+snapshot) — "
            f"skipping re-process on restart"
        )
        return True

    any_step_failed = False
    any_step_attempted = False

    # Build waves: group steps by dependency depth for concurrent execution
    # within each wave. Steps are grouped such that all dependencies of
    # steps in wave N are satisfied in waves 0..N-1, allowing concurrent
    # execution within a wave without violating SPEC-SCHED-011 dependencies.
    step_depths = {step["name"]: _compute_step_dependency_depth(step["name"]) for step in STEPS}
    max_depth = max((d for d in step_depths.values()), default=-1)

    # Process steps in depth order (each depth level becomes a wave)
    for depth_level in range(max_depth + 1):
        # Collect all steps at this depth that should be attempted
        current_wave: List[Tuple[int, str]] = []

        for index, step in enumerate(STEPS):
            step_name = step["name"]

            # Skip steps before resume point
            if index < resume_index:
                succeeded_this_run.add(step_name)
                continue

            # Skip steps not at this depth level
            if step_depths[step_name] != depth_level:
                continue

            # Check for stop_before_step
            if step_name == stop_before_step:
                return not any_step_failed

            # Skip non-backfillable steps during backfill
            if is_backfill and not step["is_backfillable"]:
                logger.info(f"Skipping non-backfillable step '{step_name}' for {run_date} (backfill)")
                continue

            # SPEC-SCHED-011: dependency check
            deps = _STEP_DEPS.get(step_name, [])
            unmet = [d for d in deps if d not in succeeded_this_run]
            if unmet:
                reason = f"dependency not met: {unmet}"
                checkpoint_manager.save_checkpoint(
                    run_date, step_name, status="skipped", error_message=reason, is_backfill=is_backfill
                )
                logger.warning(f"Skipping '{step_name}' for {run_date} — {reason}")
                continue

            current_wave.append((index, step_name))

        # Skip empty waves
        if not current_wave:
            continue

        # [2026-09-05 review fix] The lock must be held for the wave's ACTUAL
        # execution, not just checked-then-released before it — an earlier
        # version of this change released the lock immediately after
        # confirming it was free, leaving a race window where a second
        # process's own lock-check would see "available" and start running
        # concurrently, exactly the double-run scenario this lock exists to
        # prevent. fcntl.flock is held per-process (per open file
        # description), not per-thread, so holding it for the whole wave
        # does NOT serialize the ThreadPoolExecutor threads inside
        # _execute_wave against each other — they never separately acquire
        # this lock, only the outer `with` block does. DuckDB write-lock
        # contention between the concurrent threads' own DB connections is
        # separately handled by get_duckdb_connection's retry-with-backoff
        # logic (SPEC-SCHED-013), which is unrelated to this process-level
        # advisory lock.
        with pipeline_run_lock() as acquired:
            if not acquired:
                if any_step_attempted:
                    logger.warning(
                        f"run_steps_for_date({run_date}): lock acquired by another "
                        f"run before this wave could start, after this invocation "
                        "already completed earlier steps — stopping here"
                    )
                    return False
                logger.warning(
                    f"run_steps_for_date({run_date}): another run is already in progress "
                    f"(cross-process lock held) — skipping this call"
                )
                return True

            wave_failed = _execute_wave(
                current_wave, run_date, step_runner, checkpoint_manager,
                succeeded_this_run, is_backfill
            )
            if wave_failed:
                any_step_failed = True
            any_step_attempted = True

    return not any_step_failed


def run_backfill(
    gap_dates: List[date_type],
    step_runner: StepRunner,
    checkpoint_manager: CheckpointManager,
) -> List[date_type]:
    """
    Process missing dates chronologically, oldest first.

    [2026-09-05] At MULTI_DAY_BACKFILL_THRESHOLD_DAYS gap dates or more,
    routes the 'compute_features' step through the ticker-first hybrid
    batch script (ingestion/scheduler/batch_feature_backfill.py) instead of
    running it once per date via step_runner — see that module's docstring
    for why (a real multi-week gap made the date-first per-date path hang).

    Automatic fallback (verified via dry-run 2026-09-05, not separately
    coded — an emergent property of phase 3 below reusing the plain
    per-date path): if the batch script fails to produce a date's output,
    that date's checkpoint is left at compute_features='failed', so phase
    3's normal run_steps_for_date call resumes AT compute_features for
    that one date and retries it via the original date-first step_runner
    path. This only affects the specific date(s) the batch call failed
    for — not a return to date-first for the whole gap — and is still
    protected by the 2026-09-05 SIGTERM+30-min-timeout fix in
    features/matrix_builder.py if that fallback attempt would otherwise
    hang. If the fallback also fails, the date is correctly excluded from
    this function's return value and retried on the next startup, same as
    any other failed backfill date.
    Below the threshold (the ordinary single-missed-day case), behavior is
    unchanged: each date runs its full step sequence via step_runner alone.

    Returns the subset of gap_dates that completed successfully.
    """
    from config.settings import MULTI_DAY_BACKFILL_THRESHOLD_DAYS

    ordered = sorted(gap_dates)

    if len(ordered) < MULTI_DAY_BACKFILL_THRESHOLD_DAYS:
        succeeded = []
        for gap_date in ordered:
            ok = run_steps_for_date(gap_date, step_runner, checkpoint_manager, is_backfill=True)
            if ok:
                succeeded.append(gap_date)
            else:
                logger.warning(f"Backfill for {gap_date} incomplete — will retry on next startup")
        return succeeded

    from ingestion.scheduler.batch_feature_backfill import run_batch_compute_features

    logger.info(
        f"run_backfill: {len(ordered)} gap dates >= MULTI_DAY_BACKFILL_THRESHOLD_DAYS "
        f"({MULTI_DAY_BACKFILL_THRESHOLD_DAYS}) — routing compute_features through the "
        f"hybrid batch script"
    )

    # Phase 1: every date runs its own steps up to (not including)
    # compute_features — these are per-date downloads/adjustments, cheap
    # and inherently date-scoped, so no benefit to batching them.
    for gap_date in ordered:
        run_steps_for_date(
            gap_date, step_runner, checkpoint_manager, is_backfill=True,
            stop_before_step="compute_features",
        )

    # Phase 2: one hybrid-script call computes compute_features for every
    # date in the range at once; reconciles the checkpoint per date itself.
    run_batch_compute_features(ordered, checkpoint_manager)

    # Phase 3: resume each date from wherever its checkpoint now sits
    # (compute_features's successor, if phase 2 succeeded for that date) —
    # run_steps_for_date's own get_resume_step handles this with no special
    # casing needed here.
    succeeded = []
    for gap_date in ordered:
        ok = run_steps_for_date(gap_date, step_runner, checkpoint_manager, is_backfill=True)
        if ok:
            succeeded.append(gap_date)
        else:
            logger.warning(f"Backfill for {gap_date} incomplete — will retry on next startup")
    return succeeded
