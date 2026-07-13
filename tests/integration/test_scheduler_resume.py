"""
tests/integration/test_scheduler_resume.py

Phase: 0.3 (Scheduler & Checkpoint Engine)
Specs: SPEC-SCHED-002, SPEC-SCHED-005, SPEC-SCHED-006, SPEC-SCHED-010
Owner: Platform / Scheduler
Consumers: CI, pytest

Full-pipeline integration test: CheckpointManager and run_steps_for_date
working together against a real (in-memory) SQLite checkpoint store —
simulate a crash partway through a run, restart, and verify the pipeline
RESUMES from the failed step rather than re-executing from the start.
"""

from datetime import date

import pytest

from ingestion.scheduler.checkpoint import STEP_NAMES, CheckpointManager
from ingestion.scheduler.pipeline_scheduler import run_steps_for_date


@pytest.fixture(autouse=True)
def _isolated_pipeline_run_lock(tmp_path, monkeypatch):
    """
    run_steps_for_date acquires a real cross-process fcntl.flock on
    config.settings.PIPELINE_RUN_LOCK_PATH. Without this, this test
    collides with whatever real process holds the production lock file
    (e.g. the actual scheduler service, if it's mid-run) and silently
    gets skipped — the exact failure mode found while working on the
    Pipeline & Monitoring Remediation plan (2026-07-10): this file was
    missing the isolation fixture tests/unit/test_scheduler.py already
    uses for the same reason.
    """
    import config.settings as settings_mod

    monkeypatch.setattr(settings_mod, "PIPELINE_RUN_LOCK_PATH", tmp_path / "pipeline_run.lock")


def test_pipeline_resumes_not_restarts_after_crash():
    """
    SPEC-SCHED-002: "If pipeline crashes at step 7, next run resumes from
    step 7 (does not re-execute steps 1 to N-1)."

    Run 1: a step_runner that crashes on 'adjust_prices'.
    Verify all prior steps are checkpointed 'success' and 'adjust_prices' is 'failed'.

    Run 2 ("restart"): a step_runner that always succeeds. Verify it
    executes ONLY 'adjust_prices' onward — steps 1-2 must NOT re-run —
    and that the run now completes successfully end to end.
    """
    checkpoint_manager = CheckpointManager(in_memory=True)
    run_date = date(2026, 2, 2)
    executed_run1 = []

    def crashing_runner(step_date, step_name):
        executed_run1.append(step_name)
        if step_name == "adjust_prices":
            raise RuntimeError("simulated crash")

    ok = run_steps_for_date(
        run_date, crashing_runner, checkpoint_manager, is_backfill=False
    )

    assert ok is False
    # Derived from STEP_NAMES rather than hardcoded: this test previously
    # hardcoded the pre-A20/A25 step list and silently went stale (still
    # asserting the old 8-step chain) once data_integrity_check and
    # publish_and_snapshot were added — caught while working on the
    # Pipeline & Monitoring Remediation plan (2026-07-10). Deriving the
    # expected prefix from STEP_NAMES means a future STEPS change can't
    # silently desync this assertion again.
    crash_index = STEP_NAMES.index("adjust_prices")
    assert executed_run1 == STEP_NAMES[: crash_index + 1]
    assert checkpoint_manager.load_checkpoint(run_date) == STEP_NAMES[crash_index - 1]
    assert checkpoint_manager.get_resume_step(run_date) == "adjust_prices"

    executed_run2 = []

    def succeeding_runner(step_date, step_name):
        executed_run2.append(step_name)

    ok2 = run_steps_for_date(
        run_date, succeeding_runner, checkpoint_manager, is_backfill=False
    )

    assert ok2 is True
    # RESUME, not restart: steps already succeeded must not appear in the second run.
    for already_succeeded in STEP_NAMES[:crash_index]:
        assert already_succeeded not in executed_run2
    assert executed_run2 == STEP_NAMES[crash_index:]
    assert checkpoint_manager.load_checkpoint(run_date) == STEP_NAMES[-1]
    assert checkpoint_manager.get_resume_step(run_date) is None


def test_repeated_failure_keeps_resuming_from_same_step():
    """
    SPEC-SCHED-002: if the resumed step fails again, the next run must
    still resume from that same step — never skip past a step that has
    never succeeded.
    """
    checkpoint_manager = CheckpointManager(in_memory=True)
    run_date = date(2026, 2, 3)

    def always_fails_at_compute_features(step_date, step_name):
        if step_name == "compute_features":
            raise RuntimeError("still broken")

    for _ in range(3):
        ok = run_steps_for_date(
            run_date,
            always_fails_at_compute_features,
            checkpoint_manager,
            is_backfill=False,
        )
        assert ok is False
        assert checkpoint_manager.get_resume_step(run_date) == "compute_features"

    # Every step before compute_features succeeded once and is never
    # re-attempted on subsequent resumes — derived from STEP_NAMES (see
    # test_pipeline_resumes_not_restarts_after_crash's comment above for
    # why this isn't hardcoded). load_checkpoint returns the *highest-
    # index* successful step, not "the step right before the failure":
    # publish_and_snapshot only depends_on ["download_fno", "adjust_prices"]
    # (checkpoint.py's STEPS), both satisfied here, so it still succeeds
    # even though the earlier compute_features/run_models/etc chain never
    # does — it is the true highest-index success in this scenario.
    compute_index = STEP_NAMES.index("compute_features")
    assert "publish_and_snapshot" not in STEP_NAMES[:compute_index]
    assert checkpoint_manager.load_checkpoint(run_date) == "publish_and_snapshot"
    assert checkpoint_manager.get_succeeded_steps(run_date) >= set(STEP_NAMES[:compute_index])
