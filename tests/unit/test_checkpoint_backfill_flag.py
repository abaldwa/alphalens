"""
tests/unit/test_checkpoint_backfill_flag.py

Regression coverage for A30 (FeatureBacklog.md): pipeline_checkpoints rows
now carry an is_backfill column so a step that ran via run_backfill/
run_morning_catchup_sequence (is_backfill=True) can be told apart from a
same-day live run (is_backfill=False) — both by CheckpointManager directly
and via run_steps_for_date's actual call pattern.
"""
from datetime import date

from ingestion.scheduler.checkpoint import CheckpointManager
from ingestion.scheduler.pipeline_scheduler import run_steps_for_date


def _is_backfill_flag(ckpt: CheckpointManager, run_date: date, step_name: str) -> bool:
    from datastore.api.db import get_sqlite_connection

    with get_sqlite_connection(ckpt._db_path) as conn:
        row = conn.execute(
            "SELECT is_backfill FROM pipeline_checkpoints WHERE date = ? AND step_name = ?",
            (run_date.isoformat(), step_name),
        ).fetchone()
    assert row is not None, f"no checkpoint row for {run_date}/{step_name}"
    return bool(row[0])


def test_save_checkpoint_defaults_is_backfill_to_false():
    ckpt = CheckpointManager(in_memory=True)
    run_date = date(2026, 3, 1)
    ckpt.save_checkpoint(run_date, "download_bhavcopy", status="success")
    assert _is_backfill_flag(ckpt, run_date, "download_bhavcopy") is False


def test_save_checkpoint_records_is_backfill_true_when_passed():
    ckpt = CheckpointManager(in_memory=True)
    run_date = date(2026, 3, 1)
    ckpt.save_checkpoint(run_date, "download_bhavcopy", status="success", is_backfill=True)
    assert _is_backfill_flag(ckpt, run_date, "download_bhavcopy") is True


def test_run_steps_for_date_live_run_marks_every_step_not_backfill():
    ckpt = CheckpointManager(in_memory=True)
    run_date = date(2026, 3, 2)

    def noop_runner(step_date, step_name):
        pass

    run_steps_for_date(run_date, noop_runner, ckpt, is_backfill=False)

    assert _is_backfill_flag(ckpt, run_date, "download_bhavcopy") is False
    assert _is_backfill_flag(ckpt, run_date, "compute_features") is False


def test_run_steps_for_date_backfill_run_marks_backfillable_steps_as_backfill():
    ckpt = CheckpointManager(in_memory=True)
    run_date = date(2026, 3, 3)

    def noop_runner(step_date, step_name):
        pass

    run_steps_for_date(run_date, noop_runner, ckpt, is_backfill=True)

    assert _is_backfill_flag(ckpt, run_date, "download_bhavcopy") is True
    assert _is_backfill_flag(ckpt, run_date, "compute_features") is True
    # paper_trade is not backfillable — never attempted, so it must have no
    # checkpoint row at all (not a False row, an absent one).
    from datastore.api.db import get_sqlite_connection

    with get_sqlite_connection(ckpt._db_path) as conn:
        row = conn.execute(
            "SELECT 1 FROM pipeline_checkpoints WHERE date = ? AND step_name = 'paper_trade'",
            (run_date.isoformat(),),
        ).fetchone()
    assert row is None
