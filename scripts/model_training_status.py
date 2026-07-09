#!/usr/bin/env python3
"""
Model Training Status Checker
SPEC-MODEL-005, SPEC-SCHED-007

Reports, for every model this scheduler knows how to train
(_MODEL_TRAINING_SCRIPT_MAP in ingestion/scheduler/pipeline_scheduler.py),
whether it has ever been trained, whether it is overdue per
datastore/models/registry.json, and the model_training job's own
scheduler_heartbeats status/next-run-time.

Usage: python scripts/model_training_status.py
"""

import json
import sys
from datetime import date as date_cls
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import (
    DEFAULT_TRAINING_INTERVAL_DAYS,
    MODELS_DIR,
    PIPELINE_LOG_DB_PATH,
    RETRAIN_OVERDUE_MULTIPLIER,
)
from config.timezone import now_ist
from datastore.api.db import get_sqlite_connection
from datastore.api.utils.scheduler_status import get_next_run_times
from ingestion.scheduler.pipeline_scheduler import _MODEL_TRAINING_SCRIPT_MAP


def _load_registry() -> dict:
    registry_path = Path(MODELS_DIR) / "registry.json"
    if not registry_path.exists():
        return {}
    with registry_path.open() as f:
        return json.load(f)


def _model_rows(registry: dict) -> list:
    today = now_ist().date()
    known_models = sorted(
        set(registry.keys()) | {name for name, script in _MODEL_TRAINING_SCRIPT_MAP.items() if script is not None}
    )
    rows = []
    for model_name in known_models:
        meta = registry.get(model_name, {})
        last_train_str = meta.get("last_trained_date")
        interval_days = meta.get("training_interval_days", DEFAULT_TRAINING_INTERVAL_DAYS)
        script = _MODEL_TRAINING_SCRIPT_MAP.get(model_name, "(not scheduler-mapped)")
        if not last_train_str:
            rows.append((model_name, "never", "-", interval_days, "NEVER TRAINED", script))
            continue
        last_train = date_cls.fromisoformat(last_train_str)
        days_since = (today - last_train).days
        threshold = interval_days * RETRAIN_OVERDUE_MULTIPLIER
        status = "OVERDUE" if days_since > threshold else "OK"
        rows.append((model_name, last_train_str, str(days_since), interval_days, status, script))
    return rows


def _heartbeat_row(job_id: str) -> dict:
    try:
        with get_sqlite_connection(PIPELINE_LOG_DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT last_attempt_at, last_status, last_error, last_success_at "
                "FROM scheduler_heartbeats WHERE job_id = ?",
                (job_id,),
            )
            row = cursor.fetchone()
    except Exception as exc:
        return {"error": str(exc)}
    if row is None:
        return {}
    return {
        "last_attempt_at": row[0],
        "last_status": row[1],
        "last_error": row[2],
        "last_success_at": row[3],
    }


def print_status() -> None:
    print("=" * 88)
    print("MODEL TRAINING STATUS  (SPEC-MODEL-005, SPEC-SCHED-007)")
    print("=" * 88)

    registry = _load_registry()
    rows = _model_rows(registry)

    header = f"{'MODEL':<20}{'LAST TRAINED':<14}{'DAYS SINCE':<12}{'INTERVAL':<10}{'STATUS':<16}{'TRAINER MODULE'}"
    print(header)
    print("-" * len(header))
    for model_name, last_trained, days_since, interval_days, status, script in rows:
        print(f"{model_name:<20}{last_trained:<14}{days_since:<12}{interval_days!s:<10}{status:<16}{script}")

    n_never = sum(1 for r in rows if r[4] == "NEVER TRAINED")
    n_overdue = sum(1 for r in rows if r[4] == "OVERDUE")
    print()
    print(f"{len(rows)} model(s) tracked — {n_never} never trained, {n_overdue} overdue.")

    print()
    print("-" * 88)
    print("SCHEDULER JOB: model_training")
    print("-" * 88)
    hb = _heartbeat_row("model_training")
    if not hb:
        print("  No heartbeat recorded yet — job has never fired (e.g. no Saturday 12:00 IST run yet).")
    elif "error" in hb:
        print(f"  Could not read scheduler_heartbeats: {hb['error']}")
    else:
        print(f"  last_attempt_at:  {hb['last_attempt_at']}")
        print(f"  last_status:      {hb['last_status']}")
        print(f"  last_success_at:  {hb['last_success_at']}")
        if hb.get("last_error"):
            print(f"  last_error:       {hb['last_error']}")

    next_runs = get_next_run_times()
    next_run = next_runs.get("model_training")
    print(f"  next_scheduled:   {next_run.isoformat() if next_run else 'unknown'}")
    print("=" * 88)


if __name__ == "__main__":
    print_status()
    sys.exit(0)
