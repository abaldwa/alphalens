"""
datastore/schema/create_signals.py

Phase: 0.2 (DataStore Schema & API Shell)
Specs: SPEC-DS-004, SPEC-DS-005, SPEC-DS-007, SPEC-SCHED-002
Owner: Platform / DataStore
Consumers: ingestion/scheduler, systems/ml_signal_engine, datastore/api, backtest

Creates the transactional pipeline log (SQLite) and Store 4 — Signals
(DuckDB) tables.

Engine split per SPEC-DS-007 ("DuckDB for analytical stores ... SQLite only
for transactional stores: pipeline_log, scheduler, checkpoints"):
- pipeline_runs is a transactional, single-row-per-run log used by the
  scheduler's checkpoint/resume protocol (SPEC-SCHED-002) -> SQLite.
- pipeline_checkpoints is the per-step companion to pipeline_runs
  (SPEC-SCHED-002, SPEC-SCHED-005, SPEC-SCHED-010): one row per
  (date, step_name), read/written by ingestion/scheduler/checkpoint.py
  -> SQLite, same file as pipeline_runs.
- ml_signals, ml_multibagger, ml_forensic are batch-written, analytically
  queried model outputs (architecture doc, "Store 4: Signals Store
  (DuckDB)") -> DuckDB, alongside every other system's signal tables.

[AS BUILT, P1.7] ml_signals.exit_urgency was a Phase 0.2 placeholder typed
VARCHAR; the actual ExitSignalModel (P1.6) produces a 0-100 float, and the
table had zero rows written by any tested code path, so this is a safe
in-place type fix, not a breaking schema migration. Also added
exit_survival_5d/21d/63d (DOUBLE) — P1.6's ExitSignalModel.predict_full()
output contract wasn't fully reflected in this Phase 0.2 schema.
"""

import logging
from pathlib import Path
from typing import Optional

from datastore.api.db import get_duckdb_connection, get_sqlite_connection

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SQLite: transactional pipeline log (SPEC-SCHED-002 checkpoint/resume)
# ---------------------------------------------------------------------------
_CREATE_PIPELINE_RUNS = """
    CREATE TABLE IF NOT EXISTS pipeline_runs (
        run_id INTEGER PRIMARY KEY AUTOINCREMENT,
        date DATE NOT NULL,
        started_at TIMESTAMP NOT NULL,
        completed_at TIMESTAMP,
        status TEXT NOT NULL,
        stocks_processed INTEGER DEFAULT 0,
        error_message TEXT
    )
"""

_PIPELINE_RUNS_TABLE = {"pipeline_runs": _CREATE_PIPELINE_RUNS}

# [AS BUILT, P1.7] Summary drift-check log, one row per pipeline run date —
# ingestion/quality/drift_monitor.py.PSIMonitor.check_drift() itself only
# returns an in-memory {feature: {psi, status}} dict; this table persists the
# worst (highest-PSI) result per date so datastore/api/routers/system.py's
# GET /health can report drift status without re-running PSI checks.
# SQLite (transactional, one row per run) per SPEC-DS-007, same file as
# pipeline_runs/pipeline_checkpoints.
_CREATE_PIPELINE_DRIFT_LOG = """
    CREATE TABLE IF NOT EXISTS pipeline_drift_log (
        date DATE PRIMARY KEY,
        worst_feature VARCHAR,
        worst_psi DOUBLE,
        worst_status VARCHAR NOT NULL,
        n_features_checked INTEGER NOT NULL DEFAULT 0,
        checked_at TIMESTAMP NOT NULL
    )
"""

_PIPELINE_DRIFT_LOG_TABLE = {"pipeline_drift_log": _CREATE_PIPELINE_DRIFT_LOG}

# [AS BUILT, SPEC-SCHED-013] One row per recurring scheduled job
# (job_id='daily_pipeline' | 'backfill_catchup'), upserted on EVERY
# invocation attempt — success or failure — by
# ingestion/scheduler/pipeline_scheduler.py's job wrappers. Lets GET
# /health (and an operator) tell "this job hasn't fired in N hours" apart
# from "it fires but keeps failing", neither of which was previously
# observable without reading the scheduler process's own log file by
# hand. Written because a real, multi-day-running scheduler process's
# job silently stopped firing entirely after one crash, with nothing
# anywhere recording that it had gone quiet — see BuildLog.md "Scheduler/
# DuckDB concurrency resilience".
_CREATE_SCHEDULER_HEARTBEATS = """
    CREATE TABLE IF NOT EXISTS scheduler_heartbeats (
        job_id VARCHAR PRIMARY KEY,
        last_attempt_at TIMESTAMP NOT NULL,
        last_status VARCHAR NOT NULL,
        last_error TEXT,
        last_success_at TIMESTAMP
    )
"""

_SCHEDULER_HEARTBEATS_TABLE = {"scheduler_heartbeats": _CREATE_SCHEDULER_HEARTBEATS}

# SPEC-SCHED-002, SPEC-SCHED-005, SPEC-SCHED-010: per-step checkpoint log,
# companion to pipeline_runs. One row per (date, step_name); upserted by
# ingestion/scheduler/checkpoint.py.CheckpointManager.save_checkpoint().
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

_PIPELINE_CHECKPOINTS_TABLE = {"pipeline_checkpoints": _CREATE_PIPELINE_CHECKPOINTS}

# ---------------------------------------------------------------------------
# DuckDB: Store 4 — Signals (analytical, batch-written model outputs)
# ---------------------------------------------------------------------------
# SPEC-DS-004: written by ML Signal Engine, daily
_CREATE_ML_SIGNALS = """
    CREATE TABLE IF NOT EXISTS ml_signals (
        date DATE NOT NULL,
        ticker VARCHAR NOT NULL,
        model_name VARCHAR NOT NULL,
        model_version VARCHAR NOT NULL,
        signal_direction VARCHAR,
        buy_prob DOUBLE,
        hold_prob DOUBLE,
        sell_prob DOUBLE,
        q10_return DOUBLE,
        q50_return DOUBLE,
        q90_return DOUBLE,
        meta_label VARCHAR,
        meta_prob DOUBLE,
        conformal_lower DOUBLE,
        conformal_upper DOUBLE,
        pnd_score DOUBLE,
        pnd_phase VARCHAR,
        pnd_block BOOLEAN,
        hmm_regime VARCHAR,
        hmm_regime_prob DOUBLE,
        hmm_stability DOUBLE,
        exit_urgency DOUBLE,
        exit_type VARCHAR,
        exit_survival_5d DOUBLE,
        exit_survival_21d DOUBLE,
        exit_survival_63d DOUBLE,
        shap_top5_json VARCHAR,
        PRIMARY KEY (date, ticker, model_name)
    )
"""

# SPEC-DS-004: written by ML Signal Engine, weekly
_CREATE_ML_MULTIBAGGER = """
    CREATE TABLE IF NOT EXISTS ml_multibagger (
        date DATE NOT NULL,
        ticker VARCHAR NOT NULL,
        mb_probability DOUBLE,
        mb_tier VARCHAR,
        mb_archetype VARCHAR,
        survival_6m DOUBLE,
        survival_12m DOUBLE,
        survival_24m DOUBLE,
        survival_36m DOUBLE,
        -- [AS BUILT, P2.6] MultibaggerModel.predict_full() (M-08,
        -- systems/ml_signal_engine/models/multibagger/multibagger_model.py)
        -- emits SURVIVAL_HORIZONS_MONTHS = (6, 12, 18, 24, 36) — this
        -- Phase 0.2 DDL only had 4 of the 5 horizons (missing 18m). Added
        -- here rather than silently dropping a real model output column
        -- when wiring up P2.6's scoring script.
        survival_18m DOUBLE,
        shap_top5_json VARCHAR,
        analogues_json VARCHAR,
        PRIMARY KEY (date, ticker)
    )
"""

# SPEC-DS-004: written by ML Signal Engine, quarterly
_CREATE_ML_FORENSIC = """
    CREATE TABLE IF NOT EXISTS ml_forensic (
        date DATE NOT NULL,
        ticker VARCHAR NOT NULL,
        beneish_m DOUBLE,
        altman_z DOUBLE,
        piotroski_f DOUBLE,
        ohlson_o DOUBLE,
        dechow_f DOUBLE,
        sloan_accrual DOUBLE,
        benford_mad DOUBLE,
        forensic_composite DOUBLE,
        forensic_flag BOOLEAN,
        -- [AS BUILT, P2.6] forensic_flag (BOOLEAN) is kept as "blocked"
        -- semantics (forensic_composite > forensic_ml.py's
        -- FORENSIC_BLOCK_THRESHOLD=60 — "BLOCKED from all buy
        -- recommendations"). forensic_ml.py's actual flag taxonomy is
        -- 5-level (green/yellow/orange/red/black, FLAG_LEVELS) — that
        -- richer label is added here as forensic_flag_label, not silently
        -- collapsed into the boolean. Used by the P2.6 dashboard's
        -- "forensic alert count (red/amber breakdown)".
        forensic_flag_label VARCHAR,
        forensic_ml_prob DOUBLE,
        shap_top5_json VARCHAR,
        pattern_match VARCHAR,
        PRIMARY KEY (date, ticker)
    )
"""

_SIGNAL_TABLES = {
    "ml_signals": _CREATE_ML_SIGNALS,
    "ml_multibagger": _CREATE_ML_MULTIBAGGER,
    "ml_forensic": _CREATE_ML_FORENSIC,
}

# [AS BUILT, P2.6] Same idempotent ALTER TABLE pattern as
# create_normalised.py's _MIGRATE_ADDED_COLUMNS — see that module's
# docstring for why CREATE TABLE IF NOT EXISTS alone cannot reach an
# already-created table file.
_MIGRATE_ADDED_COLUMNS = {
    "ml_multibagger": [
        "ALTER TABLE ml_multibagger ADD COLUMN IF NOT EXISTS survival_18m DOUBLE",
    ],
    "ml_forensic": [
        "ALTER TABLE ml_forensic ADD COLUMN IF NOT EXISTS forensic_flag_label VARCHAR",
    ],
}


def _migrate_added_columns(conn) -> None:
    """Idempotently ALTER any signal table whose schema has grown since it may have first been created."""
    for table_name, statements in _MIGRATE_ADDED_COLUMNS.items():
        for ddl in statements:
            conn.execute(ddl)
        logger.info(f"Ensured added columns present: {table_name}")


def create_pipeline_runs_schema(db_path: Optional[Path] = None, in_memory: bool = False) -> None:
    """
    Create the pipeline_runs SQLite table (transactional checkpoint log).

    Idempotent — safe to call multiple times.

    Args:
        db_path: Path to .db file. If None and in_memory=False, uses
            config.settings.PIPELINE_LOG_DB_PATH.
        in_memory: If True, create the table in an in-memory SQLite database
            (db_path is ignored). Used by tests/unit/test_schema.py.
    """
    if in_memory:
        db_path = None
    elif db_path is None:
        from config.settings import PIPELINE_LOG_DB_PATH

        db_path = PIPELINE_LOG_DB_PATH
        db_path.parent.mkdir(parents=True, exist_ok=True)

    with get_sqlite_connection(db_path) as conn:
        cursor = conn.cursor()
        for table_name, ddl in _PIPELINE_RUNS_TABLE.items():
            cursor.execute(ddl)
            logger.info(f"Ensured table exists: {table_name}")
        conn.commit()

    logger.info(f"Pipeline log schema ready at {db_path if db_path else ':memory:'}")


def create_pipeline_checkpoints_schema(db_path: Optional[Path] = None, in_memory: bool = False) -> None:
    """
    Create the pipeline_checkpoints SQLite table (per-step checkpoint log).

    Idempotent — safe to call multiple times. Also created defensively by
    CheckpointManager itself on first use; calling this explicitly is only
    needed to provision the schema ahead of time (e.g. in deployment setup).

    Args:
        db_path: Path to .db file. If None and in_memory=False, uses
            config.settings.PIPELINE_LOG_DB_PATH (same file as pipeline_runs).
        in_memory: If True, create the table in an in-memory SQLite database
            (db_path is ignored). Used by tests/unit/test_schema.py.
    """
    if in_memory:
        db_path = None
    elif db_path is None:
        from config.settings import PIPELINE_LOG_DB_PATH

        db_path = PIPELINE_LOG_DB_PATH
        db_path.parent.mkdir(parents=True, exist_ok=True)

    with get_sqlite_connection(db_path) as conn:
        cursor = conn.cursor()
        for table_name, ddl in _PIPELINE_CHECKPOINTS_TABLE.items():
            cursor.execute(ddl)
            logger.info(f"Ensured table exists: {table_name}")
        conn.commit()

    logger.info(f"Pipeline checkpoints schema ready at {db_path if db_path else ':memory:'}")


def create_pipeline_drift_log_schema(db_path: Optional[Path] = None, in_memory: bool = False) -> None:
    """
    Create the pipeline_drift_log SQLite table (one summary PSI row per pipeline run date).

    Idempotent — safe to call multiple times.

    Args:
        db_path: Path to .db file. If None and in_memory=False, uses
            config.settings.PIPELINE_LOG_DB_PATH (same file as pipeline_runs).
        in_memory: If True, create the table in an in-memory SQLite database
            (db_path is ignored). Used by tests/unit/test_schema.py.
    """
    if in_memory:
        db_path = None
    elif db_path is None:
        from config.settings import PIPELINE_LOG_DB_PATH

        db_path = PIPELINE_LOG_DB_PATH
        db_path.parent.mkdir(parents=True, exist_ok=True)

    with get_sqlite_connection(db_path) as conn:
        cursor = conn.cursor()
        for table_name, ddl in _PIPELINE_DRIFT_LOG_TABLE.items():
            cursor.execute(ddl)
            logger.info(f"Ensured table exists: {table_name}")
        conn.commit()

    logger.info(f"Pipeline drift log schema ready at {db_path if db_path else ':memory:'}")


def create_scheduler_heartbeats_schema(db_path: Optional[Path] = None, in_memory: bool = False) -> None:
    """
    Create the scheduler_heartbeats SQLite table (one row per recurring job).

    Idempotent — safe to call multiple times.

    Args:
        db_path: Path to .db file. If None and in_memory=False, uses
            config.settings.PIPELINE_LOG_DB_PATH (same file as pipeline_runs).
        in_memory: If True, create the table in an in-memory SQLite database
            (db_path is ignored). Used by tests/unit/test_schema.py.
    """
    if in_memory:
        db_path = None
    elif db_path is None:
        from config.settings import PIPELINE_LOG_DB_PATH

        db_path = PIPELINE_LOG_DB_PATH
        db_path.parent.mkdir(parents=True, exist_ok=True)

    with get_sqlite_connection(db_path) as conn:
        cursor = conn.cursor()
        for table_name, ddl in _SCHEDULER_HEARTBEATS_TABLE.items():
            cursor.execute(ddl)
            logger.info(f"Ensured table exists: {table_name}")
        conn.commit()

    logger.info(f"Scheduler heartbeats schema ready at {db_path if db_path else ':memory:'}")


def create_signal_tables_schema(db_path: Optional[Path] = None, in_memory: bool = False) -> None:
    """
    Create Store 4 (Signals) DuckDB tables: ml_signals, ml_multibagger, ml_forensic.

    Idempotent — safe to call multiple times.

    Args:
        db_path: Path to .duckdb file. If None and in_memory=False, uses
            config.settings.SIGNALS_DUCKDB_PATH.
        in_memory: If True, create the schema in an in-memory DuckDB
            (db_path is ignored). Used by tests/unit/test_schema.py.
    """
    if in_memory:
        db_path = None
    elif db_path is None:
        from config.settings import SIGNALS_DUCKDB_PATH

        db_path = SIGNALS_DUCKDB_PATH
        db_path.parent.mkdir(parents=True, exist_ok=True)

    with get_duckdb_connection(db_path) as conn:
        for table_name, ddl in _SIGNAL_TABLES.items():
            conn.execute(ddl)
            logger.info(f"Ensured table exists: {table_name}")
        _migrate_added_columns(conn)

    logger.info(f"Signals schema ready at {db_path if db_path else ':memory:'}")


def create_schema(
    sqlite_path: Optional[Path] = None,
    duckdb_path: Optional[Path] = None,
    in_memory: bool = False,
) -> None:
    """
    Create the full signals layer: pipeline_runs + pipeline_checkpoints
    (SQLite) + signal tables (DuckDB).

    Args:
        sqlite_path: Path for the pipeline log SQLite file, shared by
            pipeline_runs and pipeline_checkpoints
            (default: settings.PIPELINE_LOG_DB_PATH)
        duckdb_path: Path for signal tables DuckDB file (default: settings.SIGNALS_DUCKDB_PATH)
        in_memory: If True, both stores are created in-memory (paths ignored)
    """
    create_pipeline_runs_schema(db_path=sqlite_path, in_memory=in_memory)
    create_pipeline_checkpoints_schema(db_path=sqlite_path, in_memory=in_memory)
    create_pipeline_drift_log_schema(db_path=sqlite_path, in_memory=in_memory)
    create_scheduler_heartbeats_schema(db_path=sqlite_path, in_memory=in_memory)
    create_signal_tables_schema(db_path=duckdb_path, in_memory=in_memory)


def list_tables() -> dict:
    """Return {engine: [table names]} created by this module."""
    return {
        "sqlite": (
            list(_PIPELINE_RUNS_TABLE.keys())
            + list(_PIPELINE_CHECKPOINTS_TABLE.keys())
            + list(_PIPELINE_DRIFT_LOG_TABLE.keys())
            + list(_SCHEDULER_HEARTBEATS_TABLE.keys())
        ),
        "duckdb": list(_SIGNAL_TABLES.keys()),
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    create_schema()
