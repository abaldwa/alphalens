# AlphaLens — Scheduler, Checkpointing & Observability
## Flexible scheduling · Checkpoint-resume · Unlimited backfill · Configurable observability

---

## Design Principles

1. **No hardcoded times.** The pipeline does not assume "run at 4:00 PM." It runs when
   triggered — either by a cron schedule you configure, or manually, or on startup.
   You have 15 hours between market close (3:30 PM) and next market open (9:15 AM)
   to complete all computation. The system does not care when within that window it runs.

2. **Checkpoint-resume on failure.** Every pipeline run consists of ordered steps. Each
   step writes a checkpoint on success. If the pipeline crashes at step 7 of 14, the
   next run resumes from step 7 — it does not re-execute steps 1–6.

3. **Unlimited backfill.** Whether you missed 1 day or 30 days, the gap detector finds
   every missing trading day and backfills them in chronological order with checkpointing.
   There is no maximum gap window.

4. **Observable by default, silent in production.** Every pipeline step emits structured
   logs, timing metrics, and data quality counters. Observability is on by default in
   development. A single config flag turns it off (or reduces to errors-only) in production.

---

## Scheduler Architecture

### Two Scheduling Modes (configurable per job)

```python
# config/settings.py

SCHEDULER_MODE = 'linear'  # Options: 'linear', 'timestamp', 'manual'

# 'linear':    Steps run sequentially when pipeline is triggered.
#              No clock dependency. Trigger can be cron, manual, or on-startup.
#
# 'timestamp': Steps run at specific times (e.g., option chain at 15:25 IST).
#              Use ONLY for time-sensitive scrapers that must run intraday.
#
# 'manual':    Pipeline runs only when explicitly triggered by the user.
#              Useful during development and debugging.
```

### Job Definition

Every job in the system is defined with these properties:

```python
from dataclasses import dataclass, field
from typing import Optional, List

@dataclass
class PipelineJob:
    id: str                           # Unique job identifier
    name: str                         # Human-readable name
    function: callable                # Python function to execute
    depends_on: List[str] = field(default_factory=list)  # Job IDs this depends on
    schedule_mode: str = 'linear'     # 'linear' | 'timestamp' | 'manual'
    schedule_time: Optional[str] = None  # HH:MM IST (only if mode='timestamp')
    checkpoint_enabled: bool = True   # Write checkpoint on success
    retry_count: int = 3              # Retries on failure before marking failed
    retry_delay_seconds: int = 60     # Delay between retries
    timeout_seconds: int = 3600       # Max runtime before kill (1 hour default)
    is_backfillable: bool = True      # Can this job run for historical dates?
    skip_on_holiday: bool = True      # Skip if date is NSE holiday
    tier_scope: str = 'all'           # 'all' | 'tier_1_2' | 'tier_1_only'
```

### Pipeline Steps (ordered, with dependencies)

```python
PIPELINE_STEPS = [
    # === DATA COLLECTION ===
    PipelineJob(
        id='option_chain_scrape',
        name='Option Chain Snapshot',
        function=scrape_option_chain,
        schedule_mode='timestamp',       # This one IS time-sensitive
        schedule_time='15:25',           # Must run before market close
        is_backfillable=False,           # Cannot backfill from archives
        tier_scope='fno_only',
    ),
    PipelineJob(
        id='bhavcopy_download',
        name='NSE Bhavcopy Download',
        function=download_bhavcopy,
        schedule_mode='linear',          # Runs when pipeline triggers
        depends_on=[],                   # No dependencies
    ),
    PipelineJob(
        id='fno_download',
        name='F&O Bhavcopy Download',
        function=download_fno_bhavcopy,
        schedule_mode='linear',
        depends_on=[],
    ),
    PipelineJob(
        id='macro_download',
        name='Macro Data Fetch',
        function=download_macro_data,
        schedule_mode='linear',
        depends_on=[],
    ),

    # === DATA PROCESSING ===
    PipelineJob(
        id='oracle_sync',
        name='Sync from Oracle Object Storage',
        function=sync_from_oracle,
        schedule_mode='linear',
        depends_on=[],                   # Independent — pulls what Oracle collected
    ),
    PipelineJob(
        id='data_validation',
        name='Validate Downloaded Data',
        function=validate_raw_data,
        schedule_mode='linear',
        depends_on=['bhavcopy_download', 'oracle_sync'],
    ),
    PipelineJob(
        id='corporate_action_check',
        name='Check & Apply Corporate Actions',
        function=apply_corporate_actions,
        schedule_mode='linear',
        depends_on=['data_validation'],
    ),
    PipelineJob(
        id='insert_ohlcv',
        name='Insert Adjusted OHLCV into DataStore',
        function=insert_adjusted_ohlcv,
        schedule_mode='linear',
        depends_on=['corporate_action_check'],
    ),

    # === FEATURE COMPUTATION ===
    PipelineJob(
        id='features_technical',
        name='Compute 76 Core Technical Features',
        function=compute_technical_features,
        schedule_mode='linear',
        depends_on=['insert_ohlcv'],
    ),
    PipelineJob(
        id='features_intraday',
        name='Compute Intraday Pattern Features',
        function=compute_intraday_features,
        schedule_mode='linear',
        depends_on=['insert_ohlcv'],
    ),
    PipelineJob(
        id='features_calendar',
        name='Compute Calendar Features',
        function=compute_calendar_features,
        schedule_mode='linear',
        depends_on=[],                   # No data dependency
    ),
    PipelineJob(
        id='features_macro',
        name='Compute Macro Features',
        function=compute_macro_features,
        schedule_mode='linear',
        depends_on=['macro_download'],
    ),
    PipelineJob(
        id='features_pnd',
        name='Compute P&D Detection Features',
        function=compute_pnd_features,
        schedule_mode='linear',
        depends_on=['features_technical'],
    ),
    PipelineJob(
        id='features_fundamental',
        name='Load & Compute Fundamental Features (PIT)',
        function=compute_fundamental_features,
        schedule_mode='linear',
        depends_on=['insert_ohlcv'],
        is_backfillable=True,
    ),
    PipelineJob(
        id='features_governance',
        name='Load & Compute Governance Features',
        function=compute_governance_features,
        schedule_mode='linear',
        depends_on=['insert_ohlcv'],
    ),
    PipelineJob(
        id='assemble_matrix',
        name='Assemble Feature Matrix → Parquet',
        function=assemble_feature_matrix,
        schedule_mode='linear',
        depends_on=[
            'features_technical', 'features_intraday', 'features_calendar',
            'features_macro', 'features_pnd', 'features_fundamental',
            'features_governance',
        ],
    ),

    # === QUALITY & MONITORING ===
    PipelineJob(
        id='quality_checks',
        name='Data Quality Validation + PSI Drift',
        function=run_quality_checks,
        schedule_mode='linear',
        depends_on=['assemble_matrix'],
    ),

    # === MODEL INFERENCE (today only, never during backfill) ===
    PipelineJob(
        id='hmm_regime',
        name='HMM Regime Detection',
        function=run_hmm_regime,
        schedule_mode='linear',
        depends_on=['quality_checks'],
        is_backfillable=False,           # Only run for today
    ),
    PipelineJob(
        id='pnd_prefilter',
        name='P&D Pre-Filter (blocks score > 60)',
        function=run_pnd_prefilter,
        schedule_mode='linear',
        depends_on=['hmm_regime'],
        is_backfillable=False,
    ),
    PipelineJob(
        id='signal_models',
        name='Signal Models 5d/21d/63d + Meta-Labeler + Conformal',
        function=run_signal_models,
        schedule_mode='linear',
        depends_on=['pnd_prefilter'],
        is_backfillable=False,
    ),
    PipelineJob(
        id='exit_signals',
        name='Exit Signal Model for Held Positions',
        function=run_exit_signals,
        schedule_mode='linear',
        depends_on=['signal_models'],
        is_backfillable=False,
    ),
    PipelineJob(
        id='generate_alerts',
        name='Generate Alerts + Write Outputs',
        function=generate_alerts_and_outputs,
        schedule_mode='linear',
        depends_on=['exit_signals'],
    ),
    PipelineJob(
        id='retrain_check',
        name='Check Model Retrain Schedule',
        function=check_retrain_due,
        schedule_mode='linear',
        depends_on=['generate_alerts'],
    ),
]

# Weekly job (not part of daily pipeline)
WEEKLY_JOBS = [
    PipelineJob(
        id='multibagger_scan',
        name='Multibagger Weekly Scan (Monday)',
        function=run_multibagger_model,
        schedule_mode='linear',          # Triggers after Monday daily pipeline
        depends_on=['signal_models'],
        is_backfillable=False,
    ),
]
```

---

## Checkpoint-Resume Engine

### How Checkpointing Works

Every pipeline run creates a checkpoint file in DuckDB:

```sql
-- In pipeline DuckDB (or SQLite for scheduler — transactional)
CREATE TABLE pipeline_checkpoints (
    run_id TEXT NOT NULL,             -- UUID for this pipeline run
    run_date TEXT NOT NULL,           -- Trading date being processed
    step_id TEXT NOT NULL,            -- Job ID from PIPELINE_STEPS
    step_index INTEGER NOT NULL,      -- Order position (0-based)
    status TEXT NOT NULL,             -- 'pending' | 'running' | 'success' | 'failed' | 'skipped'
    started_at TEXT,                  -- ISO timestamp
    completed_at TEXT,                -- ISO timestamp
    duration_seconds REAL,
    error_message TEXT,               -- If failed, the exception message
    retry_count INTEGER DEFAULT 0,
    rows_processed INTEGER,           -- Data quality counter
    PRIMARY KEY (run_id, step_id)
);

CREATE TABLE pipeline_runs (
    run_id TEXT PRIMARY KEY,
    run_date TEXT NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    status TEXT NOT NULL,             -- 'running' | 'completed' | 'partial' | 'failed'
    is_backfill INTEGER DEFAULT 0,
    total_steps INTEGER,
    completed_steps INTEGER,
    failed_steps INTEGER,
    skipped_steps INTEGER,
    notes TEXT
);
```

### Checkpoint-Resume Logic

```python
# ingestion/scheduler/checkpoint_engine.py

import uuid
import sqlite3
from datetime import datetime
import logging

log = logging.getLogger('checkpoint')

class CheckpointEngine:
    """
    Manages pipeline checkpoints for resume-on-failure.
    If a pipeline run crashes at step 7, the next run for the same date
    resumes from step 7 — steps 1–6 are not re-executed.
    """

    def __init__(self, db_path='datastore/normalised/pipeline_log.db'):
        self.conn = sqlite3.connect(db_path)
        self._ensure_tables()

    def start_run(self, run_date: str, steps: list, is_backfill: bool = False) -> str:
        """Start a new pipeline run or resume an existing incomplete one."""

        # Check for existing incomplete run for this date
        existing = self.conn.execute("""
            SELECT run_id FROM pipeline_runs
            WHERE run_date = ? AND status IN ('running', 'partial', 'failed')
            ORDER BY started_at DESC LIMIT 1
        """, (run_date,)).fetchone()

        if existing:
            run_id = existing[0]
            log.info(f"RESUMING existing run {run_id} for {run_date}")
            # Mark as running again
            self.conn.execute(
                "UPDATE pipeline_runs SET status = 'running' WHERE run_id = ?",
                (run_id,))
            self.conn.commit()
            return run_id

        # New run
        run_id = str(uuid.uuid4())[:12]
        self.conn.execute("""
            INSERT INTO pipeline_runs
            (run_id, run_date, started_at, status, is_backfill, total_steps,
             completed_steps, failed_steps, skipped_steps)
            VALUES (?, ?, ?, 'running', ?, ?, 0, 0, 0)
        """, (run_id, run_date, datetime.now().isoformat(),
              int(is_backfill), len(steps)))

        # Create checkpoint entries for all steps
        for i, step in enumerate(steps):
            self.conn.execute("""
                INSERT INTO pipeline_checkpoints
                (run_id, run_date, step_id, step_index, status)
                VALUES (?, ?, ?, ?, 'pending')
            """, (run_id, run_date, step.id, i))

        self.conn.commit()
        log.info(f"NEW run {run_id} for {run_date} ({len(steps)} steps)")
        return run_id

    def get_resume_point(self, run_id: str) -> int:
        """Return the step_index to resume from (first non-success step)."""
        result = self.conn.execute("""
            SELECT MIN(step_index) FROM pipeline_checkpoints
            WHERE run_id = ? AND status != 'success'
        """, (run_id,)).fetchone()
        return result[0] if result[0] is not None else 0

    def mark_step_started(self, run_id: str, step_id: str):
        self.conn.execute("""
            UPDATE pipeline_checkpoints
            SET status = 'running', started_at = ?, retry_count = retry_count
            WHERE run_id = ? AND step_id = ?
        """, (datetime.now().isoformat(), run_id, step_id))
        self.conn.commit()

    def mark_step_success(self, run_id: str, step_id: str,
                           duration: float, rows: int = 0):
        self.conn.execute("""
            UPDATE pipeline_checkpoints
            SET status = 'success', completed_at = ?,
                duration_seconds = ?, rows_processed = ?
            WHERE run_id = ? AND step_id = ?
        """, (datetime.now().isoformat(), duration, rows, run_id, step_id))
        self.conn.execute("""
            UPDATE pipeline_runs
            SET completed_steps = completed_steps + 1
            WHERE run_id = ?
        """, (run_id,))
        self.conn.commit()

    def mark_step_failed(self, run_id: str, step_id: str,
                          error: str, retry_num: int):
        self.conn.execute("""
            UPDATE pipeline_checkpoints
            SET status = 'failed', completed_at = ?,
                error_message = ?, retry_count = ?
            WHERE run_id = ? AND step_id = ?
        """, (datetime.now().isoformat(), error, retry_num, run_id, step_id))
        self.conn.execute("""
            UPDATE pipeline_runs
            SET failed_steps = failed_steps + 1
            WHERE run_id = ?
        """, (run_id,))
        self.conn.commit()

    def mark_step_skipped(self, run_id: str, step_id: str, reason: str):
        self.conn.execute("""
            UPDATE pipeline_checkpoints
            SET status = 'skipped', error_message = ?
            WHERE run_id = ? AND step_id = ?
        """, (reason, run_id, step_id))
        self.conn.execute("""
            UPDATE pipeline_runs
            SET skipped_steps = skipped_steps + 1
            WHERE run_id = ?
        """, (run_id,))
        self.conn.commit()

    def complete_run(self, run_id: str):
        failed = self.conn.execute(
            "SELECT COUNT(*) FROM pipeline_checkpoints WHERE run_id = ? AND status = 'failed'",
            (run_id,)).fetchone()[0]
        status = 'completed' if failed == 0 else 'partial'
        self.conn.execute("""
            UPDATE pipeline_runs
            SET status = ?, completed_at = ?
            WHERE run_id = ?
        """, (status, datetime.now().isoformat(), run_id))
        self.conn.commit()
        return status
```

### Pipeline Runner with Checkpoint-Resume

```python
# ingestion/scheduler/pipeline_runner.py

import time
import logging
from config.settings import OBSERVABILITY_ENABLED, OBSERVABILITY_LEVEL

log = logging.getLogger('pipeline')

class PipelineRunner:
    """
    Executes pipeline steps with checkpoint-resume, retry, and observability.
    """

    def __init__(self, checkpoint_engine, steps, observability=None):
        self.ckpt = checkpoint_engine
        self.steps = steps
        self.obs = observability

    def run(self, run_date: str, is_backfill: bool = False):
        """
        Execute all pipeline steps for a date.
        If resuming, skips already-completed steps.
        If backfilling, skips non-backfillable steps (model inference).
        """
        # Determine which steps apply
        active_steps = self.steps
        if is_backfill:
            active_steps = [s for s in self.steps if s.is_backfillable]

        run_id = self.ckpt.start_run(run_date, active_steps, is_backfill)
        resume_from = self.ckpt.get_resume_point(run_id)

        if resume_from > 0:
            log.info(f"Resuming from step {resume_from} "
                     f"({active_steps[resume_from].name})")

        for i, step in enumerate(active_steps):
            if i < resume_from:
                continue  # Already completed in previous run

            # Check dependencies
            deps_met = self._check_dependencies(run_id, step)
            if not deps_met:
                self.ckpt.mark_step_skipped(
                    run_id, step.id,
                    f"Dependency not met: {step.depends_on}")
                log.warning(f"SKIP {step.name}: dependency not met")
                continue

            # Execute with retry
            success = self._execute_with_retry(run_id, run_date, step)

            if not success and step.id in ('bhavcopy_download', 'insert_ohlcv',
                                             'assemble_matrix'):
                # Critical step failed — stop pipeline for this date
                log.error(f"CRITICAL step {step.name} failed — stopping run")
                self.ckpt.complete_run(run_id)
                return 'failed'

            # Non-critical failures: log and continue
            if not success:
                log.warning(f"Non-critical step {step.name} failed — continuing")

        status = self.ckpt.complete_run(run_id)
        log.info(f"Pipeline {run_id} for {run_date}: {status}")

        if self.obs:
            self.obs.emit_pipeline_complete(run_id, run_date, status)

        return status

    def _execute_with_retry(self, run_id, run_date, step) -> bool:
        for attempt in range(1, step.retry_count + 1):
            self.ckpt.mark_step_started(run_id, step.id)
            start_time = time.time()

            try:
                if self.obs:
                    self.obs.emit_step_start(step.id, step.name, run_date)

                result = step.function(run_date)
                duration = time.time() - start_time
                rows = result.get('rows_processed', 0) if isinstance(result, dict) else 0

                self.ckpt.mark_step_success(run_id, step.id, duration, rows)

                if self.obs:
                    self.obs.emit_step_complete(step.id, duration, rows)

                log.info(f"✓ {step.name} ({duration:.1f}s, {rows} rows)")
                return True

            except Exception as e:
                duration = time.time() - start_time
                log.error(f"✗ {step.name} attempt {attempt}/{step.retry_count}: {e}")

                if self.obs:
                    self.obs.emit_step_error(step.id, str(e), attempt)

                if attempt < step.retry_count:
                    log.info(f"  Retrying in {step.retry_delay_seconds}s...")
                    time.sleep(step.retry_delay_seconds)
                else:
                    self.ckpt.mark_step_failed(run_id, step.id, str(e), attempt)
                    return False

    def _check_dependencies(self, run_id, step) -> bool:
        if not step.depends_on:
            return True
        for dep_id in step.depends_on:
            result = self.ckpt.conn.execute("""
                SELECT status FROM pipeline_checkpoints
                WHERE run_id = ? AND step_id = ?
            """, (run_id, dep_id)).fetchone()
            if not result or result[0] != 'success':
                return False
        return True
```

---

## Gap Detection and Unlimited Backfill

### No Maximum Gap Window

The gap detector has no limit on how many days it can backfill. Whether you missed
1 day or 60 days, it processes every missing trading day in chronological order with
full checkpointing.

```python
# ingestion/scheduler/gap_detector.py

def detect_and_fill_gaps(checkpoint_engine, pipeline_runner, steps):
    """
    Detect all missing trading days and backfill with checkpointing.
    No maximum gap window — works for 1 day or 100 days.
    """
    conn = checkpoint_engine.conn

    # Find last successfully completed date
    result = conn.execute("""
        SELECT MAX(run_date) FROM pipeline_runs
        WHERE status IN ('completed', 'partial')
    """).fetchone()

    last_good_date = result[0] if result[0] else None

    if last_good_date is None:
        log.warning("No pipeline history — this is the first run. No backfill needed.")
        return []

    last_good = date.fromisoformat(last_good_date)
    today = date.today()

    # Compute ALL trading days in the gap (no maximum)
    missing_days = get_trading_days(last_good + timedelta(days=1),
                                    today - timedelta(days=1))

    if not missing_days:
        log.info("No gaps detected")
        return []

    log.warning(f"GAPS DETECTED: {len(missing_days)} trading days missed "
                f"({missing_days[0]} to {missing_days[-1]})")

    # Check for any partially completed backfill days (resume those first)
    incomplete = conn.execute("""
        SELECT DISTINCT run_date FROM pipeline_runs
        WHERE status IN ('failed', 'partial', 'running')
        AND is_backfill = 1
        ORDER BY run_date ASC
    """).fetchall()
    incomplete_dates = {row[0] for row in incomplete}

    results = []
    for gap_date in missing_days:
        date_str = gap_date.isoformat()

        if date_str in incomplete_dates:
            log.info(f"RESUMING incomplete backfill for {date_str}")
        else:
            log.info(f"BACKFILLING {date_str} "
                     f"({missing_days.index(gap_date)+1}/{len(missing_days)})")

        # Backfill steps = all steps EXCEPT model inference
        status = pipeline_runner.run(date_str, is_backfill=True)
        results.append({'date': date_str, 'status': status})

        if status == 'failed':
            log.warning(f"Backfill for {date_str} failed — "
                        f"will resume on next startup. Continuing to next date.")
            # Continue to next date — don't stop entire backfill

    return results
```

---

## Observability System

### Design: On by Default, Off in Production

```python
# config/settings.py

# Observability configuration
OBSERVABILITY_ENABLED = True          # Master switch
OBSERVABILITY_LEVEL = 'debug'         # 'debug' | 'info' | 'warning' | 'error' | 'off'

# When OBSERVABILITY_LEVEL = 'off': no metrics, no structured logs, minimal console
# When OBSERVABILITY_LEVEL = 'error': only errors logged
# When OBSERVABILITY_LEVEL = 'info': step start/complete + errors (production default)
# When OBSERVABILITY_LEVEL = 'debug': everything including per-stock timings
```

### Observability Implementation

```python
# ingestion/scheduler/observability.py

import logging
import time
import json
from datetime import datetime
from config.settings import OBSERVABILITY_ENABLED, OBSERVABILITY_LEVEL

class Observability:
    """
    Structured logging + metrics for pipeline observability.
    Fully configurable: can be turned off entirely for production.
    """

    LEVELS = {'off': 0, 'error': 1, 'warning': 2, 'info': 3, 'debug': 4}

    def __init__(self):
        self.enabled = OBSERVABILITY_ENABLED
        self.level = self.LEVELS.get(OBSERVABILITY_LEVEL, 3)
        self.log = logging.getLogger('observability')
        self.metrics = {}  # In-memory metrics buffer
        self._setup_logging()

    def _setup_logging(self):
        if not self.enabled or self.level == 0:
            self.log.disabled = True
            return

        level_map = {1: logging.ERROR, 2: logging.WARNING,
                     3: logging.INFO, 4: logging.DEBUG}
        self.log.setLevel(level_map.get(self.level, logging.INFO))

        # Structured JSON log format
        handler = logging.FileHandler('datastore/logs/observability.jsonl')
        handler.setFormatter(logging.Formatter('%(message)s'))
        self.log.addHandler(handler)

        # Console handler (human-readable)
        console = logging.StreamHandler()
        console.setFormatter(logging.Formatter(
            '%(asctime)s [%(levelname)s] %(message)s',
            datefmt='%H:%M:%S'))
        self.log.addHandler(console)

    def emit_step_start(self, step_id: str, step_name: str, run_date: str):
        if self.level < 3: return  # info level required
        self._emit('step_start', {
            'step_id': step_id, 'step_name': step_name,
            'run_date': run_date, 'timestamp': datetime.now().isoformat()
        })

    def emit_step_complete(self, step_id: str, duration: float, rows: int = 0):
        if self.level < 3: return
        self._emit('step_complete', {
            'step_id': step_id, 'duration_seconds': round(duration, 2),
            'rows_processed': rows, 'timestamp': datetime.now().isoformat()
        })
        # Track metrics
        self.metrics[step_id] = {
            'last_duration': duration, 'last_rows': rows,
            'last_run': datetime.now().isoformat()
        }

    def emit_step_error(self, step_id: str, error: str, attempt: int):
        if self.level < 1: return  # Always emit errors unless fully off
        self._emit('step_error', {
            'step_id': step_id, 'error': error,
            'attempt': attempt, 'timestamp': datetime.now().isoformat()
        })

    def emit_pipeline_complete(self, run_id: str, run_date: str, status: str):
        if self.level < 3: return
        self._emit('pipeline_complete', {
            'run_id': run_id, 'run_date': run_date, 'status': status,
            'timestamp': datetime.now().isoformat(),
            'step_metrics': self.metrics
        })

    def emit_data_quality(self, check_name: str, passed: bool, details: dict):
        if self.level < 2: return  # warning level
        self._emit('data_quality', {
            'check': check_name, 'passed': passed, 'details': details,
            'timestamp': datetime.now().isoformat()
        })

    def emit_drift_alert(self, feature: str, psi_value: float, severity: str):
        if self.level < 1: return  # Always emit drift alerts
        self._emit('drift_alert', {
            'feature': feature, 'psi': round(psi_value, 4),
            'severity': severity, 'timestamp': datetime.now().isoformat()
        })

    def emit_backfill_progress(self, current: int, total: int, date: str):
        if self.level < 3: return
        self._emit('backfill_progress', {
            'current': current, 'total': total, 'date': date,
            'pct': round(current / total * 100, 1)
        })

    def _emit(self, event_type: str, data: dict):
        record = {'event': event_type, **data}
        self.log.info(json.dumps(record))

    def get_metrics_summary(self) -> dict:
        """Return current metrics buffer for dashboard display."""
        return self.metrics

    def get_health_status(self) -> dict:
        """Return system health for /api/v1/system/health endpoint."""
        return {
            'observability_enabled': self.enabled,
            'observability_level': OBSERVABILITY_LEVEL,
            'steps_tracked': len(self.metrics),
            'last_run': max(
                (m['last_run'] for m in self.metrics.values()),
                default=None
            )
        }


# Convenience: create a no-op observability when disabled
class NoOpObservability:
    def emit_step_start(self, *a, **kw): pass
    def emit_step_complete(self, *a, **kw): pass
    def emit_step_error(self, *a, **kw): pass
    def emit_pipeline_complete(self, *a, **kw): pass
    def emit_data_quality(self, *a, **kw): pass
    def emit_drift_alert(self, *a, **kw): pass
    def emit_backfill_progress(self, *a, **kw): pass
    def get_metrics_summary(self): return {}
    def get_health_status(self): return {'observability_enabled': False}


def create_observability():
    if OBSERVABILITY_ENABLED:
        return Observability()
    return NoOpObservability()
```

### Main Entry Point

```python
# ingestion/scheduler/main.py

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.triggers.cron import CronTrigger
from config.settings import SCHEDULER_MODE
from ingestion.scheduler.checkpoint_engine import CheckpointEngine
from ingestion.scheduler.pipeline_runner import PipelineRunner
from ingestion.scheduler.gap_detector import detect_and_fill_gaps
from ingestion.scheduler.observability import create_observability
from ingestion.scheduler.pipeline_steps import PIPELINE_STEPS, WEEKLY_JOBS

def main():
    obs = create_observability()
    ckpt = CheckpointEngine()
    runner = PipelineRunner(ckpt, PIPELINE_STEPS, obs)

    # Step 1: Always detect and fill gaps on startup
    gaps = detect_and_fill_gaps(ckpt, runner, PIPELINE_STEPS)
    if gaps:
        print(f"Backfill complete: {len(gaps)} days processed")

    # Step 2: Run today's pipeline
    from datetime import date
    today = date.today().isoformat()
    runner.run(today, is_backfill=False)

    # Step 3: Start scheduler for future runs (if not manual mode)
    if SCHEDULER_MODE == 'manual':
        print("Manual mode — exiting after single run")
        return

    scheduler = BlockingScheduler(
        jobstores={'default': SQLAlchemyJobStore(
            url='sqlite:///datastore/normalised/scheduler.db'
        )}
    )

    if SCHEDULER_MODE == 'timestamp':
        # Time-based triggers
        scheduler.add_job(
            lambda: runner.run(date.today().isoformat()),
            CronTrigger(hour=16, minute=30, day_of_week='mon-fri',
                        timezone='Asia/Kolkata'),
            id='daily_pipeline', replace_existing=True,
            misfire_grace_time=86400 * 30, coalesce=True,
        )
    elif SCHEDULER_MODE == 'linear':
        # Run once daily, any time after market close
        # Trigger: cron at a configurable time, or manual
        scheduler.add_job(
            lambda: runner.run(date.today().isoformat()),
            CronTrigger(hour=18, minute=0, day_of_week='mon-fri',
                        timezone='Asia/Kolkata'),
            id='daily_pipeline', replace_existing=True,
            misfire_grace_time=86400 * 30,  # 30-day grace — never expires
            coalesce=True,
        )

    scheduler.start()


if __name__ == '__main__':
    main()
```

---

## Failure Scenarios

| Scenario | Behaviour |
|----------|-----------|
| Laptop off 1 day | On startup: gap detector finds 1 day, backfills data+features, then runs today |
| Laptop off 10 days | On startup: gap detector finds ~7 trading days, backfills each chronologically with checkpoints, then runs today |
| Laptop off 30 days | Same — backfills ~21 trading days. No maximum gap limit |
| Pipeline crashes at step 7 of 14 | Steps 1–6 checkpointed as 'success'. Next run resumes from step 7 |
| Pipeline crashes at step 7, then step 7 fails again on retry | Step 7 marked 'failed' after 3 retries. Steps 8+ still execute (unless they depend on step 7). Run marked 'partial' |
| Backfill day 3 of 7 fails | Day 3 marked 'failed'. Days 4–7 still process. Day 3 retried on next startup (incomplete run detected) |
| Oracle Cloud down for 5 days | Option chain data lost for those 5 days. All other data backfillable from NSE archives. Gap detector tries Oracle first, falls back to NSE automatically |
| Internet down mid-pipeline | Current step retries 3 times. If still down, step fails. Pipeline resumes from that step on next internet-available startup |
| NSE holiday misidentified as gap | NSE holiday calendar (`config/nse_holidays.py`) prevents this. No backfill attempted |
| Feature computation fails for 1 stock | Step continues for remaining 499 stocks. That stock gets NaN features. Logged but not fatal |
