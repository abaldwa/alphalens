# A46 — pipeline_scheduler.py per-concern split: module boundaries

Reference for the next dedicated refactoring session.

## Current state

`ingestion/scheduler/pipeline_scheduler.py` — 3,375 lines, a monolith covering:
- Cross-process advisory lock (fcntl.flock)
- Run recording: pipeline_runs INSERT/UPDATE, heartbeats, job timing
- Per-date step execution: run_steps_for_date, run_backfill, StepRunner type
- Startup sequences: run_startup_sequence, run_morning_catchup_sequence
- Job targets: all _execute_*_job functions (picklable by APScheduler)
- Schedule registration: all schedule_* functions
- Model training: _MODEL_TRAINING_SCRIPT_MAP, _trigger_model_retrain

## Proposed submodules (verified: each loads cleanly standalone)

| Module | Lines | Contains |
|---|---|---|
| `pipeline_run_lock.py` | ~50 | `pipeline_run_lock()` contextmanager |
| `run_recording.py` | ~160 | `_record_pipeline_run_started`, `_record_pipeline_run`, `_record_heartbeat`, `_job_timer_start`, `_job_timer_stats` |
| `pipeline_steps.py` | ~147 | `StepRunner`, `_STEP_DEPS`, `run_steps_for_date`, `run_backfill` |
| `pipeline_startup.py` | ~95 | `run_startup_sequence`, `run_morning_catchup_sequence` |
| `scheduler_jobs.py` | ~880 | All `_execute_*_job()` and `schedule_*()` functions, `_MODEL_TRAINING_SCRIPT_MAP`, `_MODEL_TRAINING_GROUPS`, `create_jobstore`, `create_scheduler` |
| `pipeline_scheduler.py` (facade) | ~60 | Re-exports from all 5 submodules. All existing imports continue to work. |

## Blocker — test monkeypatch patterns

15 test files import 50+ symbols from `pipeline_scheduler` and monkeypatch on the
module directly (e.g., `monkeypatch.setattr(sched_mod, "now_ist", ...)`,
`monkeypatch.setattr(ps, "_record_heartbeat", ...)`).

The A46 split creates `from ... import ...` bindings in the submodules that
monkeypatch.setattr on the facade **cannot reach**. Two fixes exist:

### Option A: Patch consumer modules too (preferred, less invasive)
In each test that patches a facade attribute, also patch the consumer module:
```python
monkeypatch.setattr(facade, "pipeline_run_lock", fake_lock)
import ingestion.scheduler.pipeline_steps as _ps
import ingestion.scheduler.scheduler_jobs as _sj
monkeypatch.setattr(_ps, "pipeline_run_lock", fake_lock)
monkeypatch.setattr(_sj, "pipeline_run_lock", fake_lock)
```
This requires updating ~10-15 test functions across 2-3 test files.

### Option B: Import through the facade in submodules (creates circular imports)
Submodules import from the facade, but the facade imports from submodules — not
viable without restructuring.

## Strategy for next session

1. **Create submodules one at a time** (verified: they load cleanly)
2. **Build the facade** (verified: all exports resolve)
3. **Update 3 test files for the new module paths** (the real work):
   - `tests/unit/test_scheduler.py` (10 monkeypatch.setattr calls on facade)
   - `tests/unit/test_pipeline_scheduler_utils.py` (3 calls)
   - `tests/unit/test_daily_pipeline.py` (2 calls)
   - `tests/unit/test_record_heartbeat_job_run_log.py` (0 calls — doesn't mock)
4. **Replace original with facade + submodules atomically**
5. **Run full test suite** — 110 scheduler tests must all pass
6. **Add `# TODO: delete after ML40-2.3` markers** to facade