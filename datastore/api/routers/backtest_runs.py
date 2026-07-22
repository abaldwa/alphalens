"""
datastore/api/routers/backtest_runs.py

Phase: Unified Backtest & Paper Trading Umbrella, Phase 3
(BacktestUmbrellaPlan.md at the repo root)
Owner: Platform / DataStore
Consumers: Phase 4's unified Backtest frontend page

Read/list API over the new backtest_runs DuckDB table (Store 6,
config.settings.BACKTEST_DUCKDB_PATH, datastore/schema/create_backtest.py)
— the unified run-record store every channel's Backtest/Walk-Forward run
writes into via backtest/core/run_store.py::save_run_result().

Deliberately a NEW router (prefix /api/v1/backtest, same base as the
existing backtest_reports.py router but disjoint sub-paths: /runs vs
/reports) rather than a modification of backtest_reports.py — that
existing router is a live-used read-only passthrough for the legacy
backtest/reports/*.json files backing the current /ml-backtest page, and
per the "wrap, don't refactor" principle applied throughout this
initiative, it is left untouched. The two routers coexist under the same
prefix; Phase 4's frontend cutover (not this phase) decides whether/how
to eventually retire the legacy one.

No general write endpoints here: runs are written by
backtest/core/run_store.py, called from wherever a
BacktestOrchestrator/WalkForwardRunner run is kicked off (a script
today; Phase 5/6's background job runner later) — not from an API
request. This keeps "who can trigger a potentially expensive multi-year
backtest" a deliberate, out-of-band decision rather than an open HTTP
endpoint.

Two deliberate exceptions, both single, specifically named jobs (not a
general "run any backtest" endpoint) — same deliberate-single-purpose-
trigger pattern datastore/api/routers/ops.py's POST /steps/{step_name}/
force already uses for other expensive jobs, each run as a detached
background subprocess so the request returns immediately and progress is
polled separately:

- /iterative/trigger + /iterative/status/{job_id}: backtest/run_iterative_
  backtest.py's MetaLabeler retrain loop.
- /orchestrator/trigger + /orchestrator/status/{run_id}: backtest/
  run_orchestrator_backtest.py, driving backtest/core/engine.py's
  BacktestOrchestrator (Technical/Fundamental/Momentum channels) against
  real data from the Backtest page's UI. Status here is answered straight
  from backtest_runs (GET /runs/{run_id}) rather than a separate report
  file — the orchestrator driver already writes there via save_run_result()
  as its very last step, so "does this run_id exist yet" already IS
  "is it done."
"""

import json
import logging
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from backtest.core.run_store import get_run, get_run_lineage, get_signal_counts, list_runs
from backtest.core.feature_log import query_feature_log
from config.settings import BACKTEST_DUCKDB_PATH
from datastore.api.db import get_duckdb_connection

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/backtest", tags=["Backtest"])

_REPORTS_DIR = Path(__file__).resolve().parents[3] / "backtest" / "reports"
_TRIGGER_LOGS_DIR = _REPORTS_DIR / "iterative_trigger_logs"


class BacktestRunSummary(BaseModel):
    run_id: str
    parent_run_id: Optional[str] = None
    channel: str
    strategy_id: str
    horizon_bucket: str
    mode: str
    start_date: str
    end_date: str
    capital_mode: str
    initial_capital: float
    created_at: str
    # Channel-specific run config (technical: template_name; fundamental:
    # preset; momentum: top_n/lookback_months) — surfaced so the Runs table
    # can show the actual strategy name (e.g. "E2"), not just strategy_id,
    # which may be a freeform label like "Test1" that says nothing about
    # which template/preset actually ran.
    config: Optional[dict] = None
    metrics: Optional[dict] = None
    data_gaps: List[dict] = []
    integrity_passed: Optional[bool] = None
    live_eligible: bool = False
    # From backtest_feature_log's decision_taken column (core/engine.py's
    # _log_feature) — 0 for a run predating feature logging, not None, so
    # the Runs table can sort/display it uniformly without a null check.
    buy_signal_count: int = 0
    sell_signal_count: int = 0
    # Per-Bull/Bear/Sideways-segment performance (backtest/core/
    # regime_breakdown.py) — [] when the run wasn't given a regime_conn.
    regime_breakdown: List[dict] = []


class BacktestRunListResponse(BaseModel):
    runs: List[BacktestRunSummary]


class BacktestRunLineageResponse(BaseModel):
    run_id: str
    lineage: List[BacktestRunSummary]


class FeatureLogRow(BaseModel):
    ticker: str
    as_of_date: str
    horizon_bucket: str
    feature_vector: dict
    signal_output: Optional[str] = None
    decision_taken: str


class FeatureLogResponse(BaseModel):
    run_id: str
    rows: List[FeatureLogRow]


def _summary(row: dict, signal_counts: Optional[dict] = None) -> BacktestRunSummary:
    counts = (signal_counts or {}).get(row["run_id"], {})
    return BacktestRunSummary(
        **{k: row[k] for k in BacktestRunSummary.model_fields if k in row},
        buy_signal_count=counts.get("buy", 0),
        sell_signal_count=counts.get("sell", 0),
    )


@router.get("/runs", response_model=BacktestRunListResponse)
async def list_backtest_runs(
    channel: Optional[str] = Query(None, description="Filter: technical | fundamental | ml | momentum"),
    mode: Optional[str] = Query(None, description="Filter: backtest | walk_forward | paper"),
    strategy_id: Optional[str] = Query(None),
    limit: int = Query(100, le=500),
) -> BacktestRunListResponse:
    """List runs across all four channels, most recent first — the unified
    view Phase 4's frontend results table renders."""
    with get_duckdb_connection(BACKTEST_DUCKDB_PATH, persist=False, read_only=True) as conn:
        rows = list_runs(conn, channel=channel, mode=mode, strategy_id=strategy_id, limit=limit)
        signal_counts = get_signal_counts(conn, [r["run_id"] for r in rows])
    return BacktestRunListResponse(runs=[_summary(r, signal_counts) for r in rows])


@router.get("/runs/{run_id}", response_model=BacktestRunSummary)
async def get_backtest_run(run_id: str) -> BacktestRunSummary:
    with get_duckdb_connection(BACKTEST_DUCKDB_PATH, persist=False, read_only=True) as conn:
        row = get_run(conn, run_id)
        if row is None:
            raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found")
        signal_counts = get_signal_counts(conn, [run_id])
    return _summary(row, signal_counts)


@router.get("/runs/{run_id}/lineage", response_model=BacktestRunLineageResponse)
async def get_backtest_run_lineage(run_id: str) -> BacktestRunLineageResponse:
    """Parent_run_id chain, oldest first — the feedback-loop 'compare to
    parent run' view (BacktestUmbrellaPlan.md's Feature-Vector Logging &
    Feedback Loop section)."""
    with get_duckdb_connection(BACKTEST_DUCKDB_PATH, persist=False, read_only=True) as conn:
        chain = get_run_lineage(conn, run_id)
        if not chain:
            raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found")
        signal_counts = get_signal_counts(conn, [r["run_id"] for r in chain])
    return BacktestRunLineageResponse(run_id=run_id, lineage=[_summary(r, signal_counts) for r in chain])


@router.get("/runs/{run_id}/feature_log", response_model=FeatureLogResponse)
async def get_backtest_run_feature_log(run_id: str) -> FeatureLogResponse:
    """Every logged decision for a run — the feature-reengineering
    feedback loop's read side (backtest/core/feature_log.py::query_feature_log)."""
    with get_duckdb_connection(BACKTEST_DUCKDB_PATH, persist=False, read_only=True) as conn:
        run_exists = get_run(conn, run_id) is not None
        if not run_exists:
            raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found")
        rows = query_feature_log(conn, run_id)
    for r in rows:
        r["as_of_date"] = str(r["as_of_date"])
    return FeatureLogResponse(run_id=run_id, rows=[FeatureLogRow(**r) for r in rows])


class IterativeRetrainTriggerResponse(BaseModel):
    job_id: str
    status: str = "started"


class IterativeRetrainStatusResponse(BaseModel):
    job_id: str
    status: str  # "running" | "completed" | "failed" | "unknown"
    report: Optional[Dict[str, Any]] = None
    log_tail: Optional[str] = None


@router.post("/iterative/trigger", response_model=IterativeRetrainTriggerResponse)
async def trigger_iterative_retrain(
    horizon_days: int = Query(5),
    folds: int = Query(4),
    max_iterations: Optional[int] = Query(None),
) -> IterativeRetrainTriggerResponse:
    """
    Menu-triggered launch of backtest/run_iterative_backtest.py's
    MetaLabeler retrain loop (see module docstring for why this is the
    one deliberate exception to "no write/trigger endpoints here"). Runs
    as a detached subprocess — this request returns immediately with a
    job_id; poll GET /iterative/status/{job_id} for progress/results.
    """
    job_id = uuid.uuid4().hex[:12]
    _TRIGGER_LOGS_DIR.mkdir(parents=True, exist_ok=True)
    log_path = _TRIGGER_LOGS_DIR / f"{job_id}.log"

    cmd = [
        sys.executable, "-m", "backtest.run_iterative_backtest",
        "--horizon-days", str(horizon_days), "--folds", str(folds), "--report-suffix", job_id,
    ]
    if max_iterations is not None:
        cmd += ["--max-iterations", str(max_iterations)]

    logger.info(f"backtest_runs.trigger_iterative_retrain: job_id={job_id} cmd={' '.join(cmd)}")
    with open(log_path, "w") as log_fh:
        subprocess.Popen(cmd, stdout=log_fh, stderr=subprocess.STDOUT, start_new_session=True)

    return IterativeRetrainTriggerResponse(job_id=job_id)


@router.get("/iterative/status/{job_id}", response_model=IterativeRetrainStatusResponse)
async def get_iterative_retrain_status(job_id: str) -> IterativeRetrainStatusResponse:
    """Polled by the frontend after /iterative/trigger — "completed" once
    backtest/reports/iterative_retrain_{job_id}.json exists (written as
    the very last step of run_iterative_backtest.py's run), "failed" if
    the log shows the process exited without writing that report,
    "running" otherwise."""
    import json

    report_path = _REPORTS_DIR / f"iterative_retrain_{job_id}.json"
    log_path = _TRIGGER_LOGS_DIR / f"{job_id}.log"

    if report_path.exists():
        with open(report_path) as fh:
            report = json.load(fh)
        return IterativeRetrainStatusResponse(job_id=job_id, status="completed", report=report)

    if not log_path.exists():
        return IterativeRetrainStatusResponse(job_id=job_id, status="unknown")

    log_tail = "".join(log_path.read_text(errors="replace").splitlines(keepends=True)[-40:])
    # A traceback in the log with no report file means the subprocess
    # died before writing one — surfaced as failed rather than an
    # indefinite "running".
    status = "failed" if "Traceback (most recent call last)" in log_tail else "running"
    return IterativeRetrainStatusResponse(job_id=job_id, status=status, log_tail=log_tail)


_ORCHESTRATOR_TRIGGER_LOGS_DIR = _REPORTS_DIR / "orchestrator_trigger_logs"


class OrchestratorTriggerResponse(BaseModel):
    run_id: str
    status: str = "started"


class OrchestratorStatusResponse(BaseModel):
    run_id: str
    status: str  # "running" | "completed" | "failed" | "unknown"
    run: Optional[BacktestRunSummary] = None
    log_tail: Optional[str] = None


@router.post("/orchestrator/trigger", response_model=OrchestratorTriggerResponse)
async def trigger_orchestrator_backtest(
    channel: str = Query(..., description="technical | fundamental | momentum"),
    strategy_id: Optional[str] = Query(
        None, description="Defaults to the codified {channel}_{descriptor}_{horizon}_{YYYYMMDD} form"
    ),
    horizon_bucket: Optional[str] = Query(
        None, description="Defaults per channel/template per the Explainer's published style table"
    ),
    start_date: str = Query(..., description="YYYY-MM-DD"),
    end_date: str = Query(..., description="YYYY-MM-DD"),
    capital_mode: str = Query("lump"),
    initial_capital: float = Query(1_000_000.0),
    sip_amount: Optional[float] = Query(None),
    universe_spec: str = Query("curated"),
    max_tickers: Optional[int] = Query(None),
    min_history_days: int = Query(60),
    template_name: Optional[str] = Query(None, description="technical channel: a screener template name"),
    preset: Optional[str] = Query(None, description="fundamental channel: a SCREENER_PRESETS key"),
    top_n: int = Query(10),
    lookback_months: int = Query(6, description="momentum channel only"),
) -> OrchestratorTriggerResponse:
    """
    Menu-triggered launch of backtest/run_orchestrator_backtest.py, which
    drives backtest/core/engine.py's BacktestOrchestrator against real
    data for one of the Technical/Fundamental/Momentum adapters. Runs as
    a detached subprocess — this request returns immediately with a
    run_id (generated here, not left to the script's own default) so the
    frontend can poll GET /orchestrator/status/{run_id} — which, once the
    run completes, IS also its GET /runs/{run_id} row (see module
    docstring).
    """
    run_id = f"orch_{channel}_{uuid.uuid4().hex[:12]}"
    _ORCHESTRATOR_TRIGGER_LOGS_DIR.mkdir(parents=True, exist_ok=True)
    log_path = _ORCHESTRATOR_TRIGGER_LOGS_DIR / f"{run_id}.log"

    cmd = [
        sys.executable, "-m", "backtest.run_orchestrator_backtest",
        "--channel", channel,
        "--start-date", start_date, "--end-date", end_date, "--capital-mode", capital_mode,
        "--initial-capital", str(initial_capital), "--universe-spec", universe_spec,
        "--min-history-days", str(min_history_days), "--top-n", str(top_n),
        "--lookback-months", str(lookback_months), "--run-id", run_id,
    ]
    if strategy_id:
        cmd += ["--strategy-id", strategy_id]
    if horizon_bucket:
        cmd += ["--horizon-bucket", horizon_bucket]
    if sip_amount is not None:
        cmd += ["--sip-amount", str(sip_amount)]
    if max_tickers is not None:
        cmd += ["--max-tickers", str(max_tickers)]
    if template_name:
        cmd += ["--template-name", template_name]
    if preset:
        cmd += ["--preset", preset]

    logger.info(f"backtest_runs.trigger_orchestrator_backtest: run_id={run_id} cmd={' '.join(cmd)}")
    with open(log_path, "w") as log_fh:
        subprocess.Popen(cmd, stdout=log_fh, stderr=subprocess.STDOUT, start_new_session=True)

    return OrchestratorTriggerResponse(run_id=run_id)


@router.get("/orchestrator/status/{run_id}", response_model=OrchestratorStatusResponse)
async def get_orchestrator_status(run_id: str) -> OrchestratorStatusResponse:
    """"completed" once `run_id` exists in backtest_runs (save_run_result()
    is the last thing run_orchestrator_backtest.py does), "failed" if the
    subprocess's log shows a traceback with no row yet, "running" otherwise."""
    with get_duckdb_connection(BACKTEST_DUCKDB_PATH, persist=False, read_only=True) as conn:
        row = get_run(conn, run_id)
        if row is not None:
            signal_counts = get_signal_counts(conn, [run_id])
            return OrchestratorStatusResponse(run_id=run_id, status="completed", run=_summary(row, signal_counts))

    log_path = _ORCHESTRATOR_TRIGGER_LOGS_DIR / f"{run_id}.log"
    if not log_path.exists():
        return OrchestratorStatusResponse(run_id=run_id, status="unknown")

    log_tail = "".join(log_path.read_text(errors="replace").splitlines(keepends=True)[-40:])
    status = "failed" if "Traceback (most recent call last)" in log_tail else "running"
    return OrchestratorStatusResponse(run_id=run_id, status=status, log_tail=log_tail)


_QUEUE_DEFS_DIR = _REPORTS_DIR / "queue_defs"
_QUEUE_LOGS_DIR = _REPORTS_DIR / "queue_trigger_logs"


class StrategyQueueJob(BaseModel):
    """One job in a strategy queue — see backtest/run_strategy_queue.py's
    module docstring for the exact allowed fields per `kind`."""

    kind: str  # "orchestrator" | "iterative_retrain"
    channel: Optional[str] = None
    strategy_id: Optional[str] = None
    horizon_bucket: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    capital_mode: Optional[str] = None
    initial_capital: Optional[float] = None
    sip_amount: Optional[float] = None
    universe_spec: Optional[str] = None
    max_tickers: Optional[int] = None
    min_history_days: Optional[int] = None
    template_name: Optional[str] = None
    preset: Optional[str] = None
    top_n: Optional[int] = None
    lookback_months: Optional[int] = None
    horizon_days: Optional[int] = None
    seed: Optional[int] = None
    max_real_tickers: Optional[int] = None
    max_iterations: Optional[int] = None
    plateau_patience: Optional[int] = None
    min_dsr_threshold: Optional[float] = None
    max_random_feature_accuracy: Optional[float] = None
    folds: Optional[int] = None


class StrategyQueueTriggerRequest(BaseModel):
    jobs: List[StrategyQueueJob]
    continue_on_failure: bool = False


class StrategyQueueTriggerResponse(BaseModel):
    queue_id: str
    status: str = "started"


class StrategyQueueJobStatus(BaseModel):
    job_index: int
    kind: str
    label: str
    status: str  # "queued" | "running" | "completed" | "failed" | "skipped"


class StrategyQueueStatusResponse(BaseModel):
    queue_id: str
    status: str  # "running" | "completed" | "failed" | "unknown"
    summary: Optional[Dict[str, Any]] = None
    log_tail: Optional[str] = None
    # Per-job Queued/Running/Completed breakdown (backtest/run_strategy_
    # queue.py's progress file) — [] for a queue triggered before this
    # field existed, or once `summary` is populated (jobs' final state is
    # in summary.results by then; this is for while it's still running).
    jobs: List[StrategyQueueJobStatus] = []


@router.post("/queue/trigger", response_model=StrategyQueueTriggerResponse)
async def trigger_strategy_queue(request: StrategyQueueTriggerRequest) -> StrategyQueueTriggerResponse:
    """
    Menu-triggered launch of backtest/run_strategy_queue.py — schedules
    every job in `request.jobs` to run SEQUENTIALLY (see that module's
    docstring for why sequential/subprocess-isolated), so an operator can
    queue up several strategies (and an iterative retrain) once instead
    of triggering each one by hand and waiting between them.
    """
    if not request.jobs:
        raise HTTPException(status_code=400, detail="jobs must be non-empty")

    queue_id = f"queue_{uuid.uuid4().hex[:12]}"
    _QUEUE_DEFS_DIR.mkdir(parents=True, exist_ok=True)
    _QUEUE_LOGS_DIR.mkdir(parents=True, exist_ok=True)

    queue_def_path = _QUEUE_DEFS_DIR / f"{queue_id}.json"
    with open(queue_def_path, "w") as fh:
        json.dump({"jobs": [job.model_dump(exclude_none=True) for job in request.jobs]}, fh, indent=2)

    log_path = _QUEUE_LOGS_DIR / f"{queue_id}.log"
    cmd = [
        sys.executable, "-m", "backtest.run_strategy_queue",
        "--queue-file", str(queue_def_path), "--report-suffix", queue_id,
    ]
    if request.continue_on_failure:
        cmd.append("--continue-on-failure")

    logger.info(f"backtest_runs.trigger_strategy_queue: queue_id={queue_id} jobs={len(request.jobs)} cmd={' '.join(cmd)}")
    with open(log_path, "w") as log_fh:
        subprocess.Popen(cmd, stdout=log_fh, stderr=subprocess.STDOUT, start_new_session=True)

    return StrategyQueueTriggerResponse(queue_id=queue_id)


@router.get("/queue/status/{queue_id}", response_model=StrategyQueueStatusResponse)
async def get_strategy_queue_status(queue_id: str) -> StrategyQueueStatusResponse:
    """"completed" once backtest/reports/strategy_queue_{queue_id}.json
    exists (run_strategy_queue.py's own summary, written as its last
    step) AND every job passed; "failed" if that summary exists but some
    job didn't (including a queue stopped early on failure), or if the
    log shows a traceback with no summary yet; "running" otherwise. The
    summary is attached in both the completed and failed cases so the
    frontend can show whichever jobs did finish either way."""
    import json as json_module

    summary_path = _REPORTS_DIR / f"strategy_queue_{queue_id}.json"
    if summary_path.exists():
        with open(summary_path) as fh:
            summary = json_module.load(fh)
        status = "completed" if summary.get("all_passed") else "failed"
        return StrategyQueueStatusResponse(queue_id=queue_id, status=status, summary=summary)

    progress_path = _REPORTS_DIR / f"strategy_queue_progress_{queue_id}.json"
    jobs: List[StrategyQueueJobStatus] = []
    if progress_path.exists():
        with open(progress_path) as fh:
            progress = json_module.load(fh)
        jobs = [StrategyQueueJobStatus(**j) for j in progress.get("jobs", [])]

    log_path = _QUEUE_LOGS_DIR / f"{queue_id}.log"
    if not log_path.exists():
        return StrategyQueueStatusResponse(queue_id=queue_id, status="unknown", jobs=jobs)

    log_tail = "".join(log_path.read_text(errors="replace").splitlines(keepends=True)[-40:])
    status = "failed" if "Traceback (most recent call last)" in log_tail else "running"
    return StrategyQueueStatusResponse(queue_id=queue_id, status=status, log_tail=log_tail, jobs=jobs)


class ActiveQueuesResponse(BaseModel):
    queue_ids: List[str]


@router.get("/queue/active", response_model=ActiveQueuesResponse)
async def list_active_queues(limit: int = Query(10, le=50)) -> ActiveQueuesResponse:
    """Queue_ids still running (a trigger log exists with no final summary
    yet), most-recently-triggered first — lets the Backtest page discover
    and display a queue's progress even if it was triggered from a
    different browser session, the CLI, or an API call rather than this
    page's own panel (which otherwise only knows about queues it
    personally triggered, via client-side state)."""
    if not _QUEUE_LOGS_DIR.exists():
        return ActiveQueuesResponse(queue_ids=[])
    log_files = sorted(_QUEUE_LOGS_DIR.glob("*.log"), key=lambda p: p.stat().st_mtime, reverse=True)
    active = [
        p.stem for p in log_files
        if not (_REPORTS_DIR / f"strategy_queue_{p.stem}.json").exists()
    ]
    return ActiveQueuesResponse(queue_ids=active[:limit])
