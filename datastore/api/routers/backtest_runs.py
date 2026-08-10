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

import psutil
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel

from backtest.core.run_store import count_runs, get_run, get_run_lineage, get_signal_counts, list_experiments, list_runs
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
    total_count: int


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
    limit: int = Query(1000, le=2000),
    sort_by: str = Query("created_at", description="'created_at' (most recent first) or 'cagr' (highest CAGR first)"),
) -> BacktestRunListResponse:
    """List runs across all four channels — the unified view Phase 4's
    frontend results table renders. Defaults to most-recent-first; pass
    sort_by=cagr for the "Top N by CAGR" leaderboard view."""
    with get_duckdb_connection(BACKTEST_DUCKDB_PATH, persist=False, read_only=True) as conn:
        rows = list_runs(conn, channel=channel, mode=mode, strategy_id=strategy_id, limit=limit, sort_by=sort_by)
        signal_counts = get_signal_counts(conn, [r["run_id"] for r in rows])
        total = count_runs(conn, channel=channel, mode=mode, strategy_id=strategy_id)
    return BacktestRunListResponse(runs=[_summary(r, signal_counts) for r in rows], total_count=total)


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


class ExperimentRow(BaseModel):
    """One backtest_runs row for the Experiments comparison page — the
    270-job exit-variant x template/preset matrix (backtest/reports/
    experiment_matrix_45x6.json). Metrics are unpacked from metrics_json
    (backtest/core/metrics.py's BacktestMetrics — `sharpe` was added
    2026-07-26 for REV6's deflated_sharpe_ratio wiring; `sortino`/`calmar`
    predate it. 2026-07-27: `sharpe`/`turnover_ratio` were being computed
    by the backend but silently dropped here — added below so every
    computed ratio the frontend asks for is actually surfaced, not just
    sortino) rather than nesting the raw dict, so the frontend table can
    sort/filter on these columns directly."""

    run_id: str
    strategy_id: str
    channel: str
    exit_policy_variant: Optional[str] = None
    regime_label: Optional[str] = None
    horizon_bucket: str
    created_at: str
    cagr: Optional[float] = None
    xirr: Optional[float] = None
    sharpe: Optional[float] = None
    sortino: Optional[float] = None
    calmar: Optional[float] = None
    max_drawdown: Optional[float] = None
    win_rate: Optional[float] = None
    profit_factor: Optional[float] = None
    turnover_ratio: Optional[float] = None
    avg_days_held: Optional[float] = None
    n_trades: Optional[int] = None
    excess_return: Optional[float] = None
    # True iff trade_log_path was recorded for this run — the frontend
    # renders the download link/button only when this is true, rather than
    # exposing the raw filesystem path itself (that path is server-side
    # only; download goes through GET /experiments/{run_id}/trade_log).
    has_trade_log: bool = False


class ExperimentListResponse(BaseModel):
    experiments: List[ExperimentRow]


def _experiment_row(row: dict) -> ExperimentRow:
    metrics = row.get("metrics") or {}
    return ExperimentRow(
        run_id=row["run_id"],
        strategy_id=row["strategy_id"],
        channel=row["channel"],
        exit_policy_variant=row.get("exit_policy_variant"),
        regime_label=row.get("regime_label"),
        horizon_bucket=row["horizon_bucket"],
        created_at=row["created_at"],
        cagr=metrics.get("cagr"),
        xirr=metrics.get("xirr"),
        sharpe=metrics.get("sharpe"),
        sortino=metrics.get("sortino"),
        calmar=metrics.get("calmar"),
        max_drawdown=metrics.get("max_drawdown"),
        win_rate=metrics.get("win_rate"),
        profit_factor=metrics.get("profit_factor"),
        turnover_ratio=metrics.get("turnover_ratio"),
        avg_days_held=metrics.get("avg_days_held"),
        n_trades=metrics.get("n_trades"),
        excess_return=metrics.get("excess_return"),
        has_trade_log=bool(row.get("trade_log_path")),
    )


@router.get("/experiments", response_model=ExperimentListResponse)
async def list_backtest_experiments(
    strategy_id: Optional[str] = Query(None),
    channel: Optional[str] = Query(None, description="technical | fundamental | ml | momentum"),
    exit_policy_variant: Optional[str] = Query(
        None, description="baseline | condition | combined | trailing | atr_adaptive | regime_conditional"
    ),
    regime_label: Optional[str] = Query(None, description="bull | bear | sideways"),
    limit: int = Query(500, le=2000),
) -> ExperimentListResponse:
    """Backing endpoint for the Experiments page — every run in
    backtest_runs (most recent first), unpacked to the metrics the page
    compares Entry-template x Exit-variant combinations on. Empty
    `experiments: []` (not an error) when the table has no rows yet, e.g.
    before the 270-job experiment_matrix_45x6.json queue has started
    populating it."""
    with get_duckdb_connection(BACKTEST_DUCKDB_PATH, persist=False, read_only=True) as conn:
        rows = list_experiments(
            conn,
            strategy_id=strategy_id,
            channel=channel,
            exit_policy_variant=exit_policy_variant,
            regime_label=regime_label,
            limit=limit,
        )
    return ExperimentListResponse(experiments=[_experiment_row(r) for r in rows])


@router.get("/experiments/{run_id}/trade_log")
async def download_experiment_trade_log(run_id: str) -> FileResponse:
    """Streams a run's trade_log_{run_id}.csv by run_id — never exposes
    the raw server-side trade_log_path to the client; the frontend only
    ever links to this route."""
    with get_duckdb_connection(BACKTEST_DUCKDB_PATH, persist=False, read_only=True) as conn:
        row = get_run(conn, run_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found")
    trade_log_path = row.get("trade_log_path")
    if not trade_log_path or not Path(trade_log_path).exists():
        raise HTTPException(status_code=404, detail=f"No trade log on disk for run '{run_id}'")
    return FileResponse(
        trade_log_path, media_type="text/csv", filename=f"trade_log_{run_id}.csv"
    )


# ---------------------------------------------------------------------------
# Cross-strategy trade queries (backtest_trades, loaded by
# scripts/load_trade_books_to_db.py).
#
# /experiments/{run_id}/trade_log above streams ONE run's CSV. These endpoints
# answer the different question "every trade across all strategies", which
# previously required globbing ~3,800 CSV files because nothing wrote trades to
# a table. Strategy identity is denormalised onto each row, so filtering by
# strategy/template needs no join.
# ---------------------------------------------------------------------------

_TRADE_COLUMNS = (
    "run_id, strategy_id, template_name, exit_variant, ticker, qty, "
    "buy_date, buy_price, sale_date, sale_price, stock_rank, "
    "pnl_inr, pnl_pct, exit_reason, holding_days, buy_value, sale_value, financial_year"
)


def _trades_table_missing(conn) -> bool:
    """True when backtest_trades has not been created yet (fresh DB, or no
    queue has run). Callers return an empty result rather than a 500."""
    return not conn.execute(
        "SELECT COUNT(*) FROM duckdb_tables() WHERE table_name = 'backtest_trades'"
    ).fetchone()[0]


@router.get("/trades")
async def list_trades(
    strategy_id: Optional[str] = Query(None),
    template_name: Optional[str] = Query(None),
    ticker: Optional[str] = Query(None),
    financial_year: Optional[str] = Query(None, description="e.g. FY2007-08"),
    run_id: Optional[str] = Query(None),
    exit_reason: Optional[str] = Query(None),
    limit: int = Query(500, le=10_000),
    offset: int = Query(0, ge=0),
) -> Dict[str, Any]:
    """Individual trades, filterable and paginated. `total` is the unpaginated
    count so the caller can page without re-querying."""
    filters, params = [], []
    for col, val in (
        ("strategy_id", strategy_id), ("template_name", template_name),
        ("ticker", ticker), ("financial_year", financial_year),
        ("run_id", run_id), ("exit_reason", exit_reason),
    ):
        if val:
            filters.append(f"{col} = ?")
            params.append(val)
    where = f"WHERE {' AND '.join(filters)}" if filters else ""

    with get_duckdb_connection(BACKTEST_DUCKDB_PATH, persist=False, read_only=True) as conn:
        if _trades_table_missing(conn):
            return {"trades": [], "total": 0, "limit": limit, "offset": offset}
        total = conn.execute(f"SELECT COUNT(*) FROM backtest_trades {where}", params).fetchone()[0]
        rows = conn.execute(
            f"SELECT {_TRADE_COLUMNS} FROM backtest_trades {where} "
            "ORDER BY sale_date DESC, ticker LIMIT ? OFFSET ?",
            [*params, limit, offset],
        ).fetchdf()
    return {
        "trades": json.loads(rows.to_json(orient="records", date_format="iso")),
        "total": total,
        "limit": limit,
        "offset": offset,
    }


@router.get("/trades/summary")
async def trades_summary(
    group_by: str = Query("strategy", pattern="^(strategy|financial_year|exit_reason|ticker)$"),
    strategy_id: Optional[str] = Query(None),
) -> Dict[str, Any]:
    """Aggregate P&L/trade counts. `group_by=financial_year` gives the
    year-by-year realised picture that matches the per-year tax treatment."""
    column = {
        "strategy": "strategy_id", "financial_year": "financial_year",
        "exit_reason": "exit_reason", "ticker": "ticker",
    }[group_by]
    where, params = ("WHERE strategy_id = ?", [strategy_id]) if strategy_id else ("", [])

    with get_duckdb_connection(BACKTEST_DUCKDB_PATH, persist=False, read_only=True) as conn:
        if _trades_table_missing(conn):
            return {"group_by": group_by, "rows": []}
        rows = conn.execute(
            f"""SELECT {column} AS key,
                       COUNT(*) AS n_trades,
                       SUM(pnl_inr) AS pnl_inr,
                       AVG(pnl_pct) AS avg_pnl_pct,
                       AVG(holding_days) AS avg_holding_days,
                       AVG(CASE WHEN pnl_inr > 0 THEN 1.0 ELSE 0.0 END) AS win_rate
                FROM backtest_trades {where}
                GROUP BY 1 ORDER BY 1""",
            params,
        ).fetchdf()
    return {"group_by": group_by, "rows": json.loads(rows.to_json(orient="records"))}


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
    # [BUG FIX, 4th fundamental-strategies review, item 4] "integrity_check_
    # failed" added — the row existing in backtest_runs previously always
    # meant "completed" even when integrity_passed is False (CRITICAL
    # SPEC-BT-001 checks failed), which was invisible to any API/frontend
    # caller of this endpoint.
    status: str  # "running" | "completed" | "integrity_check_failed" | "failed" | "unknown"
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
    is the last thing run_orchestrator_backtest.py does) AND its persisted
    integrity_passed is not False; "integrity_check_failed" if the row
    exists but integrity_passed is False (2026-07-28, 4th fundamental-
    strategies review, item 4 — previously indistinguishable from a clean
    "completed" run here); "failed" if the subprocess's log shows a
    traceback with no row yet, "running" otherwise."""
    with get_duckdb_connection(BACKTEST_DUCKDB_PATH, persist=False, read_only=True) as conn:
        row = get_run(conn, run_id)
        if row is not None:
            signal_counts = get_signal_counts(conn, [run_id])
            status = "integrity_check_failed" if row.get("integrity_passed") is False else "completed"
            return OrchestratorStatusResponse(run_id=run_id, status=status, run=_summary(row, signal_counts))

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
    exit_variant: Optional[str] = None
    regime_method: Optional[str] = None
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


def _queue_runner_is_alive(queue_id: str) -> bool:
    """True iff a `run_strategy_queue` process for this queue_id is still
    running. A queue's trigger log/progress file has no way to record that
    its own process was killed or crashed without writing a final summary
    (e.g. killed by systemd-oomd, or the host restarting mid-run) — without
    this check, list_active_queues() would report such a queue as "running"
    forever, which is exactly what showed up as phantom entries in the
    Backtest page's Active Strategies board (2026-07-22)."""
    needle = f"--report-suffix {queue_id}"
    for proc in psutil.process_iter(["cmdline"]):
        try:
            cmdline = proc.info["cmdline"]
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
        if cmdline and "run_strategy_queue" in " ".join(cmdline) and needle in " ".join(cmdline):
            return True
    return False


@router.get("/queue/active", response_model=ActiveQueuesResponse)
async def list_active_queues(limit: int = Query(10, le=50)) -> ActiveQueuesResponse:
    """Queue_ids still running (a trigger log exists with no final summary
    yet, AND the run_strategy_queue process is actually still alive),
    most-recently-triggered first — lets the Backtest page discover and
    display a queue's progress even if it was triggered from a different
    browser session, the CLI, or an API call rather than this page's own
    panel (which otherwise only knows about queues it personally
    triggered, via client-side state)."""
    if not _QUEUE_LOGS_DIR.exists():
        return ActiveQueuesResponse(queue_ids=[])
    log_files = sorted(_QUEUE_LOGS_DIR.glob("*.log"), key=lambda p: p.stat().st_mtime, reverse=True)
    active = [
        p.stem for p in log_files
        if not (_REPORTS_DIR / f"strategy_queue_{p.stem}.json").exists() and _queue_runner_is_alive(p.stem)
    ]
    return ActiveQueuesResponse(queue_ids=active[:limit])
