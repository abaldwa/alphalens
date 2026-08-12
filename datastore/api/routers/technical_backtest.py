"""
datastore/api/routers/technical_backtest.py

Phase: Technical Analysis Momentum-parity backtest reporting (2026-08-01)
Owner: Platform / Backtest
Consumers: frontend/src/pages/technical/experimentation.tsx,
    recommended-strategies.tsx

New router (kept separate from datastore/api/routers/technical.py, which
is the existing live screener/alerts/analytics surface at /api/v1/ta —
this one is purely the new sweep-report reporting layer, same relationship
datastore/api/routers/momentum.py's /experimentation endpoints have to the
rest of momentum.py). Exact trigger/status/read pattern as momentum.py's
_launch_trigger/_trigger_status (same detached-background-subprocess
design, same polling contract) — reused here rather than reimplemented, so
the existing frontend SweepTriggerButton component works against these
endpoints unmodified.

Read endpoints return the report JSON file's raw dict rather than a
strict per-field Pydantic model (unlike momentum.py's ExperimentationReport)
— the three Technical sweep reports (experimentation/filter_overlays/
recommended_strategies) have meaningfully different variant shapes
(recommended_strategies variants additionally nest a signal_failures dict,
filter_overlays variants carry a "filter" key, etc.), and a shared strict
schema would either force lossy field omission or three near-duplicate
schemas for no real benefit over the frontend just consuming the JSON
directly (same shape scripts/run_technical_*.py already write it in).
"""

import json
import logging
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/technical_backtest", tags=["Technical Backtest"])

_REPORTS_DIR = Path(__file__).resolve().parents[3] / "backtest" / "reports" / "technical"
_TRIGGER_LOGS_DIR = _REPORTS_DIR / "trigger_logs"


class TriggerResponse(BaseModel):
    job_id: str
    status: str = "started"


class TriggerStatusResponse(BaseModel):
    job_id: str
    status: str  # "running" | "completed" | "failed" | "unknown"
    log_tail: Optional[str] = None
    report_file: Optional[str] = None


def _launch_trigger(module: str, job_prefix: str, extra_args: Optional[list] = None) -> TriggerResponse:
    job_id = f"{job_prefix}_{uuid.uuid4().hex[:10]}"
    _TRIGGER_LOGS_DIR.mkdir(parents=True, exist_ok=True)
    log_path = _TRIGGER_LOGS_DIR / f"{job_id}.log"
    cmd = [sys.executable, "-m", module, *(extra_args or [])]
    logger.info(f"technical_backtest._launch_trigger: job_id={job_id} cmd={' '.join(cmd)}")
    with open(log_path, "w") as log_fh:
        subprocess.Popen(cmd, stdout=log_fh, stderr=subprocess.STDOUT, start_new_session=True)
    return TriggerResponse(job_id=job_id)


def _trigger_status(job_id: str, report_glob: str) -> TriggerStatusResponse:
    log_path = _TRIGGER_LOGS_DIR / f"{job_id}.log"
    if not log_path.exists():
        return TriggerStatusResponse(job_id=job_id, status="unknown")

    log_tail = "".join(log_path.read_text(errors="replace").splitlines(keepends=True)[-40:])
    launched_at = log_path.stat().st_mtime
    newer_reports = sorted(
        (p for p in _REPORTS_DIR.glob(report_glob) if p.stat().st_mtime >= launched_at - 5),
        key=lambda p: p.stat().st_mtime,
    )
    if newer_reports:
        return TriggerStatusResponse(
            job_id=job_id, status="completed", log_tail=log_tail, report_file=newer_reports[-1].name,
        )
    if "Traceback (most recent call last)" in log_tail:
        return TriggerStatusResponse(job_id=job_id, status="failed", log_tail=log_tail)
    return TriggerStatusResponse(job_id=job_id, status="running", log_tail=log_tail)


def _read_latest_report(glob_pattern: str, not_found_detail: str) -> Dict[str, Any]:
    files = sorted(_REPORTS_DIR.glob(glob_pattern))
    if not files:
        raise HTTPException(status_code=404, detail=not_found_detail)
    latest = files[-1]
    data = json.loads(latest.read_text())
    data["report_file"] = latest.name
    return data


# --------------------------------------------------------------- experimentation

@router.get("/experimentation")
async def get_experimentation() -> Dict[str, Any]:
    """scripts/run_technical_experimentation.py's baseline sweep — every
    (template, exit_variant, max_hold_days, top_n) variant. 404 until that
    script has been run at least once."""
    return _read_latest_report(
        "technical_experimentation_*.json", "No technical experimentation report found yet",
    )


@router.post("/experimentation/trigger", response_model=TriggerResponse)
async def trigger_experimentation(quick: bool = False) -> TriggerResponse:
    """Launches scripts/run_technical_experimentation.py as a detached
    subprocess; poll /experimentation/trigger/status/{job_id}. quick=true
    runs the reduced grid (5 templates, 1 max_hold_days, 1 top_n) for
    faster iteration instead of the full 3,528-config sweep."""
    return _launch_trigger(
        "scripts.run_technical_experimentation", "technical_experimentation",
        extra_args=["--quick"] if quick else None,
    )


@router.get("/experimentation/trigger/status/{job_id}", response_model=TriggerStatusResponse)
async def get_experimentation_trigger_status(job_id: str) -> TriggerStatusResponse:
    return _trigger_status(job_id, "technical_experimentation_*.json")


# --------------------------------------------------------------- filter overlays

@router.get("/filter_overlays")
async def get_filter_overlays() -> Dict[str, Any]:
    """scripts/run_technical_filter_overlays.py's 5-entry-filter robustness
    sweep (liquidity_floor/quality_gated/downtrend_filter/circuit_lock_proxy/
    regime_conditional), each run individually against the same
    template x top_n grid."""
    return _read_latest_report(
        "technical_filter_overlays_*.json", "No technical filter-overlays report found yet",
    )


@router.post("/filter_overlays/trigger", response_model=TriggerResponse)
async def trigger_filter_overlays(quick: bool = False) -> TriggerResponse:
    return _launch_trigger(
        "scripts.run_technical_filter_overlays", "technical_filter_overlays",
        extra_args=["--quick"] if quick else None,
    )


@router.get("/filter_overlays/trigger/status/{job_id}", response_model=TriggerStatusResponse)
async def get_filter_overlays_trigger_status(job_id: str) -> TriggerStatusResponse:
    return _trigger_status(job_id, "technical_filter_overlays_*.json")


# ------------------------------------------------------------ long-history comparison

# Written by scripts/build_ta_comparison_report.py. Note this one lives in
# backtest/reports/ rather than backtest/reports/technical/ (where the three
# sweep reports above live), because it is built from the orchestrator run
# reports that the queue writes there — hence its own path rather than
# _read_latest_report.
_COMPARISON_REPORT = Path(__file__).resolve().parents[3] / "backtest" / "reports" / "ta_comparison_2009.json"


@router.get("/comparison")
async def get_comparison() -> Dict[str, Any]:
    """The 2009-2026 Technical comparison dataset: every template x exit variant
    with CAGR/Sharpe/Sortino/Calmar, per-FY year-on-year returns, rolling
    2/3/4/5-year return distributions, and the annual-reset (income) ledger for
    both LTCG regimes.

    CONSUMERS MUST HONOUR `measure_3_status`. The annual-reset figures are
    provisional: FY tax is reported but not actually debited from the portfolio,
    so equity compounds tax-free (see backtest/ta_comparison_report.py). Every
    annual_reset entry additionally carries `unverified: true` so that a client
    reading past the top-level flag still cannot render them as final by
    accident. The lump measures carry no such caveat.
    """
    if not _COMPARISON_REPORT.exists():
        raise HTTPException(
            status_code=404,
            detail=("No comparison report yet — run scripts/build_ta_comparison_report.py "
                    "'backtest/reports/orchestrator_ta2009_*_job*.json'"),
        )
    data = json.loads(_COMPARISON_REPORT.read_text())
    data["report_file"] = _COMPARISON_REPORT.name
    return data


@router.get("/trade_book")
async def get_trade_book(
    run_id: str,
    limit: int = 500,
    offset: int = 0,
    outcome: Optional[str] = None,
    financial_year: Optional[str] = None,
) -> Dict[str, Any]:
    """One run's trades, paginated, straight from backtest_trades.

    Paginated rather than whole because the store holds 1.85M rows and a single
    run can carry 18,000 — enough to hang a browser tab. `outcome` filters to
    "win"/"loss"; `financial_year` narrows to one FY label (e.g. "FY2019-20").

    Read-only connection, opened per request and closed immediately: DuckDB is
    single-writer and a long-lived reader here is what starved job tails during
    the August sweep ("16 retries exhausted"). A short read is safe; a held one
    is not.
    """
    import duckdb

    from config.settings import BACKTEST_DUCKDB_PATH

    limit = max(1, min(limit, 2000))
    where = ["run_id = ?"]
    params: list = [run_id]
    if outcome == "win":
        where.append("pnl_inr > 0")
    elif outcome == "loss":
        where.append("pnl_inr <= 0")
    if financial_year:
        where.append("financial_year = ?")
        params.append(financial_year)
    clause = " AND ".join(where)

    try:
        conn = duckdb.connect(str(BACKTEST_DUCKDB_PATH), read_only=True)
    except Exception as exc:  # pragma: no cover - depends on live DB state
        raise HTTPException(status_code=503, detail=f"backtest store unavailable: {exc}") from exc
    try:
        total, wins, gross = conn.execute(
            f"SELECT COUNT(*), COUNT(*) FILTER (WHERE pnl_inr > 0), COALESCE(SUM(pnl_inr), 0) "
            f"FROM backtest_trades WHERE {clause}", params,
        ).fetchone()
        if total == 0 and offset == 0:
            raise HTTPException(status_code=404, detail=f"no trades stored for run {run_id}")
        cols = ["ticker", "qty", "buy_date", "buy_price", "sale_date", "sale_price",
                "pnl_inr", "pnl_pct", "exit_reason", "holding_days", "financial_year"]
        rows = conn.execute(
            f"SELECT {', '.join(cols)} FROM backtest_trades WHERE {clause} "
            f"ORDER BY sale_date, ticker LIMIT ? OFFSET ?", [*params, limit, offset],
        ).fetchall()
    finally:
        conn.close()

    return {
        "run_id": run_id,
        "total": total,
        "wins": wins,
        "losses": total - wins,
        "net_pnl_inr": gross,
        "limit": limit,
        "offset": offset,
        # pnl_pct is stored as a FRACTION (-0.05 == -5%); the client converts
        # once, on render. Flagged here because reading it as a percentage has
        # already caused one wrong analysis in this project.
        "pnl_pct_is_fraction": True,
        "trades": [dict(zip(cols, r)) for r in rows],
    }


# --------------------------------------------------------- recommended strategies

@router.get("/recommended_strategies")
async def get_recommended_strategies() -> Dict[str, Any]:
    """scripts/run_technical_recommended_strategies.py's composite filter
    strategies (Balanced/Risk-Managed/Max-Defensive) across every template,
    plus curated cross-style combo strategies (TechnicalComboAdapter) and a
    per-variant signal-failure breakdown (losing trades with their entry
    signal snapshot)."""
    return _read_latest_report(
        "technical_recommended_strategies_*.json", "No technical recommended-strategies report found yet",
    )


@router.get("/template_leaderboard")
async def get_template_leaderboard() -> Dict[str, Any]:
    """Every technical screener template's BEST stored backtest run, ranked by
    excess return over a real Nifty 500 buy-and-hold over that run's own window.

    2026-08-09. Two things this deliberately does that a naive leaderboard
    would get wrong:

    1. Excess return is computed against the benchmark for EACH RUN'S OWN
       date window, not one global number. The 46 original templates were
       swept over 2016-2026 while Category T ran 2021-2026, and those windows
       have materially different benchmark CAGRs (12.5% vs 13.7%) and very
       different market character. Runs from different windows are returned
       in separate `windows` groups and must not be ranked against each other.
    2. Older runs have `benchmark_cagr` NULL in metrics_json (they predate
       benchmark capture), so excess is recomputed here from real index_ohlcv
       closes rather than being reported as a loss. A NULL benchmark means
       "unknown", never "underperformed".
    """
    import duckdb
    import pandas as pd

    from config.settings import BACKTEST_DUCKDB_PATH, DUCKDB_PATH

    try:
        bt = duckdb.connect(str(BACKTEST_DUCKDB_PATH), read_only=True)
        nm = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    except Exception as exc:  # pragma: no cover - depends on live DB state
        raise HTTPException(status_code=503, detail=f"backtest store unavailable: {exc}") from exc

    rows = bt.execute(
        """
        SELECT config_json, metrics_json, start_date, end_date, integrity_passed, dsr, exit_policy_variant
        FROM backtest_runs WHERE channel = 'technical' AND metrics_json IS NOT NULL
        """
    ).fetchall()

    _bench_cache: Dict[str, Optional[float]] = {}

    def _benchmark(start, end) -> Optional[float]:
        key = f"{start}->{end}"
        if key in _bench_cache:
            return _bench_cache[key]
        df = nm.execute(
            "SELECT close FROM index_ohlcv WHERE index_name = 'Nifty 500' "
            "AND date BETWEEN ? AND ? AND close IS NOT NULL ORDER BY date",
            [start, end],
        ).df()
        years = (pd.Timestamp(end) - pd.Timestamp(start)).days / 365.25
        # <2 closes or a degenerate window can't produce a real CAGR — None
        # (unknown), never 0.0, so it can't masquerade as a flat benchmark.
        val = None
        if len(df) >= 2 and years > 0 and df.close.iloc[0] > 0:
            val = float((df.close.iloc[-1] / df.close.iloc[0]) ** (1 / years) - 1)
        _bench_cache[key] = val
        return val

    best: Dict[tuple, Dict[str, Any]] = {}
    for config_json, metrics_json, start, end, integrity, dsr, exit_variant in rows:
        try:
            cfg = json.loads(config_json) or {}
            metrics = json.loads(metrics_json) or {}
        except (TypeError, ValueError):
            continue
        template = cfg.get("template_name")
        sharpe = metrics.get("sharpe")
        if not template or sharpe is None:
            continue
        window = f"{start}->{end}"
        bench = metrics.get("benchmark_cagr")
        if bench is None:
            bench = _benchmark(start, end)
        cagr = metrics.get("cagr")
        entry = {
            "template": template,
            "window": window,
            "start_date": str(start),
            "end_date": str(end),
            "cagr": cagr,
            "benchmark_cagr": bench,
            "excess_return": (cagr - bench) if (cagr is not None and bench is not None) else None,
            "sharpe": sharpe,
            "sortino": metrics.get("sortino"),
            "max_drawdown": metrics.get("max_drawdown"),
            "win_rate": metrics.get("win_rate"),
            "profit_factor": metrics.get("profit_factor"),
            "n_trades": metrics.get("n_trades"),
            "turnover_ratio": metrics.get("turnover_ratio"),
            "top_n": cfg.get("top_n"),
            "exit_variant": exit_variant,
            "integrity_passed": bool(integrity) if integrity is not None else None,
            "dsr": dsr,
            "category": "T" if template.startswith("T") and template[1:].isdigit() else template[:1],
        }
        key = (template, window)
        if key not in best or sharpe > best[key]["sharpe"]:
            best[key] = entry

    entries = list(best.values())
    windows: Dict[str, Dict[str, Any]] = {}
    for e in entries:
        grp = windows.setdefault(
            e["window"], {"window": e["window"], "start_date": e["start_date"],
                          "end_date": e["end_date"], "benchmark_cagr": e["benchmark_cagr"], "entries": []},
        )
        grp["entries"].append(e)
    for grp in windows.values():
        grp["entries"].sort(key=lambda e: (e["excess_return"] is None, -(e["excess_return"] or 0)))
        grp["n_templates"] = len(grp["entries"])
        grp["n_beating_benchmark"] = sum(1 for e in grp["entries"] if (e["excess_return"] or 0) > 0)
        grp["n_integrity_passed"] = sum(1 for e in grp["entries"] if e["integrity_passed"])

    ordered = sorted(windows.values(), key=lambda g: (-g["n_templates"], g["window"]))
    return {
        "generated_at": pd.Timestamp.utcnow().isoformat(),
        "n_templates": len({e["template"] for e in entries}),
        "n_runs_considered": len(rows),
        "windows": ordered,
        "caveats": [
            "Runs from different date windows are NOT comparable — the 46 original templates were "
            "swept over 2016-2026, Category T over 2021-2026. Compare within a window group only.",
            "integrity_passed=false means the run failed backtest/core/integrity_checker.py; treat "
            "those rows as unvalidated regardless of how good the returns look.",
            "excess_return=null means the benchmark could not be computed for that window, not that "
            "the strategy underperformed.",
        ],
    }


@router.post("/recommended_strategies/trigger", response_model=TriggerResponse)
async def trigger_recommended_strategies(quick: bool = False) -> TriggerResponse:
    return _launch_trigger(
        "scripts.run_technical_recommended_strategies", "technical_recommended_strategies",
        extra_args=["--quick"] if quick else None,
    )


@router.get("/recommended_strategies/trigger/status/{job_id}", response_model=TriggerStatusResponse)
async def get_recommended_strategies_trigger_status(job_id: str) -> TriggerStatusResponse:
    return _trigger_status(job_id, "technical_recommended_strategies_*.json")
