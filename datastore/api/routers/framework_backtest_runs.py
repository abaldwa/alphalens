"""
datastore/api/routers/framework_backtest_runs.py

Read-only API over framework_backtest_runs (momentum_framework/results/
db_writer.py's table, same DB as backtest_runs -- config.settings.
BACKTEST_DUCKDB_PATH) -- the new native-engine campaign results table
(momentum_framework/scripts/run_full_campaign.py writes here), distinct
from the legacy backtest_runs table backtest_runs.py already serves.

DECOUPLING (explicit user instruction, 2026-09-04, ahead of a frontend
rewrite): config_json/metrics_json are raw JSON blobs on the DB row --
this router parses them server-side into a flat, typed response so the
frontend never has to know the blob's internal shape. If that shape
changes (a new config field, a renamed metric), only this router's
_row_to_summary() needs updating, not every page that reads this table.

New router (own prefix), not an addition to backtest_runs.py -- same
"wrap, don't refactor" rationale that file's own docstring already
states for its own existence relative to backtest_reports.py.
"""

import json
from datetime import date as date_type
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from backtest.core.tax import Transaction as TaxTransaction
from backtest.core.tax import fy_net_tax, group_by_financial_year
from backtest.core.tax import total_tax as compute_total_tax
from config.benchmarks import RANK_BAND_BENCHMARKS
from config.settings import BACKTEST_DUCKDB_PATH
from datastore.api.db import get_duckdb_connection
from momentum_framework.results.stale_results import STALE_RESULTS_CUTOFF_AT

router = APIRouter(prefix="/api/v1/framework-backtest", tags=["Framework Backtest"])


class FrameworkRunSummary(BaseModel):
    run_id: str
    strategy_id: str
    strategy_code: str
    band_id: int
    top_n: Optional[int] = None
    lookback_months: Optional[int] = None
    rebalance_cadence_days: Optional[int] = None
    position_sizing: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    cagr: Optional[float] = None
    sharpe_ratio: Optional[float] = None
    sortino_ratio: Optional[float] = None
    calmar_ratio: Optional[float] = None
    max_drawdown: Optional[float] = None
    win_rate: Optional[float] = None
    volatility_annualized: Optional[float] = None
    trade_count: int
    run_executed_at: Optional[str] = None


class FrameworkRunListResponse(BaseModel):
    runs: List[FrameworkRunSummary]
    total: int


def _row_to_summary(row: Dict[str, Any]) -> FrameworkRunSummary:
    config = json.loads(row["config_json"]) if row.get("config_json") else {}
    metrics = json.loads(row["metrics_json"]) if row.get("metrics_json") else {}
    return FrameworkRunSummary(
        run_id=row["run_id"],
        strategy_id=row["strategy_id"],
        strategy_code=row["strategy_code"],
        band_id=row["band_id"],
        top_n=config.get("top_n"),
        lookback_months=config.get("lookback_months"),
        rebalance_cadence_days=config.get("rebalance_cadence_days"),
        position_sizing=config.get("position_sizing"),
        start_date=str(row["start_date"]) if row.get("start_date") else None,
        end_date=str(row["end_date"]) if row.get("end_date") else None,
        cagr=metrics.get("cagr"),
        sharpe_ratio=metrics.get("sharpe_ratio"),
        sortino_ratio=metrics.get("sortino_ratio"),
        calmar_ratio=metrics.get("calmar_ratio"),
        max_drawdown=metrics.get("max_drawdown"),
        win_rate=metrics.get("win_rate"),
        volatility_annualized=metrics.get("volatility_annualized"),
        trade_count=row["trade_count"],
        run_executed_at=str(row["run_executed_at"]) if row.get("run_executed_at") else None,
    )


@router.get("/runs", response_model=FrameworkRunListResponse)
async def list_framework_runs(
    strategy_code: Optional[str] = Query(None, description="Filter to one strategy code, e.g. R01"),
    band_id: Optional[int] = Query(None, description="Filter to one band_id"),
    include_stale: bool = Query(
        False,
        description=(
            "If false (default), excludes every run with run_executed_at "
            "before the 2026-09-06 remediation fixes (see "
            "momentum_framework/results/stale_results.py) -- those runs "
            "predate the portfolio-weighting fix (B1), the R07/R11-R13 "
            "crash-guard config (B3/B6), the top_n grid change (E1/E2), and "
            "costs+tax (E5), and are not comparable to current results. Set "
            "true to see everything, e.g. for a deliberate before/after "
            "comparison -- old rows are never deleted."
        ),
    ),
    limit: int = Query(500, le=5000),
    offset: int = Query(0, ge=0),
) -> FrameworkRunListResponse:
    where = []
    params: List[Any] = []
    if strategy_code:
        where.append("strategy_code = ?")
        params.append(strategy_code)
    if band_id is not None:
        where.append("band_id = ?")
        params.append(band_id)
    if not include_stale:
        where.append("run_executed_at >= ?")
        params.append(STALE_RESULTS_CUTOFF_AT)
    where_clause = f"WHERE {' AND '.join(where)}" if where else ""

    with get_duckdb_connection(BACKTEST_DUCKDB_PATH, persist=False, read_only=True) as conn:
        total = conn.execute(
            f"SELECT COUNT(*) FROM framework_backtest_runs {where_clause}", params
        ).fetchone()[0]
        rows = conn.execute(
            f"""
            SELECT run_id, strategy_id, strategy_code, band_id, start_date, end_date,
                   config_json, metrics_json, trade_count, run_executed_at
            FROM framework_backtest_runs
            {where_clause}
            ORDER BY run_executed_at DESC
            LIMIT ? OFFSET ?
            """,
            params + [limit, offset],
        ).fetchdf()

    runs = [_row_to_summary(row.to_dict()) for _, row in rows.iterrows()]
    return FrameworkRunListResponse(runs=runs, total=total)


# ---------------------------------------------------------------------------
# Per-run analytics: yearly returns, rolling (arbitrary date-range) returns,
# trade quality and benchmark comparison.
#
# [2026-09-05, explicit user instruction] Bring the Long Term CAGR / Regular
# Returns / Pre-Tax / Post-Tax / Window / Benchmark / Trade Quality features
# from /backtest-report/metrics onto the campaign sweep. Those features read
# from a pre-generated per-strategy StrategyReport JSON; framework runs have
# no such thing, only raw trades (framework_backtest_trades). So this
# computes the same figures FROM those trades, using the same canonical
# tax engine (backtest/core/tax.py) the legacy report is built on, rather
# than re-deriving tax/fiscal-year rules a second time.
#
# SCOPED TO ONE RUN AT A TIME, deliberately. The legacy report's controls
# operate over a handful of named strategies; this campaign has 1,500+ point
# configs. Computing FY-netted tax for all of them on every toggle would be
# both expensive and not a meaningful comparison — a reader picks one config
# to inspect, exactly as they would pick one strategy on the legacy report.
# ---------------------------------------------------------------------------

CAMPAIGN_INITIAL_CAPITAL = 1_000_000.0
"""momentum_framework/scripts/run_campaign.py::INITIAL_CAPITAL -- every
campaign run starts from the same fixed capital, so a return is just
pnl / this constant. Not imported from that module directly: it is a
script entry point (drags in the orchestrator/db_writer/write-lock
machinery), not a library -- same "wrap, don't refactor" reasoning as
this file's own docstring."""

# M13 (this sweep's own band) and any band the size-tier mapping doesn't
# reach fall back to the broadest index actually present in
# benchmark_performance. config/benchmarks.py's RANK_BAND_BENCHMARKS names
# three indices (Nifty 100, Nifty Next 50, Nifty Midcap 100) that were never
# backfilled into that table -- those bands fall back too.
_BENCHMARK_FALLBACK_INDEX = "Nifty Microcap 250"


def _fy_label(fy_end: date_type) -> str:
    """2016-03-31 -> "FY2015-16", matching the legacy report's fiscal-year
    labelling (features/backtest-report/core/fiscalYears.ts)."""
    start_year = fy_end.year - 1
    return f"FY{start_year}-{str(fy_end.year)[2:]}"


def _run_band_id(conn: Any, run_id: str) -> int:
    row = conn.execute(
        "SELECT band_id FROM framework_backtest_runs WHERE run_id = ?", [run_id]
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"Unknown run_id: {run_id}")
    return int(row[0])


def _load_closed_trades(
    conn: Any,
    run_id: str,
    from_date: Optional[date_type] = None,
    to_date: Optional[date_type] = None,
) -> List[TaxTransaction]:
    """Closed trades for one run as tax.py Transactions, optionally
    restricted to those that CLOSED within [from_date, to_date]. An open
    position contributes no realized transaction either way -- this module
    only ever reports realized returns, same as the legacy report."""
    where = ["run_id = ?", "sale_date IS NOT NULL", "sale_price IS NOT NULL"]
    params: List[Any] = [run_id]
    if from_date is not None:
        where.append("sale_date >= ?")
        params.append(from_date)
    if to_date is not None:
        where.append("sale_date <= ?")
        params.append(to_date)
    rows = conn.execute(
        f"""
        SELECT ticker, buy_date, sale_date, buy_price, sale_price, qty
        FROM framework_backtest_trades
        WHERE {' AND '.join(where)}
        """,
        params,
    ).fetchall()
    return [
        TaxTransaction(
            ticker=ticker,
            buy_date=buy_date,
            sell_date=sale_date,
            buy_price=buy_price,
            sell_price=sale_price,
            quantity=int(round(qty)),
        )
        for ticker, buy_date, sale_date, buy_price, sale_price, qty in rows
    ]


class YearlyReturnRow(BaseModel):
    fy_label: str
    fy_end: str
    trade_count: int
    win_rate: Optional[float] = None
    gross_pnl: float
    tax_paid: float
    opening_capital: float
    pre_tax_return_pct: float
    post_tax_return_pct: float


class YearlyReturnsResponse(BaseModel):
    run_id: str
    initial_capital: float
    rows: List[YearlyReturnRow]


def _fy_capital_track(
    txns: List[TaxTransaction], initial_capital: float
) -> List[Dict[str, Any]]:
    """Walk closed trades FY by FY, compounding two running capital bases
    (pre-tax and post-tax) forward from `initial_capital` -- each FY's
    return is realized gross/tax-netted P&L divided by THAT FY's own
    opening capital, not a constant.

    BUG THIS REPLACES (found 2026-09-05): both /yearly-returns and
    /rolling-return used to divide every FY's/window's absolute-rupee P&L
    by a hardcoded CAMPAIGN_INITIAL_CAPITAL (Rs 10L), regardless of how
    large the book had actually compounded to by that point. Since the
    underlying trades' position sizes grow every year (the real engine
    reinvests gains -- verified against framework_backtest_trades: this
    run's avg trade size grew from ~Rs1.2L in 2009 to ~Rs3.7cr in 2026),
    a perfectly ordinary year's absolute P&L divided by the tiny original
    capital produced nonsensical "returns" like 1374%/4547%/-954% that
    then got compounded again in the frontend -- runaway numbers with no
    relationship to the actual (correctly-computed, see metrics_json.cagr)
    equity curve. Fix: track capital as this function does, so each FY's
    return_pct is relative to what was actually at risk that FY.
    """
    pre_tax_capital = initial_capital
    post_tax_capital = initial_capital
    rows: List[Dict[str, Any]] = []
    for fy_end, fy_txns in sorted(group_by_financial_year(txns).items()):
        gross_pnl = sum(t.gain for t in fy_txns)
        tax_paid = fy_net_tax(fy_txns)
        wins = sum(1 for t in fy_txns if t.gain > 0)

        pre_tax_return_pct = gross_pnl / pre_tax_capital if pre_tax_capital > 0 else 0.0
        post_tax_return_pct = (
            (gross_pnl - tax_paid) / post_tax_capital if post_tax_capital > 0 else 0.0
        )

        rows.append(
            {
                "fy_end": fy_end,
                "trade_count": len(fy_txns),
                "win_rate": wins / len(fy_txns) if fy_txns else None,
                "gross_pnl": gross_pnl,
                "tax_paid": tax_paid,
                "opening_capital": pre_tax_capital,
                "opening_capital_post_tax": post_tax_capital,
                "pre_tax_return_pct": pre_tax_return_pct,
                "post_tax_return_pct": post_tax_return_pct,
            }
        )

        pre_tax_capital *= 1 + pre_tax_return_pct
        post_tax_capital *= 1 + post_tax_return_pct
    return rows


def _compounded_capital_before(
    txns: List[TaxTransaction], initial_capital: float, before: date_type
) -> Dict[str, float]:
    """Opening capital (pre-tax and post-tax basis) a rolling-return window
    starting at `before` should use -- the FY-compounded capital carried
    forward from every FY that closed strictly before that date. `txns`
    must be the run's FULL closed-trade history, not window-scoped (the
    same FY-compounding walk _fy_capital_track() does, just stopped at the
    window boundary instead of the end of the run)."""
    pre_tax_capital = initial_capital
    post_tax_capital = initial_capital
    for row in _fy_capital_track(txns, initial_capital):
        if row["fy_end"] >= before:
            break
        pre_tax_capital = row["opening_capital"] * (1 + row["pre_tax_return_pct"])
        post_tax_capital = row["opening_capital_post_tax"] * (1 + row["post_tax_return_pct"])
    return {"pre_tax": pre_tax_capital, "post_tax": post_tax_capital}


@router.get("/runs/{run_id}/yearly-returns", response_model=YearlyReturnsResponse)
async def get_yearly_returns(run_id: str) -> YearlyReturnsResponse:
    """Year-on-year realized return, gross and FY-netted post-tax -- the
    data behind both the Long Term CAGR and Regular Returns views (the
    frontend picks which column to show; both are computed here so a mode
    toggle costs no extra round trip). Each FY's return is against that
    FY's own opening (compounded) capital -- see _fy_capital_track()."""
    with get_duckdb_connection(BACKTEST_DUCKDB_PATH, persist=False, read_only=True) as conn:
        txns = _load_closed_trades(conn, run_id)

    rows = [
        YearlyReturnRow(
            fy_label=_fy_label(r["fy_end"]),
            fy_end=r["fy_end"].isoformat(),
            trade_count=r["trade_count"],
            win_rate=r["win_rate"],
            gross_pnl=r["gross_pnl"],
            tax_paid=r["tax_paid"],
            opening_capital=r["opening_capital"],
            pre_tax_return_pct=r["pre_tax_return_pct"],
            post_tax_return_pct=r["post_tax_return_pct"],
        )
        for r in _fy_capital_track(txns, CAMPAIGN_INITIAL_CAPITAL)
    ]
    return YearlyReturnsResponse(run_id=run_id, initial_capital=CAMPAIGN_INITIAL_CAPITAL, rows=rows)


class RollingReturnResponse(BaseModel):
    run_id: str
    from_date: str
    to_date: str
    years: float
    trade_count: int
    win_rate: Optional[float] = None
    gross_pnl: float
    tax_paid: float
    opening_capital: float
    pre_tax_return_pct: float
    post_tax_return_pct: float
    pre_tax_cagr: Optional[float] = None
    post_tax_cagr: Optional[float] = None


@router.get("/runs/{run_id}/rolling-return", response_model=RollingReturnResponse)
async def get_rolling_return(
    run_id: str,
    from_date: date_type = Query(..., description="Window start (inclusive), YYYY-MM-DD"),
    to_date: date_type = Query(..., description="Window end (inclusive), YYYY-MM-DD"),
) -> RollingReturnResponse:
    """Realized return for an arbitrary date range -- "what did this config
    return between these two dates" -- by re-slicing the run's own trades to
    those that CLOSED in the window, rather than re-simulating the backtest.
    Tax is still computed per-Financial-Year within the window (a trade
    closing mid-FY nets against the rest of that FY's trades even if the FY
    itself starts before `from_date`), consistent with how tax is charged in
    reality: it is not reset by an arbitrary reporting window.
    """
    if to_date <= from_date:
        raise HTTPException(status_code=400, detail="to_date must be after from_date")

    with get_duckdb_connection(BACKTEST_DUCKDB_PATH, persist=False, read_only=True) as conn:
        all_txns = _load_closed_trades(conn, run_id)

    opening = _compounded_capital_before(all_txns, CAMPAIGN_INITIAL_CAPITAL, from_date)
    txns = [
        t for t in all_txns
        if t.sell_date is not None and from_date <= t.sell_date <= to_date
    ]

    gross_pnl = sum(t.gain for t in txns)
    tax_paid = compute_total_tax(txns)
    wins = sum(1 for t in txns if t.gain > 0)
    years = (to_date - from_date).days / 365.25
    pre_tax_return_pct = gross_pnl / opening["pre_tax"] if opening["pre_tax"] > 0 else 0.0
    post_tax_return_pct = (
        (gross_pnl - tax_paid) / opening["post_tax"] if opening["post_tax"] > 0 else 0.0
    )

    def _cagr(return_pct: float) -> Optional[float]:
        base = 1.0 + return_pct
        if years <= 0 or base <= 0:
            return None
        return float(base ** (1.0 / years) - 1.0)

    return RollingReturnResponse(
        run_id=run_id,
        from_date=from_date.isoformat(),
        to_date=to_date.isoformat(),
        years=years,
        trade_count=len(txns),
        win_rate=wins / len(txns) if txns else None,
        gross_pnl=gross_pnl,
        tax_paid=tax_paid,
        opening_capital=opening["pre_tax"],
        pre_tax_return_pct=pre_tax_return_pct,
        post_tax_return_pct=post_tax_return_pct,
        pre_tax_cagr=_cagr(pre_tax_return_pct),
        post_tax_cagr=_cagr(post_tax_return_pct),
    )


class TradeQualityResponse(BaseModel):
    run_id: str
    trade_count: int
    win_rate: Optional[float] = None
    avg_win_pct: Optional[float] = None
    avg_loss_pct: Optional[float] = None
    avg_holding_days: Optional[float] = None
    best_trade_pct: Optional[float] = None
    worst_trade_pct: Optional[float] = None
    profit_factor: Optional[float] = None


@router.get("/runs/{run_id}/trade-quality", response_model=TradeQualityResponse)
async def get_trade_quality(
    run_id: str,
    from_date: Optional[date_type] = Query(None, description="Restrict to trades closed on/after this date"),
    to_date: Optional[date_type] = Query(None, description="Restrict to trades closed on/before this date"),
) -> TradeQualityResponse:
    """"% trades won" is how OFTEN the config is right; avg win/loss is how
    MUCH it makes or loses when it is -- same distinction the legacy Trade
    Quality tab draws. Computed straight from framework_backtest_trades's
    own pnl_pct/holding_days, not re-derived from pnl_inr."""
    with get_duckdb_connection(BACKTEST_DUCKDB_PATH, persist=False, read_only=True) as conn:
        where = ["run_id = ?", "sale_date IS NOT NULL", "pnl_pct IS NOT NULL"]
        params: List[Any] = [run_id]
        if from_date is not None:
            where.append("sale_date >= ?")
            params.append(from_date)
        if to_date is not None:
            where.append("sale_date <= ?")
            params.append(to_date)
        rows = conn.execute(
            f"SELECT pnl_pct, holding_days FROM framework_backtest_trades WHERE {' AND '.join(where)}",
            params,
        ).fetchall()

    if not rows:
        return TradeQualityResponse(run_id=run_id, trade_count=0)

    pcts = [r[0] for r in rows]
    holding = [r[1] for r in rows if r[1] is not None]
    wins = [p for p in pcts if p > 0]
    losses = [p for p in pcts if p < 0]
    gross_win = sum(wins)
    gross_loss = abs(sum(losses))

    return TradeQualityResponse(
        run_id=run_id,
        trade_count=len(pcts),
        win_rate=len(wins) / len(pcts),
        avg_win_pct=(sum(wins) / len(wins)) if wins else None,
        avg_loss_pct=(sum(losses) / len(losses)) if losses else None,
        avg_holding_days=(sum(holding) / len(holding)) if holding else None,
        best_trade_pct=max(pcts),
        worst_trade_pct=min(pcts),
        profit_factor=(gross_win / gross_loss) if gross_loss > 0 else None,
    )


class BenchmarkComparisonResponse(BaseModel):
    run_id: str
    band_id: int
    index_name: Optional[str] = None
    is_fallback_index: bool = False
    benchmark_cagr: Optional[float] = None
    benchmark_sharpe_ratio: Optional[float] = None
    benchmark_max_drawdown: Optional[float] = None
    benchmark_period_start: Optional[str] = None
    benchmark_period_end: Optional[str] = None
    note: Optional[str] = None


@router.get("/runs/{run_id}/benchmark", response_model=BenchmarkComparisonResponse)
async def get_benchmark_comparison(run_id: str) -> BenchmarkComparisonResponse:
    """The buy-and-hold index that best matches the run's band, per
    config/benchmarks.py's RANK_BAND_BENCHMARKS -- same mapping the legacy
    report uses. benchmark_performance stores one whole-period row per
    index (2009 - present), not a per-FY series, so this is a whole-period
    comparison rather than a year-matched one; the note field says so
    explicitly rather than implying a closer match than the data supports.
    """
    with get_duckdb_connection(BACKTEST_DUCKDB_PATH, persist=False, read_only=True) as conn:
        band_id = _run_band_id(conn, run_id)
        mapped_index = RANK_BAND_BENCHMARKS.get(band_id)
        for index_name, is_fallback in (
            [(mapped_index, False)] if mapped_index else []
        ) + [(_BENCHMARK_FALLBACK_INDEX, True)]:
            row = conn.execute(
                """
                SELECT index_name, cagr, sharpe_ratio, max_drawdown, period_start, period_end
                FROM benchmark_performance WHERE index_name = ?
                """,
                [index_name],
            ).fetchone()
            if row is not None:
                return BenchmarkComparisonResponse(
                    run_id=run_id,
                    band_id=band_id,
                    index_name=row[0],
                    is_fallback_index=is_fallback,
                    benchmark_cagr=row[1],
                    benchmark_sharpe_ratio=row[2],
                    benchmark_max_drawdown=row[3],
                    benchmark_period_start=str(row[4]) if row[4] else None,
                    benchmark_period_end=str(row[5]) if row[5] else None,
                    note=(
                        f"{mapped_index} is this band's mapped index but has no row in "
                        f"benchmark_performance; showing {_BENCHMARK_FALLBACK_INDEX} instead. "
                        if is_fallback and mapped_index
                        else None
                    ),
                )

    return BenchmarkComparisonResponse(
        run_id=run_id,
        band_id=band_id,
        note="No benchmark data available for this band.",
    )
