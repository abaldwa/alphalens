"""
datastore/api/routers/valuation.py

Phase: 3
Specs: SPEC-VAL-001, SPEC-VAL-002, SPEC-VAL-003, SPEC-VAL-005
Owner: Platform / Valuation
Consumers: dashboard, backtest, external API clients

Valuation REST endpoints — thin controllers that delegate to ValuationEngine.

Routes:
  GET /api/v1/valuation/{ticker}              — single stock valuation
  GET /api/v1/valuation/{ticker}/sensitivity  — WACC × growth sensitivity table
  GET /api/v1/valuation/batch/ranked          — batch valuation ranked by MoS
  GET /api/v1/valuation/{ticker}/history      — historical intrinsic values

[AS BUILT, SPEC-SCHED-013] persist=False on all DuckDB reads.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query

from config.settings import SIGNALS_DUCKDB_PATH
from config.universe import load_universe_raw
from datastore.api.db import get_duckdb_connection
from systems.damodaran_valuation.dcf.models import FCFFInputs, FCFFTwoStageModel
from systems.damodaran_valuation.valuation_engine import ValuationEngine, ValuationResult

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/valuation", tags=["Valuation"])

# Shared engine instance (singleton per process).  Write-back enabled.
_engine = ValuationEngine(mc_simulations=2_000, write_signals=True)


def _result_to_dict(r: ValuationResult) -> Dict[str, Any]:
    """Serialize ValuationResult to a JSON-serialisable dict."""
    return {
        "ticker": r.ticker,
        "as_of_date": r.as_of_date,
        "lifecycle_stage": r.lifecycle_stage,
        "intrinsic_value": r.intrinsic_value,
        "current_price": r.current_price,
        "valuation_gap_pct": r.valuation_gap_pct,
        "margin_of_safety": r.margin_of_safety,
        "wacc": r.wacc,
        "cost_of_equity": r.cost_of_equity,
        "terminal_value_pct": r.terminal_value_pct,
        "dcf_model_type": r.dcf_model_type,
        "scenario_bull": r.scenario_bull,
        "scenario_base": r.scenario_base,
        "scenario_bear": r.scenario_bear,
        "mc_probability_undervalued": r.mc_probability_undervalued,
        "relative_pe_gap": r.relative_pe_gap,
        "data_quality": r.data_quality,
        "error": r.error,
    }


@router.get("/batch/ranked")
async def get_batch_ranked(
    tickers: Optional[str] = Query(
        default=None,
        description="Comma-separated list of NSE tickers; omit for full universe.",
    ),
    as_of_date: Optional[str] = Query(default=None, description="ISO date YYYY-MM-DD"),
    limit: int = Query(default=50, ge=1, le=500, description="Max results"),
    n_workers: int = Query(default=4, ge=1, le=16),
) -> Dict[str, Any]:
    """
    Batch valuation ranked by margin_of_safety (most undervalued first).

    Parameters
    ----------
    tickers : str, optional
        Comma-separated ticker list.  If omitted, the active Nifty 500 universe
        is used (slower — expect 5–15 min).
    as_of_date : str, optional
        Point-in-time date.
    limit : int
        Maximum results to return (default 50).
    n_workers : int
        Parallelism (default 4).

    Returns
    -------
    dict
        {count, as_of_date, results: [ValuationResult, ...]}
    """
    if tickers:
        ticker_list = [t.strip().upper() for t in tickers.split(",") if t.strip()]
    else:
        try:
            univ = load_universe_raw()
            ticker_list = univ["ticker"].dropna().tolist()
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Universe load failed: {exc}")

    results = _engine.value_universe(ticker_list, as_of_date=as_of_date, n_workers=n_workers)

    ranked = sorted(
        [r for r in results if r and r.margin_of_safety is not None],
        key=lambda r: r.margin_of_safety,  # type: ignore[arg-type]
        reverse=True,
    )

    return {
        "count": len(ranked),
        "as_of_date": as_of_date,
        "results": [_result_to_dict(r) for r in ranked[:limit]],
    }


@router.get("/{ticker}")
async def get_valuation(
    ticker: str,
    as_of_date: Optional[str] = Query(default=None, description="ISO date YYYY-MM-DD"),
) -> Dict[str, Any]:
    """
    Compute and return full Damodaran valuation for one ticker.

    Parameters
    ----------
    ticker : str
        NSE ticker symbol (e.g. RELIANCE).
    as_of_date : str, optional
        Point-in-time valuation date (defaults to today).

    Returns
    -------
    dict
        Full ValuationResult as JSON.

    Raises
    ------
    404
        If fundamentals are insufficient (< 4 quarters).
    500
        On unexpected computation failure.
    """
    ticker = ticker.upper()
    try:
        result = _engine.value_stock(ticker, as_of_date=as_of_date)
    except RuntimeError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        logger.error(f"Valuation error for {ticker}: {exc}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc))

    return _result_to_dict(result)


@router.get("/{ticker}/sensitivity")
async def get_sensitivity(
    ticker: str,
    as_of_date: Optional[str] = Query(default=None),
    wacc_steps: int = Query(default=3, ge=1, le=5, description="±steps of 1% WACC"),
    growth_steps: int = Query(default=3, ge=1, le=5, description="±steps of 1% growth"),
) -> Dict[str, Any]:
    """
    WACC × terminal-growth sensitivity table for one ticker.

    Produces a grid of intrinsic values with WACC varying ±``wacc_steps``%
    and terminal growth varying ±``growth_steps``% around the base case.

    Parameters
    ----------
    ticker : str
        NSE ticker symbol.
    as_of_date : str, optional
        Point-in-time date.
    wacc_steps : int
        Number of steps in each direction for WACC (default 3 → ±3 %).
    growth_steps : int
        Number of steps in each direction for growth (default 3 → ±3 %).

    Returns
    -------
    dict
        {base_wacc, base_growth, table: [[{wacc, growth, intrinsic_value}, ...]]}
    """
    ticker = ticker.upper()
    try:
        base = _engine.value_stock(ticker, as_of_date=as_of_date)
    except RuntimeError as exc:
        raise HTTPException(status_code=404, detail=str(exc))

    if base.wacc is None or base.intrinsic_value is None:
        raise HTTPException(
            status_code=422,
            detail=f"Cannot compute sensitivity for {ticker}: no base-case intrinsic value.",
        )

    # Re-run valuation at each grid point (re-uses same fundamentals via engine)
    # We tweak WACC directly by building a mini FCFFTwoStageModel grid.
    base_wacc = base.wacc
    base_g = 0.05  # standard terminal growth baseline

    table: List[Dict[str, Any]] = []
    model = FCFFTwoStageModel()

    wacc_range = [base_wacc + i * 0.01 for i in range(-wacc_steps, wacc_steps + 1)]
    growth_range = [base_g + i * 0.01 for i in range(-growth_steps, growth_steps + 1)]

    # We need FCFF components from fundamentals — re-load them
    try:
        from systems.damodaran_valuation.valuation_engine import (
            _load_fundamentals,
            _latest_row,
            _safe_float,
            _compute_revenue_cagr,
        )
        aod = as_of_date or __import__("datetime").date.today().isoformat()
        fund_df = _load_fundamentals(ticker, aod)
        latest = _latest_row(fund_df)
        revenue = _safe_float(latest.get("revenue"), 1.0)
        ebit_margin = _safe_float(latest.get("operating_margin"), 0.10)
        ebit = revenue * ebit_margin
        dep = _safe_float(latest.get("depreciation"), 0.0)
        capex = _safe_float(latest.get("capex"), 0.0)
        nwc = revenue * 0.03
        shares = _safe_float(latest.get("shares_outstanding"), 1.0)
        debt = _safe_float(latest.get("total_debt"), 0.0)
        cash = _safe_float(latest.get("cash_and_equivalents"), 0.0)
        g_high = _compute_revenue_cagr(fund_df, 3)

        for w in wacc_range:
            for g in growth_range:
                if w <= g:
                    table.append({"wacc": round(w, 4), "terminal_growth": round(g, 4), "intrinsic_value": None})
                    continue
                inp = FCFFInputs(
                    ebit=ebit, tax_rate=0.25, depreciation=dep, capex=capex,
                    change_in_nwc=nwc, wacc=w,
                    high_growth_rate=g_high, revenue=revenue,
                    terminal_growth_rate=g, high_growth_years=5,
                    shares_outstanding=shares, total_debt=debt, cash=cash,
                )
                try:
                    r = model.value(inp)
                    table.append({
                        "wacc": round(w, 4),
                        "terminal_growth": round(g, 4),
                        "intrinsic_value": round(r.intrinsic_value, 2),
                    })
                except Exception:
                    table.append({"wacc": round(w, 4), "terminal_growth": round(g, 4), "intrinsic_value": None})

    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Sensitivity computation failed: {exc}")

    return {
        "ticker": ticker,
        "base_wacc": round(base_wacc, 4),
        "base_terminal_growth": base_g,
        "table": table,
    }


@router.get("/{ticker}/history")
async def get_valuation_history(
    ticker: str,
    start_date: Optional[str] = Query(default=None, description="ISO date YYYY-MM-DD"),
    end_date: Optional[str] = Query(default=None, description="ISO date YYYY-MM-DD"),
) -> Dict[str, Any]:
    """
    Historical intrinsic values for one ticker from valuation_signals table.

    Parameters
    ----------
    ticker : str
        NSE ticker symbol.
    start_date : str, optional
        Start of date range.
    end_date : str, optional
        End of date range (defaults to today).

    Returns
    -------
    dict
        {ticker, count, history: [{date, intrinsic_value, margin_of_safety, ...}]}
    """
    ticker = ticker.upper()
    try:
        with get_duckdb_connection(SIGNALS_DUCKDB_PATH, persist=False, read_only=True) as conn:
            # Check if table exists
            tables = conn.execute("SHOW TABLES").fetchall()
            table_names = [t[0] for t in tables]
            if "valuation_signals" not in table_names:
                return {"ticker": ticker, "count": 0, "history": []}

            clauses = ["ticker = ?"]
            params: list = [ticker]
            if start_date:
                clauses.append("date >= ?")
                params.append(start_date)
            if end_date:
                clauses.append("date <= ?")
                params.append(end_date)

            where = " AND ".join(clauses)
            rows = conn.execute(
                f"""
                SELECT date, lifecycle_stage, intrinsic_value,
                       valuation_gap_pct, margin_of_safety,
                       wacc, dcf_model_type, mc_probability_undervalued
                FROM valuation_signals
                WHERE {where}
                ORDER BY date DESC
                """,
                params,
            ).fetchall()

    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"History query failed: {exc}")

    cols = [
        "date", "lifecycle_stage", "intrinsic_value",
        "valuation_gap_pct", "margin_of_safety",
        "wacc", "dcf_model_type", "mc_probability_undervalued",
    ]
    history = [dict(zip(cols, r)) for r in rows]

    return {"ticker": ticker, "count": len(history), "history": history}
