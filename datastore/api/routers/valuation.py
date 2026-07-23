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
from datetime import date as date_type
from typing import Any, Dict, List, Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query

from config.settings import DUCKDB_PATH, SIGNALS_DUCKDB_PATH
from config.universe import load_universe_raw
from datastore.api.db import get_duckdb_connection
from systems.damodaran_valuation.dcf.models import FCFFInputs, FCFFTwoStageModel
from systems.damodaran_valuation.dcf.wacc import SECTOR_UNLEVERED_BETAS
from systems.damodaran_valuation.relative.pe_regression import RelativePERegression
from systems.damodaran_valuation.valuation_engine import (
    ValuationEngine,
    ValuationResult,
    _compute_revenue_cagr,
    _get_sector,
    _latest_row,
    _load_current_price,
    _load_fundamentals,
    _safe_float,
)

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
    max_tier: Optional[int] = Query(
        default=None,
        ge=1,
        le=6,
        description=(
            "Restrict the universe scan to tier<=max_tier (1=Nifty50, 2=NiftyNext50, "
            "3=Midcap150, 4=Smallcap250, 6=broader NSE). Ignored if `tickers` is set. "
            "Lets the dashboard offer a fast Nifty-50/100/500 scope instead of always "
            "scanning the full ~2000+ stock universe."
        ),
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
    max_tier : int, optional
        Restrict to tier<=max_tier when `tickers` is not given.
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
            if max_tier is not None:
                univ = univ[univ["tier"] <= max_tier]
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
    # Center the grid on the terminal growth rate actually used for this
    # ticker's lifecycle-stage base case (0.02-0.06 depending on stage),
    # not a hardcoded 0.05 — a DECLINING-stage stock's base case uses 0.02,
    # so a fixed 0.05 center misrepresented the sensitivity anchor point.
    base_g = base.terminal_growth_rate if base.terminal_growth_rate is not None else 0.05

    table: List[Dict[str, Any]] = []
    model = FCFFTwoStageModel()

    wacc_range = [base_wacc + i * 0.01 for i in range(-wacc_steps, wacc_steps + 1)]
    growth_range = [base_g + i * 0.01 for i in range(-growth_steps, growth_steps + 1)]

    # We need FCFF components from fundamentals — re-load them
    try:
        from systems.damodaran_valuation.valuation_engine import _load_market_cap_cr

        aod = as_of_date or date_type.today().isoformat()
        fund_df = _load_fundamentals(ticker, aod)
        latest = _latest_row(fund_df)
        revenue = _safe_float(latest.get("revenue"), 1.0)
        ebit_margin = _safe_float(latest.get("operating_margin"), 0.10)
        ebit = revenue * ebit_margin
        dep = _safe_float(latest.get("depreciation"), 0.0)
        capex = _safe_float(latest.get("capex"), 0.0)
        nwc = revenue * 0.03
        # Same crore-units conversion as valuation_engine.value_stock() — FCFF
        # models expect shares_outstanding in crore units, but `fundamentals`
        # stores an absolute share count (and is NULL for most rows), so
        # derive it from market_cap_cr / price when missing rather than
        # defaulting to a fabricated 1.0 (see BuildLog.md 2026-07-04).
        market_cap_cr = _load_market_cap_cr(ticker)
        current_price_for_shares = _load_current_price(ticker, aod)
        shares_abs = _safe_float(latest.get("shares_outstanding"), default=0.0) or None
        if shares_abs is None and market_cap_cr and current_price_for_shares:
            shares_abs = (market_cap_cr * 1e7) / current_price_for_shares
        shares = (shares_abs / 1e7) if shares_abs else 1.0
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


def _ttm_pe(ticker: str, fund_df: pd.DataFrame, aod: str) -> Optional[float]:
    """TTM P/E from real data — `fundamentals` has no `pe_ratio` column, so
    this derives it from trailing-4-quarter EPS (sum, not the single latest
    quarter — avoids seasonality) and the current OHLCV close."""
    if "eps" not in fund_df.columns:
        return None
    eps_vals = fund_df["eps"].dropna().head(4)
    if len(eps_vals) < 4:
        return None
    ttm_eps = float(eps_vals.sum())
    if ttm_eps <= 0:
        return None
    price = _load_current_price(ticker, aod)
    if price is None:
        return None
    return price / ttm_eps


@router.get("/{ticker}/relative")
async def get_relative_valuation(
    ticker: str,
    as_of_date: Optional[str] = Query(default=None, description="ISO date YYYY-MM-DD"),
    min_peers: int = Query(default=20, ge=3, le=50, description="Minimum sector peers required to fit the regression"),
) -> Dict[str, Any]:
    """
    Sector-relative P/E regression valuation (SPEC-VAL-002 Model 5).

    Builds a same-sector peer group from real fundamentals (config.universe's
    real sector taxonomy + each peer's own PE/EPS-growth/payout/beta), fits
    RelativePERegression on the peers, and compares the ticker's actual PE
    to the peer-implied "fair" PE. This is the actual regression the DCF
    engine's value_stock() supports via its `peer_df` parameter, but that
    parameter was never populated by any router endpoint before now — the
    other valuation endpoints all leave `relative_pe_gap` as None.

    Raises
    ------
    404
        Insufficient fundamentals for the ticker itself.
    422
        No sector found, no valid PE for the ticker, or fewer than
        ``min_peers`` sector peers with a valid PE ratio.
    """
    ticker = ticker.upper()
    aod = as_of_date or date_type.today().isoformat()

    sector = _get_sector(ticker)
    if not sector:
        raise HTTPException(status_code=422, detail=f"No sector found for {ticker} in the universe")

    beta = SECTOR_UNLEVERED_BETAS.get(sector, SECTOR_UNLEVERED_BETAS["Default"])

    univ = load_universe_raw()
    peer_tickers = univ[(univ["sector"] == sector) & (univ["ticker"] != ticker)]["ticker"].tolist()

    peer_rows: List[Dict[str, float]] = []
    for peer in peer_tickers:
        fund_df = _load_fundamentals(peer, aod)
        pe = _ttm_pe(peer, fund_df, aod)
        if pe is None or pe <= 0:
            continue
        peer_rows.append({
            "pe_ratio": pe,
            "eps_growth_3y": _compute_revenue_cagr(fund_df, years=3),
            # payout_ratio isn't a column in `fundamentals` — 0.0 degrades the
            # regression coefficient for that term rather than crashing.
            "payout_ratio": 0.0,
            "beta": beta,
        })

    if len(peer_rows) < min_peers:
        raise HTTPException(
            status_code=422,
            detail=(
                f"Only {len(peer_rows)} sector peers with a valid TTM PE for {ticker} "
                f"(sector={sector}); need >= {min_peers} to fit a regression."
            ),
        )

    fund_df = _load_fundamentals(ticker, aod)
    if len(fund_df) < 4:
        raise HTTPException(status_code=404, detail=f"Insufficient fundamentals for {ticker}")
    pe = _ttm_pe(ticker, fund_df, aod)
    if pe is None or pe <= 0:
        raise HTTPException(status_code=422, detail=f"{ticker} has no valid TTM PE (needs >=4 quarters of EPS + a current price)")
    latest = _latest_row(fund_df)
    eps = _safe_float(latest.get("eps"), default=0.0)

    reg = RelativePERegression()
    try:
        reg.fit(pd.DataFrame(peer_rows))
        result = reg.value_gap({
            "pe_ratio": pe,
            "eps_growth_3y": _compute_revenue_cagr(fund_df, years=3),
            "payout_ratio": 0.0,
            "beta": beta,
        })
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Relative PE regression failed: {exc}")

    current_price = _load_current_price(ticker, aod)
    implied_price = (eps * result.predicted_pe) if eps > 0 else None

    return {
        "ticker": ticker,
        "sector": sector,
        "as_of_date": aod,
        "actual_pe": result.actual_pe,
        "predicted_pe": result.predicted_pe,
        "gap_pct": result.gap_pct,
        "is_overvalued": result.is_overvalued,
        "r_squared": result.r_squared,
        "n_peers": result.n_peers,
        "current_price": current_price,
        "implied_price": implied_price,
        "coefficients": result.coefficients,
        # `fundamentals` has no payout_ratio column and this uses
        # sector-average beta (not the company's own) — surface which
        # regression inputs are proxies rather than blending them in
        # silently (Fix 6/14).
        "proxy_used": [
            "eps_growth_3y_from_revenue_cagr",
            "beta_sector_average",
            "payout_ratio_missing",
        ],
    }


@router.get("/pillar_summary")
async def get_valuation_pillar_summary() -> Dict[str, Any]:
    """Home page pillar-outcome card: latest `valuation_signals` snapshot.
    Deliberately does NOT call /accuracy/backtest's hit-rate logic here —
    that endpoint does a live per-ticker Python-loop join against
    ohlcv_adjusted, too expensive for a summary card that renders on every
    Home page load. Valuation also has only one real "strategy" (DCF /
    margin-of-safety, no multi-strategy leaderboard the way Technical has
    42 templates), so `top_strategy` names that one method rather than
    picking among several; `top_strategy_success_rate_pct` stays null
    rather than paying the live-backtest cost on every page load."""
    try:
        with get_duckdb_connection(SIGNALS_DUCKDB_PATH, persist=False, read_only=True) as conn:
            tables = [r[0] for r in conn.execute("SHOW TABLES").fetchall()]
            if "valuation_signals" not in tables:
                return {"as_of_date": None, "available": False, "recommendation_count": 0,
                        "avg_expected_return_pct": None, "top_strategy": None, "top_strategy_success_rate_pct": None}

            latest = conn.execute("SELECT MAX(date) FROM valuation_signals").fetchone()
            latest_date = latest[0] if latest else None
            if latest_date is None:
                return {"as_of_date": None, "available": False, "recommendation_count": 0,
                        "avg_expected_return_pct": None, "top_strategy": None, "top_strategy_success_rate_pct": None}

            row = conn.execute(
                """
                SELECT COUNT(*), AVG(valuation_gap_pct)
                FROM valuation_signals
                WHERE date = ? AND margin_of_safety > 0
                """,
                [latest_date],
            ).fetchone()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"valuation_signals query failed: {exc}")

    count, avg_gap = row if row else (0, None)
    return {
        "as_of_date": str(latest_date),
        "available": True,
        "recommendation_count": int(count or 0),
        "avg_expected_return_pct": float(avg_gap) if avg_gap is not None else None,
        "top_strategy": "DCF (Margin of Safety)",
        "top_strategy_success_rate_pct": None,
    }


@router.get("/accuracy/backtest")
async def get_valuation_accuracy(
    horizon_days: int = Query(
        default=5, ge=1, le=252,
        description="Trading-day-ish lookforward horizon (calendar days) used to price realized outcomes.",
    ),
    min_age_days: Optional[int] = Query(
        default=None,
        description="Only score valuation_signals rows at least this many calendar days old "
        "(defaults to horizon_days, so every scored row has a real, non-fabricated forward price).",
    ),
) -> Dict[str, Any]:
    """
    F6 — backtest past `valuation_signals` predictions against realized price outcomes.

    For every (ticker, date) row in `valuation_signals` old enough that
    `horizon_days` has actually elapsed, joins the entry close price (on or
    before the signal date) and the realized close price `horizon_days`
    later (on or before signal date + horizon_days, real `ohlcv_adjusted`
    rows only — no interpolation/fabrication) and checks whether the sign
    of `margin_of_safety` (undervalued vs overvalued) matches the sign of
    the realized forward return. Rows with no realized price yet (too
    recent, or the ticker has a data gap) are excluded from scoring, not
    guessed at.
    """
    min_age = min_age_days if min_age_days is not None else horizon_days
    cutoff_date = (date_type.today() - pd.Timedelta(days=min_age)).isoformat()

    try:
        with get_duckdb_connection(SIGNALS_DUCKDB_PATH, persist=False, read_only=True) as conn:
            tables = [r[0] for r in conn.execute("SHOW TABLES").fetchall()]
            if "valuation_signals" not in tables:
                return {"count": 0, "scored": 0, "hit_rate": None, "rows": []}
            sig_rows = conn.execute(
                """
                SELECT date, ticker, lifecycle_stage, intrinsic_value,
                       valuation_gap_pct, margin_of_safety
                FROM valuation_signals
                WHERE date <= ?
                ORDER BY date DESC, ticker
                """,
                [cutoff_date],
            ).fetchall()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"valuation_signals query failed: {exc}")

    if not sig_rows:
        return {"count": 0, "scored": 0, "hit_rate": None, "rows": []}

    out_rows: List[Dict[str, Any]] = []
    try:
        with get_duckdb_connection(DUCKDB_PATH, persist=False, read_only=True) as conn:
            for sig_date, ticker, lifecycle_stage, intrinsic_value, gap_pct, mos in sig_rows:
                entry_row = conn.execute(
                    "SELECT close FROM ohlcv_adjusted WHERE ticker = ? AND date <= ? "
                    "ORDER BY date DESC LIMIT 1",
                    [ticker, sig_date],
                ).fetchone()
                if not entry_row or entry_row[0] is None:
                    continue
                entry_price = float(entry_row[0])

                target_date = (pd.to_datetime(sig_date) + pd.Timedelta(days=horizon_days)).date().isoformat()
                fwd_row = conn.execute(
                    "SELECT date, close FROM ohlcv_adjusted WHERE ticker = ? AND date > ? AND date <= ? "
                    "ORDER BY date DESC LIMIT 1",
                    [ticker, sig_date, target_date],
                ).fetchone()
                if not fwd_row or fwd_row[1] is None:
                    # No real forward-priced bar strictly after the signal date — skip, don't fabricate.
                    continue
                realized_date, realized_price = fwd_row
                realized_price = float(realized_price)
                realized_return_pct = (realized_price / entry_price - 1.0) * 100.0

                predicted_undervalued = (mos is not None and mos > 0)
                realized_up = realized_return_pct > 0
                hit = (predicted_undervalued == realized_up) if mos is not None else None

                out_rows.append({
                    "ticker": ticker,
                    "signal_date": str(sig_date),
                    "lifecycle_stage": lifecycle_stage,
                    "intrinsic_value": intrinsic_value,
                    "valuation_gap_pct": gap_pct,
                    "margin_of_safety": mos,
                    "predicted_undervalued": predicted_undervalued if mos is not None else None,
                    "entry_price": entry_price,
                    "realized_date": str(realized_date),
                    "realized_price": realized_price,
                    "realized_return_pct": round(realized_return_pct, 3),
                    "hit": hit,
                })
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"ohlcv_adjusted lookup failed: {exc}")

    scored = [r for r in out_rows if r["hit"] is not None]
    hits = sum(1 for r in scored if r["hit"])
    hit_rate = (hits / len(scored)) if scored else None

    undervalued_scored = [r for r in scored if r["predicted_undervalued"]]
    overvalued_scored = [r for r in scored if not r["predicted_undervalued"]]
    avg_return_undervalued = (
        sum(r["realized_return_pct"] for r in undervalued_scored) / len(undervalued_scored)
        if undervalued_scored else None
    )
    avg_return_overvalued = (
        sum(r["realized_return_pct"] for r in overvalued_scored) / len(overvalued_scored)
        if overvalued_scored else None
    )

    return {
        "horizon_days": horizon_days,
        "count": len(out_rows),
        "scored": len(scored),
        "hits": hits,
        "hit_rate": round(hit_rate, 4) if hit_rate is not None else None,
        "avg_return_undervalued_pct": round(avg_return_undervalued, 3) if avg_return_undervalued is not None else None,
        "avg_return_overvalued_pct": round(avg_return_overvalued, 3) if avg_return_overvalued is not None else None,
        "rows": out_rows,
    }
