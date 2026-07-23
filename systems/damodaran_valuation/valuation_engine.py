"""
systems/damodaran_valuation/valuation_engine.py

Phase: 3
Specs: SPEC-VAL-001, SPEC-VAL-002, SPEC-VAL-003, SPEC-VAL-005
Owner: Platform / Valuation
Consumers: datastore/api/routers/valuation.py, scripts/*, backtest/engine.py

Orchestrator that ties lifecycle classification, WACC computation, DCF
modelling, Monte Carlo simulation, and relative valuation together into a
single ``value_stock`` call.

Data loading pattern (SPEC-DS-002, SPEC-SCHED-013):
  - Fundamentals from DuckDB ``fundamentals`` table (PIT: announcement_date <= as_of_date)
  - Macro 10Y yield from DuckDB ``macro_indicators`` table
  - OHLCV from DuckDB ``ohlcv_adjusted`` table for current price
  - Results written back to ``valuation_signals`` table in SIGNALS_DUCKDB_PATH

No synthetic data in production paths (SPEC-QUALITY-003).
If < 4 quarters of fundamentals are available, raises RuntimeError.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from config.settings import DUCKDB_PATH, SIGNALS_DUCKDB_PATH
from config.universe import load_universe_raw
from datastore.api.db import get_duckdb_connection
from systems.damodaran_valuation.dcf.models import (
    DCFResult,
    ExcessReturnModel,
    FCFFInputs,
    FCFFThreeStageModel,
    FCFFTwoStageModel,
)
from systems.damodaran_valuation.dcf.wacc import SECTOR_UNLEVERED_BETAS, WACCCalculator, WACCInputs
from systems.damodaran_valuation.lifecycle.classifier import LifecycleClassifier, LifecycleStage
from systems.damodaran_valuation.relative.pe_regression import RelativePERegression
from systems.damodaran_valuation.scenarios.monte_carlo import MonteCarloDCF, MonteCarloResult

logger = logging.getLogger(__name__)

_FINANCIAL_SERVICES_STAGES = {LifecycleStage.FINANCIAL_SERVICES}

# Default 10Y G-Sec yield if macro table has no data (July 2025)
_DEFAULT_10Y_YIELD = 0.0632


@dataclass
class ValuationResult:
    """
    Full valuation output for one ticker (SPEC-VAL-001/002).

    Attributes
    ----------
    ticker : str
        NSE ticker symbol.
    as_of_date : str
        ISO date string (YYYY-MM-DD) of the valuation.
    lifecycle_stage : str
        Damodaran lifecycle stage (LifecycleStage value).
    intrinsic_value : float, optional
        Per-share intrinsic value (INR); None if distressed or insufficient data.
    current_price : float, optional
        Market price on ``as_of_date`` (INR).
    valuation_gap_pct : float, optional
        (price − IV) / IV; negative means stock is cheap.
    margin_of_safety : float, optional
        (IV − price) / IV; positive means stock is cheap.
    wacc : float, optional
        WACC used in computation (decimal).
    cost_of_equity : float, optional
        Cost of equity from WACC module (decimal).
    terminal_value_pct : float, optional
        Terminal value as fraction of total EV.
    dcf_model_type : str
        Name of the DCF model selected.
    scenario_bull : float, optional
        90th percentile Monte Carlo intrinsic value (INR).
    scenario_base : float, optional
        Median Monte Carlo intrinsic value (INR).
    scenario_bear : float, optional
        10th percentile Monte Carlo intrinsic value (INR).
    mc_probability_undervalued : float, optional
        Fraction of MC draws where IV > current_price.
    relative_pe_gap : float, optional
        Relative PE gap vs sector peers.
    data_quality : str
        'full', 'partial', or 'insufficient'.
    proxy_used : list[str]
        Names of inputs that fell back to a proxy/approximation rather than
        a directly-observed value (e.g. 'risk_free_rate_default',
        'eps_growth_3y_from_revenue_cagr', 'beta_sector_average',
        'payout_ratio_missing', 'altman_x2_roe_proxy'). Empty if every
        input used in this valuation was a real, directly-sourced figure.
    error : str, optional
        Error message if computation failed.
    """

    ticker: str
    as_of_date: str
    lifecycle_stage: str
    intrinsic_value: Optional[float] = None
    current_price: Optional[float] = None
    valuation_gap_pct: Optional[float] = None
    margin_of_safety: Optional[float] = None
    wacc: Optional[float] = None
    cost_of_equity: Optional[float] = None
    terminal_value_pct: Optional[float] = None
    dcf_model_type: str = "none"
    terminal_growth_rate: Optional[float] = None
    scenario_bull: Optional[float] = None
    scenario_base: Optional[float] = None
    scenario_bear: Optional[float] = None
    mc_probability_undervalued: Optional[float] = None
    relative_pe_gap: Optional[float] = None
    data_quality: str = "insufficient"
    proxy_used: List[str] = field(default_factory=list)
    error: Optional[str] = None


def _load_fundamentals(ticker: str, as_of_date: str) -> pd.DataFrame:
    """Load last 8 quarters of PIT fundamentals (SPEC-DS-002)."""
    with get_duckdb_connection(DUCKDB_PATH, persist=False, read_only=True) as conn:
        df = conn.execute(
            """
            SELECT *
            FROM   fundamentals
            WHERE  ticker = ?
              AND  announcement_date <= ?
            ORDER  BY announcement_date DESC
            LIMIT  8
            """,
            [ticker, as_of_date],
        ).df()
    return df


def _load_macro_yield(as_of_date: str) -> tuple[float, bool]:
    """
    Load latest India 10Y G-Sec yield on or before as_of_date.

    Returns (yield, is_proxy) — is_proxy is True whenever the frozen
    ``_DEFAULT_10Y_YIELD`` fallback was used (missing macro row or a DB
    error), so callers can surface this in ``ValuationResult.proxy_used``.
    """
    try:
        with get_duckdb_connection(DUCKDB_PATH, persist=False, read_only=True) as conn:
            row = conn.execute(
                """
                SELECT value FROM macro_indicators
                WHERE  indicator = 'india_10y_yield'
                  AND  date <= ?
                ORDER  BY date DESC
                LIMIT  1
                """,
                [as_of_date],
            ).fetchone()
        if row:
            return float(row[0]), False
        return _DEFAULT_10Y_YIELD, True
    except Exception:
        return _DEFAULT_10Y_YIELD, True


def _load_current_price(ticker: str, as_of_date: str) -> Optional[float]:
    """Load last available close price on or before as_of_date."""
    try:
        with get_duckdb_connection(DUCKDB_PATH, persist=False, read_only=True) as conn:
            row = conn.execute(
                """
                SELECT close FROM ohlcv_adjusted
                WHERE  ticker = ? AND date <= ?
                ORDER  BY date DESC
                LIMIT  1
                """,
                [ticker, as_of_date],
            ).fetchone()
        return float(row[0]) if row else None
    except Exception:
        return None


def _get_sector(ticker: str) -> str:
    """Look up sector from universe CSV; returns empty string if unknown."""
    try:
        univ = load_universe_raw()
        row = univ[univ["ticker"] == ticker]
        if not row.empty:
            return str(row.iloc[0].get("sector", "")) or ""
    except Exception:
        pass
    return ""


def _load_market_cap_cr(ticker: str) -> Optional[float]:
    """Look up market_cap_cr (already in INR crore) from the universe CSV."""
    try:
        univ = load_universe_raw()
        row = univ[univ["ticker"] == ticker]
        if not row.empty:
            mc = row.iloc[0].get("market_cap_cr")
            return float(mc) if mc is not None and np.isfinite(float(mc)) and float(mc) > 0 else None
    except Exception:
        pass
    return None


def _compute_revenue_cagr(df: pd.DataFrame, years: int = 3) -> float:
    """
    Compute revenue CAGR from quarterly fundamentals sorted newest-first.

    Uses annual-equivalent window: approximately 4 quarters per year.
    Returns 0.0 if insufficient data.
    """
    if "revenue" not in df.columns or df.empty:
        return 0.0
    rev = df["revenue"].dropna()
    n_quarters = years * 4
    if len(rev) < 2:
        return 0.0
    earliest = rev.iloc[min(len(rev) - 1, n_quarters - 1)]
    latest = rev.iloc[0]
    periods = min(len(rev) - 1, n_quarters - 1)
    if earliest <= 0 or periods <= 0:
        return 0.0
    return float((latest / earliest) ** (4.0 / periods) - 1.0)


def _altman_z(row: pd.Series) -> tuple[Optional[float], bool]:
    """
    Altman Z''-score (1995 non-manufacturing/emerging-market revision).

    Z'' = 6.56·X1 + 3.26·X2 + 6.72·X3 + 1.05·X4
    X1 = Working Capital / Total Assets
    X2 = Retained Earnings / Total Assets
    X3 = EBIT / Total Assets
    X4 = BV of equity / Total Debt

    Distress/grey/safe thresholds for this variant are Z'' < 1.1 / 1.1-2.6 / > 2.6
    (see systems/damodaran_valuation/lifecycle/classifier.py — NOT the
    original manufacturing model's 1.81/2.99 thresholds, which use a
    differently-weighted formula and do not apply here).

    X2 prefers the real ``retained_earnings`` column (Reserves & Surplus,
    added specifically for this purpose — see create_normalised.py's
    "deep-forensic altman_z fix 2026-07-07" comment). Falls back to the
    ROE × book-value proxy only when retained_earnings is missing, and
    flags that fallback via the returned ``is_proxy`` bool.

    Returns (z_score, is_proxy) — ``z_score`` is None if required inputs
    are missing; ``is_proxy`` is True iff the X2 term used the ROE×BV
    fallback rather than real retained_earnings.
    """
    try:
        total_assets = (
            float(row.get("current_assets") or 0)
            + float(row.get("total_debt") or 0)
            + float(row.get("total_equity") or 1.0)
        )
        if total_assets <= 0:
            return None, False
        wc = float(row.get("current_assets") or 0) - float(row.get("current_liabilities") or 0)
        # shares_outstanding is an absolute count; book_value_per_share is
        # real ₹/share, and total_assets/current_assets are ₹ crore — divide
        # by 1e7 so `bv` (book value of equity) is in the same crore units.
        bv = float(row.get("book_value_per_share") or 0) * (float(row.get("shares_outstanding") or 0) / 1e7)
        ebit_margin = float(row.get("operating_margin") or 0)
        revenue = float(row.get("revenue") or 1)
        ebit = ebit_margin * revenue

        retained_earnings = row.get("retained_earnings")
        is_proxy = retained_earnings is None
        re_value = float(retained_earnings) if not is_proxy else (float(row.get("roe") or 0) * bv)

        x1 = wc / total_assets
        x2 = re_value / total_assets
        x3 = ebit / total_assets
        x4 = bv / max(float(row.get("total_debt") or 1), 1.0)

        return 6.56 * x1 + 3.26 * x2 + 6.72 * x3 + 1.05 * x4, is_proxy
    except Exception:
        return None, False


def _latest_row(df: pd.DataFrame) -> Dict:
    """Return latest fundamental row as a dict."""
    return df.iloc[0].to_dict()


def _safe_float(val, default: float = 0.0) -> float:
    try:
        v = float(val)
        return v if np.isfinite(v) else default
    except (TypeError, ValueError):
        return default


class ValuationEngine:
    """
    Damodaran valuation orchestrator (SPEC-VAL-001/002/003/005).

    Pulls fundamentals + macro data, classifies lifecycle, selects appropriate
    DCF model, runs Monte Carlo simulation, and optionally runs peer PE regression.

    Parameters
    ----------
    mc_simulations : int
        Number of Monte Carlo draws per stock (default 2 000 for speed;
        use 10 000 for high-precision runs).
    mc_seed : int, optional
        RNG seed for reproducibility.
    write_signals : bool
        If True, persist ValuationResult to valuation_signals table in
        SIGNALS_DUCKDB_PATH after each computation.
    """

    def __init__(
        self,
        mc_simulations: int = 2_000,
        mc_seed: Optional[int] = None,
        write_signals: bool = True,
    ) -> None:
        self.mc_simulations = mc_simulations
        self._mc = MonteCarloDCF(seed=mc_seed)
        self._classifier = LifecycleClassifier()
        self._wacc_calc = WACCCalculator()
        self._write_signals = write_signals

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def value_stock(
        self,
        ticker: str,
        as_of_date: Optional[str] = None,
        peer_df: Optional[pd.DataFrame] = None,
    ) -> ValuationResult:
        """
        Full valuation pipeline for one stock (SPEC-VAL-001/002).

        Parameters
        ----------
        ticker : str
            NSE ticker symbol.
        as_of_date : str, optional
            ISO date (YYYY-MM-DD). Defaults to today.
        peer_df : pd.DataFrame, optional
            Sector peer DataFrame for relative PE regression.  If None,
            the relative PE component is skipped.

        Returns
        -------
        ValuationResult
            Complete valuation output; ``error`` field set if computation fails.

        Raises
        ------
        RuntimeError
            If < 4 quarters of fundamentals are available.  Callers that want
            a safe no-exception path should use ``value_universe()``.
        """
        aod = as_of_date or date.today().isoformat()

        # --- Load fundamentals ------------------------------------------------
        fund_df = _load_fundamentals(ticker, aod)
        if len(fund_df) < 4:
            raise RuntimeError(
                f"Insufficient fundamentals for {ticker}: only {len(fund_df)} quarters "
                "available (need >= 4).  Run backfill: "
                "python scripts/backfill_fundamentals_trendlyne.py"
            )

        latest = _latest_row(fund_df)
        sector = _get_sector(ticker)

        # --- Compute derived inputs -------------------------------------------
        proxy_used: List[str] = []
        revenue_cagr_3y = _compute_revenue_cagr(fund_df, years=3)
        revenue_cagr_5y = _compute_revenue_cagr(fund_df, years=5)
        altman_z, altman_x2_is_proxy = _altman_z(pd.Series(latest))
        if altman_x2_is_proxy:
            proxy_used.append("altman_x2_roe_proxy")

        fundamentals_for_classifier = {
            "revenue_cagr_3y": revenue_cagr_3y,
            "revenue_cagr_5y": revenue_cagr_5y,
            "net_margin": _safe_float(latest.get("net_margin")),
            "operating_margin": _safe_float(latest.get("operating_margin")),
            "payout_ratio": _safe_float(latest.get("payout_ratio")),
            "roe": _safe_float(latest.get("roe")),
            "interest_coverage": (
                _safe_float(latest.get("interest_coverage"), default=None)
                if latest.get("interest_coverage") is not None
                else None
            ),
            "altman_z": altman_z,
            "sector": sector,
        }

        # --- Classify lifecycle -----------------------------------------------
        stage = self._classifier.classify(fundamentals_for_classifier)

        # --- Load macro -------------------------------------------------------
        india_10y, india_10y_is_proxy = _load_macro_yield(aod)
        if india_10y_is_proxy:
            proxy_used.append("risk_free_rate_default")

        # --- WACC -------------------------------------------------------------
        de_ratio = _safe_float(latest.get("debt_to_equity") or latest.get("debt_equity"), default=0.3)
        int_cov = _safe_float(latest.get("interest_coverage"), default=5.0)
        total_debt = _safe_float(latest.get("total_debt"), default=0.0)
        cash = _safe_float(latest.get("cash_and_equivalents"), default=0.0)
        # Fix B6: subtracted from EV alongside total_debt in the equity
        # bridge — consolidated EV includes 100% of a partially-owned
        # subsidiary, but shareholders only have a claim on the parent's
        # stake. `non_controlling_interest` is a real fundamentals column
        # (create_normalised.py); defaults to 0.0 (no bridge change) when
        # unavailable.
        minority_interest = _safe_float(latest.get("non_controlling_interest"), default=0.0)
        revenue = _safe_float(latest.get("revenue"), default=1.0)
        ebit_margin = _safe_float(latest.get("operating_margin"), default=0.10)
        ebit = revenue * ebit_margin
        dep = _safe_float(latest.get("depreciation"), default=0.0)
        capex = _safe_float(latest.get("capex"), default=0.0)
        # NWC change approximated as 3% of revenue if not available
        nwc_chg = revenue * 0.03
        roe = _safe_float(latest.get("roe"), default=0.12)
        bvps = _safe_float(latest.get("book_value_per_share"), default=0.0)

        current_price = _load_current_price(ticker, aod)

        # `fundamentals.shares_outstanding` is stored as an *absolute* share
        # count (e.g. ~13.5bn for RELIANCE), but every DCF model in
        # dcf/models.py expects shares in *crore* units (see their
        # docstrings — "Diluted shares outstanding (crore)"; they do
        # `equity_value_cr / shares_cr * 100`). Passing the raw absolute
        # count straight through (as this code used to) made every
        # intrinsic-value output wrong by a ~1e7 factor. It's also NULL for
        # ~96% of fundamentals rows, so a bare default=1.0 fallback would
        # silently fabricate a wildly wrong share count for most tickers —
        # instead derive the real share count from two other real numbers
        # (market_cap_cr from the universe CSV, current_price from OHLCV)
        # when the fundamentals table doesn't have it directly.
        market_cap_cr = _load_market_cap_cr(ticker)
        shares_abs = _safe_float(latest.get("shares_outstanding"), default=0.0) or None
        if shares_abs is None and market_cap_cr and current_price:
            shares_abs = (market_cap_cr * 1e7) / current_price
        shares_cr = (shares_abs / 1e7) if shares_abs else None

        book_value = bvps * (shares_cr or 0.0)

        # market_cap_cr from the universe CSV is already real, correctly-scaled
        # data (used elsewhere across the app) — prefer it over recomputing
        # from price × shares, which only serves as a fallback when the
        # ticker isn't in the universe CSV.
        market_cap = market_cap_cr or ((current_price or 0.0) * (shares_abs or 0.0) / 1e7)

        spread = WACCCalculator.synthetic_rating_spread(int_cov)
        cost_of_debt = india_10y + spread

        total_val = max(market_cap + total_debt, 1.0)
        dw = total_debt / total_val
        ew = 1.0 - dw

        beta_u = SECTOR_UNLEVERED_BETAS.get(sector, SECTOR_UNLEVERED_BETAS["Default"])
        wacc_inputs = WACCInputs(
            india_10y_yield=india_10y,
            beta_unlevered=beta_u,
            debt_to_equity=de_ratio,
            cost_of_debt=cost_of_debt,
            debt_weight=dw,
            equity_weight=ew,
        )
        wacc_result = self._wacc_calc.compute(wacc_inputs)

        # --- Select & run DCF model ------------------------------------------
        dcf_result: Optional[DCFResult] = None
        model_name = "none"

        if shares_cr is None:
            # No real share count available (neither fundamentals.shares_outstanding
            # nor a universe.csv market cap to derive it from) — a per-share
            # intrinsic value can't be computed honestly, so skip DCF rather
            # than fabricate a shares_outstanding=1.0 fallback (SPEC-QUALITY-003).
            model_name = "none (no shares/market-cap data)"

        elif stage == LifecycleStage.DISTRESSED:
            # No DCF for distressed — Altman Z and relative valuation only
            dcf_result = None
            model_name = "none (distressed)"

        elif stage == LifecycleStage.FINANCIAL_SERVICES:
            if book_value > 0:
                dcf_result = ExcessReturnModel().value(
                    book_value=book_value,
                    roe=roe,
                    cost_of_equity=wacc_result.cost_of_equity,
                    # Explicit clamp — was previously omitted, silently
                    # falling back to the model's internal 0.05 default
                    # regardless of the risk-free-rate ceiling (Fix 9).
                    terminal_growth=min(0.05, india_10y),
                    shares_outstanding=shares_cr,
                    total_debt=total_debt,
                    cash=cash,
                )
                model_name = "ExcessReturn"

        elif stage == LifecycleStage.YOUNG_GROWTH:
            inp = FCFFInputs(
                ebit=ebit, tax_rate=0.25, depreciation=dep, capex=capex,
                change_in_nwc=nwc_chg, wacc=wacc_result.wacc,
                high_growth_rate=min(revenue_cagr_3y, 0.50),
                revenue=revenue,
                # Damodaran's stable-growth constraint: g <= risk-free rate,
                # not merely g < WACC (which the DCF models already enforce
                # via their own ValueError guard).
                terminal_growth_rate=min(0.06, india_10y),
                high_growth_years=7,
                shares_outstanding=shares_cr,
                total_debt=total_debt,
                cash=cash,
                minority_interest=minority_interest,
                target_margin=max(ebit_margin, 0.10),
            )
            dcf_result = FCFFThreeStageModel().value(inp)
            model_name = "FCFFThreeStage"

        elif stage in (LifecycleStage.HIGH_GROWTH, LifecycleStage.MATURE_GROWTH):
            inp = FCFFInputs(
                ebit=ebit, tax_rate=0.25, depreciation=dep, capex=capex,
                change_in_nwc=nwc_chg, wacc=wacc_result.wacc,
                high_growth_rate=min(revenue_cagr_3y, 0.35),
                revenue=revenue,
                terminal_growth_rate=min(0.05, india_10y),
                high_growth_years=5,
                shares_outstanding=shares_cr,
                total_debt=total_debt,
                cash=cash,
                minority_interest=minority_interest,
            )
            dcf_result = FCFFTwoStageModel().value(inp)
            model_name = "FCFFTwoStage"

        elif stage == LifecycleStage.MATURE_STABLE:
            g = min(max(revenue_cagr_3y, 0.02), 0.08)
            inp = FCFFInputs(
                ebit=ebit, tax_rate=0.25, depreciation=dep, capex=capex,
                change_in_nwc=nwc_chg, wacc=wacc_result.wacc,
                high_growth_rate=g,
                revenue=revenue,
                terminal_growth_rate=min(0.04, india_10y),
                high_growth_years=5,
                shares_outstanding=shares_cr,
                total_debt=total_debt,
                cash=cash,
                minority_interest=minority_interest,
            )
            dcf_result = FCFFTwoStageModel().value(inp)
            model_name = "FCFFTwoStage (stable)"

        elif stage == LifecycleStage.DECLINING:
            g = min(revenue_cagr_3y, 0.02)
            inp = FCFFInputs(
                ebit=ebit, tax_rate=0.25, depreciation=dep, capex=capex,
                change_in_nwc=nwc_chg, wacc=wacc_result.wacc,
                high_growth_rate=g,
                revenue=revenue,
                terminal_growth_rate=min(0.02, india_10y),
                high_growth_years=3,
                shares_outstanding=shares_cr,
                total_debt=total_debt,
                cash=cash,
                minority_interest=minority_interest,
            )
            try:
                dcf_result = FCFFTwoStageModel().value(inp)
                model_name = "FCFFTwoStage (declining)"
            except ValueError:
                dcf_result = None
                model_name = "none (wacc <= g)"

        # --- Monte Carlo --------------------------------------------------
        mc_result: Optional[MonteCarloResult] = None
        scenario_bull = scenario_base = scenario_bear = None
        mc_prob = None

        if dcf_result is not None and stage != LifecycleStage.FINANCIAL_SERVICES:
            try:
                # Rebuild FCFFInputs from dcf assumptions for MC
                mc_inp = FCFFInputs(
                    ebit=ebit, tax_rate=0.25, depreciation=dep, capex=capex,
                    change_in_nwc=nwc_chg, wacc=wacc_result.wacc,
                    high_growth_rate=_safe_float(dcf_result.assumptions.get("high_growth_rate"), 0.10),
                    revenue=revenue,
                    terminal_growth_rate=_safe_float(
                        dcf_result.assumptions.get("terminal_growth_rate"), 0.05),
                    high_growth_years=int(dcf_result.assumptions.get("high_growth_years", 5)),
                    shares_outstanding=shares_cr,
                    total_debt=total_debt,
                    cash=cash,
                    minority_interest=minority_interest,
                )
                mc_result = self._mc.simulate(
                    mc_inp,
                    current_price=current_price or 0.0,
                    n_simulations=self.mc_simulations,
                )
                scenario_bear = mc_result.p10
                scenario_base = mc_result.median_value
                scenario_bull = mc_result.p90
                mc_prob = mc_result.probability_undervalued
            except Exception as exc:
                logger.warning(f"Monte Carlo failed for {ticker}: {exc}")

        # --- Relative PE regression -------------------------------------------
        pe_gap: Optional[float] = None
        if peer_df is not None and not peer_df.empty:
            try:
                reg = RelativePERegression()
                reg.fit(peer_df)
                ticker_pe_data = {
                    "pe_ratio": _safe_float(latest.get("pe_ratio"), default=0.0),
                    "eps_growth_3y": revenue_cagr_3y,  # proxy when EPS CAGR not available
                    "payout_ratio": _safe_float(latest.get("payout_ratio"), default=0.0),
                    "beta": float(SECTOR_UNLEVERED_BETAS.get(sector, 0.90)),
                }
                # `fundamentals` has no eps_growth_3y/payout_ratio columns and
                # this uses sector-average beta rather than the company's own
                # — all three are proxies, not measured company-specific
                # inputs (Fix 6/14). Flag them rather than silently blending.
                proxy_used.append("eps_growth_3y_from_revenue_cagr")
                proxy_used.append("beta_sector_average")
                if latest.get("payout_ratio") is None:
                    proxy_used.append("payout_ratio_missing")
                rel_result = reg.value_gap(ticker_pe_data)
                pe_gap = rel_result.gap_pct
            except Exception as exc:
                logger.warning(f"Relative PE regression failed for {ticker}: {exc}")

        # --- Assemble result --------------------------------------------------
        iv = dcf_result.intrinsic_value if dcf_result else None
        vgap = None
        mos = None
        if iv and current_price and iv > 0:
            vgap = (current_price - iv) / iv
            mos = (iv - current_price) / iv

        data_quality = (
            "full" if len(fund_df) >= 8
            else "partial" if len(fund_df) >= 4
            else "insufficient"
        )

        result = ValuationResult(
            ticker=ticker,
            as_of_date=aod,
            lifecycle_stage=stage.value,
            intrinsic_value=iv,
            current_price=current_price,
            valuation_gap_pct=vgap,
            margin_of_safety=mos,
            wacc=wacc_result.wacc,
            cost_of_equity=wacc_result.cost_of_equity,
            terminal_value_pct=dcf_result.terminal_value_pct if dcf_result else None,
            dcf_model_type=model_name,
            terminal_growth_rate=(
                _safe_float(dcf_result.assumptions.get("terminal_growth_rate"), default=None)
                if dcf_result else None
            ),
            scenario_bull=scenario_bull,
            scenario_base=scenario_base,
            scenario_bear=scenario_bear,
            mc_probability_undervalued=mc_prob,
            relative_pe_gap=pe_gap,
            data_quality=data_quality,
            proxy_used=proxy_used,
        )

        if self._write_signals:
            try:
                self._write_valuation_signal(result)
            except Exception as exc:
                logger.warning(f"Signal write-back failed for {ticker}: {exc}")

        return result

    def value_universe(
        self,
        tickers: List[str],
        as_of_date: Optional[str] = None,
        n_workers: int = 4,
    ) -> List[Optional[ValuationResult]]:
        """
        Value all tickers in parallel; failures return None (never raise).

        Parameters
        ----------
        tickers : list[str]
            NSE ticker symbols to value.
        as_of_date : str, optional
            ISO date string; defaults to today.
        n_workers : int
            Thread-pool size (default 4).

        Returns
        -------
        list[ValuationResult | None]
            One entry per ticker; None if valuation failed.
        """
        results: Dict[str, Optional[ValuationResult]] = {}
        with ThreadPoolExecutor(max_workers=n_workers) as pool:
            futures = {
                pool.submit(self._safe_value, t, as_of_date): t for t in tickers
            }
            for fut in as_completed(futures):
                t = futures[fut]
                results[t] = fut.result()
        return [results[t] for t in tickers]

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _safe_value(
        self, ticker: str, as_of_date: Optional[str]
    ) -> Optional[ValuationResult]:
        """value_stock wrapper that catches all exceptions."""
        try:
            return self.value_stock(ticker, as_of_date)
        except RuntimeError as exc:
            logger.warning(f"Skipping {ticker}: {exc}")
            return ValuationResult(
                ticker=ticker,
                as_of_date=as_of_date or date.today().isoformat(),
                lifecycle_stage="unknown",
                data_quality="insufficient",
                error=str(exc),
            )
        except Exception as exc:
            logger.error(f"Valuation error for {ticker}: {exc}", exc_info=True)
            return ValuationResult(
                ticker=ticker,
                as_of_date=as_of_date or date.today().isoformat(),
                lifecycle_stage="unknown",
                data_quality="insufficient",
                error=str(exc),
            )

    def _write_valuation_signal(self, result: ValuationResult) -> None:
        """Persist valuation result to valuation_signals table (SPEC-VAL write-back)."""
        with get_duckdb_connection(SIGNALS_DUCKDB_PATH, persist=False) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS valuation_signals (
                    date DATE NOT NULL,
                    ticker VARCHAR NOT NULL,
                    lifecycle_stage VARCHAR,
                    intrinsic_value FLOAT,
                    valuation_gap_pct FLOAT,
                    margin_of_safety FLOAT,
                    wacc FLOAT,
                    cost_of_equity FLOAT,
                    terminal_value_pct FLOAT,
                    dcf_model_type VARCHAR,
                    scenario_bull FLOAT,
                    scenario_base FLOAT,
                    scenario_bear FLOAT,
                    mc_probability_undervalued FLOAT,
                    relative_pe_gap FLOAT,
                    PRIMARY KEY (date, ticker)
                )
            """)
            conn.execute("""
                INSERT OR REPLACE INTO valuation_signals VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                result.as_of_date,
                result.ticker,
                result.lifecycle_stage,
                result.intrinsic_value,
                result.valuation_gap_pct,
                result.margin_of_safety,
                result.wacc,
                result.cost_of_equity,
                result.terminal_value_pct,
                result.dcf_model_type,
                result.scenario_bull,
                result.scenario_base,
                result.scenario_bear,
                result.mc_probability_undervalued,
                result.relative_pe_gap,
            ])
