"""
features/fundamental.py

Phase: 2.1 (Fundamental Data Ingestion + PIT Validation)
Specs: SPEC-FEAT-002, SPEC-PIPE-003 (CRITICAL), SPEC-PIPE-004, SPEC-SOLID-005
Owner: Platform / Features
Consumers: features/matrix_builder (wired in P2.3), systems/ml_signal_engine

Computes the fundamental feature set enumerated in this phase's build
prompt (CLAUDE_CODE_PROMPTS.md P2.1): Growth(6) + Profitability(6) +
Capital efficiency(4) + Leverage(4) + Working capital(4) + Valuation(3) +
Staleness(3, MANDATORY) = 30 named features — not the "28" in the same
prompt's summary line. Same per-category-vs-header count mismatch already
flagged and resolved the same way (trust the explicit enumeration, not
the header arithmetic) for features/technical.py (P1.1, BuildLog.md) and
features/pnd_features.py. 01_features.md's older "Core Fundamental
Features (28)" list uses different field names for several of the same
ideas (e.g. revenue_growth_3yr_cagr vs. this prompt's revenue_cagr_3yr,
peg_ratio/price_to_book/mcap_to_sales vs. pb_ratio/current_ratio) — the
P2.1 build prompt's literal list is implemented here, per this project's
established precedent that the prompt text in CLAUDE_CODE_PROMPTS.md
governs over older reference docs when the two diverge (see BuildLog.md
"P1.2 addendum").

SPEC-PIPE-003 (CRITICAL): every row this module consumes comes from
DataStoreClient.get_fundamentals_history(), which is already PIT-filtered
server-side (announcement_date <= as_of, datastore/api/routers/
fundamentals.py). This module performs NO date filtering of its own — it
only *sequences* among already-PIT-eligible rows (e.g. "the quarter
immediately before the latest one") using quarter_end_date as the
chronological sort key. That is a different operation from PIT
*availability* filtering, which SPEC-PIPE-003 forbids keying off
quarter_end_date for — sequencing already-known historical quarters does
not introduce look-ahead bias, since every row involved was already
established as observable-by-as_of before this module ever sees it.

Several Phase 2 ratios (gross_margin, capex_intensity, roic,
net_debt_to_ebitda) need raw line items the original P0.2 `fundamentals`
schema didn't carry (gross_profit, capex, current_assets,
current_liabilities, total_debt, cash_and_equivalents) — added to the
table in this same phase (datastore/schema/create_normalised.py, safe
in-place addition since the table has had zero rows since P0.2). roic
uses operating_margin * revenue as an EBIT proxy and a flat assumed
effective tax rate (config.settings.ASSUMED_TAX_RATE) — Screener.in does
not expose a clean reported-EBIT or effective-tax-rate line item, so this
is a documented approximation, not exact GAAP ROIC.
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import httpx
import numpy as np
import pandas as pd

from config.settings import ASSUMED_TAX_RATE, RESULTS_PENDING_THRESHOLD_DAYS, Z_SCORE_CLIP
from datastore.client import DataStoreClient

logger = logging.getLogger(__name__)

GROWTH_FEATURES = [
    "revenue_growth_yoy", "revenue_growth_qoq", "pat_growth_yoy",
    "eps_growth_yoy", "ebitda_growth_yoy", "revenue_cagr_3yr",
]
PROFITABILITY_FEATURES = [
    "gross_margin", "operating_margin", "ebitda_margin", "net_margin", "roe", "roce",
]
CAPITAL_EFFICIENCY_FEATURES = ["asset_turnover", "capex_intensity", "fcf_conversion", "roic"]
LEVERAGE_FEATURES = ["debt_to_equity", "interest_coverage", "net_debt_to_ebitda", "current_ratio"]
WORKING_CAPITAL_FEATURES = ["inventory_days", "receivable_days", "payable_days", "cash_conversion_cycle"]
VALUATION_FEATURES = ["pe_ratio", "pb_ratio", "ev_to_ebitda"]
# SPEC-PIPE-003 (MANDATORY): always computed, never normalized (binary/bounded, not a ratio)
STALENESS_FEATURES = ["days_since_results", "quarter_age_pct", "results_pending_flag"]
# Value/quality composites (Piotroski-on-Value, Magic Formula, Quality-Value
# Composite, FCF Yield + Low Debt, GARP) need a few extra ratios that aren't
# covered by the P2.1 feature list above. All are pure arithmetic over
# columns already computed in this function — no new raw data ingestion.
# `net_working_capital`/`net_fixed_assets` are not stored anywhere; the
# same formulas already used ad-hoc elsewhere in the codebase
# (current_assets - current_liabilities in features/deep_forensic.py's
# Altman Z X1 term; property_plant_equipment + cwip as a fixed-assets
# proxy) are reused here rather than re-derived independently.
VALUE_QUALITY_FEATURES = [
    "ev_ebit_yield", "fcf_ev_yield", "magic_formula_roc", "book_to_market", "cfo_to_pat",
]

# Second wave of strategies (QGLP, Moat, Longevity, Turnaround, Earnings
# Re-rating, Contrarian Recovery, Capital Allocation Quality, Sector-Leader
# Compounders, etc.) need multi-year rolling stats and 1-year deltas that
# aren't in either feature set above. All are pure arithmetic over
# already-fetched history rows (same _find_quarter/_quarters_back helpers
# used for the existing 3yr revenue CAGR) — no new raw data ingestion,
# just a longer lookback window (see lookback_years default bump below).
MULTIYEAR_FEATURES = [
    "avg_roce_5y", "margin_stability_5y", "earnings_volatility_5y", "sales_cagr_5y", "delta_roce_3y",
    "avg_ebitda_margin_5y",
]
DELTA_1Y_FEATURES = [
    "eps_acceleration", "margin_expansion", "delta_roa_1y", "delta_current_ratio_1y",
    "delta_long_term_debt_to_assets_1y", "delta_operating_cash_flow_1y",
    "receivable_days_change", "inventory_days_change",
]
# company_age_years needs listing_date (stock_master, not the fundamentals
# history rows this module otherwise relies on exclusively) — passed in by
# the caller as an optional pre-fetched value, same pattern as
# ticker_ohlcv, rather than this module making its own API call.
SIZE_AGE_FEATURES = ["company_age_years", "dilution_3y", "market_cap"]
CAPITAL_ALLOCATION_FEATURES = ["reinvestment_rate", "capital_allocation_efficiency"]

# SPEC-FEAT-002: only the ratio-style features are sector-relative z-scored —
# staleness flags are deliberately excluded (a 0/1 flag or a [0,1]-bounded
# pct has no meaningful "sector mean", and z-scoring it would only smear
# its meaning).
RATIO_FEATURES = (
    GROWTH_FEATURES + PROFITABILITY_FEATURES + CAPITAL_EFFICIENCY_FEATURES
    + LEVERAGE_FEATURES + WORKING_CAPITAL_FEATURES + VALUATION_FEATURES + VALUE_QUALITY_FEATURES
    + MULTIYEAR_FEATURES + DELTA_1Y_FEATURES + SIZE_AGE_FEATURES + CAPITAL_ALLOCATION_FEATURES
)
FUNDAMENTAL_FEATURES: List[str] = RATIO_FEATURES + STALENESS_FEATURES  # 54 total

# [2026-07-28] features/fundamental_cache.py's event-driven cache split.
# These 7 divide a raw quarterly number by today's price/enterprise value
# (via close/market_cap) — the only RATIO_FEATURES that genuinely need
# recomputing every trading day. Everything else in RATIO_FEATURES is a
# pure function of the latest PIT-eligible quarter's raw financials and is
# byte-identical every day until that ticker's announcement_date advances
# (see compute_fundamental_features_panel's raw_cache parameter).
PRICE_DEPENDENT_FEATURES = [
    "pe_ratio", "pb_ratio", "ev_to_ebitda", "ev_ebit_yield", "fcf_ev_yield", "book_to_market", "market_cap",
]
CACHEABLE_RATIO_FEATURES = [f for f in RATIO_FEATURES if f not in PRICE_DEPENDENT_FEATURES]

# Unit convention (ingestion/scrapers/screener.py's module docstring): every
# monetary `fundamentals` column is in RUPEE CRORE; book_value_per_share/
# close are raw rupees-per-share and shares_outstanding is a raw share
# count, so price x shares must be divided by CRORE before mixing with any
# fundamentals-table column. Module-level (was a local inside
# compute_fundamental_features) so _priced_inputs_from_row/
# _compute_priced_features below share the exact same constant.
CRORE = 1e7


def _safe_col(row: Optional["pd.Series"], col: str) -> float:
    """
    Safe column lookup: NaN if `row` is None, the column is absent
    entirely, or present-but-null. `pd.Series.get(col, default)` only
    substitutes `default` when `col` is missing from the index — a
    present-but-None value (the normal case for any optional fundamentals
    field, e.g. screener.py never populates cash_and_equivalents) is
    returned as-is, not the default. A real bug here
    (`latest.get("total_debt", 0.0)` silently returning None instead of
    0.0) crashed ev_to_ebitda's arithmetic the first time this function
    was exercised against data with any None field — see BuildLog.md "P2.1".
    """
    if row is None or col not in row.index:
        return np.nan
    val = row[col]
    return np.nan if val is None or pd.isna(val) else val


def _priced_inputs_from_row(latest_row: "pd.Series") -> Dict[str, float]:
    """
    The raw per-quarter inputs PRICE_DEPENDENT_FEATURES need, extracted
    from one fundamentals row — cacheable alongside CACHEABLE_RATIO_FEATURES
    since none of these change until the next quarter either. Shared by
    both the cold-compute path (compute_fundamental_features) and the
    cache-hit fast path (compute_fundamental_features_panel) so the two
    can never diverge.
    """
    bvps = _safe_col(latest_row, "book_value_per_share")
    shares = _safe_col(latest_row, "shares_outstanding")
    equity = (bvps * shares) / CRORE if pd.notna(bvps) and pd.notna(shares) else np.nan
    return {
        "shares": shares,
        "total_debt": _safe_col(latest_row, "total_debt"),
        "cash": _safe_col(latest_row, "cash_and_equivalents"),
        "eps": _safe_col(latest_row, "eps"),
        "book_value_per_share": bvps,
        "ebitda": _safe_col(latest_row, "ebitda"),
        "ebit": _safe_col(latest_row, "ebit"),
        "fcf": _safe_col(latest_row, "fcf"),
        "equity": equity,
    }


def _compute_priced_features(priced_inputs: Dict[str, float], close: Optional[float]) -> Dict[str, float]:
    """The 7 PRICE_DEPENDENT_FEATURES, from cached per-quarter inputs + today's close."""
    shares = priced_inputs["shares"]
    market_cap = (close * shares) / CRORE if close is not None and pd.notna(shares) else np.nan
    total_debt_v, cash_v = priced_inputs["total_debt"], priced_inputs["cash"]
    enterprise_value = (
        market_cap + (total_debt_v if pd.notna(total_debt_v) else 0.0)
        - (cash_v if pd.notna(cash_v) else 0.0)
    ) if pd.notna(market_cap) else np.nan
    return {
        "pe_ratio": _safe_div(close, priced_inputs["eps"]) if close is not None else np.nan,
        "pb_ratio": _safe_div(close, priced_inputs["book_value_per_share"]) if close is not None else np.nan,
        "ev_to_ebitda": _safe_div(enterprise_value, priced_inputs["ebitda"]),
        "ev_ebit_yield": _safe_div(priced_inputs["ebit"], enterprise_value),
        "fcf_ev_yield": _safe_div(priced_inputs["fcf"], enterprise_value),
        "book_to_market": _safe_div(priced_inputs["equity"], market_cap),
        "market_cap": market_cap,
    }


def _resolve_latest_quarter_row(rows: "Optional[List]") -> "Optional[pd.Series]":
    """
    Cheap peek at which quarter is PIT-eligible-latest — a sort + iloc[-1],
    none of the multi-year rolling-window walks compute_fundamental_features
    does afterward. Used to build the fundamental_cache key (and, on a
    cache miss, to extract priced inputs) without paying for the expensive
    part twice.
    """
    if not rows:
        return None
    history = pd.DataFrame(rows)
    history["quarter_end_date"] = pd.to_datetime(history["quarter_end_date"])
    return history.sort_values("quarter_end_date").iloc[-1]


def compute_staleness(announcement_date: datetime, current_date: datetime) -> Dict[str, float]:
    """
    SPEC-PIPE-003: staleness features derived purely from announcement_date.

    Parameters
    ----------
    announcement_date : datetime
        The most recent PIT-eligible quarter's announcement_date.
    current_date : datetime
        The feature computation date (as_of).

    Returns
    -------
    dict
        days_since_results, quarter_age_pct (clipped to [0, 1]),
        results_pending_flag (1 if overdue past the threshold).

    Spec References
    ----------------
    SPEC-PIPE-003: "Staleness features always computed: days_since_results,
    quarter_age_pct, results_pending_flag."

    Raises
    ------
    ValueError
        If current_date is before announcement_date (would imply
        look-ahead — the caller passed a row that wasn't PIT-eligible).
    """
    days = (current_date - announcement_date).days
    if days < 0:
        raise ValueError(
            f"current_date ({current_date}) is before announcement_date ({announcement_date}) — "
            "this row was not PIT-eligible as of current_date"
        )
    return {
        "days_since_results": float(days),
        "quarter_age_pct": min(days / 63.0, 1.0),
        "results_pending_flag": int(days > RESULTS_PENDING_THRESHOLD_DAYS),
    }


def _safe_growth(current: Optional[float], prior: Optional[float]) -> float:
    """(current - prior) / abs(prior); NaN if prior is missing or zero."""
    if current is None or prior is None or pd.isna(current) or pd.isna(prior) or prior == 0:
        return np.nan
    return (current - prior) / abs(prior)


def _safe_div(numerator: Optional[float], denominator: Optional[float]) -> float:
    if numerator is None or denominator is None or pd.isna(numerator) or pd.isna(denominator) or denominator == 0:
        return np.nan
    return numerator / denominator


def _find_quarter(history: pd.DataFrame, fiscal_year: int, quarter: int) -> Optional[pd.Series]:
    match = history[(history["fiscal_year"] == fiscal_year) & (history["quarter"] == quarter)]
    return match.iloc[0] if len(match) else None


def _quarters_back(fiscal_year: int, quarter: int, n: int) -> tuple:
    """Walk back n quarters from (fiscal_year, quarter), wrapping across fiscal years."""
    total = (fiscal_year * 4 + (quarter - 1)) - n
    return total // 4, (total % 4) + 1


def _latest_close_on_or_before(
    client: DataStoreClient,
    ticker: str,
    as_of: datetime,
    ticker_ohlcv: "Optional[pd.DataFrame]" = None,
) -> Optional[float]:
    """Most recent OHLCV close with date <= as_of (PIT-safe).

    When ticker_ohlcv (a pre-fetched per-ticker DataFrame slice of the bulk
    panel) is provided, no API call is made — the data is already in memory.
    """
    if ticker_ohlcv is not None and not ticker_ohlcv.empty:
        window = ticker_ohlcv[ticker_ohlcv["date"] <= pd.Timestamp(as_of)]
        if window.empty:
            return None
        return float(window.sort_values("date").iloc[-1]["close"])
    rows = client.get_ohlcv(ticker, from_date=as_of - timedelta(days=14), to_date=as_of)
    if not rows:
        return None
    return sorted(rows, key=lambda r: r["date"])[-1]["close"]


def compute_fundamental_features(
    client: DataStoreClient,
    ticker: str,
    as_of: datetime,
    lookback_years: int = 6,
    pre_loaded_rows: "Optional[List]" = None,
    ticker_ohlcv: "Optional[pd.DataFrame]" = None,
    listing_date: "Optional[datetime]" = None,
) -> Dict[str, Any]:
    """
    Compute all 30 raw (not yet sector-z-scored) fundamental features for one ticker.

    Parameters
    ----------
    client : DataStoreClient
        SPEC-DS-002: all fundamentals/OHLCV access goes through the API.
    ticker : str
    as_of : datetime
        PIT reference date.
    lookback_years : int
        History window requested from the API — default 6 to cover the
        5-year rolling stats' 20-quarter lookback plus slack (bumped from
        4, which only covered the 3-year CAGR's 12 quarters).
    listing_date : datetime, optional
        stock_master.listing_date, pre-fetched by the caller (same pattern
        as ticker_ohlcv) — used only for company_age_years. NaN if omitted.

    Returns
    -------
    dict
        feature_name -> value for all 54 FUNDAMENTAL_FEATURES (30 P2.1 +
        5 value/quality + 19 multi-year/delta/size/capital-allocation
        features added for the second wave of strategies), or all-NaN
        with results_pending_flag=1 if no PIT-eligible quarter exists yet.

    Spec References
    ----------------
    SPEC-PIPE-003 (CRITICAL), SPEC-FEAT-002.

    PIT Assumptions
    ----------------
    Trusts DataStoreClient.get_fundamentals_history()'s server-side PIT
    filter entirely; never re-derives availability from quarter_end_date.

    Raises
    ------
    None — missing/insufficient history degrades to NaN features, not an exception.
    """
    rows = pre_loaded_rows if pre_loaded_rows is not None else client.get_fundamentals_history(
        ticker, as_of, lookback_years=lookback_years
    )
    if not rows:
        result = {f: np.nan for f in FUNDAMENTAL_FEATURES}
        result["results_pending_flag"] = 1
        result["days_since_results"] = np.nan
        result["quarter_age_pct"] = np.nan
        return result

    history = pd.DataFrame(rows)
    history["quarter_end_date"] = pd.to_datetime(history["quarter_end_date"])
    history["announcement_date"] = pd.to_datetime(history["announcement_date"])
    history = history.sort_values("quarter_end_date").reset_index(drop=True)

    latest = history.iloc[-1]
    fy, q = int(latest["fiscal_year"]), int(latest["quarter"])

    qoq_fy, qoq_q = _quarters_back(fy, q, 1)
    yoy_fy, yoy_q = _quarters_back(fy, q, 4)
    cagr_fy, cagr_q = _quarters_back(fy, q, 12)

    qoq_prior = _find_quarter(history, qoq_fy, qoq_q)
    yoy_prior = _find_quarter(history, yoy_fy, yoy_q)
    cagr_base = _find_quarter(history, cagr_fy, cagr_q)

    v = _safe_col  # module-level (features/fundamental_cache.py's cache-hit path reuses it too)

    revenue_cagr_3yr = np.nan
    if cagr_base is not None and pd.notna(v(cagr_base, "revenue")) and v(cagr_base, "revenue") > 0 \
            and pd.notna(v(latest, "revenue")) and v(latest, "revenue") > 0:
        revenue_cagr_3yr = (v(latest, "revenue") / v(cagr_base, "revenue")) ** (1.0 / 3.0) - 1.0

    # Unit convention (ingestion/scrapers/screener.py's module docstring):
    # every monetary `fundamentals` column (revenue, ebitda, total_debt,
    # cash_and_equivalents, ...) is in RUPEE CRORE. book_value_per_share/
    # close are raw rupees-per-share and shares_outstanding is a raw share
    # count, so price x shares must be divided by CRORE to land in the
    # same unit before mixing with any fundamentals-table column — a real
    # bug caught here (and independently in screener.py's debt_to_equity)
    # before either was ever exercised against real data; see BuildLog.md "P2.1".
    # priced_inputs/priced (7 PRICE_DEPENDENT_FEATURES) split out to
    # module-level helpers — features/fundamental_cache.py's cache-hit path
    # in compute_fundamental_features_panel reuses these exact same
    # functions so a cached day and a freshly-computed day can never diverge.
    priced_inputs = _priced_inputs_from_row(latest)
    equity = priced_inputs["equity"]
    invested_capital = np.nan
    if pd.notna(equity):
        total_debt = priced_inputs["total_debt"] if pd.notna(priced_inputs["total_debt"]) else 0.0
        cash = priced_inputs["cash"] if pd.notna(priced_inputs["cash"]) else 0.0
        invested_capital = equity + total_debt - cash
    op_margin, revenue = v(latest, "operating_margin"), v(latest, "revenue")
    ebit_proxy = op_margin * revenue if pd.notna(op_margin) and pd.notna(revenue) else np.nan
    nopat = ebit_proxy * (1.0 - ASSUMED_TAX_RATE) if pd.notna(ebit_proxy) else np.nan

    close = _latest_close_on_or_before(client, ticker, as_of, ticker_ohlcv=ticker_ohlcv)
    priced = _compute_priced_features(priced_inputs, close)
    market_cap = priced["market_cap"]

    total_debt_v, cash_v = priced_inputs["total_debt"], priced_inputs["cash"]
    inv_days, rec_days, pay_days = v(latest, "inventory_days"), v(latest, "receivable_days"), v(latest, "payable_days")

    ca, cl = v(latest, "current_assets"), v(latest, "current_liabilities")
    net_working_capital = (ca - cl) if pd.notna(ca) and pd.notna(cl) else np.nan
    ppe, cwip = v(latest, "property_plant_equipment"), v(latest, "cwip")
    net_fixed_assets = (
        (ppe if pd.notna(ppe) else 0.0) + (cwip if pd.notna(cwip) else 0.0)
        if pd.notna(ppe) or pd.notna(cwip) else np.nan
    )
    # cfo_proxy = fcf + capex is an APPROXIMATION of operating cash flow,
    # not an algebraic identity: `fcf` is a raw value from the upstream
    # source (Trendlyne/NSE XBRL — ingestion/scrapers/screener.py does NOT
    # compute it), not derived in this codebase as cfo - capex. If the
    # source's FCF used a different capex figure/period than our own
    # `capex` column, this will diverge from the company's actually
    # reported operating cash flow. [2026-07-25 model-review correction —
    # this comment previously implied exactness it doesn't have.]
    cfo_proxy = (
        v(latest, "fcf") + v(latest, "capex")
        if pd.notna(v(latest, "fcf")) and pd.notna(v(latest, "capex")) else np.nan
    )

    # ---- Multi-year rolling stats (5-year window = 20 quarters back) ----
    # [KNOWN DATA ARTIFACT, 2026-07-28 model-review, currently unaddressed]
    # Ind-AS 116 (lease capitalization) became effective for Indian
    # reporting periods starting FY2019-20 — from that date on, operating
    # leases that previously stayed off-balance-sheet suddenly appear as a
    # right-of-use asset + a matching lease liability. Any 5-year rolling
    # window here (avg_roce_5y, margin_stability_5y, and by extension
    # delta_roce_3y/earnings_volatility_5y below) that straddles FY2019-20
    # for a company with material operating leases will show a real but
    # SPURIOUS regime break: ROCE drops (denominator gains a liability
    # that wasn't there before) and margin/earnings series can jump
    # (interest + depreciation replaces a single lease-rental expense
    # line) purely from the accounting standard change, not from any
    # change in the underlying business. A future reviewer diffing
    # pre-/post-2019 values for an early-2020s-and-earlier window should
    # not mistake this for genuine noise or a real deterioration/
    # improvement — no accounting adjustment is attempted here; this is
    # deliberately just documentation of a known, currently-unmitigated
    # limitation of these features for any window spanning that boundary.
    five_yr_fy, five_yr_q = _quarters_back(fy, q, 20)
    five_yr_window = history[
        (history["fiscal_year"] > five_yr_fy) | ((history["fiscal_year"] == five_yr_fy) & (history["quarter"] >= five_yr_q))
    ]
    five_yr_base = _find_quarter(history, five_yr_fy, five_yr_q)
    sales_cagr_5y = np.nan
    if five_yr_base is not None and pd.notna(v(five_yr_base, "revenue")) and v(five_yr_base, "revenue") > 0 \
            and pd.notna(v(latest, "revenue")) and v(latest, "revenue") > 0:
        sales_cagr_5y = (v(latest, "revenue") / v(five_yr_base, "revenue")) ** (1.0 / 5.0) - 1.0

    roce_series = pd.to_numeric(five_yr_window["roce"], errors="coerce").dropna() if "roce" in five_yr_window.columns else pd.Series(dtype=float)
    avg_roce_5y = float(roce_series.mean()) if len(roce_series) else np.nan
    margin_series = (
        pd.to_numeric(five_yr_window["ebitda_margin"], errors="coerce").dropna()
        if "ebitda_margin" in five_yr_window.columns else pd.Series(dtype=float)
    )
    margin_stability_5y = float(-margin_series.std()) if len(margin_series) >= 2 else np.nan
    # Used by Normalization Value's normalized_ebit — an ebit_margin-over-
    # full-cycle series isn't tracked separately from ebitda_margin, so the
    # already-computed 5yr ebitda_margin window doubles as the cycle-average
    # margin proxy (documented approximation, same tradeoff as roic's proxy above).
    avg_ebitda_margin_5y = float(margin_series.mean()) if len(margin_series) else np.nan
    eps_series = (
        pd.to_numeric(five_yr_window["eps"], errors="coerce").dropna()
        if "eps" in five_yr_window.columns else pd.Series(dtype=float)
    )
    # Coefficient of variation (stdev / mean|eps|) as an earnings-volatility
    # proxy — computing a true stdev-of-YoY-growth series would need a
    # second nested lookback per quarter; this is the same tradeoff
    # documented for roic's flat-tax-rate approximation above.
    earnings_volatility_5y = (
        float(eps_series.std() / abs(eps_series.mean())) if len(eps_series) >= 2 and eps_series.mean() != 0 else np.nan
    )

    three_yr_fy, three_yr_q = _quarters_back(fy, q, 12)
    three_yr_base = _find_quarter(history, three_yr_fy, three_yr_q)
    delta_roce_3y = (
        v(latest, "roce") - v(three_yr_base, "roce")
        if pd.notna(v(latest, "roce")) and pd.notna(v(three_yr_base, "roce")) else np.nan
    )

    # ---- 1-year deltas (need one more quarter back from yoy_prior) ----
    two_yr_fy, two_yr_q = _quarters_back(fy, q, 8)
    two_yr_prior = _find_quarter(history, two_yr_fy, two_yr_q)
    eps_growth_yoy = _safe_growth(v(latest, "eps"), v(yoy_prior, "eps"))
    eps_growth_yoy_prior = _safe_growth(v(yoy_prior, "eps"), v(two_yr_prior, "eps"))
    eps_acceleration = (
        eps_growth_yoy - eps_growth_yoy_prior
        if pd.notna(eps_growth_yoy) and pd.notna(eps_growth_yoy_prior) else np.nan
    )
    margin_expansion = (
        v(latest, "ebitda_margin") - v(yoy_prior, "ebitda_margin")
        if pd.notna(v(latest, "ebitda_margin")) and pd.notna(v(yoy_prior, "ebitda_margin")) else np.nan
    )
    roa_t = _safe_div(v(latest, "pat"), v(latest, "total_assets"))
    roa_yoy = _safe_div(v(yoy_prior, "pat"), v(yoy_prior, "total_assets"))
    delta_roa_1y = roa_t - roa_yoy if pd.notna(roa_t) and pd.notna(roa_yoy) else np.nan
    cr_t = _safe_div(v(latest, "current_assets"), v(latest, "current_liabilities"))
    cr_yoy = _safe_div(v(yoy_prior, "current_assets"), v(yoy_prior, "current_liabilities"))
    delta_current_ratio_1y = cr_t - cr_yoy if pd.notna(cr_t) and pd.notna(cr_yoy) else np.nan
    ltd_ta_t = _safe_div(v(latest, "borrowings_noncurrent"), v(latest, "total_assets"))
    ltd_ta_yoy = _safe_div(v(yoy_prior, "borrowings_noncurrent"), v(yoy_prior, "total_assets"))
    delta_long_term_debt_to_assets_1y = (
        ltd_ta_t - ltd_ta_yoy if pd.notna(ltd_ta_t) and pd.notna(ltd_ta_yoy) else np.nan
    )
    cfo_proxy_yoy = (
        v(yoy_prior, "fcf") + v(yoy_prior, "capex")
        if pd.notna(v(yoy_prior, "fcf")) and pd.notna(v(yoy_prior, "capex")) else np.nan
    )
    delta_operating_cash_flow_1y = _safe_growth(cfo_proxy, cfo_proxy_yoy)
    receivable_days_change = (
        v(latest, "receivable_days") - v(yoy_prior, "receivable_days")
        if pd.notna(v(latest, "receivable_days")) and pd.notna(v(yoy_prior, "receivable_days")) else np.nan
    )
    inventory_days_change = (
        v(latest, "inventory_days") - v(yoy_prior, "inventory_days")
        if pd.notna(v(latest, "inventory_days")) and pd.notna(v(yoy_prior, "inventory_days")) else np.nan
    )

    # ---- Size/age, reinvestment/capital-allocation ----
    company_age_years = (
        (as_of - listing_date).days / 365.25 if listing_date is not None else np.nan
    )
    dilution_3y = _safe_growth(v(latest, "shares_outstanding"), v(three_yr_base, "shares_outstanding"))
    reinvestment_rate = _safe_div(v(latest, "capex"), cfo_proxy)
    delta_ebit_1y = (
        v(latest, "ebit") - v(yoy_prior, "ebit")
        if pd.notna(v(latest, "ebit")) and pd.notna(v(yoy_prior, "ebit")) else np.nan
    )
    capital_allocation_efficiency = _safe_div(delta_ebit_1y, v(three_yr_base, "retained_earnings"))

    features: Dict[str, Any] = {
        # Growth (6)
        "revenue_growth_yoy": _safe_growth(v(latest, "revenue"), v(yoy_prior, "revenue")),
        "revenue_growth_qoq": _safe_growth(v(latest, "revenue"), v(qoq_prior, "revenue")),
        "pat_growth_yoy": _safe_growth(v(latest, "pat"), v(yoy_prior, "pat")),
        "eps_growth_yoy": _safe_growth(v(latest, "eps"), v(yoy_prior, "eps")),
        "ebitda_growth_yoy": _safe_growth(v(latest, "ebitda"), v(yoy_prior, "ebitda")),
        "revenue_cagr_3yr": revenue_cagr_3yr,
        # Profitability (6)
        "gross_margin": _safe_div(v(latest, "gross_profit"), v(latest, "revenue")),
        "operating_margin": v(latest, "operating_margin"),
        "ebitda_margin": v(latest, "ebitda_margin"),
        "net_margin": v(latest, "net_margin"),
        "roe": v(latest, "roe"),
        "roce": v(latest, "roce"),
        # Capital efficiency (4)
        "asset_turnover": v(latest, "asset_turnover"),
        "capex_intensity": _safe_div(v(latest, "capex"), v(latest, "revenue")),
        "fcf_conversion": _safe_div(v(latest, "fcf"), v(latest, "pat")),
        "roic": _safe_div(nopat, invested_capital),
        # Leverage (4)
        "debt_to_equity": v(latest, "debt_to_equity"),
        "interest_coverage": v(latest, "interest_coverage"),
        "net_debt_to_ebitda": _safe_div(
            (total_debt_v - cash_v) if pd.notna(total_debt_v) and pd.notna(cash_v) else np.nan,
            v(latest, "ebitda"),
        ),
        "current_ratio": _safe_div(v(latest, "current_assets"), v(latest, "current_liabilities")),
        # Working capital (4)
        "inventory_days": inv_days,
        "receivable_days": rec_days,
        "payable_days": pay_days,
        "cash_conversion_cycle": (
            inv_days + rec_days - pay_days
            if pd.notna(inv_days) and pd.notna(rec_days) and pd.notna(pay_days) else np.nan
        ),
        # Valuation (3) + value/quality (Piotroski-on-Value, Magic Formula,
        # Quality-Value Composite, FCF Yield + Low Debt, GARP) — the 7
        # PRICE_DEPENDENT_FEATURES, from the shared _compute_priced_features
        # helper (same one the cache-hit path in
        # compute_fundamental_features_panel calls).
        "pe_ratio": priced["pe_ratio"],
        "pb_ratio": priced["pb_ratio"],
        "ev_to_ebitda": priced["ev_to_ebitda"],
        "ev_ebit_yield": priced["ev_ebit_yield"],
        "fcf_ev_yield": priced["fcf_ev_yield"],
        "magic_formula_roc": _safe_div(
            v(latest, "ebit"),
            (net_working_capital + net_fixed_assets)
            if pd.notna(net_working_capital) and pd.notna(net_fixed_assets) else np.nan,
        ),
        "book_to_market": priced["book_to_market"],
        "cfo_to_pat": _safe_div(cfo_proxy, v(latest, "pat")),
        # Multi-year rolling stats (QGLP, Moat, Longevity, Sector-Leader,
        # Capital Allocation Quality).
        "avg_roce_5y": avg_roce_5y,
        "margin_stability_5y": margin_stability_5y,
        "earnings_volatility_5y": earnings_volatility_5y,
        "sales_cagr_5y": sales_cagr_5y,
        "delta_roce_3y": delta_roce_3y,
        "avg_ebitda_margin_5y": avg_ebitda_margin_5y,
        # 1-year deltas (Turnaround, Earnings Re-rating, Contrarian Recovery,
        # Story+Numbers Confirmation).
        "eps_acceleration": eps_acceleration,
        "margin_expansion": margin_expansion,
        "delta_roa_1y": delta_roa_1y,
        "delta_current_ratio_1y": delta_current_ratio_1y,
        "delta_long_term_debt_to_assets_1y": delta_long_term_debt_to_assets_1y,
        "delta_operating_cash_flow_1y": delta_operating_cash_flow_1y,
        "receivable_days_change": receivable_days_change,
        "inventory_days_change": inventory_days_change,
        # Size/age, reinvestment/capital allocation.
        "company_age_years": company_age_years,
        "dilution_3y": dilution_3y,
        "market_cap": market_cap,
        "reinvestment_rate": reinvestment_rate,
        "capital_allocation_efficiency": capital_allocation_efficiency,
    }
    features.update(compute_staleness(latest["announcement_date"].to_pydatetime(), as_of))
    return features


def _sector_relative_zscore(df: pd.DataFrame, columns: List[str], sector_col: str = "sector") -> pd.DataFrame:
    """
    SPEC-FEAT-002: z = (x - sector_mean) / (sector_std + 1e-8), clipped to [-5, +5].

    Parameters
    ----------
    df : pd.DataFrame
        Must contain `sector_col` and every column in `columns`.
    columns : list of str
        Columns to normalize in place (returned as new columns, same names).
    sector_col : str

    Returns
    -------
    pd.DataFrame
        Copy of df with `columns` replaced by their sector-relative z-scores.
    """
    out = df.copy()
    for col in columns:
        sector_mean = out.groupby(sector_col)[col].transform("mean")
        sector_std = out.groupby(sector_col)[col].transform("std")
        z = (out[col] - sector_mean) / (sector_std + 1e-8)
        out[col] = z.clip(lower=-Z_SCORE_CLIP, upper=Z_SCORE_CLIP)
    return out


def compute_fundamental_features_panel(
    client: DataStoreClient,
    tickers: List[str],
    as_of: datetime,
    sector_map: Dict[str, str],
    data_cache=None,
    ohlcv_panel: "Optional[pd.DataFrame]" = None,
    listing_date_map: "Optional[Dict[str, datetime]]" = None,
    raw_cache: "Optional[Dict[Tuple[str, int, int], Dict[str, Any]]]" = None,
    cache_misses_out: "Optional[Dict[Tuple[str, int, int], Dict[str, Any]]]" = None,
) -> pd.DataFrame:
    """
    Compute the full 54-feature fundamental panel for many tickers, with
    the 51 ratio features sector-relative z-scored (SPEC-FEAT-002).

    Parameters
    ----------
    client : DataStoreClient
    tickers : list of str
    as_of : datetime
        PIT reference date, shared across the whole panel.
    sector_map : dict
        ticker -> sector (e.g. from config.universe.load_universe()).
        Tickers with no sector mapping fall into a single "UNKNOWN" group.
    raw_cache : dict, optional
        [2026-07-28] features/fundamental_cache.py's event-driven cache —
        {(ticker, fiscal_year, quarter): {"ratios": ..., "priced_inputs":
        ..., "announcement_date": iso_str}}, typically
        load_fundamental_raw_cache()'s full return value, loaded ONCE per
        process and passed to every date's panel build. None (default)
        preserves the original always-recompute behavior exactly — this
        is purely opt-in, so retrain_phase2.py (the other caller, which
        doesn't pass this) is completely unaffected.

        When provided: for each ticker, cheaply peeks at which quarter is
        PIT-eligible-latest (a sort, not the full multi-year rolling-window
        walk) to build a cache key. A hit skips compute_fundamental_features
        entirely — CACHEABLE_RATIO_FEATURES come straight from the cache,
        PRICE_DEPENDENT_FEATURES are recomputed fresh from cached
        priced_inputs + today's close (still genuinely daily). A miss runs
        the full computation as before and adds the new entry to `raw_cache`
        in place (so later tickers in the same call, and later dates in the
        same process, see it immediately).
    cache_misses_out : dict, optional
        If provided, populated with only this call's NEW cache entries (a
        small subset of `raw_cache`) — the caller's cue for what actually
        needs persisting (e.g. via save_fundamental_raw_cache_entries),
        instead of rewriting the entire cache to disk every date.

    Returns
    -------
    pd.DataFrame
        One row per ticker, columns = ['ticker'] + FUNDAMENTAL_FEATURES.
        Ratio features are sector-z-scored; staleness features are raw.

    Spec References
    ----------------
    SPEC-PIPE-004: the per-ticker loop here is I/O orchestration (one API
    call per ticker), not vectorized math — same exemption documented in
    features/matrix_builder.py's _fetch_ohlcv_panel. The z-score
    computation itself is fully vectorized via groupby.transform.
    """
    records = []
    for ticker in tickers:
        try:
            pre_rows = data_cache.get_fundamentals(ticker, as_of) if data_cache is not None else None
            t_ohlcv = (
                ohlcv_panel[ohlcv_panel["ticker"] == ticker] if ohlcv_panel is not None else None
            )
            listing_date = (listing_date_map or {}).get(ticker)

            if raw_cache is None:
                feats = compute_fundamental_features(
                    client, ticker, as_of, pre_loaded_rows=pre_rows, ticker_ohlcv=t_ohlcv,
                    listing_date=listing_date,
                )
            else:
                # rows_for_peek always fetched exactly once (cache-preloaded
                # or fresh) and threaded through as pre_loaded_rows below —
                # never a second, duplicate client.get_fundamentals_history
                # call regardless of hit/miss.
                rows_for_peek = (
                    pre_rows if pre_rows is not None
                    else client.get_fundamentals_history(ticker, as_of, lookback_years=6)
                )
                latest_row = _resolve_latest_quarter_row(rows_for_peek)
                if latest_row is None:
                    feats = compute_fundamental_features(
                        client, ticker, as_of, pre_loaded_rows=rows_for_peek, ticker_ohlcv=t_ohlcv,
                        listing_date=listing_date,
                    )
                else:
                    cache_key = (ticker, int(latest_row["fiscal_year"]), int(latest_row["quarter"]))
                    cached = raw_cache.get(cache_key)
                    # [BUG FIX, 2026-07-28 model-review] Restatement invalidation:
                    # the cache key is only (ticker, fiscal_year, quarter) — if a
                    # company files a CORRECTED quarterly result for the same
                    # (fiscal_year, quarter), the only PIT-eligible signal this
                    # data source exposes that something changed is a new
                    # announcement_date on that row (rows_for_peek/latest_row are
                    # always freshly fetched live above, never cached). Compare it
                    # against the announcement_date recorded when the entry was
                    # cached; a mismatch means the filing was corrected/restated
                    # since caching and must be treated as a miss (falls through
                    # to a full recompute + cache overwrite below), not served stale.
                    # [BUG FIX, 2026-07-28 second model-review] A missing
                    # (NaN/NaT) announcement_date must NEVER participate in
                    # the cache — pd.Timestamp(NaT).isoformat() returns the
                    # literal string "NaT", which would (a) compare equal to
                    # a previously-cached "NaT" and be wrongly treated as "no
                    # restatement", and (b) blow up datetime.fromisoformat()
                    # on the cache-hit path below, which the broad except
                    # Exception further up would then silently downgrade to
                    # all-NaN fundamentals for this ticker forever. Force an
                    # explicit miss (never read OR write a cache entry keyed
                    # by a missing announcement_date) instead.
                    if pd.isna(latest_row["announcement_date"]):
                        cached = None
                        fresh_announcement_date = None
                    else:
                        fresh_announcement_date = pd.Timestamp(latest_row["announcement_date"]).isoformat()
                        if cached is not None and cached.get("announcement_date") != fresh_announcement_date:
                            logger.info(
                                f"fundamental_raw_cache: {ticker} FY{latest_row['fiscal_year']}Q{latest_row['quarter']} "
                                f"announcement_date changed ({cached.get('announcement_date')} -> "
                                f"{fresh_announcement_date}) — treating as a restatement and invalidating the cached entry"
                            )
                            cached = None
                    if cached is not None:
                        close = _latest_close_on_or_before(client, ticker, as_of, ticker_ohlcv=t_ohlcv)
                        feats = dict(cached["ratios"])
                        feats.update(_compute_priced_features(cached["priced_inputs"], close))
                        feats.update(
                            compute_staleness(datetime.fromisoformat(cached["announcement_date"]), as_of)
                        )
                    else:
                        feats = compute_fundamental_features(
                            client, ticker, as_of, pre_loaded_rows=rows_for_peek, ticker_ohlcv=t_ohlcv,
                            listing_date=listing_date,
                        )
                        # Never write a cache entry keyed by a missing
                        # announcement_date — there's nothing to compare a
                        # future restatement against, so it would sit there
                        # unable to ever be correctly invalidated.
                        if fresh_announcement_date is not None:
                            new_entry = {
                                "ratios": {k: feats[k] for k in CACHEABLE_RATIO_FEATURES},
                                "priced_inputs": _priced_inputs_from_row(latest_row),
                                "announcement_date": fresh_announcement_date,
                            }
                            raw_cache[cache_key] = new_entry
                            if cache_misses_out is not None:
                                cache_misses_out[cache_key] = new_entry
        except httpx.RequestError as exc:
            logger.error(
                f"Fundamentals fetch failed for {ticker} with a connection error ({exc}) — "
                "DataStore API is very likely unreachable; aborting the panel build rather than "
                "silently writing NaN fundamentals for the rest of the universe"
            )
            raise
        except Exception as exc:
            logger.warning(f"fundamental features failed for {ticker}: {exc}")
            feats = {f: np.nan for f in FUNDAMENTAL_FEATURES}
        feats["ticker"] = ticker
        feats["sector"] = sector_map.get(ticker, "UNKNOWN")
        records.append(feats)

    panel = pd.DataFrame(records)
    panel = _sector_relative_zscore(panel, RATIO_FEATURES, sector_col="sector")
    return panel[["ticker"] + FUNDAMENTAL_FEATURES]
