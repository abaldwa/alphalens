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
from typing import Any, Dict, List, Optional

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

# SPEC-FEAT-002: only the ratio-style features are sector-relative z-scored —
# staleness flags are deliberately excluded (a 0/1 flag or a [0,1]-bounded
# pct has no meaningful "sector mean", and z-scoring it would only smear
# its meaning).
RATIO_FEATURES = (
    GROWTH_FEATURES + PROFITABILITY_FEATURES + CAPITAL_EFFICIENCY_FEATURES
    + LEVERAGE_FEATURES + WORKING_CAPITAL_FEATURES + VALUATION_FEATURES
)
FUNDAMENTAL_FEATURES: List[str] = RATIO_FEATURES + STALENESS_FEATURES  # 30 total


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
    lookback_years: int = 4,
    pre_loaded_rows: "Optional[List]" = None,
    ticker_ohlcv: "Optional[pd.DataFrame]" = None,
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
        History window requested from the API — must cover at least the
        3-year CAGR's 12-quarter lookback plus one quarter of slack.

    Returns
    -------
    dict
        feature_name -> value for all 30 FUNDAMENTAL_FEATURES, or all-NaN
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

    def v(row: Optional[pd.Series], col: str) -> float:
        """
        Safe column lookup: NaN if `row` is None, the column is absent
        entirely, or present-but-null. `pd.Series.get(col, default)` only
        substitutes `default` when `col` is missing from the index — a
        present-but-None value (the normal case for any optional
        fundamentals field, e.g. screener.py never populates
        cash_and_equivalents) is returned as-is, not the default. A real
        bug here (`latest.get("total_debt", 0.0)` silently returning None
        instead of 0.0) crashed ev_to_ebitda's arithmetic the first time
        this function was exercised against data with any None field —
        see BuildLog.md "P2.1".
        """
        if row is None or col not in row.index:
            return np.nan
        val = row[col]
        return np.nan if val is None or pd.isna(val) else val

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
    CRORE = 1e7
    bvps, shares = v(latest, "book_value_per_share"), v(latest, "shares_outstanding")
    equity = (bvps * shares) / CRORE if pd.notna(bvps) and pd.notna(shares) else np.nan
    invested_capital = np.nan
    if pd.notna(equity):
        total_debt = v(latest, "total_debt") if pd.notna(v(latest, "total_debt")) else 0.0
        cash = v(latest, "cash_and_equivalents") if pd.notna(v(latest, "cash_and_equivalents")) else 0.0
        invested_capital = equity + total_debt - cash
    op_margin, revenue = v(latest, "operating_margin"), v(latest, "revenue")
    ebit_proxy = op_margin * revenue if pd.notna(op_margin) and pd.notna(revenue) else np.nan
    nopat = ebit_proxy * (1.0 - ASSUMED_TAX_RATE) if pd.notna(ebit_proxy) else np.nan

    close = _latest_close_on_or_before(client, ticker, as_of, ticker_ohlcv=ticker_ohlcv)
    market_cap = (close * shares) / CRORE if close is not None and pd.notna(shares) else np.nan

    total_debt_v, cash_v = v(latest, "total_debt"), v(latest, "cash_and_equivalents")
    inv_days, rec_days, pay_days = v(latest, "inventory_days"), v(latest, "receivable_days"), v(latest, "payable_days")

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
        # Valuation (3)
        "pe_ratio": _safe_div(close, v(latest, "eps")) if close is not None else np.nan,
        "pb_ratio": _safe_div(close, v(latest, "book_value_per_share")) if close is not None else np.nan,
        "ev_to_ebitda": _safe_div(
            (market_cap + (total_debt_v if pd.notna(total_debt_v) else 0.0)
             - (cash_v if pd.notna(cash_v) else 0.0))
            if pd.notna(market_cap) else np.nan,
            v(latest, "ebitda"),
        ),
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
) -> pd.DataFrame:
    """
    Compute the full 30-feature fundamental panel for many tickers, with
    the 27 ratio features sector-relative z-scored (SPEC-FEAT-002).

    Parameters
    ----------
    client : DataStoreClient
    tickers : list of str
    as_of : datetime
        PIT reference date, shared across the whole panel.
    sector_map : dict
        ticker -> sector (e.g. from config.universe.load_universe()).
        Tickers with no sector mapping fall into a single "UNKNOWN" group.

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
            feats = compute_fundamental_features(
                client, ticker, as_of, pre_loaded_rows=pre_rows, ticker_ohlcv=t_ohlcv
            )
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
