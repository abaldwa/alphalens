"""
features/governance.py

Phase: 2.1 (Fundamental Data Ingestion + PIT Validation)
Specs: SPEC-FEAT-002, SPEC-PIPE-003 (CRITICAL), SPEC-PIPE-004, SPEC-SOLID-005
Owner: Platform / Features
Consumers: features/matrix_builder (wired in P2.3), systems/ml_signal_engine

Computes the 12 governance features named in this phase's build prompt
(CLAUDE_CODE_PROMPTS.md P2.1): promoter_pct, promoter_change_qoq,
promoter_pledge, promoter_pledge_change_qoq, fii_pct, fii_change_qoq,
dii_pct, dii_change_qoq, mf_pct, mf_change_qoq,
promoter_pledge_spiral_flag, institutional_conviction_flag.

[AS BUILT] 01_features.md's older "Governance Features (12)" list uses a
different shape (promoter_holding_change_4q, institutional_total_change,
no composite flags) — same prompt-vs-doc divergence already documented in
features/fundamental.py's module docstring; the P2.1 build prompt's
literal list is implemented here.

A 13th feature, institutional_ownership_pct (= fii_pct + dii_pct + mf_pct),
was added later for the Under-followed Growth Improvers and
Governance-Aware Quality Growth strategies — a rollup of fields already
computed above, not new raw data.

`mf_pct` here is the BSE shareholding-pattern aggregate (one number per
quarter, `shareholding.mf_pct`) — distinct from the scheme-level monthly
AMFI holdings detail (`mf_scheme_count`, `mf_new_entry_count`, etc.)
that P2.2's features/mf_holdings.py computes from a different source and
a different PIT rule (5th of next month, not filing_date).

SPEC-PIPE-003 (CRITICAL): every row consumed here comes from
DataStoreClient.get_shareholding_history(), already PIT-filtered
server-side on filing_date (never quarter_end_date). Sequencing among
already-eligible rows uses quarter_end_date purely as a chronological
sort key — see features/fundamental.py's module docstring for why that is
not a PIT violation.
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from datastore.client import DataStoreClient

logger = logging.getLogger(__name__)

GOVERNANCE_FEATURES: List[str] = [
    "promoter_pct", "promoter_change_qoq", "promoter_pledge", "promoter_pledge_change_qoq",
    "fii_pct", "fii_change_qoq", "dii_pct", "dii_change_qoq", "mf_pct", "mf_change_qoq",
    "promoter_pledge_spiral_flag", "institutional_conviction_flag",
    # Added for Under-followed Growth Improvers (percentile_rank_asc target)
    # and Governance-Aware Quality Growth — simple rollup of the 3 existing
    # institutional-ownership fields above, not a new data source.
    "institutional_ownership_pct",
]

# promoter_pledge_spiral_flag: pledge > this AND price falling over the lookback window
PLEDGE_SPIRAL_THRESHOLD_PCT = 20.0
PRICE_FALL_LOOKBACK_DAYS = 63  # ~1 quarter, consistent with quarter_age_pct's 63-day convention


def _safe_change(current, prior) -> float:
    if current is None or prior is None or pd.isna(current) or pd.isna(prior):
        return np.nan
    return current - prior


def _sum_if_any_present(*values) -> float:
    """Sum of the non-NaN values; NaN only if every value is missing (treats a
    missing FII/DII/MF row as 0% of that category, not as unknown-total)."""
    present = [v for v in values if v is not None and pd.notna(v)]
    return float(sum(present)) if present else np.nan


def compute_governance_features(
    client: DataStoreClient,
    ticker: str,
    as_of: datetime,
    lookback_years: int = 2,
    pre_loaded_rows=None,
    ticker_ohlcv: Optional[pd.DataFrame] = None,
) -> Dict[str, Any]:
    """
    Compute all 13 governance features for one ticker.

    Parameters
    ----------
    client : DataStoreClient
        SPEC-DS-002: all shareholding/OHLCV access goes through the API.
    ticker : str
    as_of : datetime
        PIT reference date.
    lookback_years : int
        History window requested from the API — 2 years comfortably
        covers the single quarter-over-quarter comparison these features need.

    Returns
    -------
    dict
        feature_name -> value for all 13 GOVERNANCE_FEATURES. All-NaN
        (flags 0) if no PIT-eligible shareholding row exists yet.

    Spec References
    ----------------
    SPEC-PIPE-003 (CRITICAL), SPEC-FEAT-002.

    PIT Assumptions
    ----------------
    Trusts DataStoreClient.get_shareholding_history()'s server-side PIT
    filter (filing_date <= as_of) entirely.

    Raises
    ------
    None — missing/insufficient history degrades to NaN/0, not an exception.
    """
    rows = pre_loaded_rows if pre_loaded_rows is not None else client.get_shareholding_history(
        ticker, as_of, lookback_years=lookback_years
    )
    if not rows:
        result = {f: np.nan for f in GOVERNANCE_FEATURES}
        result["promoter_pledge_spiral_flag"] = 0
        result["institutional_conviction_flag"] = 0
        return result

    history = pd.DataFrame(rows)
    history["quarter_end_date"] = pd.to_datetime(history["quarter_end_date"])
    history = history.sort_values("quarter_end_date").reset_index(drop=True)

    latest = history.iloc[-1]
    qoq_prior = history.iloc[-2] if len(history) >= 2 else None

    def prior(col):
        return qoq_prior[col] if qoq_prior is not None else None

    promoter_pledge_change_qoq = _safe_change(latest.get("promoter_pledge"), prior("promoter_pledge"))

    # promoter_pledge_spiral_flag: pledge above threshold AND price has fallen
    # over the lookback window — a classic distress signal (forced-selling risk).
    spiral_flag = 0
    if pd.notna(latest.get("promoter_pledge")) and latest["promoter_pledge"] > PLEDGE_SPIRAL_THRESHOLD_PCT:
        if ticker_ohlcv is not None and not ticker_ohlcv.empty:
            cutoff = pd.Timestamp(as_of) - pd.Timedelta(days=PRICE_FALL_LOOKBACK_DAYS)
            window = ticker_ohlcv[
                (ticker_ohlcv["date"] >= cutoff) & (ticker_ohlcv["date"] <= pd.Timestamp(as_of))
            ].sort_values("date")
            if len(window) >= 2:
                price_falling = float(window.iloc[-1]["close"]) < float(window.iloc[0]["close"])
                spiral_flag = int(price_falling)
        else:
            price_rows = client.get_ohlcv(
                ticker, from_date=as_of - timedelta(days=PRICE_FALL_LOOKBACK_DAYS), to_date=as_of
            )
            if len(price_rows) >= 2:
                ordered = sorted(price_rows, key=lambda r: r["date"])
                price_falling = ordered[-1]["close"] < ordered[0]["close"]
                spiral_flag = int(price_falling)

    # institutional_conviction_flag: FII + DII + MF holding all increased QoQ
    conviction_flag = 0
    if qoq_prior is not None:
        fii_up = pd.notna(latest.get("fii_pct")) and pd.notna(prior("fii_pct")) and latest["fii_pct"] > prior("fii_pct")
        dii_up = pd.notna(latest.get("dii_pct")) and pd.notna(prior("dii_pct")) and latest["dii_pct"] > prior("dii_pct")
        mf_up = pd.notna(latest.get("mf_pct")) and pd.notna(prior("mf_pct")) and latest["mf_pct"] > prior("mf_pct")
        conviction_flag = int(fii_up and dii_up and mf_up)

    return {
        "promoter_pct": latest.get("promoter_pct", np.nan),
        "promoter_change_qoq": _safe_change(latest.get("promoter_pct"), prior("promoter_pct")),
        "promoter_pledge": latest.get("promoter_pledge", np.nan),
        "promoter_pledge_change_qoq": promoter_pledge_change_qoq,
        "fii_pct": latest.get("fii_pct", np.nan),
        "fii_change_qoq": _safe_change(latest.get("fii_pct"), prior("fii_pct")),
        "dii_pct": latest.get("dii_pct", np.nan),
        "dii_change_qoq": _safe_change(latest.get("dii_pct"), prior("dii_pct")),
        "mf_pct": latest.get("mf_pct", np.nan),
        "mf_change_qoq": _safe_change(latest.get("mf_pct"), prior("mf_pct")),
        "promoter_pledge_spiral_flag": spiral_flag,
        "institutional_conviction_flag": conviction_flag,
        "institutional_ownership_pct": _sum_if_any_present(
            latest.get("fii_pct"), latest.get("dii_pct"), latest.get("mf_pct")
        ),
    }


def compute_governance_features_panel(
    client: DataStoreClient,
    tickers: List[str],
    as_of: datetime,
    data_cache=None,
    ohlcv_panel: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Compute the 12-feature governance panel for many tickers.

    Unlike features/fundamental.py's panel function, governance features
    are NOT sector-z-scored — promoter/FII/DII/MF holding percentages and
    the two composite flags are already bounded/categorical, not the kind
    of unbounded ratio SPEC-FEAT-002's z-score normalization targets
    (the spec's normalization rule is scoped to "Fundamental features",
    01_features.md's separate "Governance Features (12)" section does not
    repeat that instruction).

    Parameters
    ----------
    client : DataStoreClient
    tickers : list of str
    as_of : datetime
        PIT reference date, shared across the whole panel.

    Returns
    -------
    pd.DataFrame
        One row per ticker, columns = ['ticker'] + GOVERNANCE_FEATURES.

    Spec References
    ----------------
    SPEC-PIPE-004: the per-ticker loop is I/O orchestration (one API call
    per ticker), same exemption as features/fundamental.py's panel function.
    """
    records = []
    for ticker in tickers:
        try:
            pre_rows = data_cache.get_shareholding(ticker, as_of) if data_cache is not None else None
            t_ohlcv = (
                ohlcv_panel[ohlcv_panel["ticker"] == ticker] if ohlcv_panel is not None else None
            )
            feats = compute_governance_features(
                client, ticker, as_of, pre_loaded_rows=pre_rows, ticker_ohlcv=t_ohlcv
            )
        except Exception as exc:
            logger.warning(f"governance features failed for {ticker}: {exc}")
            feats = {f: np.nan for f in GOVERNANCE_FEATURES}
            feats["promoter_pledge_spiral_flag"] = 0
            feats["institutional_conviction_flag"] = 0
        feats["ticker"] = ticker
        records.append(feats)

    panel = pd.DataFrame(records)
    return panel[["ticker"] + GOVERNANCE_FEATURES]
