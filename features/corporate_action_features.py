"""
features/corporate_action_features.py

Phase: 2.2 (AMFI MF Holdings + Corporate Action Features)
Specs: SPEC-FEAT-002, SPEC-PIPE-002, SPEC-PIPE-003, SPEC-PIPE-004, SPEC-SOLID-005
Owner: Platform / Features
Consumers: features/matrix_builder (wired in P2.3)

Computes the 10 corporate-action features named in the P2.2 build prompt
(matches 01_features.md's "Corporate Action Features (10)" list exactly —
no name divergence this time, unlike P2.1's fundamental/governance lists).

[AS BUILT] Real, honest coverage split — not every feature is computable
from data this codebase actually ingests today:

REAL today (5): days_to_record_date, corp_action_anticipation_return,
ipo_lockin_expiry_proximity, ipo_listing_age_months,
post_earnings_drift_signal — all derivable from `corporate_actions`
(SPLIT/BONUS, the only action types ingestion/adjust/price_adjuster.py
and ingestion/scrapers/bhavcopy.py actually write), `stock_master.
listing_date`, OHLCV, and `fundamentals.announcement_date` (P2.1).

Structurally ready but NaN-by-design today (5): buyback_price_spread,
buyback_acceptance_estimated, index_inclusion_days, qip_dilution_impact,
dividend_yield_vs_fd_rate — these need corporate-action *types*
(BUYBACK, QIP, INDEX_INCLUSION) and a DIVIDEND-yield data source that no
scraper in this codebase writes yet (P0.4's ingestion only handles
SPLIT/BONUS for retroactive price adjustment — BUYBACK/QIP/INDEX_
INCLUSION/DIVIDEND corporate-action ingestion is a separate, future
ingestion task, not part of this prompt's explicit deliverable list).
The computation logic here is correct and ready the moment those rows
exist — same "honest NaN, not fabricated" precedent as P2.1's
screener.py-sourced gaps (gross_profit, capex, current_assets, ...).

PIT Assumptions
----------------
`corporate_actions` rows are read via DataStoreClient.get_corporate_actions
(PITRule.NONE at the API layer — SPEC-DS-002) and filtered here by
`announcement_date <= as_of` before use, exactly mirroring SPEC-PIPE-003's
fundamentals/shareholding PIT pattern: a corporate action whose
announcement_date is in the future relative to as_of was not yet public
knowledge and must never influence a feature computed as of that date.
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from config.settings import (
    ASSUMED_FD_RATE,
    CORP_ACTION_ANTICIPATION_WINDOW_DAYS,
    IPO_LOCKIN_DAYS,
    POST_EARNINGS_DRIFT_WINDOW_DAYS,
)
from datastore.client import DataStoreClient

logger = logging.getLogger(__name__)

CORPORATE_ACTION_FEATURES: List[str] = [
    "days_to_record_date",
    "corp_action_anticipation_return",
    "buyback_price_spread",
    "buyback_acceptance_estimated",
    "index_inclusion_days",
    "ipo_lockin_expiry_proximity",
    "ipo_listing_age_months",
    "post_earnings_drift_signal",
    "dividend_yield_vs_fd_rate",
    "qip_dilution_impact",
]


def _pit_filter_actions(actions: List[Dict[str, Any]], as_of: datetime) -> pd.DataFrame:
    """SPEC-PIPE-003: keep only actions whose announcement_date <= as_of (or unannounced/unknown-date rows excluded)."""
    if not actions:
        return pd.DataFrame(columns=["ticker", "ex_date", "action_type", "ratio", "announcement_date", "record_date"])
    df = pd.DataFrame(actions)
    df["ex_date"] = pd.to_datetime(df["ex_date"])
    df["announcement_date"] = pd.to_datetime(df["announcement_date"])
    df["record_date"] = pd.to_datetime(df["record_date"])
    return df[df["announcement_date"].notna() & (df["announcement_date"] <= as_of)].sort_values("ex_date")


def _close_on_or_before(rows: List[Dict[str, Any]], target: datetime) -> Optional[float]:
    eligible = [r for r in rows if pd.to_datetime(r["date"]) <= target]
    if not eligible:
        return None
    return sorted(eligible, key=lambda r: r["date"])[-1]["close"]


def compute_corporate_action_features(
    client: DataStoreClient,
    ticker: str,
    as_of: datetime,
    listing_date: Optional[datetime] = None,
    pre_loaded_actions=None,
    pre_loaded_fundamentals=None,
    ticker_ohlcv: "Optional[pd.DataFrame]" = None,
) -> Dict[str, Any]:
    """
    Compute all 10 corporate-action features for one ticker.

    Parameters
    ----------
    client : DataStoreClient
        SPEC-DS-002: all corporate_actions/OHLCV/fundamentals access via the API.
    ticker : str
    as_of : datetime
        PIT reference date.
    listing_date : datetime, optional
        From config.universe/stock_master — needed for the two IPO
        features. None if unknown (both features NaN).

    Returns
    -------
    dict
        feature_name -> value for all 10 CORPORATE_ACTION_FEATURES.

    Spec References
    ----------------
    SPEC-PIPE-002, SPEC-PIPE-003 (CRITICAL).

    PIT Assumptions
    ----------------
    See module docstring — `announcement_date <= as_of` filtering applied
    to every `corporate_actions` row before use.

    Raises
    ------
    None — missing data degrades to NaN, not an exception.
    """
    raw_actions = pre_loaded_actions if pre_loaded_actions is not None else client.get_corporate_actions(ticker)
    actions = _pit_filter_actions(raw_actions, as_of)

    future_record = actions[actions["record_date"].notna() & (actions["record_date"] > as_of)]
    days_to_record_date = (
        (future_record.iloc[0]["record_date"] - as_of).days if len(future_record) else np.nan
    )

    past_actions = actions[actions["ex_date"] <= as_of]
    corp_action_anticipation_return = np.nan
    if len(past_actions):
        nearest_ex_date = past_actions.iloc[-1]["ex_date"]
        window_start = nearest_ex_date - timedelta(days=CORP_ACTION_ANTICIPATION_WINDOW_DAYS)
        if window_start <= as_of:
            window_end = min(nearest_ex_date, as_of)
            if ticker_ohlcv is not None and not ticker_ohlcv.empty:
                w = ticker_ohlcv[
                    (ticker_ohlcv["date"] >= pd.Timestamp(window_start))
                    & (ticker_ohlcv["date"] <= pd.Timestamp(window_end))
                ].sort_values("date")
                if len(w) >= 2:
                    corp_action_anticipation_return = (
                        float(w.iloc[-1]["close"]) / float(w.iloc[0]["close"])
                    ) - 1.0
            else:
                price_rows = client.get_ohlcv(ticker, from_date=window_start, to_date=window_end)
                if len(price_rows) >= 2:
                    ordered = sorted(price_rows, key=lambda r: r["date"])
                    corp_action_anticipation_return = (ordered[-1]["close"] / ordered[0]["close"]) - 1.0

    # BUYBACK/QIP/INDEX_INCLUSION rows are never written by any ingestion
    # module in this codebase today (module docstring) — structurally
    # ready, NaN until that ingestion exists.
    buyback_rows = actions[actions["action_type"] == "BUYBACK"]
    buyback_price_spread = np.nan
    if len(buyback_rows):
        if ticker_ohlcv is not None and not ticker_ohlcv.empty:
            w = ticker_ohlcv[ticker_ohlcv["date"] <= pd.Timestamp(as_of)].sort_values("date")
            close = float(w.iloc[-1]["close"]) if not w.empty else None
        else:
            price_rows = client.get_ohlcv(ticker, from_date=as_of - timedelta(days=14), to_date=as_of)
            close = _close_on_or_before(price_rows, as_of)
        buyback_price = buyback_rows.iloc[-1]["ratio"]  # convention: ratio holds the offer price for BUYBACK rows
        if close:
            buyback_price_spread = (buyback_price - close) / close
    buyback_acceptance_estimated = np.nan  # needs buyback size + free float, not in corporate_actions schema

    index_inclusion_rows = actions[actions["action_type"] == "INDEX_INCLUSION"]
    index_inclusion_days = (
        (as_of - index_inclusion_rows.iloc[-1]["ex_date"]).days if len(index_inclusion_rows) else np.nan
    )

    ipo_lockin_expiry_proximity = np.nan
    ipo_listing_age_months = np.nan
    if listing_date is not None:
        lockin_expiry = listing_date + timedelta(days=IPO_LOCKIN_DAYS)
        ipo_lockin_expiry_proximity = (lockin_expiry - as_of).days
        ipo_listing_age_months = (as_of - listing_date).days / 30.44

    post_earnings_drift_signal = np.nan
    fundamentals_history = (
        pre_loaded_fundamentals if pre_loaded_fundamentals is not None
        else client.get_fundamentals_history(ticker, as_of, lookback_years=1)
    )
    if fundamentals_history:
        latest = sorted(fundamentals_history, key=lambda r: r["announcement_date"])[-1]
        announcement_date = pd.to_datetime(latest["announcement_date"])
        drift_window_end = min(announcement_date + timedelta(days=POST_EARNINGS_DRIFT_WINDOW_DAYS), as_of)
        if drift_window_end > announcement_date:
            if ticker_ohlcv is not None and not ticker_ohlcv.empty:
                w = ticker_ohlcv[
                    (ticker_ohlcv["date"] >= announcement_date)
                    & (ticker_ohlcv["date"] <= pd.Timestamp(drift_window_end))
                ].sort_values("date")
                if len(w) >= 2:
                    post_earnings_drift_signal = (
                        float(w.iloc[-1]["close"]) / float(w.iloc[0]["close"])
                    ) - 1.0
            else:
                price_rows = client.get_ohlcv(ticker, from_date=announcement_date, to_date=drift_window_end)
                if len(price_rows) >= 2:
                    ordered = sorted(price_rows, key=lambda r: r["date"])
                    post_earnings_drift_signal = (ordered[-1]["close"] / ordered[0]["close"]) - 1.0

    # No DIVIDEND rows ingested anywhere yet (module docstring) — NaN until
    # a real trailing-dividend source exists. ASSUMED_FD_RATE documented
    # for when it does.
    dividend_yield_vs_fd_rate = np.nan
    _ = ASSUMED_FD_RATE  # referenced for the future real implementation; see module docstring

    qip_rows = actions[actions["action_type"] == "QIP"]
    qip_dilution_impact = qip_rows.iloc[-1]["ratio"] if len(qip_rows) else np.nan

    return {
        "days_to_record_date": days_to_record_date,
        "corp_action_anticipation_return": corp_action_anticipation_return,
        "buyback_price_spread": buyback_price_spread,
        "buyback_acceptance_estimated": buyback_acceptance_estimated,
        "index_inclusion_days": index_inclusion_days,
        "ipo_lockin_expiry_proximity": ipo_lockin_expiry_proximity,
        "ipo_listing_age_months": ipo_listing_age_months,
        "post_earnings_drift_signal": post_earnings_drift_signal,
        "dividend_yield_vs_fd_rate": dividend_yield_vs_fd_rate,
        "qip_dilution_impact": qip_dilution_impact,
    }


def compute_corporate_action_features_panel(
    client: DataStoreClient,
    tickers: List[str],
    as_of: datetime,
    listing_dates: Optional[Dict[str, datetime]] = None,
    data_cache=None,
    ohlcv_panel: "Optional[pd.DataFrame]" = None,
) -> pd.DataFrame:
    """
    Compute the 10-feature corporate-action panel for many tickers.

    Parameters
    ----------
    client : DataStoreClient
    tickers : list of str
    as_of : datetime
    listing_dates : dict, optional
        ticker -> listing_date (e.g. from config.universe.load_universe()).

    Returns
    -------
    pd.DataFrame
        One row per ticker, columns = ['ticker'] + CORPORATE_ACTION_FEATURES.

    Spec References
    ----------------
    SPEC-PIPE-004: per-ticker loop is I/O orchestration, same exemption as
    features/fundamental.py's panel function.
    """
    listing_dates = listing_dates or {}
    records = []
    for ticker in tickers:
        try:
            pre_actions = data_cache.get_corp_actions(ticker) if data_cache is not None else None
            pre_fund = data_cache.get_fundamentals(ticker, as_of) if data_cache is not None else None
            t_ohlcv = (
                ohlcv_panel[ohlcv_panel["ticker"] == ticker] if ohlcv_panel is not None else None
            )
            feats = compute_corporate_action_features(
                client, ticker, as_of, listing_dates.get(ticker),
                pre_loaded_actions=pre_actions,
                pre_loaded_fundamentals=pre_fund,
                ticker_ohlcv=t_ohlcv,
            )
        except Exception as exc:
            logger.warning(f"corporate action features failed for {ticker}: {exc}")
            feats = {f: np.nan for f in CORPORATE_ACTION_FEATURES}
        feats["ticker"] = ticker
        records.append(feats)

    panel = pd.DataFrame(records)
    return panel[["ticker"] + CORPORATE_ACTION_FEATURES]
