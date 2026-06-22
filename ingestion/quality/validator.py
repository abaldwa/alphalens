"""
ingestion/quality/validator.py

Phase: 0.6 (Data Quality & Observability)
Specs: SPEC-PIPE-005, SPEC-SYS-003
Owner: Platform / Ingestion
Consumers: ingestion/scheduler/daily_pipeline, ingestion/scrapers/bhavcopy

Canonical bhavcopy quality gate (SPEC-PIPE-005): cross-checks a downloaded
bhavcopy against the expected universe (missing tickers), flags single-day
price anomalies, and applies the SPEC-SYS-003 completeness gate
(>= MIN_STOCKS_FOR_INFERENCE stocks) to the returned 'ok' verdict.

This logic previously lived inline in ingestion/scrapers/bhavcopy.py
(API_SPEC.md's validate_bhavcopy(df, expected_tickers) -> dict contract).
Moved here so SPEC-PIPE-005's quality checks have one home in
ingestion/quality rather than living inside a scraper module (SOLID-S).
ingestion/scrapers/bhavcopy.py now re-exports this function — no caller's
import needs to change.
"""

import logging
from typing import List

import pandas as pd

from config.settings import MIN_STOCKS_FOR_INFERENCE

logger = logging.getLogger(__name__)

# SPEC-PIPE-005: a single-day price move beyond this, with no recorded
# corporate action, is flagged for review (not a hard error — real
# corp-action moves are reconciled separately against corporate_actions,
# not filtered out here). A single bhavcopy row has no prior-day close to
# diff against, so close-vs-open is used as the intraday move proxy.
ANOMALY_PCT_CHANGE_THRESHOLD = 30.0


def validate_bhavcopy(df: pd.DataFrame, expected_tickers: List[str]) -> dict:
    """
    Validate a downloaded bhavcopy DataFrame against the expected universe.

    Parameters
    ----------
    df : pd.DataFrame
        Output of ingestion.scrapers.bhavcopy.download_bhavcopy() — must
        have 'ticker', 'open', 'close' columns.
    expected_tickers : list of str
        The universe this bhavcopy should cover (e.g. config/universe.py).

    Returns
    -------
    dict
        {'ok': bool, 'missing': list of str, 'anomalies': list of str,
         'stock_count': int}.
        'missing': expected_tickers absent from df.
        'anomalies': tickers with |close/open - 1| > ANOMALY_PCT_CHANGE_THRESHOLD%
        and no recorded corporate action (a proxy flag for review; real
        corp-action moves are reconciled separately, not filtered out here).
        'stock_count': len(df) — rows actually present in the bhavcopy.
        'ok' is False if any tickers are missing, any anomalies are found,
        OR stock_count < MIN_STOCKS_FOR_INFERENCE (SPEC-SYS-003 completeness
        gate — "Proceed to model inference only if >= 450/500 stocks").

    Spec References
    ----------------
    SPEC-PIPE-005: null/anomaly/range/completeness quality checks.
    SPEC-SYS-003: completeness gate, >= 450/500 stocks.

    PIT Assumptions
    ----------------
    None.

    Raises
    ------
    None
    """
    stock_count = len(df)
    present = set(df["ticker"])
    missing = sorted(set(expected_tickers) - present)

    pct_change = (df["close"] - df["open"]).abs() / df["open"].replace(0, pd.NA) * 100
    anomalies = sorted(df.loc[pct_change > ANOMALY_PCT_CHANGE_THRESHOLD, "ticker"].tolist())

    completeness_ok = stock_count >= MIN_STOCKS_FOR_INFERENCE
    if not completeness_ok:
        logger.warning(
            f"SPEC-SYS-003 completeness gate failed: {stock_count} stocks "
            f"< {MIN_STOCKS_FOR_INFERENCE} required"
        )

    ok = completeness_ok and not missing and not anomalies
    return {"ok": ok, "missing": missing, "anomalies": anomalies, "stock_count": stock_count}
