"""
ingestion/quality/baseline_runner.py

Phase: 0.6 (Data Quality & Observability)
Specs: SPEC-PIPE-005, SPEC-DS-007
Owner: Platform / Ingestion
Consumers: operator (manual, run after the OHLCV backfill is complete)

One-time/periodic operator script: loads ~2 years of OHLCV history from
Store 2's ohlcv_adjusted DuckDB table (SPEC-DS-007), derives a small set
of stationary, PSI-appropriate columns from it, and calls
ingestion.quality.drift_monitor.PSIMonitor.compute_baseline() to produce
datastore/features/baseline/stats_baseline.pkl — the reference distribution
ingestion.quality.drift_monitor.PSIMonitor.check_drift() compares each new
day against (SPEC-PIPE-005).

NOTE on data source (corrected from an earlier version of this file): "load
2 years of existing data ... must run after backfill is complete" (this
module's originating task) refers to the OHLCV backfill
(ingestion/backfill_runner.py / SPEC-PIPE-001), which already exists and
has data today — NOT to the Phase 1 76-feature matrix
(features/matrix_builder.py), which doesn't exist yet. An earlier version
of this file read from FEATURES_DAILY_DIR (Store 3 Parquets) instead,
which meant it could never produce a baseline until Phase 1 was built —
contradicting its own task instruction, which has no such Phase-1
dependency (compare ingestion/quality/drift_monitor.py's daily *check*,
which the same task explicitly defers with "after feature matrix is
built" — baseline computation has no equivalent qualifier). Fixed to read
from ohlcv_adjusted, which is real and populated now.

raw OHLCV price levels (open/high/low/close) are not themselves PSI-
appropriate — they're non-stationary (a stock's price trends over years
regardless of any real distributional shift in behavior), so PSI on raw
price would just measure long-run price drift, not drift worth alerting
on. This module instead derives return_1d, volume, and delivery_pct —
already-stationary quantities directly computable from ohlcv_adjusted
without needing the full Phase 1 technical-indicator suite
(features/technical.py). This is a deliberate, minimal Phase 0.6
stand-in, not a duplicate of future feature computation (SPEC-SOLID-002:
feature computation belongs in features/, not ingestion/quality/) — once
features/matrix_builder.py exists, swap load_ohlcv_history() for a Parquet
read from FEATURES_DAILY_DIR; PSIMonitor.compute_baseline() itself doesn't
care about the data source, so no other change is needed.
"""

import argparse
import logging
from datetime import date, timedelta
from typing import Optional

import pandas as pd

from config.settings import DUCKDB_PATH
from config.timezone import now_ist
from datastore.api.db import get_duckdb_connection
from ingestion.quality.drift_monitor import PSIMonitor

logger = logging.getLogger(__name__)

BASELINE_WINDOW_YEARS = 2  # "Compute PSI baseline: load 2 years of existing data"

_SELECT_OHLCV_HISTORY = """
    SELECT date, ticker, close, volume, delivery_pct
    FROM ohlcv_adjusted
    WHERE date >= ? AND date <= ?
    ORDER BY ticker, date
"""


def load_ohlcv_history(
    db_path: Optional[str] = None,
    end_date: Optional[date] = None,
    years: int = BASELINE_WINDOW_YEARS,
) -> pd.DataFrame:
    """
    Load OHLCV history from ohlcv_adjusted covering the last `years` years.

    Parameters
    ----------
    db_path : str, optional
        Defaults to config.settings.DUCKDB_PATH.
    end_date : date, optional
        Defaults to today.
    years : int
        Lookback window (default 2 — "load 2 years of existing data").

    Returns
    -------
    pd.DataFrame
        Columns: date, ticker, close, volume, delivery_pct. One row per
        (date, ticker).

    Spec References
    ----------------
    SPEC-PIPE-005, SPEC-DS-007 (Store 2: ohlcv_adjusted DuckDB table).

    PIT Assumptions
    ----------------
    None — ohlcv_adjusted is same-day, publicly available price data with
    no announcement-date lag (SPEC-PIPE-001).

    Raises
    ------
    FileNotFoundError
        If no rows are found in the requested window — the OHLCV backfill
        (ingestion/backfill_runner.py) must run first.
    """
    end_date = end_date or now_ist().date()
    start_date = end_date - timedelta(days=365 * years)
    db_path = db_path or DUCKDB_PATH

    with get_duckdb_connection(db_path) as conn:
        ohlcv = conn.execute(_SELECT_OHLCV_HISTORY, [start_date, end_date]).df()

    if ohlcv.empty:
        raise FileNotFoundError(
            f"No ohlcv_adjusted rows found for {start_date}..{end_date}. "
            "Run the OHLCV backfill first (ingestion/backfill_runner.py) — "
            "see ingestion/quality/baseline_runner.py's module docstring."
        )

    logger.info(
        f"Loaded OHLCV history ({start_date}..{end_date}): "
        f"{len(ohlcv)} rows, {ohlcv['ticker'].nunique()} tickers"
    )
    return ohlcv


def _derive_baseline_features(ohlcv: pd.DataFrame) -> pd.DataFrame:
    """
    Derive a minimal, stationary feature set from raw OHLCV history.

    Parameters
    ----------
    ohlcv : pd.DataFrame
        Output of load_ohlcv_history() — columns date, ticker, close,
        volume, delivery_pct.

    Returns
    -------
    pd.DataFrame
        Columns: return_1d, volume, delivery_pct. One row per
        (date, ticker) observation (the first observation per ticker is
        dropped — return_1d is undefined for it).

    Spec References
    ----------------
    SPEC-PIPE-005.

    PIT Assumptions
    ----------------
    None — derived purely from already-PIT-correct OHLCV.

    Raises
    ------
    None
    """
    ohlcv = ohlcv.sort_values(["ticker", "date"])
    ohlcv = ohlcv.assign(return_1d=ohlcv.groupby("ticker")["close"].pct_change())
    return ohlcv.dropna(subset=["return_1d"])[["return_1d", "volume", "delivery_pct"]]


def run(
    db_path: Optional[str] = None,
    end_date: Optional[date] = None,
    years: int = BASELINE_WINDOW_YEARS,
) -> dict:
    """
    Load OHLCV history, derive baseline features, and compute + persist
    the PSI baseline.

    Returns
    -------
    dict
        The baseline dict returned by PSIMonitor.compute_baseline()
        ({feature_name: {'bin_edges': ..., 'baseline_pct': ...}}).

    Spec References
    ----------------
    SPEC-PIPE-005.

    Raises
    ------
    FileNotFoundError
        See load_ohlcv_history().
    """
    ohlcv = load_ohlcv_history(db_path=db_path, end_date=end_date, years=years)
    matrix = _derive_baseline_features(ohlcv)
    monitor = PSIMonitor()
    return monitor.compute_baseline(matrix)


def main() -> None:
    """CLI entry point: `python -m ingestion.quality.baseline_runner`."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(
        description="Compute the PSI drift baseline from 2 years of OHLCV history "
        "(must run after the OHLCV backfill is complete)."
    )
    parser.add_argument("--years", type=int, default=BASELINE_WINDOW_YEARS)
    args = parser.parse_args()

    baseline = run(years=args.years)
    print(f"PSI baseline computed for {len(baseline)} features.", flush=True)


if __name__ == "__main__":
    main()
