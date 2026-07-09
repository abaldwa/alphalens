"""
datastore/api/utils/feature_store.py

Phase: 3.x (TA/FA API Scaffolding)
Specs: SPEC-TA-004, SPEC-FA-008
Owner: Platform / DataStore
Consumers: datastore/api/routers/technical.py, datastore/api/routers/fundamental_analysis.py

Shared read helpers over the daily feature Parquet store
(config.settings.FEATURES_DAILY_DIR, written by features/matrix_builder.py)
— both the Technical Analysis and Fundamental Analysis API routers read
already-computed columns from the same files, just different column
subsets. Factored out so neither router duplicates the "find the latest
day / read one day's file" logic.
"""

from datetime import datetime
from typing import Optional

import pandas as pd

from config.settings import FEATURES_DAILY_DIR
from datastore.api.db import get_duckdb_connection


def latest_feature_day() -> Optional[str]:
    """Most recent date (YYYY-MM-DD) with a written feature Parquet, or None."""
    if not FEATURES_DAILY_DIR.exists():
        return None
    files = sorted(FEATURES_DAILY_DIR.glob("*.parquet"))
    return files[-1].stem if files else None


def read_feature_day(date_str: str) -> Optional[pd.DataFrame]:
    """Full universe's feature row for one day, or None if that day has no file."""
    day_path = FEATURES_DAILY_DIR / f"{date_str}.parquet"
    if not day_path.exists():
        return None
    return pd.read_parquet(day_path)


def read_feature_row(ticker: str, date_str: str) -> Optional[pd.Series]:
    """One ticker's row from one day's feature Parquet, or None."""
    df = read_feature_day(date_str)
    if df is None:
        return None
    rows = df[df["ticker"] == ticker]
    return rows.iloc[0] if not rows.empty else None


def resolve_date(date: Optional[str]) -> Optional[str]:
    return date or latest_feature_day()


def read_feature_range(ticker: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """
    One ticker's feature rows across a date range, read via a single DuckDB
    `read_parquet()` glob query rather than opening one Parquet file per
    calendar day (item #10 / AF-3: the old `/api/v1/features/{ticker}` route
    in datastore/api/main.py looped `pd.date_range(start, end)` and opened
    one file per day — a linear file-open cost against FEATURES_DAILY_DIR's
    4,792+ daily files since the ~2006 backfill). DuckDB's own Parquet
    metadata (min/max per file) prunes files outside [start_date, end_date]
    itself, so this is Option A from the backlog note: zero writer-side
    change, no new partitioning scheme.

    Uses an in-memory DuckDB connection (db_path=None) purely to run the
    glob query — FEATURES_DAILY_DIR's Parquet files aren't a DuckDB-owned
    file, so there's no cross-process lock to coordinate the way there is
    for DUCKDB_PATH/SIGNALS_DUCKDB_PATH.

    Returns
    -------
    pd.DataFrame
        Columns: date, ticker, plus all feature columns. Empty (zero-row,
        but correctly-columned) DataFrame if FEATURES_DAILY_DIR has no
        Parquet files at all or none match.
    """
    if not FEATURES_DAILY_DIR.exists() or not any(FEATURES_DAILY_DIR.glob("*.parquet")):
        return pd.DataFrame(columns=["date", "ticker"])

    glob_path = str(FEATURES_DAILY_DIR / "*.parquet")
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    with get_duckdb_connection(None, read_only=False, persist=True) as conn:
        query = (
            "SELECT * FROM read_parquet(?) "
            "WHERE ticker = ? AND date BETWEEN ? AND ? "
            "ORDER BY date ASC"
        )
        return conn.execute(query, [glob_path, ticker, start_str, end_str]).fetchdf()
