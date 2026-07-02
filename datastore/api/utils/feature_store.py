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

from typing import Optional

import pandas as pd

from config.settings import FEATURES_DAILY_DIR


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
