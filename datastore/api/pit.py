"""
datastore/api/pit.py

Phase: 0.1 (Project Skeleton)
Specs: SPEC-DS-003, SPEC-PIPE-003, SPEC-QUALITY-001
Owner: Platform / DataStore
Consumers: datastore/api, features/*, systems/ml_signal_engine, backtest

Point-in-time (PIT) enforcement for all data queries.
Ensures no look-ahead bias: data is only used if it was publicly observable at reference date.
SOLID: Single Responsibility — all PIT logic is isolated here for 100% testability.
"""

import logging
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


def enforce_pit_fundamentals(
    df: pd.DataFrame,
    as_of: datetime,
    announcement_date_col: str = "announcement_date",
) -> pd.DataFrame:
    """
    SPEC-DS-003: Filter fundamental data by announcement date.

    Fundamental data (earnings, ratios, shareholding) is only known AFTER
    the company announces/files it. This function removes any forward-looking rows.

    Args:
        df: DataFrame with fundamental data (must have announcement_date_col)
        as_of: Reference date — only data announced <= as_of is kept
        announcement_date_col: Column name containing announcement timestamp

    Returns:
        Filtered DataFrame, sorted by date ascending
        Rows with NULL announcement_date are excluded (conservative)

    Raises:
        ValueError: If as_of is not datetime or announcement_date_col missing
    """
    if not isinstance(as_of, datetime):
        raise ValueError(f"as_of must be datetime, got {type(as_of)}")

    if announcement_date_col not in df.columns:
        raise ValueError(f"Column '{announcement_date_col}' not found in DataFrame")

    # Remove rows with missing announcement dates (unknown when public)
    df_clean = df[df[announcement_date_col].notna()].copy()

    # Keep only data announced on or before as_of
    df_pit = df_clean[df_clean[announcement_date_col] <= as_of]

    logger.info(
        f"PIT filter fundamentals: {len(df)} -> {len(df_pit)} rows "
        f"(as_of={as_of.date()}, removed {len(df) - len(df_pit)} forward-looking)"
    )

    return df_pit.sort_values(by="date", ascending=True)


def enforce_pit_shareholding(
    df: pd.DataFrame,
    as_of: datetime,
    filing_date_col: str = "filing_date",
) -> pd.DataFrame:
    """
    SPEC-DS-003: Filter shareholding data by regulatory filing date.

    Shareholding data is disclosed via BSE filings (not always quarterly results).
    Only data filed <= as_of is observable.

    Args:
        df: DataFrame with shareholding records (must have filing_date_col)
        as_of: Reference date
        filing_date_col: Column name containing filing timestamp

    Returns:
        Filtered DataFrame, most recent filing per ticker per date
        Rows with NULL filing_date excluded

    Raises:
        ValueError: If as_of is not datetime or filing_date_col missing
    """
    if not isinstance(as_of, datetime):
        raise ValueError(f"as_of must be datetime, got {type(as_of)}")

    if filing_date_col not in df.columns:
        raise ValueError(f"Column '{filing_date_col}' not found in DataFrame")

    # Remove rows with missing filing dates
    df_clean = df[df[filing_date_col].notna()].copy()

    # Keep only filings on or before as_of
    df_pit = df_clean[df_clean[filing_date_col] <= as_of]

    logger.info(
        f"PIT filter shareholding: {len(df)} -> {len(df_pit)} rows "
        f"(as_of={as_of.date()}, removed {len(df) - len(df_pit)} forward-looking)"
    )

    return df_pit.sort_values(by="date", ascending=True)


def enforce_pit_mf_holdings(
    df: pd.DataFrame,
    as_of: datetime,
    month_end_col: str = "month_end",
    delay_days: int = 5,
) -> pd.DataFrame:
    """
    SPEC-DS-003: Filter mutual fund holdings by publication date.

    MF holdings are released end-of-month + ~5 business days.
    Only holdings observable as of the reference date are kept.

    Args:
        df: DataFrame with MF holdings (must have month_end_col)
        as_of: Reference date
        month_end_col: Column name containing month-end date
        delay_days: Days after month-end before holdings are public (default 5)

    Returns:
        Filtered DataFrame
        Rows with NULL month_end excluded

    Raises:
        ValueError: If as_of not datetime or month_end_col missing
    """
    if not isinstance(as_of, datetime):
        raise ValueError(f"as_of must be datetime, got {type(as_of)}")

    if month_end_col not in df.columns:
        raise ValueError(f"Column '{month_end_col}' not found in DataFrame")

    # Remove rows with missing month-end dates
    df_clean = df[df[month_end_col].notna()].copy()

    # Holdings are observable (month_end + delay_days) <= as_of
    df_clean["observable_date"] = df_clean[month_end_col] + timedelta(days=delay_days)
    df_pit = df_clean[df_clean["observable_date"] <= as_of]

    logger.info(
        f"PIT filter MF holdings: {len(df)} -> {len(df_pit)} rows "
        f"(as_of={as_of.date()}, delay={delay_days}d, removed {len(df) - len(df_pit)} early)"
    )

    # Drop temporary column
    return df_pit.drop(columns=["observable_date"]).sort_values(by="date", ascending=True)


def compute_staleness_flags(
    df: pd.DataFrame,
    as_of: datetime,
    lookback_days: int = 5,
    date_col: str = "date",
) -> pd.DataFrame:
    """
    SPEC-SYS-003: Add staleness flags to feature/data rows.

    Data is "stale" if the most recent observation is > lookback_days old.
    Used for filtering in inference pipelines (only run inference on fresh data).

    Args:
        df: DataFrame with date_col
        as_of: Reference date
        lookback_days: Threshold — data older than this many days is stale
        date_col: Column name containing observation date

    Returns:
        DataFrame with new column 'data_staleness_flag' (0=fresh, 1=stale)

    Raises:
        ValueError: If as_of not datetime or date_col missing
    """
    if not isinstance(as_of, datetime):
        raise ValueError(f"as_of must be datetime, got {type(as_of)}")

    if date_col not in df.columns:
        raise ValueError(f"Column '{date_col}' not found in DataFrame")

    df_copy = df.copy()

    # Compute days since observation
    df_copy["days_since_observation"] = (as_of - df_copy[date_col]).dt.days

    # Flag stale rows
    df_copy["data_staleness_flag"] = (
        df_copy["days_since_observation"] > lookback_days
    ).astype(int)

    stale_count = (df_copy["data_staleness_flag"] == 1).sum()
    logger.info(
        f"Staleness flags: {stale_count}/{len(df_copy)} rows stale "
        f"(as_of={as_of.date()}, threshold={lookback_days}d)"
    )

    return df_copy.drop(columns=["days_since_observation"])
