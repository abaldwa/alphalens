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
from typing import Any, List

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

    return df_pit.sort_values(by=announcement_date_col, ascending=True)


def get_fundamentals_pit(conn: Any, tickers: List[str], as_of: datetime) -> pd.DataFrame:
    """
    2026-07-20 (BacktestUmbrellaPlan.md Truthful Review Gap #2 fix):
    genuinely point-in-time fundamentals, reading the append-only
    `fundamentals_history` table (datastore/schema/create_normalised.py,
    features/fundamental_source_priority.append_fundamentals_history)
    instead of the live, mutable `fundamentals` table.

    Filters on BOTH real PIT keys:
    - `announcement_date <= as_of` (SPEC-DS-003, same filter
      enforce_pit_fundamentals already applies)
    - `recorded_at <= as_of` (NEW — when this snapshot was actually
      written to the DB). Without this second filter, a restatement
      recorded today for FY2018Q1 would leak into a backtest run "as of"
      2018 — the exact lookahead-bias bug this function exists to close.

    Takes the LATEST `recorded_at` snapshot per (ticker, fiscal_year,
    quarter) among rows satisfying both filters — i.e. "the most
    up-to-date value a trader could have actually known as of `as_of`",
    not the first-ever or the globally-latest value.

    Parameters
    ----------
    conn : an open DuckDB connection (any read mode).
    tickers : candidate ticker list to restrict the query to.
    as_of : reference date — both PIT filters above are applied against it.

    Returns
    -------
    pd.DataFrame
        One row per (ticker, fiscal_year, quarter) that was knowable as
        of `as_of`, using each such quarter's latest as-of-`as_of`
        snapshot. Empty DataFrame (with no columns) if `tickers` is empty
        or `fundamentals_history` has no matching rows — never raises on
        missing data, consistent with the No-Mock-Data Policy (missing
        real data is reported as absence, not fabricated).
    """
    if not isinstance(as_of, datetime):
        raise ValueError(f"as_of must be datetime, got {type(as_of)}")
    if not tickers:
        return pd.DataFrame()

    placeholders = ",".join("?" for _ in tickers)
    df = conn.execute(
        f"""
        SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY ticker, fiscal_year, quarter ORDER BY recorded_at DESC
            ) AS rn
            FROM fundamentals_history
            WHERE ticker IN ({placeholders})
              AND CAST(announcement_date AS DATE) <= ?
              AND CAST(recorded_at AS TIMESTAMP) <= ?
        )
        WHERE rn = 1
        """,
        list(tickers) + [as_of.date(), as_of],
    ).df()

    if df.empty:
        return df
    return df.drop(columns=["rn", "history_id", "recorded_at"]).sort_values(
        by="announcement_date", ascending=True
    ).reset_index(drop=True)


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

    return df_pit.sort_values(by=filing_date_col, ascending=True)


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
    return df_pit.drop(columns=["observable_date"]).sort_values(by=month_end_col, ascending=True)


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
