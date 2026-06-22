"""
config/universe.py

Phase: 0
Specs: SPEC-SYS-001, SPEC-SYS-011, SPEC-DS-001
Owner: Platform / DataStore
Consumers: ingestion, features, systems/ml_signal_engine, backtest

Loads the stock universe from config/nifty500_universe.csv and applies the
tier/ADTV/market-cap filters defined by the active UNIVERSE_PROFILE in
config/settings.py. The universe is query-driven, not hardcoded (SPEC-SYS-011):
expanding from phase_1 to phase_2/phase_3/full_nse is a config change only,
never a code change.

NOTE: config/nifty500_universe.csv shipped with this skeleton is a small
STARTER sample, not the full Nifty 500 constituent list. Replace it with the
official list (downloadable from the NSE archives) before running the pipeline.
"""

import logging

import pandas as pd

from config.settings import MIN_ADTV_CR, MIN_MCAP_CR, TIER_THRESHOLD, UNIVERSE_CSV_PATH

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = [
    "ticker",
    "company_name",
    "sector",
    "tier",
    "market_cap_cr",
    "adtv_cr",
    "is_fno_eligible",
    "is_nifty500",
]


def load_universe_raw() -> pd.DataFrame:
    """
    Load the full unfiltered stock master CSV.

    Parameters
    ----------
    None

    Returns
    -------
    pd.DataFrame
        One row per ticker with REQUIRED_COLUMNS.

    Spec References
    ----------------
    SPEC-SYS-001: Universe coverage source.

    PIT Assumptions
    ----------------
    None — stock_master is a slowly-changing reference table, not a PIT join.

    Raises
    ------
    FileNotFoundError
        If config/nifty500_universe.csv does not exist.
    ValueError
        If required columns are missing from the CSV.
    """
    if not UNIVERSE_CSV_PATH.exists():
        raise FileNotFoundError(
            f"Universe CSV not found at {UNIVERSE_CSV_PATH}. "
            "Populate it with the official Nifty 500 constituent list."
        )

    df = pd.read_csv(UNIVERSE_CSV_PATH)

    missing = set(REQUIRED_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"Universe CSV is missing required columns: {sorted(missing)}")

    return df


def load_universe() -> pd.DataFrame:
    """
    Load the stock universe filtered by the active UNIVERSE_PROFILE.

    Parameters
    ----------
    None

    Returns
    -------
    pd.DataFrame
        Filtered to tier <= TIER_THRESHOLD, adtv_cr >= MIN_ADTV_CR,
        market_cap_cr >= MIN_MCAP_CR, per config/settings.py. A row with
        market_cap_cr == 0 ("not yet sourced" — see config/build_universe.py's
        module docstring) is treated as PASSING the market-cap filter
        rather than failing it; market_cap_cr == 0 means "unknown", not
        "definitely below the threshold", and treating unknown as a hard
        exclusion would silently drop every ticker for which market cap
        hasn't been backfilled yet rather than flag the gap. This is an
        explicit, documented, temporary relaxation (operator decision —
        see BuildLog.md "Universe expansion" entry) — once real
        market_cap_cr values are backfilled for all tickers, every row
        will go through the normal >= MIN_MCAP_CR check as before.

    Spec References
    ----------------
    SPEC-SYS-001: Universe coverage.
    SPEC-SYS-011: Configurable universe expansion via UNIVERSE_PROFILE.

    PIT Assumptions
    ----------------
    None — stock_master is a slowly-changing reference table, not a PIT join.

    Raises
    ------
    FileNotFoundError
        If config/nifty500_universe.csv does not exist.
    ValueError
        If required columns are missing from the CSV.
    """
    df = load_universe_raw()

    # adtv_cr == 0 and market_cap_cr == 0 both mean "not yet sourced" (see
    # config/build_universe.py), not "definitely below threshold" — treated
    # as passing rather than excluded, same reasoning for both columns.
    # Without this, a freshly-built universe (every row starts at 0 for
    # both, before ingestion/backfill_runner.py has downloaded any price
    # history to compute real adtv_cr from) would filter out 100% of
    # tickers, including get_tickers() returning [] — which would mean
    # backfill_runner.py never downloads anything in the first place.
    market_cap_known_and_sufficient = df["market_cap_cr"] >= MIN_MCAP_CR
    market_cap_not_yet_sourced = df["market_cap_cr"] == 0
    adtv_known_and_sufficient = df["adtv_cr"] >= MIN_ADTV_CR
    adtv_not_yet_sourced = df["adtv_cr"] == 0

    filtered = df[
        (df["tier"] <= TIER_THRESHOLD)
        & (adtv_known_and_sufficient | adtv_not_yet_sourced)
        & (market_cap_known_and_sufficient | market_cap_not_yet_sourced)
    ].copy()

    logger.info(
        "Universe resolved: %d/%d stocks pass filters (tier<=%s, adtv_cr>=%s, mcap_cr>=%s)",
        len(filtered),
        len(df),
        TIER_THRESHOLD,
        MIN_ADTV_CR,
        MIN_MCAP_CR,
    )

    return filtered


def get_tickers() -> list[str]:
    """
    Return the filtered universe as a flat list of ticker symbols.

    Parameters
    ----------
    None

    Returns
    -------
    list[str]

    Spec References
    ----------------
    SPEC-SYS-001, SPEC-SYS-011

    PIT Assumptions
    ----------------
    None.

    Raises
    ------
    FileNotFoundError
        If config/nifty500_universe.csv does not exist.
    ValueError
        If required columns are missing from the CSV.
    """
    return load_universe()["ticker"].tolist()
