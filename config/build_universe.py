"""
config/build_universe.py

Phase: 0.5 (FYERS Historical Backfill)
Specs: SPEC-SYS-001, SPEC-SYS-011
Owner: Platform / DataStore
Consumers: config/nifty500_universe.csv (one-time/periodic generator, not imported at runtime)

Two universe-building strategies:

1. build_universe_csv() — Nifty 500 only (~500 stocks)
   Fetches NSE's official index constituent lists from NSE archives.
   Use this when you only want the Nifty 500 universe (phase_1 profile).

2. build_full_nse_universe_from_db() — Full active NSE universe (~2492 stocks)
   Pulls all active tickers from ohlcv_adjusted in the DuckDB database,
   merges with Nifty 500 constituent data for tier/sector/ISIN enrichment,
   and assigns tier=6 for non-Nifty500 stocks (broader NSE active universe).
   Use this for full_nse profile training on all 2492 stocks.

Sourced for real, from NSE archives (verified live):
  - ticker, company_name, sector, is_nifty500  <- ind_nifty500list.csv
  - tier                                       <- membership in NSE's own
    Nifty 50 / Next 50 / Midcap 150 / Smallcap 250 index lists (tier 1-4);
    every other Nifty 500 member is tier 5; non-Nifty500 DB tickers = tier 6.

NOT sourced — left as explicit placeholders, NOT fabricated:
  - market_cap_cr: NSE's free archives do not publish bulk market cap.
    Set to 0 for every row. config/universe.py's phase_1 filter normally
    requires market_cap_cr >= MIN_MCAP_CR (500cr) — left as 0/unfiltered
    here per explicit operator decision (see BuildLog.md "Universe
    expansion" entry); MUST be backfilled from a real source before this
    filter should be trusted again.
  - adtv_cr: requires actual traded volume history, which doesn't exist
    until after the price backfill runs. Set to 0 here; a separate
    one-time pass (compute_adtv_from_ohlcv() in this module) fills it in
    AFTER ingestion/backfill_runner.py has downloaded price/volume data.
  - is_fno_eligible: NSE's free fo_mktlots.csv archive now serves a PDF,
    not the CSV this project's ingestion/scrapers/fno.py expects (a
    separate, pre-existing bug in fno.py's URL — F&O bhavcopy fetch
    returns 404 against current NSE archive paths). Rather than
    fabricate this, every row defaults to False with a documented gap.
"""

import logging
from pathlib import Path
from typing import Optional

import pandas as pd

from config.settings import UNIVERSE_CSV_PATH
from ingestion.scrapers.bhavcopy import _nse_session

logger = logging.getLogger(__name__)

NSE_INDEX_LIST_URLS = {
    1: "https://archives.nseindia.com/content/indices/ind_nifty50list.csv",
    2: "https://archives.nseindia.com/content/indices/ind_niftynext50list.csv",
    3: "https://archives.nseindia.com/content/indices/ind_niftymidcap150list.csv",
    4: "https://archives.nseindia.com/content/indices/ind_niftysmallcap250list.csv",
}
NSE_NIFTY500_URL = "https://archives.nseindia.com/content/indices/ind_nifty500list.csv"

OUTPUT_COLUMNS = [
    "ticker", "company_name", "sector", "tier",
    "market_cap_cr", "adtv_cr", "is_fno_eligible", "is_nifty500", "isin",
]


def _fetch_index_csv(url: str) -> pd.DataFrame:
    """Fetch one NSE index-constituent CSV (Company Name, Industry, Symbol, Series, ISIN Code)."""
    session = _nse_session()
    response = session.get(url, timeout=15)
    response.raise_for_status()
    import io

    df = pd.read_csv(io.StringIO(response.text))
    df.columns = [c.strip() for c in df.columns]
    return df


def build_universe_csv(output_path: Optional[Path] = None) -> pd.DataFrame:
    """
    Build the full Nifty 500 universe CSV from NSE's official index lists.

    Parameters
    ----------
    output_path : Path, optional
        Defaults to config.settings.UNIVERSE_CSV_PATH. The existing file
        (if any — e.g. the 20-ticker starter sample) is overwritten.

    Returns
    -------
    pd.DataFrame
        The written DataFrame, OUTPUT_COLUMNS.

    Spec References
    ----------------
    SPEC-SYS-001: Universe coverage source.
    SPEC-SYS-011: Configurable universe expansion.

    PIT Assumptions
    ----------------
    None — index membership is a current snapshot, not a PIT-joined field.
    Re-running this later will reflect NSE's *current* index membership,
    not a historical one (acceptable for SPEC-SYS-011's quarterly tier
    review cadence; not acceptable as a substitute for point-in-time
    index membership in a backtest).

    Raises
    ------
    requests.RequestException
        If any NSE archive fetch fails (not retried — re-run the whole
        function; this is a manual/periodic operator action, not a
        production pipeline step subject to SPEC-PIPE-001's retry rules).
    """
    nifty500 = _fetch_index_csv(NSE_NIFTY500_URL)
    nifty500["Symbol"] = nifty500["Symbol"].str.strip()

    tier_by_ticker = {}
    for tier, url in NSE_INDEX_LIST_URLS.items():
        df = _fetch_index_csv(url)
        for ticker in df["Symbol"].str.strip():
            tier_by_ticker.setdefault(ticker, tier)
        logger.info(f"tier {tier}: {len(df)} constituents fetched")

    out = pd.DataFrame(
        {
            "ticker": nifty500["Symbol"],
            "company_name": nifty500["Company Name"],
            "sector": nifty500["Industry"],
            "tier": nifty500["Symbol"].map(lambda t: tier_by_ticker.get(t, 5)),
            "market_cap_cr": 0,
            "adtv_cr": 0,
            "is_fno_eligible": False,
            "is_nifty500": True,
            "isin": nifty500["ISIN Code"],
        }
    ).drop_duplicates(subset="ticker").reset_index(drop=True)

    output_path = output_path or UNIVERSE_CSV_PATH
    out[OUTPUT_COLUMNS].to_csv(output_path, index=False)
    logger.info(f"Wrote {len(out)} tickers to {output_path}")
    return out[OUTPUT_COLUMNS]


def compute_adtv_from_ohlcv(
    universe_csv_path: Optional[Path] = None,
    db_path: Optional[Path] = None,
    window_days: int = 20,
) -> pd.DataFrame:
    """
    Recompute adtv_cr for every ticker from real downloaded OHLCV history,
    and rewrite the universe CSV in place.

    ADTV (avg daily traded value, INR crore) = mean(close * volume) over
    the most recent `window_days` rows in ohlcv_adjusted, / 1e7 (crore).

    Parameters
    ----------
    universe_csv_path : Path, optional
        Defaults to config.settings.UNIVERSE_CSV_PATH.
    db_path : Path, optional
        Defaults to config.settings.DUCKDB_PATH.
    window_days : int
        Trailing window for the average, in trading days.

    Returns
    -------
    pd.DataFrame
        The rewritten universe DataFrame.

    Spec References
    ----------------
    SPEC-SYS-001, SPEC-SYS-011

    PIT Assumptions
    ----------------
    None — this computes a current snapshot statistic (most recent
    `window_days`), used only for universe filtering, never as a PIT
    feature value.

    Raises
    ------
    FileNotFoundError
        If the universe CSV does not exist yet (run build_universe_csv()
        first).
    """
    from config.settings import DUCKDB_PATH
    from datastore.api.db import get_duckdb_connection

    universe_csv_path = universe_csv_path or UNIVERSE_CSV_PATH
    if not universe_csv_path.exists():
        raise FileNotFoundError(f"{universe_csv_path} does not exist — run build_universe_csv() first")

    df = pd.read_csv(universe_csv_path)

    with get_duckdb_connection(db_path or DUCKDB_PATH) as conn:
        adtv = conn.execute(
            """
            SELECT ticker, AVG(close * volume) / 1e7 AS adtv_cr
            FROM (
                SELECT ticker, close, volume,
                       ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
                FROM ohlcv_adjusted
            )
            WHERE rn <= ?
            GROUP BY ticker
            """,
            [window_days],
        ).df()

    df = df.merge(adtv, on="ticker", how="left", suffixes=("", "_new"))
    df["adtv_cr"] = df["adtv_cr_new"].fillna(df["adtv_cr"])
    df = df.drop(columns="adtv_cr_new")

    df[OUTPUT_COLUMNS].to_csv(universe_csv_path, index=False)
    n_updated = df["adtv_cr"].gt(0).sum()
    logger.info(f"adtv_cr updated for {n_updated}/{len(df)} tickers with downloaded price history")
    return df[OUTPUT_COLUMNS]


def build_full_nse_universe_from_db(
    output_path: Optional[Path] = None,
    db_path: Optional[Path] = None,
    active_days: int = 90,
) -> pd.DataFrame:
    """
    Build the full active NSE universe CSV (~2492 stocks) from the DuckDB database.

    Pulls every ticker present in ohlcv_adjusted with at least one row in the
    most recent `active_days` calendar days (proxy for "not delisted/suspended").
    Merges with Nifty 500 constituent data fetched from NSE archives to enrich
    tier, sector, company_name, isin, and is_nifty500; tickers not in the Nifty
    500 list get tier=6, is_nifty500=False, and empty company_name/sector/isin
    (to be backfilled via stock_master once available).

    ADTV is computed from the trailing `active_days` window in ohlcv_adjusted.
    market_cap_cr is left as 0 (same documented gap as build_universe_csv).
    is_fno_eligible is left as False (same documented gap as build_universe_csv).

    Parameters
    ----------
    output_path : Path, optional
        Defaults to config.settings.UNIVERSE_CSV_PATH. Overwrites the existing file.
    db_path : Path, optional
        Defaults to config.settings.DUCKDB_PATH.
    active_days : int
        Calendar-day lookback window to determine active tickers. Default 90.

    Returns
    -------
    pd.DataFrame
        The written DataFrame, OUTPUT_COLUMNS.

    Spec References
    ----------------
    SPEC-SYS-001, SPEC-SYS-011

    Raises
    ------
    FileNotFoundError
        If the DuckDB database does not exist.
    """
    from config.settings import DUCKDB_PATH
    from datastore.api.db import get_duckdb_connection

    db_path = db_path or DUCKDB_PATH
    if not db_path.exists():
        raise FileNotFoundError(f"DuckDB not found at {db_path} — run backfill_runner.py first")

    # --- Step 1: all active tickers + ADTV from ohlcv_adjusted ---
    with get_duckdb_connection(db_path) as conn:
        active_df = conn.execute(
            """
            SELECT
                ticker,
                AVG(close * volume) / 1e7 AS adtv_cr,
                COUNT(*) AS n_rows
            FROM ohlcv_adjusted
            WHERE date >= CURRENT_DATE - INTERVAL (?) DAY
            GROUP BY ticker
            HAVING COUNT(*) >= 5
            ORDER BY ticker
            """,
            [active_days],
        ).df()

    logger.info("ohlcv_adjusted: %d active tickers (active_days=%d)", len(active_df), active_days)

    # --- Step 2: fetch Nifty 500 constituent list for enrichment ---
    try:
        nifty500 = _fetch_index_csv(NSE_NIFTY500_URL)
        nifty500["Symbol"] = nifty500["Symbol"].str.strip()

        tier_by_ticker: dict = {}
        for tier, url in NSE_INDEX_LIST_URLS.items():
            idx_df = _fetch_index_csv(url)
            for sym in idx_df["Symbol"].str.strip():
                tier_by_ticker.setdefault(sym, tier)
            logger.info("tier %d: %d constituents fetched", tier, len(idx_df))

        n500_meta = pd.DataFrame(
            {
                "ticker": nifty500["Symbol"],
                "company_name": nifty500["Company Name"],
                "sector": nifty500["Industry"],
                "isin": nifty500["ISIN Code"],
                "is_nifty500": True,
                "tier": nifty500["Symbol"].map(lambda t: tier_by_ticker.get(t, 5)),
            }
        ).drop_duplicates(subset="ticker")
    except Exception as exc:
        logger.warning("NSE archive fetch failed (%s) — using Nifty 500 enrichment from existing CSV", exc)
        existing_path = output_path or UNIVERSE_CSV_PATH
        if existing_path.exists():
            existing = pd.read_csv(existing_path)
            n500_meta = existing[existing["is_nifty500"]][
                ["ticker", "company_name", "sector", "isin", "is_nifty500", "tier"]
            ]
        else:
            n500_meta = pd.DataFrame(
                columns=["ticker", "company_name", "sector", "isin", "is_nifty500", "tier"]
            )

    # --- Step 3: merge DB tickers with Nifty 500 metadata ---
    merged = active_df[["ticker", "adtv_cr"]].merge(n500_meta, on="ticker", how="left")

    # Non-Nifty500 stocks: tier=6, is_nifty500=False, blanks for meta fields
    merged["tier"] = merged["tier"].fillna(6).astype(int)
    merged["is_nifty500"] = merged["is_nifty500"].fillna(False)
    merged["company_name"] = merged["company_name"].fillna("")
    merged["sector"] = merged["sector"].fillna("")
    merged["isin"] = merged["isin"].fillna("")
    merged["market_cap_cr"] = 0
    merged["is_fno_eligible"] = False

    out = merged[OUTPUT_COLUMNS].drop_duplicates(subset="ticker").reset_index(drop=True)

    output_path = output_path or UNIVERSE_CSV_PATH
    out.to_csv(output_path, index=False)
    n_nifty500 = out["is_nifty500"].sum()
    n_broader = (~out["is_nifty500"]).sum()
    logger.info(
        "Wrote %d tickers to %s (%d Nifty500 tier1-5, %d broader NSE tier6)",
        len(out), output_path, n_nifty500, n_broader,
    )
    return out


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Build the universe CSV from NSE archives or DuckDB.")
    parser.add_argument(
        "--full-nse",
        action="store_true",
        help=(
            "Build the full ~2492-stock active NSE universe from ohlcv_adjusted in DuckDB "
            "(requires backfill to have run). Default: Nifty 500 only from NSE archives."
        ),
    )
    parser.add_argument(
        "--active-days",
        type=int,
        default=90,
        help="Days lookback to consider a ticker active (only used with --full-nse). Default: 90.",
    )
    args = parser.parse_args()

    if args.full_nse:
        build_full_nse_universe_from_db(active_days=args.active_days)
    else:
        build_universe_csv()
