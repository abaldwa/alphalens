"""
config/build_universe.py

Phase: 0.5 (FYERS Historical Backfill)
Specs: SPEC-SYS-001, SPEC-SYS-011
Owner: Platform / DataStore
Consumers: config/nifty500_universe.csv (one-time/periodic generator, not imported at runtime)

Builds config/nifty500_universe.csv from NSE's official index constituent
lists — replacing the 20-ticker starter sample with the real ~500-stock
Nifty 500, per config/universe.py's own docstring ("Replace it with the
official list... before running the pipeline").

Sourced for real, from NSE archives (verified live):
  - ticker, company_name, sector, is_nifty500  <- ind_nifty500list.csv
  - tier                                       <- membership in NSE's own
    Nifty 50 / Next 50 / Midcap 150 / Smallcap 250 index lists (tier 1-4);
    every other Nifty 500 member is tier 5.

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

from config.settings import RAW_DIR, UNIVERSE_CSV_PATH
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
    "market_cap_cr", "adtv_cr", "is_fno_eligible", "is_nifty500",
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


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    build_universe_csv()
