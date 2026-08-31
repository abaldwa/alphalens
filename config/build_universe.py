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

NOT sourced at build time — left as explicit placeholders, NOT fabricated:
  - market_cap_cr: NSE's free archives do not publish bulk market cap.
    Set to 0 for every row here. Two one-time passes backfill it:
      1. compute_market_cap_from_fundamentals() joins fundamentals'
         shares_outstanding (ingestion/scrapers/screener.py) to the
         latest close in ohlcv_adjusted.
      2. backfill_market_cap_from_screener_cache() recovers market_cap_cr
         directly from cached Screener.in HTML pages
         (datastore/raw/screener/{ticker}.html) for tickers pass 1 missed
         — Screener's page header scrapes market_cap_cr directly but
         export_company_data() previously discarded it after deriving
         shares_outstanding, so this re-parses the already-downloaded
         page rather than re-scraping.
    Both run via `python3 -m config.build_universe --refresh-market-cap`.
    Coverage as of 2026-07-02: 1,830/2,644 tickers non-zero. The
    remaining 814 either have no cached Screener page (162) or the
    cached page's own Market Cap field is blank (652, a scrape-time gap
    requiring a live re-scrape, not a parsing issue). Tickers still at
    market_cap_cr == 0 are treated by config/universe.py's phase_1 filter
    as "unknown, not below threshold" (unfiltered), same as before.
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
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

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

    tier_by_ticker: Dict[str, int] = {}
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


def compute_market_cap_from_fundamentals(
    universe_csv_path: Optional[Path] = None,
    db_path: Optional[Path] = None,
) -> pd.DataFrame:
    """
    Recompute market_cap_cr for every ticker from real scraped
    shares_outstanding (fundamentals) x latest close (ohlcv_adjusted),
    and rewrite the universe CSV in place.

    market_cap_cr (INR crore) = shares_outstanding * latest close / 1e7.

    Parameters
    ----------
    universe_csv_path : Path, optional
        Defaults to config.settings.UNIVERSE_CSV_PATH.
    db_path : Path, optional
        Defaults to config.settings.DUCKDB_PATH.

    Returns
    -------
    pd.DataFrame
        The rewritten universe DataFrame.

    Spec References
    ----------------
    SPEC-SYS-001, SPEC-SYS-011

    PIT Assumptions
    ----------------
    None — this computes a current snapshot statistic (latest known
    shares_outstanding x latest close), used only for universe
    filtering/display, never as a PIT feature value.

    Coverage
    --------
    Only tickers with at least one non-null shares_outstanding row in
    fundamentals (Screener.in-scraped, see ingestion/scrapers/screener.py)
    get a non-zero market_cap_cr. Tickers without it are left at 0
    (unchanged), same documented gap as before for that subset.

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
        mcap = conn.execute(
            """
            WITH latest_shares AS (
                SELECT ticker, shares_outstanding,
                       ROW_NUMBER() OVER (
                           PARTITION BY ticker ORDER BY announcement_date DESC
                       ) AS rn
                FROM fundamentals
                WHERE shares_outstanding IS NOT NULL
            ),
            latest_close AS (
                SELECT ticker, close,
                       ROW_NUMBER() OVER (
                           PARTITION BY ticker ORDER BY date DESC
                       ) AS rn
                FROM ohlcv_adjusted
            )
            SELECT s.ticker, (s.shares_outstanding * c.close) / 1e7 AS market_cap_cr
            FROM latest_shares s
            JOIN latest_close c ON c.ticker = s.ticker AND c.rn = 1
            WHERE s.rn = 1
            """
        ).df()

    df = df.merge(mcap, on="ticker", how="left", suffixes=("", "_new"))
    df["market_cap_cr"] = df["market_cap_cr_new"].fillna(df["market_cap_cr"])
    df = df.drop(columns="market_cap_cr_new")

    df[OUTPUT_COLUMNS].to_csv(universe_csv_path, index=False)
    n_updated = df["market_cap_cr"].gt(0).sum()
    logger.info(
        f"market_cap_cr updated for {n_updated}/{len(df)} tickers with scraped shares_outstanding"
    )
    return df[OUTPUT_COLUMNS]


def backfill_market_cap_from_screener_cache(
    universe_csv_path: Optional[Path] = None,
    raw_dir: Optional[Path] = None,
) -> pd.DataFrame:
    """
    Recover market_cap_cr directly from cached Screener.in HTML pages, for
    tickers compute_market_cap_from_fundamentals() couldn't fill.

    Root cause this addresses
    --------------------------
    ingestion/scrapers/screener.py's export_company_data() already scrapes
    "Market Cap" from every company page's header into a local
    market_cap_cr variable — but only uses it to *derive*
    shares_outstanding (market_cap_cr * 1e7 / current_price), then discards
    the raw value. When current_price was missing, or the ticker has no
    matching row in ohlcv_adjusted, the real scraped market cap never made
    it into fundamentals or the universe CSV at all, even though Screener
    provided it directly. This function re-parses the raw HTML (already
    downloaded to disk by a prior scrape, no new network calls) to recover
    that discarded value.

    Only fills rows still at market_cap_cr == 0 after
    compute_market_cap_from_fundamentals() — never overwrites a real
    non-zero value.

    Parameters
    ----------
    universe_csv_path : Path, optional
        Defaults to config.settings.UNIVERSE_CSV_PATH.
    raw_dir : Path, optional
        Defaults to ingestion.scrapers.screener.SCREENER_RAW_DIR.

    Returns
    -------
    pd.DataFrame
        The rewritten universe DataFrame.

    Coverage
    --------
    Bounded by how many universe tickers have a cached
    SCREENER_RAW_DIR/{ticker}.html page AND that page's header actually
    has a "Market Cap" stat. Tickers with neither a cached page nor a
    shares_outstanding-derived value stay at market_cap_cr == 0
    (documented gap, not fabricated).
    """
    from bs4 import BeautifulSoup

    from ingestion.scrapers.screener import (
        SCREENER_RAW_DIR,
        _HEADER_FIELDS,
        _parse_section_table,
    )

    universe_csv_path = universe_csv_path or UNIVERSE_CSV_PATH
    raw_dir = raw_dir or SCREENER_RAW_DIR
    if not universe_csv_path.exists():
        raise FileNotFoundError(f"{universe_csv_path} does not exist — run build_universe_csv() first")

    df = pd.read_csv(universe_csv_path)
    missing = df.loc[df["market_cap_cr"].fillna(0) <= 0, "ticker"]

    recovered: dict[str, float] = {}
    for ticker in missing:
        page = raw_dir / f"{ticker}.html"
        if not page.exists():
            continue
        soup = BeautifulSoup(page.read_text(encoding="utf-8"), "html.parser")
        header = _parse_section_table(soup, section_id=None, field_map=_HEADER_FIELDS, header_stats=True)
        mcap = header.get("market_cap_cr")
        if mcap:
            recovered[ticker] = mcap

    if recovered:
        is_missing = df["market_cap_cr"].fillna(0) <= 0
        recovered_series = df["ticker"].map(recovered)
        df.loc[is_missing, "market_cap_cr"] = recovered_series[is_missing].combine_first(
            df.loc[is_missing, "market_cap_cr"]
        )

    df[OUTPUT_COLUMNS].to_csv(universe_csv_path, index=False)
    n_updated = df["market_cap_cr"].gt(0).sum()
    logger.info(
        f"market_cap_cr recovered from cached Screener pages for "
        f"{len(recovered)} tickers ({n_updated}/{len(df)} total now non-zero)"
    )
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
    is_fno_eligible (2026-07-21 full-codebase-review REV14 fix) is now
    real, computed from fno_data's actual STO/STF trading activity in the
    trailing `active_days` window — see this function's Step 1 for detail
    — falling back to False only if fno_data isn't available at all.

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

    # [BUG FIX, 2026-07-21 full-codebase-review REV14] is_fno_eligible was
    # hardcoded False for every row because the standalone NSE
    # fo_mktlots.csv lot-size list this module's docstring describes is
    # broken (serves a PDF, not CSV). Real F&O eligibility doesn't need
    # that separate list at all: fno_db_path_for's fno_data table (real
    # NSE F&O bhavcopy, already ingested by ingestion/scrapers/fno.py —
    # confirmed 120M+ real rows spanning 2015-2026 in production) already
    # records every ticker with actual stock-option/stock-future trading
    # activity (instrument in STO/STF; IDO/IDF are index derivatives, not
    # per-ticker). Any ticker with real STO/STF rows in the trailing
    # window IS F&O eligible — a strictly better, always-available source
    # than a static lot-size list.
    fno_eligible_tickers: Set[str] = set()
    try:
        from datastore.api.db import fno_db_path_for

        fno_path = fno_db_path_for(str(db_path))
        if fno_path.exists():
            with get_duckdb_connection(fno_path, persist=False, read_only=True) as fno_conn:
                fno_df = fno_conn.execute(
                    """
                    SELECT DISTINCT ticker FROM fno_data
                    WHERE instrument IN ('STO', 'STF')
                      AND trade_date >= CURRENT_DATE - INTERVAL (?) DAY
                    """,
                    [active_days],
                ).df()
            fno_eligible_tickers = set(fno_df["ticker"])
            logger.info("fno_data: %d F&O-eligible tickers (active_days=%d)", len(fno_eligible_tickers), active_days)
        else:
            logger.warning("fno_data not found at %s — is_fno_eligible left False for every ticker", fno_path)
    except Exception as exc:
        logger.warning("Could not read fno_data for is_fno_eligible (%s) — left False for every ticker", exc)

    # --- Step 2: fetch Nifty 500 constituent list for enrichment ---
    try:
        nifty500 = _fetch_index_csv(NSE_NIFTY500_URL)
        nifty500["Symbol"] = nifty500["Symbol"].str.strip()

        tier_by_ticker: Dict[str, int] = {}
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
    merged["is_fno_eligible"] = merged["ticker"].isin(fno_eligible_tickers)

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


def build_historical_universe_from_delisted(
    db_path: Optional[Path] = None,
    include_since_year: Optional[int] = None,
    conn: Any = None,
) -> List[str]:
    """
    True historical candidate ticker pool for a momentum/cross-sectional
    backtest (2026-07-19 full-codebase-review Fix A4): the union of
    today's active-universe tickers (config.universe.load_universe_raw())
    with every ticker in the `delisted_companies` table (see
    ingestion/scrapers/nse_delisted_companies.py — NOTE that scraper's
    target endpoint is UNVERIFIED in this environment; this table may be
    empty or its contents unconfirmed until that scraper has been run
    from an environment with real NSE access).

    Closes the survivorship-bias gap `features/momentum_universe.py`'s
    `_all_candidate_tickers()` has by default: the current-snapshot
    universe CSV alone permanently excludes any ticker that delisted,
    merged, or was suspended before the CSV was last rebuilt, even for
    historical dates when that ticker legitimately belonged in a tracked
    market-cap band.

    Parameters
    ----------
    db_path : Path, optional
        Defaults to config.settings.DUCKDB_PATH. Ignored if `conn` is given.
    include_since_year : int, optional
        If set, only delisted_companies rows with delisting_date in or
        after this year are included (e.g. to bound a 10-year backtest's
        candidate pool to tickers that could plausibly have appeared in
        it). None (default) includes every delisted ticker regardless of
        delisting date.
    conn : an already-open DuckDB connection to reuse, optional (2026-07-20
        fix). Prefer this over db_path whenever the caller already has a
        connection open against the same file — DuckDB only allows one
        read-write connection OR multiple read-only connections per file,
        and this function previously always opened its OWN connection
        with default (read-write, cached) settings regardless of what the
        caller already had open. In production that caller (momentum_
        universe.py's full_rank_universe(), invoked with an
        already-open read_only=True/persist=False connection to the SAME
        live DUCKDB_PATH) would hit exactly this conflict the moment
        include_delisted=True was actually used — caught by a test
        seeding a real delisted_companies row rather than in production.

    Returns
    -------
    list of str
        Deduplicated ticker symbols: today's active universe + eligible
        delisted tickers. Empty delisted_companies table (not yet
        scraped) degrades gracefully to just today's active universe —
        never raises just because the table is empty.
    """
    from config.settings import DUCKDB_PATH
    from config.universe import load_universe_raw

    active_tickers = set(load_universe_raw()["ticker"])

    def _query(c: Any) -> Optional[pd.DataFrame]:
        query = "SELECT ticker, delisting_date FROM delisted_companies"
        params: List[date] = []
        if include_since_year is not None:
            query += " WHERE delisting_date >= ?"
            params.append(date(include_since_year, 1, 1))
        return c.execute(query, params).df()

    delisted_tickers: Set[str] = set()
    if conn is not None:
        try:
            rows = _query(conn)
            delisted_tickers = set(rows["ticker"]) if rows is not None and not rows.empty else set()
        except Exception as exc:
            logger.warning(
                "build_historical_universe_from_delisted: could not read delisted_companies "
                "via the supplied connection (table may not exist yet) — falling back to "
                "active universe only: %s", exc,
            )
    else:
        from datastore.api.db import get_duckdb_connection

        db_path = db_path or DUCKDB_PATH
        if db_path.exists():
            try:
                with get_duckdb_connection(db_path, read_only=True, persist=False) as c:
                    rows = _query(c)
                delisted_tickers = set(rows["ticker"]) if rows is not None and not rows.empty else set()
            except Exception as exc:
                logger.warning(
                    "build_historical_universe_from_delisted: could not read delisted_companies "
                    "(table may not exist yet) — falling back to active universe only: %s", exc,
                )

    combined = sorted(active_tickers | delisted_tickers)
    # NOTE: Disabled verbose logging here (2026-08-31) — when called in a tight loop
    # (as triggered by certain weight_method values), this log statement creates
    # massive I/O overhead that kills jobs. Root cause of the loop is TBD.
    # logger.info(
    #     "Historical universe: %d active + %d delisted-only = %d total tickers",
    #     len(active_tickers), len(delisted_tickers - active_tickers), len(combined),
    # )
    return combined


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
    parser.add_argument(
        "--refresh-adtv",
        action="store_true",
        help="Recompute adtv_cr from ohlcv_adjusted in place (requires price backfill to have run).",
    )
    parser.add_argument(
        "--refresh-market-cap",
        action="store_true",
        help=(
            "Recompute market_cap_cr from fundamentals.shares_outstanding x latest close "
            "in place (requires fundamentals scraping to have run), then recover any "
            "still-zero rows directly from cached Screener.in HTML pages "
            "(see backfill_market_cap_from_screener_cache)."
        ),
    )
    args = parser.parse_args()

    if args.full_nse:
        build_full_nse_universe_from_db(active_days=args.active_days)
    elif not (args.refresh_adtv or args.refresh_market_cap):
        build_universe_csv()

    if args.refresh_adtv:
        compute_adtv_from_ohlcv()
    if args.refresh_market_cap:
        compute_market_cap_from_fundamentals()
        backfill_market_cap_from_screener_cache()
