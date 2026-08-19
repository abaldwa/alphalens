"""
config/universe.py

Phase: 0
Specs: SPEC-SYS-001, SPEC-SYS-011, SPEC-DS-001
Owner: Platform / DataStore
Consumers: ingestion, features, systems/ml_signal_engine, backtest

Loads the stock universe from config/nifty500_universe.csv and applies the
tier/ADTV/market-cap filters defined by the active UNIVERSE_PROFILE in
config/settings.py. The universe is query-driven, not hardcoded (SPEC-SYS-011):
expanding from phase_1 (Nifty 500, ~501 stocks) to full_nse (~2492 active
NSE stocks) is a config change only, never a code change.

Tier scheme (assigned by config/build_universe.py):
  1 = Nifty 50, 2 = NiftyNext50, 3 = Midcap150, 4 = Smallcap250,
  5 = every other Nifty 500 member, 6 = broader NSE active (non-Nifty500).

To populate the CSV with all 2492 active stocks, run:
  python -m config.build_universe --full-nse

To populate with Nifty 500 only (phase_1), run:
  python -m config.build_universe
"""

import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

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
    "isin",
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
            "Run: python -m config.build_universe --full-nse  (for all 2492 active NSE stocks) "
            "or: python -m config.build_universe  (for Nifty 500 only)."
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
    tickers: list[str] = load_universe()["ticker"].tolist()
    return tickers


def get_tickers_for_feature_engineering() -> list[str]:
    """get_tickers() minus config.etf_exclusions.ETF_TICKERS.

    [2026-08-04] ~500 of the ~2300 rows in the filtered universe are ETFs
    (they trade under NSE's EQ series alongside genuine stocks, hence
    passing tier/adtv/mcap filters, but have no fundamentals/shareholding/
    corporate actions — see config/etf_exclusions.py's module docstring).
    ingestion/bhavcopy.py already excludes them from OHLCV download; this
    does the same for the feature-engineering entry points
    (ingestion/scheduler/daily_pipeline.py::step_compute_features,
    scripts/feature_backfill.py, scripts/feature_backfill_hybrid.py) so
    ~22% of feature-matrix compute isn't spent on tickers with nothing to
    compute. Backtest/screener/ML-scoring callers still use plain
    get_tickers() — deliberately unchanged, out of scope for this fix.
    """
    from config.etf_exclusions import ETF_TICKERS
    return [t for t in get_tickers() if t not in ETF_TICKERS]


def get_top_adtv_tickers(n: int) -> list[str]:
    """
    Top `n` tickers from the filtered universe ranked by ADTV (avg daily
    traded value, adtv_cr) descending — the liquid subset.

    Unlike get_tickers()[:n], which is alphabetical (CSV row order) and
    can slice in sparsely-traded names ahead of liquid ones, this ranks
    by adtv_cr first so the returned tickers are the most tradeable ones
    in the filtered universe. Rows with adtv_cr == 0 ("not yet sourced",
    see load_universe()'s docstring) sort last, same as genuinely
    illiquid names, since we can't distinguish "unknown" from "thin" for
    ranking purposes.

    Parameters
    ----------
    n : int
        Number of tickers to return. If n >= the filtered universe size,
        the whole universe is returned (still ADTV-sorted).

    Returns
    -------
    list[str]

    Spec References
    ----------------
    SPEC-SYS-001, SPEC-SYS-011
    """
    df = load_universe().sort_values("adtv_cr", ascending=False)
    tickers: list[str] = df["ticker"].tolist()
    return tickers[:n]


def get_market_cap_rank_map() -> Dict[str, int]:
    """
    {ticker: rank} across the filtered universe, ranked by market_cap_cr
    descending (rank 1 = largest market cap) — lets callers bucket trades
    by market-cap tier (large/mid/small) after the fact.

    This is a single static snapshot of today's universe CSV, not a
    point-in-time-correct rank as of any given historical trade date (no
    PIT market-cap history is wired into the backtest engine yet) — treat
    it as an approximate size bucket, not an exact historical rank.

    Tickers with market_cap_cr == 0 ("not yet sourced", see
    load_universe()'s docstring) are omitted entirely rather than given a
    fabricated rank; callers should treat a missing key as "unknown", not
    "smallest".

    Returns
    -------
    dict
        {ticker: rank (1-indexed int)}, only for tickers with real
        market_cap_cr data.
    """
    df = load_universe().sort_values("market_cap_cr", ascending=False).reset_index(drop=True)
    return {
        row["ticker"]: idx + 1
        for idx, row in df.iterrows()
        if row["market_cap_cr"] > 0
    }


def get_market_cap_rank_map_as_of(
    conn: Any, tickers: List[str], as_of_date: "date | datetime",
    *, shares_cache: Optional[Dict[Any, Any]] = None,
) -> Dict[str, int]:
    """
    {ticker: rank} ranked by a genuinely point-in-time market cap as of
    `as_of_date` (rank 1 = largest) — the PIT-correct replacement for
    get_market_cap_rank_map()'s static current-snapshot rank.

    market_cap(ticker, as_of_date) = shares_outstanding * close, where:
      - shares_outstanding is the latest quarter's value knowable as of
        as_of_date, from datastore.api.pit.get_fundamentals_pit (gated on
        BOTH announcement_date <= as_of_date AND recorded_at <= as_of_date
        — no lookahead, no restatement leakage).
      - close is `ticker`'s ohlcv_adjusted close on the latest trading day
        <= as_of_date (no future price used).

    Tickers missing either shares_outstanding or a close price as of
    as_of_date are OMITTED from the returned map entirely — never given a
    fabricated or fallback rank. Callers (e.g.
    backtest.core.engine.BacktestOrchestrator) must treat a missing key as
    "unknown", not "smallest", exactly as get_market_cap_rank_map() already
    documents for its own missing-data case.

    Parameters
    ----------
    conn : Any
        Open DuckDB connection to the normalised-schema DB (config.settings
        .DUCKDB_PATH) — the one hosting both `fundamentals_history` and
        `ohlcv_adjusted`. Any read mode is fine.
    tickers : List[str]
        Candidate tickers to rank (e.g. every ticker bought on this date).
    as_of_date : date | datetime
        Reference date. A bare `date` is upgraded to a midnight `datetime`
        since get_fundamentals_pit requires a datetime.

    Returns
    -------
    Dict[str, int]
        {ticker: rank (1-indexed int)}, only for tickers with both a known
        shares_outstanding and a known close price as of as_of_date. Empty
        dict if `tickers` is empty or no ticker has both.
    """
    from datastore.api.pit import get_fundamentals_pit

    if not tickers:
        return {}

    as_of_dt = as_of_date if isinstance(as_of_date, datetime) else datetime.combine(as_of_date, datetime.min.time())

    # shares_cache (2026-08-19): an OPTIONAL caller-owned {(db, as_of, ticker):
    # shares_or_None} memo. Shares outstanding as of a date is a property of
    # the TICKER, not of which other tickers were asked about, so unlike the
    # rank map itself (which ranks WITHIN `tickers` and therefore cannot be
    # shared across callers with different lists) this is safe to reuse.
    #
    # MEASURED in a warm sweep job: get_fundamentals_pit was 4.48s of a 31.2s
    # job, re-fetching the same dates for every strategy because the engine's
    # own rank-map cache lives on the orchestrator instance and dies with each
    # run. Caller-owned rather than a module global for the usual reason: a
    # long-running live process must never serve fundamentals cached at boot.
    #
    # A cached None means "known to have no shares_outstanding here" and is
    # NOT re-queried -- the missing-data convention is part of what is cached,
    # or a universe of never-reported tickers would defeat the memo entirely.
    cache_key_db = None
    to_fetch = list(tickers)
    shares_by_ticker: Dict[str, Any] = {}
    if shares_cache is not None:
        try:
            cache_key_db = next(
                (r[2] for r in conn.execute("PRAGMA database_list").fetchall() if r[2]), "__memory__"
            )
        except Exception:  # noqa: BLE001
            cache_key_db = f"__unidentified_{id(conn)}__"
        to_fetch = []
        for t in tickers:
            k = (cache_key_db, as_of_dt, t)
            if k in shares_cache:
                v = shares_cache[k]
                if v is not None:
                    shares_by_ticker[t] = v
            else:
                to_fetch.append(t)

    if to_fetch:
        fundamentals = get_fundamentals_pit(conn, to_fetch, as_of_dt)
        if fundamentals.empty or "shares_outstanding" not in fundamentals.columns:
            fetched: Dict[str, Any] = {}
        else:
            # get_fundamentals_pit already returns one row per (ticker,
            # fiscal_year, quarter) using each quarter's latest as-of-as_of_dt
            # snapshot, sorted by announcement_date ascending — take the LAST
            # row per ticker (the most recent quarter known as of as_of_date),
            # same "latest known" semantics as the function's own docstring.
            fetched = (
                fundamentals.dropna(subset=["shares_outstanding"])
                .groupby("ticker", as_index=True)
                .last()["shares_outstanding"]
                .to_dict()
            )
        shares_by_ticker.update(fetched)
        if shares_cache is not None:
            for t in to_fetch:
                shares_cache[(cache_key_db, as_of_dt, t)] = fetched.get(t)

    if not shares_by_ticker:
        return {}

    candidate_tickers = list(shares_by_ticker.keys())
    placeholders = ",".join("?" for _ in candidate_tickers)
    as_of_key = as_of_dt.date() if isinstance(as_of_dt, datetime) else as_of_dt
    price_df = conn.execute(
        f"""
        SELECT ticker, close FROM (
            SELECT ticker, close, ROW_NUMBER() OVER (
                PARTITION BY ticker ORDER BY date DESC
            ) AS rn
            FROM ohlcv_adjusted
            WHERE ticker IN ({placeholders}) AND date <= ?
        )
        WHERE rn = 1
        """,
        candidate_tickers + [as_of_key],
    ).df()

    close_by_ticker = dict(zip(price_df["ticker"], price_df["close"]))

    market_caps = {
        ticker: shares_by_ticker[ticker] * close_by_ticker[ticker]
        for ticker in candidate_tickers
        if ticker in close_by_ticker
    }
    if not market_caps:
        return {}

    ranked = sorted(market_caps.items(), key=lambda kv: kv[1], reverse=True)
    return {ticker: idx + 1 for idx, (ticker, _mcap) in enumerate(ranked)}


def get_listing_windows(
    conn: Any, tickers: List[str]
) -> Dict[str, Tuple[Optional[date], Optional[date]]]:
    """
    {ticker: (listing_date, delisting_date)} for clipping a requested date
    range to a ticker's actual traded life (e.g. before requesting FYERS
    history for a year the ticker wasn't listed yet, or was already
    delisted). listing_date comes from stock_master; delisting_date comes
    from the separate delisted_companies table (see
    datastore/schema/create_normalised.py's _CREATE_DELISTED_COMPANIES —
    NOT a stock_master column). Direct DuckDB read — ingestion is the
    writer for this data, not a DataStoreClient consumer (see
    ingestion/scrapers/fyers_backfill.py's module docstring).

    Parameters
    ----------
    conn : Any
        Open DuckDB connection to the normalised-schema DB.
    tickers : List[str]

    Returns
    -------
    dict
        ticker -> (listing_date: date | None, delisting_date: date | None).
        A ticker missing from stock_master, or with a missing
        listing_date/delisting_date, maps that side of the tuple to None
        (meaning "no known bound" — treat as always-listed / not
        delisted, never as "unknown = excluded").
    """
    if not tickers:
        return {}
    placeholders = ",".join("?" for _ in tickers)
    rows = conn.execute(
        f"""
        SELECT sm.ticker, sm.listing_date, dc.delisting_date
        FROM stock_master sm
        LEFT JOIN delisted_companies dc ON dc.ticker = sm.ticker
        WHERE sm.ticker IN ({placeholders})
        """,
        tickers,
    ).fetchall()
    windows = {ticker: (listing_date, delisting_date) for ticker, listing_date, delisting_date in rows}
    for ticker in tickers:
        windows.setdefault(ticker, (None, None))
    return windows


def clip_to_listing_window(
    listing_date: Optional[date],
    delisting_date: Optional[date],
    range_start: date,
    range_end: date,
) -> Optional[Tuple[date, date]]:
    """
    Clip [range_start, range_end] to a ticker's [listing_date, delisting_date]
    traded-life window (either bound may be None, meaning unbounded on that
    side). Used by scripts/fyers_staged_backfill.py so a year entirely
    before listing or entirely after delisting is skipped, and a partial
    year (IPO or delisting mid-year) only requests the traded portion —
    never data from before listing or after delisting.

    Parameters
    ----------
    listing_date : date | None
    delisting_date : date | None
    range_start : date
    range_end : date

    Returns
    -------
    tuple(date, date) | None
        The clipped (start, end), or None if the ranges don't overlap at
        all (nothing to request for this ticker/year).
    """
    clipped_start = max(range_start, listing_date) if listing_date else range_start
    clipped_end = min(range_end, delisting_date) if delisting_date else range_end
    if clipped_start > clipped_end:
        return None
    return (clipped_start, clipped_end)


def get_isin_to_ticker_map() -> Dict[str, str]:
    """
    ISIN -> ticker lookup, built from the full (unfiltered) universe CSV.

    Parameters
    ----------
    None

    Returns
    -------
    dict
        {isin: ticker}, over the full unfiltered universe CSV (load_universe_raw,
        not the tier/ADTV-filtered load_universe) — a holding can be a
        real position even in a stock this profile's filters would
        otherwise exclude from the investable universe (e.g. a tier-6
        broader-NSE stock an MF scheme has accumulated). Rows with a missing/
        blank ISIN are skipped.

    Spec References
    ----------------
    SPEC-SYS-001. Used by ingestion/scrapers/amfi_holdings.py to resolve
    AMC portfolio disclosures (keyed by ISIN, the only identifier SEBI's
    disclosure format guarantees) to this project's ticker symbols.

    PIT Assumptions
    ----------------
    None — ISIN-to-ticker is a static identity mapping, not a PIT join.

    Raises
    ------
    FileNotFoundError
        If config/nifty500_universe.csv does not exist.
    """
    df = load_universe_raw()
    df = df[df["isin"].notna() & (df["isin"] != "")]
    return dict(zip(df["isin"], df["ticker"]))
