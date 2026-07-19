"""
features/momentum_universe.py

Phase: FeatureBacklog.md ML38 — momentum strategy scoping/implementation
Owner: Platform / Features
Consumers: backtest/momentum_backtest.py, scripts/run_momentum_experimentation.py

Builds the 4 market-cap-rank-band universes ML38 settled on (2026-07-14
user decision, after real Nifty 50/Next 50 historical index membership
was confirmed not sourceable — see FeatureBacklog.md ML38):

    Band 1: rank 1-50 by market cap    (proxy for "Nifty 50")
    Band 2: rank 51-100 by market cap  (proxy for "Nifty Next 50")
    Band 3: rank 100-150 by market cap
    Band 4: rank 150-200 by market cap
    Band 5: rank 100-200 by market cap (a "mixed" band spanning bands 3+4,
        2026-07-14 user request — a wider, single momentum ranking across
        the whole rank-100-to-200 range rather than the two narrower
        50-wide slices)

Market cap is computed PIT-correctly the same way as
features/sector_accumulation.py's `_latest_shares_outstanding_asof`:
    market_cap(ticker, date) = ohlcv_adjusted.close(ticker, date)
                                * fundamentals.shares_outstanding
                                  (most recent row with
                                   announcement_date <= date)

Each band's constituent list is fixed on the first real trading day of
each calendar year and held constant for that entire year (2026-07-14
user decision) — never recomputed mid-year even if ranks shift — to
avoid the look-ahead survivorship bias of using a stock's final/latest
rank to decide whether it belonged in a band years earlier.
"""

import logging
from typing import Any, Dict, List

import pandas as pd

from config.universe import load_universe_raw

logger = logging.getLogger(__name__)

# (band_id, rank_start, rank_end) — 1-indexed, inclusive on both ends.
RANK_BANDS: List[tuple] = [
    (1, 1, 50),
    (2, 51, 100),
    (3, 100, 150),
    (4, 150, 200),
    (5, 100, 200),
]


def _all_candidate_tickers(include_delisted: bool = False) -> List[str]:
    """Every ticker AlphaLens has ever tracked (config/nifty500_universe.csv's
    full stock master, unfiltered by tier/ADTV/mcap) — the candidate pool a
    historical market-cap ranking is drawn from, since a stock's *current*
    tier/is_nifty500 flag says nothing about its rank 10 years ago.

    include_delisted : if True, also unions in every ticker from the
        `delisted_companies` table (2026-07-19 full-codebase-review Fix
        A4) via config.build_universe.build_historical_universe_from_delisted
        — closes the survivorship-bias gap where a ticker that delisted
        before the universe CSV was last rebuilt is otherwise permanently
        invisible to a historical backtest, even for periods when it
        legitimately belonged in a tracked market-cap band. Defaults to
        False so existing backtest results remain exactly reproducible
        unless explicitly opted in — and degrades gracefully to the
        plain CSV-only list if delisted_companies is empty/missing (e.g.
        ingestion/scrapers/nse_delisted_companies.py hasn't been run yet,
        or its unverified target endpoint hasn't been confirmed live —
        see that module's docstring)."""
    if include_delisted:
        from config.build_universe import build_historical_universe_from_delisted
        return build_historical_universe_from_delisted()
    raw = load_universe_raw()
    return raw["ticker"].tolist()


def first_trading_days_per_year(normalised_conn: Any, start_date: str, end_date: str) -> Dict[int, str]:
    """Real first-trading-day-of-year dates (from ohlcv_adjusted's own
    distinct date index — never assumed/calendar-derived) for every
    calendar year touched by [start_date, end_date]."""
    df = normalised_conn.execute(
        "SELECT DISTINCT date FROM ohlcv_adjusted WHERE date >= ? AND date <= ? ORDER BY date",
        [start_date, end_date],
    ).fetch_df()
    if df.empty:
        return {}
    df["date"] = pd.to_datetime(df["date"])
    df["year"] = df["date"].dt.year
    first_per_year = df.groupby("year")["date"].min()
    return {int(year): d.date().isoformat() for year, d in first_per_year.items()}


def market_cap_snapshot(normalised_conn: Any, tickers: List[str], as_of_date: str) -> pd.DataFrame:
    """
    Market cap for `tickers` as of `as_of_date`: the most recent real
    ohlcv_adjusted close on or before as_of_date, times shares_outstanding.

    shares_outstanding is real fundamentals data, but **2026-07-14 user
    decision**: `fundamentals.shares_outstanding` (non-null) only actually
    exists for 2024 onward in this DB (checked live: 2 rows in 2024, 7,595
    in 2025, 3,098 in 2026, zero before that) — nowhere near covering
    ML38's requested 10-year backtest window. Strict PIT (most recent
    announcement_date <= as_of_date, never later) would leave every date
    before ~2024 with zero constituents, exactly as happened on the first
    real 10-year run of this function (flat, untraded equity curve for
    2016-2025). Per explicit user sign-off, dates with no real PIT-eligible
    row instead fall back to each ticker's EARLIEST real
    shares_outstanding observation (whatever year it's from) as a flat
    proxy for all earlier dates. This is a known, accepted approximation,
    not real historical data — it silently ignores real share-count
    changes from splits/bonuses/buybacks/rights issues that happened
    before that first real observation, which is exactly the kind of
    activity that moves a stock in/out of a market-cap rank band. Rows
    using the fallback are flagged via `shares_outstanding_is_approximated`
    so any consumer can separate real-PIT results from approximated ones.
    A ticker with no real shares_outstanding observation at all (any
    date) is still excluded outright — never fabricated from nothing.

    Returns
    -------
    pd.DataFrame with columns [ticker, close, shares_outstanding,
    market_cap_cr, shares_outstanding_is_approximated]
    """
    empty = pd.DataFrame(
        columns=["ticker", "close", "shares_outstanding", "market_cap_cr", "shares_outstanding_is_approximated"]
    )
    if not tickers:
        return empty
    placeholders = ",".join("?" for _ in tickers)

    close_df = normalised_conn.execute(
        f"""
        SELECT ticker, close
        FROM (
            SELECT ticker, close, date,
                   ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
            FROM ohlcv_adjusted
            WHERE ticker IN ({placeholders}) AND date <= ?
        )
        WHERE rn = 1
        """,
        list(tickers) + [as_of_date],
    ).fetch_df()
    if close_df.empty:
        return empty

    # Every real (ticker, announcement_date, shares_outstanding) row —
    # not date-filtered here, since a fallback to the earliest-ever real
    # row (which may be AFTER as_of_date) needs the full history per
    # ticker, not just the PIT-eligible subset.
    all_fund = normalised_conn.execute(
        f"""
        SELECT ticker, announcement_date, shares_outstanding
        FROM fundamentals
        WHERE ticker IN ({placeholders}) AND shares_outstanding IS NOT NULL
        ORDER BY ticker, announcement_date
        """,
        list(tickers),
    ).fetch_df()
    if all_fund.empty:
        return empty
    all_fund["announcement_date"] = pd.to_datetime(all_fund["announcement_date"])
    as_of_ts = pd.Timestamp(as_of_date)

    pit_eligible = all_fund[all_fund["announcement_date"] <= as_of_ts]
    pit_latest = (
        pit_eligible.sort_values("announcement_date").groupby("ticker").tail(1)
        if not pit_eligible.empty
        else pit_eligible
    )
    pit_latest = pit_latest.assign(shares_outstanding_is_approximated=False)

    earliest = all_fund.sort_values("announcement_date").groupby("ticker").head(1)
    earliest = earliest.assign(shares_outstanding_is_approximated=True)
    # Only use the earliest-known fallback for tickers with no real
    # PIT-eligible row at all — never override a real PIT match.
    fallback = earliest[~earliest["ticker"].isin(pit_latest["ticker"])]

    fund_df = pd.concat([pit_latest, fallback], ignore_index=True)[
        ["ticker", "shares_outstanding", "shares_outstanding_is_approximated"]
    ]

    merged = close_df.merge(fund_df, on="ticker", how="inner")
    if merged.empty:
        return empty
    # shares_outstanding is stored in absolute share count; market cap in INR
    # crore = close (INR) * shares / 1e7, matching sector_rotation.py/
    # sector_accumulation.py's own INR-crore convention.
    merged["market_cap_cr"] = merged["close"] * merged["shares_outstanding"] / 1e7
    return merged[["ticker", "close", "shares_outstanding", "market_cap_cr", "shares_outstanding_is_approximated"]]


MAX_TRACKED_RANK = 200


def full_rank_universe(
    normalised_conn: Any, as_of_date: str, max_rank: int = MAX_TRACKED_RANK, include_delisted: bool = False
) -> pd.DataFrame:
    """Every candidate ticker's real PIT market cap as of as_of_date,
    ranked descending and truncated to the top `max_rank` — the single
    per-date DB round trip that all 4 rank bands slice from, so building
    all 4 bands for one date costs one query, not four.

    include_delisted : forwarded to _all_candidate_tickers (Fix A4) —
        see that function's docstring. Defaults to False."""
    candidates = _all_candidate_tickers(include_delisted=include_delisted)
    snapshot = market_cap_snapshot(normalised_conn, candidates, as_of_date)
    if snapshot.empty:
        return snapshot
    ranked = snapshot.sort_values("market_cap_cr", ascending=False).reset_index(drop=True)
    ranked["rank"] = ranked.index + 1
    return ranked.head(max_rank)


def rank_band_tickers(
    normalised_conn: Any, as_of_date: str, rank_start: int, rank_end: int, include_delisted: bool = False
) -> List[str]:
    """Tickers ranked [rank_start, rank_end] (1-indexed, inclusive) by real
    PIT market cap as of as_of_date, out of every ticker AlphaLens tracks."""
    ranked = full_rank_universe(
        normalised_conn, as_of_date, max_rank=max(rank_end, MAX_TRACKED_RANK), include_delisted=include_delisted
    )
    if ranked.empty:
        return []
    # rank_start/rank_end are 1-indexed; iloc is 0-indexed.
    return ranked.iloc[rank_start - 1:rank_end]["ticker"].tolist()


def all_yearly_full_rankings(
    normalised_conn: Any, start_date: str, end_date: str, max_rank: int = MAX_TRACKED_RANK,
    include_delisted: bool = False,
) -> Dict[str, pd.DataFrame]:
    """{first_trading_day_of_year_iso: ranked_dataframe(top max_rank)} for
    every calendar year in [start_date, end_date] — one DB round trip per
    year total (shared across all 4 rank bands and all lookback/rebalance
    variants), computed once by the runner script."""
    year_starts = first_trading_days_per_year(normalised_conn, start_date, end_date)
    return {
        date_str: full_rank_universe(normalised_conn, date_str, max_rank=max_rank, include_delisted=include_delisted)
        for date_str in year_starts.values()
    }


def yearly_band_universes_from_rankings(
    yearly_rankings: Dict[str, pd.DataFrame], rank_start: int, rank_end: int
) -> Dict[str, List[str]]:
    """Slice a pre-computed {year_start_date: ranked_dataframe} map (from
    all_yearly_full_rankings) down to one rank band — pure Python, no DB
    access, so this can be called once per band per experiment cheaply."""
    result = {}
    for date_str, ranked in yearly_rankings.items():
        if ranked.empty:
            result[date_str] = []
        else:
            result[date_str] = ranked.iloc[rank_start - 1:rank_end]["ticker"].tolist()
    return result


def yearly_band_approximation_flags_from_rankings(
    yearly_rankings: Dict[str, pd.DataFrame], rank_start: int, rank_end: int
) -> Dict[str, Dict[str, bool]]:
    """
    Companion to yearly_band_universes_from_rankings that preserves the
    per-ticker `shares_outstanding_is_approximated` flag dropped by the
    plain ticker-list slice above. {year_start_date: {ticker: is_approximated}}.

    Lets a downstream consumer (e.g. MomentumBacktester) distinguish real-PIT
    rank-band membership from membership derived off the pre-2024
    earliest-known-shares-outstanding fallback (see market_cap_snapshot's
    docstring) without re-querying the DB.
    """
    result: Dict[str, Dict[str, bool]] = {}
    for date_str, ranked in yearly_rankings.items():
        if ranked.empty:
            result[date_str] = {}
        else:
            band = ranked.iloc[rank_start - 1:rank_end]
            result[date_str] = dict(zip(band["ticker"], band["shares_outstanding_is_approximated"]))
    return result


def yearly_band_universes(
    normalised_conn: Any, start_date: str, end_date: str, rank_start: int, rank_end: int
) -> Dict[str, List[str]]:
    """
    {first_trading_day_of_year_iso: [tickers]} for every calendar year in
    [start_date, end_date] — one fixed constituent list per year (2026-07-14
    user decision), keyed by the real first trading day of that year so the
    backtest engine can look up "which list is active on this date" without
    ever re-deriving membership mid-year. Convenience wrapper for a single
    band; prefer all_yearly_full_rankings + yearly_band_universes_from_rankings
    when computing multiple bands so the DB is only queried once per year.
    """
    yearly_rankings = all_yearly_full_rankings(normalised_conn, start_date, end_date)
    return yearly_band_universes_from_rankings(yearly_rankings, rank_start, rank_end)
