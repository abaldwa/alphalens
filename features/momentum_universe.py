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
from datetime import date as date_type
from typing import Any, Callable, Dict, List, Optional, Tuple, Union, cast

import pandas as pd

from backtest.trade_filters import ADTV_LOOKBACK_SESSIONS
from config.universe import load_universe_raw

logger = logging.getLogger(__name__)

# (band_id, rank_start, rank_end) — 1-indexed, inclusive on both ends.
# 2026-08-14, user-specified band design. Seven bands spanning rank 1-800,
# replacing the previous five, which stopped at 200 and could not express
# the bottom three bands the per-band momentum sweep has always used
# (scripts/run_momentum_recommended_strategies.py kept 201-250/251-500/
# 501-800 as a LOCAL constant precisely because they were missing here --
# so --rank-band-id could not run three of the five bands under test).
#
# Band ids 1-4 keep their historical numbers so existing rows, reports and
# paper-trading strategies that reference them still resolve. 100-150 and
# 150-200 became 101-150 and 151-200: rank 150 previously belonged to two
# bands at once, which double-counted one stock across two "distinct"
# universes.
#
# 2026-08-18, user-confirmed: the 200/300/500 boundaries are now exclusive on
# the low side too, so the seven bands PARTITION rank 1-800 with no stock in
# two universes at once. This closes the overlap the previous note flagged as
# "left exactly as specified -- flag it if unintended"; it was unintended.
#
# CONSEQUENCE, deliberately accepted: strategy keys embed the boundaries
# (strategies/migrations/momentum.py::variant_name renders
# "..._b6_200-300_..."), so bands 6/7/8 now resolve to NEW keys and their old
# rows are superseded. This is the same thing that happened when 100-150 and
# 150-200 became 101-150 and 151-200 -- the registry still carries both
# generations for bands 3 and 4 -- and it is why the migration must be re-run
# after this change, or a --rank-band-id 6/7/8 run emits a key that resolves
# to nothing.
# 2026-08-19, user-specified numbering: the ids are now CONTIGUOUS 1-7. Ids
# 5, 6 and 7 previously did not exist / were numbered 6, 7 and 8, a hangover
# from the retired band 5 (ranks 100-200) whose slot was never reclaimed when
# it was dropped. The RANGES are unchanged by this edit -- only the labels --
# so band 5 here is exactly the universe band 6 named yesterday.
#
# Historical records (strategy_registry keys, strategy_catalog descriptors,
# backtest_runs rows) still carry the OLD ids. Read them through
# LEGACY_BAND_ID_TO_CURRENT below rather than assuming an id means the same
# band it means today.
# 2026-08-20, user-specified: TWELVE bands, not seven. Five new ranges
# (1-75, 76-160, 161-275, 276-550, 551-800) join the seven that existed, and
# all twelve are renumbered together in ascending (rank_start, rank_end)
# order -- so the numbering is a property of the RANGE, not of when the band
# was added. Rendered as M1..M12 by variant_name().
#
# THESE NO LONGER PARTITION 1-800. The twelve ranges deliberately OVERLAP:
# M1 (1-50) and M2 (1-75) both contain rank 20. That is the point -- the two
# families are alternative slicings of the same ladder, run side by side so
# they can be compared, not a single cover. Any code that assumed RANK_BANDS
# tiles 1-800 exactly once (see tests/unit/test_momentum_universe_bands.py)
# must now assert contiguity WITHIN a family instead.
#
# Note M9 (276-550) sorts BEFORE M10 (301-500): ordering is by rank_start
# first, so a wider band with a lower start precedes a narrower one. Correct
# per the stated rule, and worth stating because it reads oddly.
RANK_BANDS: List[Tuple[int, int, int]] = [
    (1, 1, 50),
    (2, 1, 75),
    (3, 51, 100),
    (4, 76, 160),
    (5, 101, 150),
    (6, 151, 200),
    (7, 161, 275),
    (8, 201, 300),
    (9, 276, 550),
    (10, 301, 500),
    (11, 501, 800),
    (12, 551, 800),
]

#: The seven ranges that predate the 2026-08-20 expansion, by their CURRENT
#: id. Kept so a caller can still sweep exactly the original slicing.
LEGACY_BAND_IDS: Tuple[int, ...] = (1, 3, 5, 6, 8, 10, 11)

#: The five ranges added on 2026-08-20.
V2_BAND_IDS: Tuple[int, ...] = (2, 4, 7, 9, 12)

#: Old band id -> current band id. Two generations of renumbering compose
#: here: the 2026-08-19 relabel (6/7/8 -> 5/6/7) and the 2026-08-20
#: twelve-band expansion. Keys are the ORIGINAL pre-2026-08-19 ids, values
#: the current ones, so one lookup takes a historical record straight to
#: today's id without the caller chaining hops. Ranges are unchanged
#: throughout -- every entry here is a pure relabel.
LEGACY_BAND_ID_TO_CURRENT: Dict[int, int] = {
    1: 1,   # 1-50
    2: 3,   # 51-100
    3: 5,   # 101-150
    4: 6,   # 151-200
    6: 8,   # 201-300  (was id 6 pre-08-19, id 5 after, id 8 now)
    7: 10,  # 301-500
    8: 11,  # 501-800
}

#: Band ids that were retired outright and have NO current equivalent. Band 5
#: was ranks 100-200 -- it overlapped bands 3 and 4 rather than partitioning
#: with them, and was dropped in commit 99a120ca. Results filed under it are
#: not comparable to any band in RANK_BANDS today.
RETIRED_BAND_IDS: Dict[int, Tuple[int, int]] = {5: (100, 200)}


def _all_candidate_tickers(include_delisted: bool = False, normalised_conn: Any = None) -> List[str]:
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
        see that module's docstring).

    normalised_conn : the caller's already-open connection to reuse for
        the delisted_companies lookup (2026-07-20 fix), forwarded to
        build_historical_universe_from_delisted's `conn` param. Without
        this, build_historical_universe_from_delisted() always opened its
        OWN read-write connection to config.settings.DUCKDB_PATH — a real
        bug, not just a style issue: in production, full_rank_universe()
        is invoked with an already-open connection to that SAME live file
        (typically read_only=True/persist=False), and DuckDB only allows
        one read-write connection OR many read-only connections per file
        at a time. The moment include_delisted=True was actually used in
        production it would have hit "Connection Error: Can't open a
        connection to same database file with a different configuration"
        — caught here by a test, not in production."""
    if include_delisted:
        from config.build_universe import build_historical_universe_from_delisted
        return build_historical_universe_from_delisted(conn=normalised_conn)
    raw = load_universe_raw()
    return cast(List[str], raw["ticker"].tolist())


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
    # fundamentals.announcement_date is VARCHAR with genuinely mixed
    # formats across sources ("2026-07-28" from XBRL vs "2026-08-14
    # 00:00:00" from Trendlyne) — format='mixed' parses each value on its
    # own terms instead of inferring one format from the first row and
    # choking on later rows that don't match it.
    all_fund["announcement_date"] = pd.to_datetime(all_fund["announcement_date"], format="mixed")
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
    normalised_conn: Any, as_of_date: str, max_rank: int = MAX_TRACKED_RANK, include_delisted: bool = False,
) -> pd.DataFrame:
    """Every candidate ticker's real PIT market cap as of as_of_date,
    ranked descending and truncated to the top `max_rank` — the single
    per-date DB round trip that all 4 rank bands slice from, so building
    all 4 bands for one date costs one query, not four.

    include_delisted : forwarded to _all_candidate_tickers (Fix A4) —
        see that function's docstring. Defaults to False. Reuses this
        function's own normalised_conn for the delisted_companies lookup
        (2026-07-20 fix) rather than opening a second, conflicting
        connection to the same file."""
    candidates = _all_candidate_tickers(include_delisted=include_delisted, normalised_conn=normalised_conn)
    snapshot = market_cap_snapshot(normalised_conn, candidates, as_of_date)
    if snapshot.empty:
        return snapshot
    ranked = snapshot.sort_values("market_cap_cr", ascending=False).reset_index(drop=True)
    ranked["rank"] = ranked.index + 1
    return ranked.head(max_rank)


def rank_band_tickers(
    normalised_conn: Any, as_of_date: str, rank_start: int, rank_end: int, include_delisted: bool = False,
) -> List[str]:
    """Tickers ranked [rank_start, rank_end] (1-indexed, inclusive) by real
    PIT market cap as of as_of_date, out of every ticker AlphaLens tracks."""
    ranked = full_rank_universe(
        normalised_conn, as_of_date, max_rank=max(rank_end, MAX_TRACKED_RANK), include_delisted=include_delisted,
    )
    if ranked.empty:
        return []
    # rank_start/rank_end are 1-indexed; iloc is 0-indexed.
    return cast(List[str], ranked.iloc[rank_start - 1:rank_end]["ticker"].tolist())


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
    result: Dict[str, List[str]] = {}
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


def yearly_rank_lookup_from_rankings(
    yearly_rankings: Dict[str, pd.DataFrame]
) -> Dict[str, Dict[str, int]]:
    """
    Companion to yearly_band_universes_from_rankings /
    yearly_band_approximation_flags_from_rankings — same
    {year_start_date: {ticker: value}} shape, but keyed over the FULL
    ranking rather than one band slice, carrying each ticker's real
    1-indexed market-cap rank. {year_start_date: {ticker: rank}}.

    Deliberately not band-sliced: its consumer (MomentumAdapter's
    sticky-promotion rule, 2026-08-05 Momentum engine consolidation
    Phase 3) needs to see ranks BETTER than its own band's rank_start —
    i.e. exactly the tickers a band slice would have dropped — to tell a
    promoted holding (moved to a higher market-cap band) apart from a
    demoted or delisted one. A ticker absent from the year's ranking
    entirely has no entry, and is never assigned a fabricated rank.
    """
    result: Dict[str, Dict[str, int]] = {}
    for date_str, ranked in yearly_rankings.items():
        if ranked.empty:
            result[date_str] = {}
        else:
            result[date_str] = {t: int(r) for t, r in zip(ranked["ticker"], ranked["rank"])}
    return result


def build_yearly_rank_band_universe_provider(
    yearly_rankings: Dict[str, pd.DataFrame], rank_start: int, rank_end: int
) -> Callable[[date_type], List[str]]:
    """
    Adapt this module's yearly-fixed rank-band universes to
    backtest/core/engine.py's `UniverseProvider = Callable[[date], List[str]]`
    contract, so a BacktestOrchestrator momentum run selects from the same
    market-cap band the standalone MomentumBacktester does instead of the
    generic "every ticker with a recent OHLCV bar" pool
    (run_orchestrator_backtest._build_config's default).

    Pre-slices the band ONCE at build time (yearly_band_universes_from_rankings)
    and closes over it — OrchestratorConfig.universe_provider is called once
    per rebalance date for the whole run, so the per-call work is reduced to
    the year lookup itself, with no DB access at all.

    The returned callable applies the same "most recent year_start <=
    as_of_date wins" convention as MomentumBacktester._active_universe: a
    year's constituent list is fixed on the first real trading day of that
    year and held for the rest of it. A date BEFORE the earliest year_start
    (i.e. no list has taken effect yet) yields an empty universe rather
    than back-dating the first year's membership onto it, which would be
    exactly the look-ahead bias the yearly-fixing convention exists to
    avoid.
    """
    band = yearly_band_universes_from_rankings(yearly_rankings, rank_start, rank_end)
    by_start = {pd.Timestamp(date_str): tickers for date_str, tickers in band.items()}

    def universe_provider(as_of: date_type) -> List[str]:
        as_of_ts = pd.Timestamp(as_of)
        applicable_starts = [d for d in by_start if d <= as_of_ts]
        if not applicable_starts:
            return []
        return list(by_start[max(applicable_starts)])

    return universe_provider


def yearly_band_universes(
    normalised_conn: Any, start_date: str, end_date: str, rank_start: int, rank_end: int,
    include_delisted: bool = False,
) -> Dict[str, List[str]]:
    """
    {first_trading_day_of_year_iso: [tickers]} for every calendar year in
    [start_date, end_date] — one fixed constituent list per year (2026-07-14
    user decision), keyed by the real first trading day of that year so the
    backtest engine can look up "which list is active on this date" without
    ever re-deriving membership mid-year. Convenience wrapper for a single
    band; prefer all_yearly_full_rankings + yearly_band_universes_from_rankings
    when computing multiple bands so the DB is only queried once per year.

    include_delisted : forwarded to all_yearly_full_rankings/
        full_rank_universe/_all_candidate_tickers (2026-07-20
        survivorship-bias fix — see BacktestUmbrellaPlan.md Truthful
        Review Gap #1). This wrapper previously had NO way to opt into
        the include_delisted candidate pool at all — a real bug, not
        just a missing convenience, since every caller of this
        particular function was silently stuck on the survivorship-
        biased current-snapshot universe regardless of intent. Defaults
        to False to keep existing callers' results unchanged unless they
        explicitly opt in.
    """
    yearly_rankings = all_yearly_full_rankings(normalised_conn, start_date, end_date, include_delisted=include_delisted)
    return yearly_band_universes_from_rankings(yearly_rankings, rank_start, rank_end)


# 2026-07-20 user decision (BacktestUmbrellaPlan.md Truthful Review Gap #5):
# real historical Nifty/Nifty500 constituent lists are not available as free
# NSE/index data in this environment. Rather than leave PIT index membership
# as an "accepted approximation" indefinitely, the decided methodology is:
# at every rebalance date, rank the full candidate pool (today's active
# universe UNION every real delisted_companies ticker, per this module's
# include_delisted fix) by real PIT market cap, and take the top N as that
# date's index-membership proxy — exactly the technique RANK_BANDS above
# already used for Nifty50/Next50, generalized to Nifty500 scale. This is
# the SAME function (full_rank_universe/rank_band_tickers) with a wider
# band, not new ranking logic — rank_band_tickers's own
# max(rank_end, MAX_TRACKED_RANK) already lifts the 200 cap whenever
# rank_end exceeds it, so no separate code path was needed to go from
# "top 200" to "top 500".
NIFTY500_PROXY_RANK: int = 500


def nifty500_proxy_universe(normalised_conn: Any, as_of_date: str, include_delisted: bool = True) -> List[str]:
    """Top NIFTY500_PROXY_RANK tickers by real PIT market cap as of
    as_of_date — the decided stand-in for "who was actually in Nifty500 on
    this historical date" (see module-level note above). include_delisted
    defaults to True here (unlike every other function in this module)
    because this function's entire purpose is being a point-in-time
    historical-membership proxy — the survivorship-bias gap this fix
    closes is exactly what a caller of THIS function is trying to avoid."""
    return rank_band_tickers(normalised_conn, as_of_date, 1, NIFTY500_PROXY_RANK, include_delisted=include_delisted)


def yearly_nifty500_proxy_universes(
    normalised_conn: Any, start_date: str, end_date: str, include_delisted: bool = True,
) -> Dict[str, List[str]]:
    """{first_trading_day_of_year_iso: [tickers]} Nifty500-proxy membership
    (see nifty500_proxy_universe), one fixed list per year — the same
    yearly-fixing convention as yearly_band_universes, so a Technical or
    Fundamental backtest's universe_provider can look this up per
    rebalance date without re-deriving membership mid-year. include_delisted
    defaults to True for the same reason as nifty500_proxy_universe."""
    return yearly_band_universes(
        normalised_conn, start_date, end_date, 1, NIFTY500_PROXY_RANK, include_delisted=include_delisted,
    )


# ---------------------------------------------------------------------------
# The momentum universe — ONE definition, for backtest, paper and live
# ---------------------------------------------------------------------------
# [2026-08-18, user decision] Momentum's universe is built in two steps, in
# this order:
#
#   1. Take the top ADTV_UNIVERSE_TOP_N stocks by trailing ADTV as of the
#      refresh date. THIS is momentum's risk control — it is what makes every
#      name in the book something a real order could fill, and it is why the
#      strategy carries no sector cap.
#   2. Rank THAT set by market cap and slice the band (1-50, 51-100, ...).
#
# The order matters and is not interchangeable. Ranking by market cap first
# and filtering by liquidity second would leave a band short of its 50 names
# whenever an illiquid large-cap occupied a slot; filtering first means every
# band is always 50 real, tradeable names.
#
# WHY A 21-TRADING-DAY GRID
# -------------------------
# Both rankings refresh every UNIVERSE_REFRESH_TRADING_DAYS, just before the
# monthly strategies rebalance. A strategy uses the most recent snapshot at or
# before its own rebalance date, so the cadences compose without any strategy
# needing to know about the grid:
#
#   weekly (5d) / biweekly (10d) -> whatever snapshot is current
#   monthly (21d)                -> the snapshot taken that same day
#   bimonthly (42d)              -> every 2nd snapshot
#   quarterly (63d)              -> every 3rd snapshot
#
# The alternative -- re-ranking daily -- would let a name drop out of the
# universe on a day no strategy is trading, which is a sell nobody decided.

#: The tradeable universe's size. Momentum's only risk control.
ADTV_UNIVERSE_TOP_N = 800

#: How often both rankings are rebuilt, in trading days. Deliberately equal to
#: the monthly rebalance cadence: the universe is refreshed just before the
#: monthly strategies act on it.
UNIVERSE_REFRESH_TRADING_DAYS = 21


#: The grid's anchor. Fixed, and deliberately NOT the run's own start date:
#: anchoring at trading_days[0] would give two backtests with different start
#: dates two different grids, and live a third -- so the same strategy would
#: refresh its universe on different days depending on who asked. A fixed
#: epoch makes the grid a property of the calendar, not of the caller.
#: 2009-04-01 is backtest/run_orchestrator_backtest.EARLIEST_RELIABLE_START,
#: the first date this project trusts its own price history.
UNIVERSE_GRID_EPOCH = date_type(2009, 4, 1)


def trading_day_calendar(normalised_conn: Any, through: Optional[str] = None) -> pd.DatetimeIndex:
    """Every real trading day from UNIVERSE_GRID_EPOCH through `through`.

    The grid is counted off THIS calendar, so every caller -- a backtest of
    any window, paper trading, live -- lands on the same refresh dates.
    """
    params: List[Any] = [UNIVERSE_GRID_EPOCH]
    clause = "date >= ?"
    if through is not None:
        clause += " AND date <= ?"
        params.append(through)
    rows = normalised_conn.execute(
        f"SELECT DISTINCT date FROM ohlcv_adjusted WHERE {clause} ORDER BY date", params,
    ).fetchall()
    return pd.DatetimeIndex([pd.Timestamp(r[0]) for r in rows])


def universe_refresh_dates(trading_days: pd.DatetimeIndex) -> List[pd.Timestamp]:
    """The 21-trading-day grid points across `trading_days`.

    `trading_days` must be a calendar anchored at UNIVERSE_GRID_EPOCH (see
    trading_day_calendar) — the grid is counted from its first element, so
    passing a window that starts elsewhere silently shifts every refresh.
    """
    return list(trading_days[::UNIVERSE_REFRESH_TRADING_DAYS])


def universe_snapshot_date(
    trading_days: pd.DatetimeIndex, as_of: Union[str, date_type, pd.Timestamp],
) -> Optional[pd.Timestamp]:
    """The grid point in force on `as_of` — the most recent refresh at or
    before it. None before the first one, which means "no universe yet", not
    "every ticker".
    """
    as_of_ts = pd.Timestamp(as_of)
    eligible = [d for d in universe_refresh_dates(trading_days) if d <= as_of_ts]
    return eligible[-1] if eligible else None


#: A ticker whose last real bar is older than this is not tradeable today --
#: not yet listed, suspended, or already delisted. Same tolerance
#: run_orchestrator_backtest's generic provider uses, and adopted here for the
#: same reason: a stale price is not evidence that a stock can be bought.
UNIVERSE_STALENESS_TOLERANCE_DAYS = 10


def liquid_universe(
    normalised_conn: Any, as_of_date: str, top_n: int = ADTV_UNIVERSE_TOP_N,
    lookback_sessions: int = ADTV_LOOKBACK_SESSIONS, include_delisted: bool = True,
) -> List[str]:
    """The top `top_n` tickers by trailing ADTV as of `as_of_date`.

    Step 1 of the universe definition. Three conventions, each load-bearing:

    * **The window ends STRICTLY BEFORE as_of_date.** Ranking on liquidity
      that includes the day's own bar uses volume that had not printed when
      the decision was made -- and on a day a name spikes on news, that is
      exactly the lookahead that promotes the stock you could not have bought.
    * **A ticker whose last bar is stale is excluded** (see
      UNIVERSE_STALENESS_TOLERANCE_DAYS). Liquidity a fortnight ago is not
      evidence that a suspended stock is tradeable now.
    * **include_delisted defaults True.** A stock alive on a past as_of_date
      belongs in that date's universe even though it later delisted; leaving
      it out is survivorship bias, and a relative-strength strategy is where
      vanishing losers flatter results most. This is momentum's own
      2026-07-20 fix, kept.

    A ticker with no volume history in the window is absent, never assumed
    liquid -- the standing missing-data convention.
    """
    candidates = _all_candidate_tickers(include_delisted=include_delisted, normalised_conn=normalised_conn)
    if not candidates:
        return []
    stale_floor = (pd.Timestamp(as_of_date) - pd.Timedelta(days=UNIVERSE_STALENESS_TOLERANCE_DAYS)).date()
    placeholders = ",".join("?" for _ in candidates)
    rows = normalised_conn.execute(
        f"""
        WITH traded AS (
            SELECT ticker, date, close * volume AS turnover
            FROM ohlcv_adjusted
            WHERE date < ? AND ticker IN ({placeholders}) AND volume > 0
        ),
        recent AS (
            SELECT ticker, turnover,
                   ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
            FROM traded
        ),
        last_bar AS (
            SELECT ticker, max(date) AS last_date FROM traded GROUP BY ticker
        )
        SELECT r.ticker, avg(r.turnover) AS adtv
        FROM recent r JOIN last_bar l ON l.ticker = r.ticker
        WHERE r.rn <= ? AND l.last_date >= ?
        GROUP BY r.ticker
        ORDER BY adtv DESC
        LIMIT ?
        """,
        [as_of_date, *candidates, lookback_sessions, stale_floor, top_n],
    ).fetchall()
    return [str(r[0]) for r in rows]


def ranked_liquid_universe(
    normalised_conn: Any, as_of_date: str,
    *, top_n_by_adtv: int = ADTV_UNIVERSE_TOP_N, include_delisted: bool = True,
) -> pd.DataFrame:
    """The liquid universe on `as_of_date`, ranked by market cap: columns
    ticker / market_cap_cr / rank.

    Extracted from momentum_band_universe on 2026-08-19 because it is the
    expensive half AND it does not depend on the rank band -- the band is the
    two-line slice below. Measured: build_momentum_universe_provider cost
    66.3s for a 17-year window (204 grid points x two DB queries), and it cost
    that again for every band and every strategy in a sweep, even though every
    one of those calls produced the SAME ranked frame per grid point.

    Returns an empty frame (not a fabricated one) when either half is missing,
    preserving momentum_band_universe's own missing-data convention.
    """
    liquid = liquid_universe(
        normalised_conn, as_of_date, top_n=top_n_by_adtv, include_delisted=include_delisted,
    )
    if not liquid:
        return pd.DataFrame(columns=["ticker", "market_cap_cr", "rank"])
    snapshot = market_cap_snapshot(normalised_conn, liquid, as_of_date)
    if snapshot.empty:
        return pd.DataFrame(columns=["ticker", "market_cap_cr", "rank"])
    ranked = snapshot.sort_values("market_cap_cr", ascending=False).reset_index(drop=True)
    ranked["rank"] = ranked.index + 1
    return ranked


def momentum_band_universe(
    normalised_conn: Any, as_of_date: str, rank_start: int, rank_end: int,
    *, top_n_by_adtv: int = ADTV_UNIVERSE_TOP_N, include_delisted: bool = True,
) -> List[str]:
    """The band's constituents on `as_of_date`: liquid universe first, then
    market-cap rank within it.

    THE one definition. Backtest, paper trading and live all resolve their
    universe through this function, so a band cannot mean one thing in a
    backtest and another thing today.
    """
    ranked = ranked_liquid_universe(
        normalised_conn, as_of_date,
        top_n_by_adtv=top_n_by_adtv, include_delisted=include_delisted,
    )
    if ranked.empty:
        return []
    band = ranked[(ranked["rank"] >= rank_start) & (ranked["rank"] <= rank_end)]
    return [str(t) for t in band["ticker"]]


def build_momentum_universe_provider(
    normalised_conn: Any, trading_days: pd.DatetimeIndex, rank_start: int, rank_end: int,
    *, top_n_by_adtv: int = ADTV_UNIVERSE_TOP_N, include_delisted: bool = True,
    snapshot_cache: Optional[Dict[Any, pd.DataFrame]] = None,
) -> Callable[[date_type], List[str]]:
    """A `UniverseProvider` over the 21-trading-day grid, for backtests.

    Resolves momentum_band_universe() ONCE per grid point and closes over the
    result, so a run costs one query per refresh (~205 for a 17-year window)
    rather than one per trading day (~4,300). The returned callable does no
    DB access at all, which matters because the orchestrator calls it inside
    its loop.

    This is the same definition live and paper trading call directly — the
    grid is a caching boundary, not a second rule. A date before the first
    refresh yields an empty universe rather than back-dating the first
    snapshot onto it, which would be look-ahead.
    """
    # snapshot_cache (2026-08-19): an OPTIONAL caller-owned dict memoising the
    # RANKED frame per grid point. The ranking is band-independent -- the band
    # is the slice below -- so a sweep over seven bands x many strategies was
    # rebuilding one identical frame per grid point every time, at a measured
    # 66.3s per provider for a 17-year window.
    #
    # Caller-owned rather than a module global on purpose: this function is
    # the BACKTEST entry point, while live and paper go through
    # current_momentum_band_universe. A process-lifetime global here would
    # also be read by any long-running live process that ever touched this
    # function, and a universe cached at boot is exactly the kind of staleness
    # that must never reach a live order. Passing None (the default) keeps the
    # original per-call behaviour byte for byte.
    cache = snapshot_cache if snapshot_cache is not None else {}
    # The DATABASE is part of the key. Without it two connections to different
    # files -- a test's temp fixture and the real store, or two windows of a
    # rehearsal DB -- collide on (date, top_n, include_delisted) and one
    # silently serves the other's universe. Caught by
    # tests/unit/test_run_orchestrator_backtest.py::TestMomentumRankBandWiring,
    # which seeds a fresh temp DB per test in one process.
    try:
        _db = next(
            (row[2] for row in normalised_conn.execute("PRAGMA database_list").fetchall() if row[2]),
            "__memory__",
        )
    except Exception:  # noqa: BLE001 -- a connection that cannot name itself
        # gets a private cache rather than sharing an ambiguous one.
        _db = f"__unidentified_{id(normalised_conn)}__"
    snapshots: Dict[Any, List[str]] = {}
    for refresh_date in universe_refresh_dates(trading_days):
        as_of = str(refresh_date.date())
        key = (_db, as_of, top_n_by_adtv, bool(include_delisted))
        ranked = cache.get(key)
        if ranked is None:
            ranked = ranked_liquid_universe(
                normalised_conn, as_of,
                top_n_by_adtv=top_n_by_adtv, include_delisted=include_delisted,
            )
            cache[key] = ranked
        if ranked.empty:
            snapshots[refresh_date] = []
        else:
            band = ranked[(ranked["rank"] >= rank_start) & (ranked["rank"] <= rank_end)]
            snapshots[refresh_date] = [str(t) for t in band["ticker"]]

    def universe_provider(as_of: date_type) -> List[str]:
        snapshot_date = universe_snapshot_date(trading_days, as_of)
        if snapshot_date is None:
            return []
        return snapshots.get(snapshot_date, [])

    return universe_provider


def current_momentum_band_universe(
    normalised_conn: Any, as_of_date: str, rank_start: int, rank_end: int,
    *, top_n_by_adtv: int = ADTV_UNIVERSE_TOP_N,
) -> List[str]:
    """The band in force on `as_of_date` — the live and paper entry point.

    Resolves the grid snapshot from the real trading calendar and then calls
    momentum_band_universe on that date, so today's live universe is the SAME
    set a backtest would have used on the same day. Calling
    momentum_band_universe directly with today's date instead would re-rank
    off-grid and could drop a held name on a day no strategy is trading.
    """
    calendar = trading_day_calendar(normalised_conn, through=as_of_date)
    snapshot_date = universe_snapshot_date(calendar, as_of_date)
    if snapshot_date is None:
        return []
    return momentum_band_universe(
        normalised_conn, str(snapshot_date.date()), rank_start, rank_end,
        top_n_by_adtv=top_n_by_adtv,
    )
