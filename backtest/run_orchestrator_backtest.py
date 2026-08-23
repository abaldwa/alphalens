"""
backtest/run_orchestrator_backtest.py

Owner: Platform / Backtest
Consumers: operator CLI (`python3 -m backtest.run_orchestrator_backtest`),
datastore/api/routers/backtest_runs.py (trigger endpoint, background)

CLI driver for backtest/core/engine.py's BacktestOrchestrator — the
shared, channel-agnostic orchestrator every Technical/Fundamental/
Momentum adapter plugs into (backtest/adapters/*.py). Unlike
run_phase1/2/3_backtest.py (which drive the OLDER, ML-specific
BacktestEngine directly), this script exists to give the newer
orchestrator a real-data entry point of its own — until now every real
invocation was a test (test_momentum_adapter.py, test_core_engine.py);
there was no way to run it against real data from the command line or
the UI.

Real OHLCV/universe/sector data is fetched via DataStoreClient (SPEC-DS-
002 — reuses run_phase1_backtest.py's fetch helpers rather than
duplicating them) and turned into the plain closures/dicts
OrchestratorConfig expects (price_lookup, universe_provider,
sector_lookup). universe_provider is built PIT-safe from that same real
OHLCV (_build_config's ticker_dates/universe_provider below): a ticker is
only offered as a candidate on dates where it actually has a recent real
trading bar, so a stock that lists mid-window or delists mid-window is
correctly absent as a candidate before it existed / after it stopped
trading — no fabricated listing/delisting dates, and no separate data
source needed since it's derived entirely from OHLCV already in hand.
This does NOT yet consult the real `delisted_companies`/`stock_master.
listing_date` tables (a belt-and-suspenders improvement flagged as
follow-up work, since delisted_companies' own scraper is unverified —
see datastore/schema/create_normalised.py's _CREATE_DELISTED_COMPANIES) —
OHLCV presence alone already closes the sharpest form of the bias (a
ticker that doesn't exist yet showing up as a candidate on day one).

The result is persisted via backtest.core.run_store.save_run_result()
into the SAME backtest_runs table every other channel/run already writes
to — so a run triggered here shows up in the existing GET /api/v1/
backtest/runs listing (and the Backtest page's Runs table) with no
separate read path needed.
"""

import argparse
import contextlib
import hashlib
import json
import logging
import os
import re
import time
import uuid
from datetime import date as date_type
from pathlib import Path
from typing import Any, Dict, Iterator, List, Literal, Optional

import numpy as np
import pandas as pd

from backtest.adapters.fundamental_adapter import BESPOKE_PRESETS, FundamentalAdapter
from backtest.adapters.momentum_adapter import MomentumAdapter
from backtest.adapters.technical_adapter import TechnicalAdapter
from backtest.adapters.technical_combo_adapter import TechnicalComboAdapter
from backtest.batch_common import exclusive_backtest_lock
from backtest.core.engine import (
    ALL_EXIT_POLICY_VARIANTS,
    BacktestOrchestrator,
    OrchestratorConfig,
    TechnicalFeatureLookup,
    build_exit_model_for_variant,
)
from backtest.core.feature_log import FeatureLogWriter
from backtest.core.horizon import HorizonBucket, sizing_for
from backtest.core.run_context import BacktestRun, BacktestRunResult
from backtest.core.run_store import save_run_result
from backtest.trade_filters import ADTV_LOOKBACK_SESSIONS as PIT_ADTV_LOOKBACK_SESSIONS
from backtest.run_phase1_backtest import _real_market_cap_map, _real_sector_map
from backtest.strategy_id import (
    CODE_TO_HORIZON,
    build_strategy_id,
    default_horizon_for_fundamental,
    default_horizon_for_momentum,
    default_horizon_for_technical,
)
from config.settings import (
    BACKTEST_DUCKDB_PATH, DUCKDB_PATH,
    DUCKDB_WRITE_LOCK_RETRY_ATTEMPTS, DUCKDB_WRITE_LOCK_RETRY_BASE_DELAY_S, DUCKDB_WRITE_LOCK_RETRY_MAX_DELAY_S,
)
from config.timezone import now_ist
from strategies.definitions import DefinitionNotFound, technical_template_style
from strategies.momentum_identity import registry_name
from features.momentum_universe import (
    MAX_TRACKED_RANK,
    RANK_BANDS,
    all_yearly_full_rankings,
    build_momentum_universe_provider,
    trading_day_calendar,
    yearly_band_approximation_flags_from_rankings,
    yearly_rank_lookup_from_rankings,
)
from config.universe import get_tickers, get_top_adtv_tickers
from config.backtest_exclusions import apply_exclusions
from datastore.api.db import get_duckdb_connection
from backtest.export_trade_book import export_trade_book
from datastore.client import DataStoreClient
from datastore.schema.create_backtest import create_backtest_schema
from datastore.schema.create_strategy_catalog import create_strategy_catalog_schema

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# A84: the module contract's default tradeable universe -- top 800 by
# POINT-IN-TIME ADTV. 800 is the figure the backtest module is specified
# against; it is a default, not a limit, and 0 disables PIT ranking entirely.
DEFAULT_PIT_ADTV_TOP_N = 800

# A96/A101: the earliest date a backtest may start. Before this, price history
# crosses the 2007-04-02 legacy/Fyers seam (69 of 473 tickers jump more than
# 2x across it) and includes corporate actions that were never applied. A
# window that reaches further back does not produce a longer track record, it
# produces a fabricated one, so it is clamped rather than honoured.
EARLIEST_RELIABLE_START = date_type(2009, 4, 1)

WINDOW_PRESETS = {"3y": 3, "5y": 5, "10y": 10, "15y": 15, "max": None}


def resolve_window(
    window: Optional[str],
    start_date: Optional[date_type],
    end_date: Optional[date_type],
    today: Optional[date_type] = None,
) -> tuple[date_type, date_type]:
    """Turn (--window | --start-date/--end-date) into concrete dates.

    Exactly one of `window` and `start_date` may be given: accepting both
    would mean silently ignoring one, and the caller would never learn which.
    """
    if window and start_date:
        raise ValueError(
            "Pass --window OR --start-date, not both: they are two ways to say "
            "the same thing and honouring one would silently discard the other."
        )
    end = end_date or (today or date_type.today())

    if window:
        key = window.strip().lower()
        if key not in WINDOW_PRESETS:
            raise ValueError(
                f"Unknown --window {window!r}. Valid: {sorted(WINDOW_PRESETS)}"
            )
        years = WINDOW_PRESETS[key]
        start = EARLIEST_RELIABLE_START if years is None else _minus_years(end, years)
    elif start_date:
        start = start_date
    else:
        raise ValueError("One of --window or --start-date is required.")

    if start < EARLIEST_RELIABLE_START:
        logger.warning(
            "window start %s precedes %s; clamping. Earlier history crosses the "
            "legacy/Fyers seam and has unrepaired corporate actions (A99-A102), "
            "so returns from it would be fabricated rather than merely noisy.",
            start, EARLIEST_RELIABLE_START,
        )
        start = EARLIEST_RELIABLE_START
    if start >= end:
        raise ValueError(f"Empty window: start {start} is not before end {end}.")
    return start, end


def _minus_years(d: date_type, years: int) -> date_type:
    """Calendar-year subtraction that survives 29 February."""
    try:
        return d.replace(year=d.year - years)
    except ValueError:
        return d.replace(year=d.year - years, day=28)

REPORTS_DIR = Path(__file__).resolve().parent / "reports"

# Feature-log spill lives on DISK, deliberately not in tempfile.gettempdir().
#
# [2026-08-11] On this host /tmp is a RAM-backed tmpfs (7.3G), so spilling
# there defeated the entire point of spill mode: the writer exists to keep
# backtest_feature_log rows OUT of RAM, and tmpfs put them straight back in
# while also competing with the running job for the same memory. Worse, /tmp
# is shared with every other process on the box — when it filled, job[1] of
# the 65-job TA queue died mid-run with OSError [Errno 122] Disk quota
# exceeded from feature_log.flush(). A spill target must be somewhere a
# long backtest cannot be starved out of.
#
# backtest/cache/ is on the root fs (~450G free) and already gitignored.
# ALPHALENS_SPILL_DIR overrides it for hosts laid out differently.
_SPILL_DIR = Path(__file__).resolve().parent / "cache" / "spill"


def _feature_log_spill_dir() -> Path:
    d = Path(os.environ.get("ALPHALENS_SPILL_DIR") or _SPILL_DIR)
    d.mkdir(parents=True, exist_ok=True)
    return d


HORIZON_BUCKET_MAP = {b.value: b for b in HorizonBucket}


def _fetch_real_ohlcv(
    max_tickers: Optional[int], min_history_days: int, start_date: date_type, end_date: date_type,
    ohlcv_snapshot_dir: Optional[str] = None, pit_adtv_top_n: Optional[int] = None,
) -> pd.DataFrame:
    """Memoised wrapper over _fetch_real_ohlcv_uncached (A87 Stage 1).

    Measured 39.5s per call and identical for every strategy in a sweep, so
    running many strategies in one process now builds it once. A single run
    in its own subprocess is unaffected -- the cache is per-process and
    empty at start.

    The frame is shared, not copied (see shared_panels.get_ohlcv): treat the
    return value as read-only.
    """
    from backtest import shared_panels

    key = shared_panels.ohlcv_key(
        max_tickers, min_history_days, start_date, end_date, ohlcv_snapshot_dir, pit_adtv_top_n,
    )
    return shared_panels.get_ohlcv(
        key,
        lambda: _fetch_real_ohlcv_uncached(
            max_tickers, min_history_days, start_date, end_date, ohlcv_snapshot_dir, pit_adtv_top_n,
        ),
    )


def _fetch_real_ohlcv_uncached(
    max_tickers: Optional[int], min_history_days: int, start_date: date_type, end_date: date_type,
    ohlcv_snapshot_dir: Optional[str] = None, pit_adtv_top_n: Optional[int] = None,
) -> pd.DataFrame:
    """Real OHLCV (config.universe's curated universe) over exactly
    [start_date, end_date] — the run's own requested window, not
    run_phase1_backtest.py's fixed 5-year lookback.

    2026-07-26 fix (backtest-reviewer sign-off): was a per-ticker
    GET /ohlcv/{ticker} loop — measured at ~7-8s/ticker under API load, so a
    ~2300-ticker universe took hours just to fetch before any backtest
    compute started (FeatureBacklog A73). Switched to one
    GET /ohlcv/_bulk call (same ohlcv_adjusted table, identical date-range
    semantics) and filter to the requested universe/min_history_days
    client-side. Row order becomes (ticker, date) alphabetical rather than
    get_tickers()'s list order — confirmed harmless, nothing downstream
    indexes into this DataFrame positionally.

    [2026-08-04] max_tickers now truncates via get_top_adtv_tickers (same
    ADTV-descending helper run_phase1_backtest.py already uses) instead of
    plain get_tickers()[:max_tickers] — the CSV's row order carries no
    liquidity meaning, so --max-tickers 800 used to be an arbitrary 800,
    not the top-800-by-ADTV a user reviewing partial sweep results expects.

    [2026-08-05] ohlcv_snapshot_dir (FeatureBacklog A73, remaining gap):
    the bulk call above is a pure function of (start_date, end_date) alone
    — a technical batch sweep (backtest/run_strategy_queue.py) launches
    each job as its own subprocess, so without this every job in a
    42-template sweep re-issued the same bulk call. When set, reads/writes
    a shared Parquet snapshot instead (backtest/core/ohlcv_prewarm.py) —
    the queue driver prewarms it once before launching jobs. None (default)
    is today's unchanged always-live-fetch behavior.

    [2026-08-20] pit_adtv_top_n DISABLES the static truncation above. Both
    filters rank on ADTV, but get_top_adtv_tickers reads the adtv_cr column
    of TODAY'S universe CSV -- one present-day snapshot applied to the whole
    window -- while the PIT stage (_build_pit_adtv_panel + universe_provider)
    ranks on a trailing 21-session mean as of each date. Running the static
    one FIRST made the PIT one a no-op: ranking 800 names and taking the top
    800 returns all 800, so every historical date screened a pool selected by
    2026 liquidity.

    Measured on the momentum band 501-800 universe: the static list retained
    16.7% of the band's names in 2009, rising monotonically to 51.7% in 2026
    -- the shape of survivorship bias, not of a liquidity filter. The 19-Aug
    grid traded 274 distinct tickers where the pre-repoint engine traded 722.

    So when the caller asks for PIT ranking, load the full curated universe
    and let the PIT stage do the selecting it was written to do. max_tickers
    still truncates statically when pit_adtv_top_n is None, which keeps every
    non-PIT caller byte-identical.
    """
    client = DataStoreClient()
    if max_tickers and pit_adtv_top_n:
        tickers = get_tickers()
    elif max_tickers:
        tickers = get_top_adtv_tickers(max_tickers)
    else:
        tickers = get_tickers()
    # [2026-08-12] Withhold any ticker whose price history has been reviewed
    # and judged unverifiable (config/backtest_exclusions.py). Empty by
    # default, so this is a no-op unless someone has deliberately populated
    # the list; applied here rather than in config/universe.py so ingestion
    # and feature computation still cover these tickers in full.
    tickers = apply_exclusions(tickers, context="backtest OHLCV")
    ticker_set = set(tickers)

    if ohlcv_snapshot_dir:
        from backtest.core.ohlcv_prewarm import get_or_fetch_ohlcv_bulk
        bulk = get_or_fetch_ohlcv_bulk(client, start_date, end_date, Path(ohlcv_snapshot_dir))
    else:
        # DataStoreClient.get_ohlcv_bulk calls .date() on its from_date/to_date
        # args (it's typed for datetime) — a plain date.fromisoformat() argparse
        # value has no .date() method, so these must be Timestamps, not dates.
        from_dt = pd.Timestamp(start_date)
        to_dt = pd.Timestamp(end_date)
        bulk = client.get_ohlcv_bulk(from_dt, to_dt)
    # high/low are carried, not dropped: _build_config detects circuit-locked
    # bars as high == low on positive volume, and A85 made that detection the
    # DEFAULT. Projecting to close/volume only (as this did) meant the
    # detection raised KeyError('high') on every run the moment it stopped
    # being opt-in — it had never executed on this path before. Keep the
    # projection explicit rather than passing the frame through untouched, so
    # a column this function relies on cannot be silently dropped upstream.
    _NEEDED = ["date", "ticker", "close", "volume", "high", "low"]
    missing = [c for c in _NEEDED if c not in bulk.columns]
    if missing:
        raise ValueError(
            f"OHLCV source is missing {missing}; circuit-lock detection needs "
            "high/low. A snapshot built before this column set was required "
            "must be rebuilt (delete it and re-run to refetch)."
        )
    bulk = bulk[bulk["ticker"].isin(ticker_set)][_NEEDED]

    counts = bulk.groupby("ticker").size()
    keep = counts[counts >= min_history_days].index
    ohlcv = bulk[bulk["ticker"].isin(keep)].reset_index(drop=True)

    if ohlcv.empty:
        raise ValueError(f"no ticker in the universe has >= {min_history_days} rows of real OHLCV in [{start_date}, {end_date}]")

    logger.info(f"real data: {ohlcv['ticker'].nunique()}/{len(tickers)} universe tickers had >= {min_history_days} rows")
    return ohlcv


# Tolerance window (calendar days) a ticker's most recent real OHLCV bar
# may be behind `as_of` and still count as "currently tradeable" —
# absorbs ordinary trading holidays / a short individual suspension
# without excluding a stock that's still genuinely listed. A gap wider
# than this (no bar at all in the last ~2 weeks) means the ticker hasn't
# listed yet or has already delisted/stopped trading as of `as_of`.
UNIVERSE_STALENESS_TOLERANCE_DAYS = 10


def _momentum_rank_band_wiring(
    rank_band_id: int, start_date: date_type, end_date: date_type,
) -> Dict[str, Any]:
    """
    (2026-08-05, Momentum engine consolidation Phase 2) Everything the
    momentum channel needs to run against a real market-cap RANK BAND
    (features/momentum_universe.py's RANK_BANDS — the same yearly-fixed
    band universes the standalone MomentumBacktester uses) instead of
    _build_config's generic "every ticker with a recent OHLCV bar" pool:

        universe_provider     -> overrides OrchestratorConfig's default
        approximation_flags   -> MomentumAdapter.exclude_approximated_mcap
        yearly_rank_lookup    -> MomentumAdapter's Phase 3 sticky-promotion
        rank_start            -> which band this instance represents

    All three are derived from ONE all_yearly_full_rankings() call (one DB
    round trip per calendar year, total) — the band slice, the
    approximation flags and the full rank map are pure re-slices of that
    same result, never separate queries.

    include_delisted=True is hard-wired, not a parameter: it is the
    module's own survivorship-bias fix (2026-07-20), every real momentum
    caller already passes it, and a rank-band universe built without it
    silently excludes any stock that delisted after the period being
    tested — precisely the bias this path exists to avoid.

    Uses its own short-lived READ-ONLY connection to DUCKDB_PATH (the
    normalised-schema DB), opened and closed before the run's other
    connections — same pattern/rationale as regime_cm/fundamentals_cm
    below, and safe alongside them since DuckDB permits many concurrent
    read-only connections to one file.
    """
    band = next((b for b in RANK_BANDS if b[0] == rank_band_id), None)
    if band is None:
        raise ValueError(
            f"unknown rank_band_id {rank_band_id!r} — must be one of {[b[0] for b in RANK_BANDS]} "
            f"(see features/momentum_universe.py::RANK_BANDS)"
        )
    _, rank_start, rank_end = band
    from backtest import shared_panels
    with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
        # NOT memoised, deliberately. Measured 2026-08-19 at only 3.6s of the
        # 72s this function cost -- the expensive half is the universe
        # provider below. A process-lifetime memo here would also hide this
        # call from the spy in
        # tests/unit/test_run_orchestrator_backtest.py::TestMomentumRankBandWiring,
        # which is what guards include_delisted=True (the 2026-07-20
        # survivorship-bias fix). Not worth 3.6s.
        yearly_rankings = all_yearly_full_rankings(
            conn, start_date.isoformat(), end_date.isoformat(),
            max_rank=max(rank_end, MAX_TRACKED_RANK), include_delisted=True,
        )
        return {
            "rank_start": rank_start,
            # [2026-08-18] THE shared definition: top 800 by ADTV, then market-cap
            # rank within that set, on the 21-trading-day grid. Replaces
            # build_yearly_rank_band_universe_provider, whose yearly market-cap
            # ranking carried no liquidity gate at all and truncated at
            # MAX_TRACKED_RANK=200 -- which silently emptied bands 6, 7 and 8
            # (201-300, 301-500, 501-800) for every orchestrator run.
            #
            # Live and paper call momentum_band_universe directly; the provider
            # only adds per-grid-point caching, so there is still one rule.
            # The EPOCH-anchored calendar, not this run's own trading_days:
            # the 21-day grid must be a property of the market calendar, or two
            # runs with different start dates would refresh on different days
            # and live would refresh on a third set.
            "universe_provider": build_momentum_universe_provider(
                conn, trading_day_calendar(conn, through=end_date.isoformat()),
                rank_start, rank_end,
                # [2026-08-19] MEASURED 66.3s per call, and the ranked frame it
                # builds per grid point is IDENTICAL for every band and every
                # strategy -- only the rank slice differs. This was the single
                # largest per-job cost in a sweep. One job = one miss, exactly
                # as before; a sweep pays it once.
                snapshot_cache=shared_panels.universe_snapshot_cache(),
            ),
            "approximation_flags": yearly_band_approximation_flags_from_rankings(
                yearly_rankings, rank_start, rank_end,
            ),
            "yearly_rank_lookup": yearly_rank_lookup_from_rankings(yearly_rankings),
        }


def _build_pit_adtv_panel(ohlcv: pd.DataFrame, lookback: int) -> pd.DataFrame:
    """(date x ticker) trailing-mean turnover, SHIFTED so each date carries
    only bars strictly before it.

    The shift is the whole point. Ranking a date on liquidity that includes
    that date's own bar uses volume which had not printed when the decision
    was made — and on a day a name spikes on news, that is exactly the
    lookahead that promotes the stock you could not have bought.
    """
    turnover = (ohlcv.assign(turnover=ohlcv["close"] * ohlcv["volume"])
                .pivot_table(index="date", columns="ticker", values="turnover", aggfunc="sum")
                .sort_index())
    return turnover.rolling(lookback, min_periods=max(2, lookback // 3)).mean().shift(1)


def _build_config_artifacts(
    ohlcv: pd.DataFrame, top_n_by_adtv: Optional[int], block_circuit_fills: bool,
) -> Dict[str, Any]:
    """The expensive, strategy-independent derivations of one OHLCV frame.

    Split out of _build_config so a sweep can share them (A87 Stage 1).
    Measured 12.7s per call, and every strategy in a sweep produced a
    byte-identical result. Nothing strategy-specific may be computed here --
    that is the property that makes sharing safe.
    """
    trading_days = pd.DatetimeIndex(sorted(ohlcv["date"].unique()))
    price_map = {(row.ticker, row.date.date()): row.close for row in ohlcv.itertuples(index=False)}

    # Each ticker's real trading dates as sorted int64 day-ordinals — a
    # fast per-(ticker, as_of) presence check via binary search.
    ticker_dates: Dict[str, "np.ndarray[Any, Any]"] = {
        ticker: np.sort(group["date"].to_numpy().astype("datetime64[D]").astype(np.int64))
        for ticker, group in ohlcv.groupby("ticker")
    }

    adtv_panel = _build_pit_adtv_panel(ohlcv, PIT_ADTV_LOOKBACK_SESSIONS) if top_n_by_adtv else None

    # Circuit-locked bars: high == low on a day that actually traded. See
    # _build_config's own note for why volume > 0 is required.
    locked_bars = set()
    if block_circuit_fills:
        locked = ohlcv[(ohlcv["high"] == ohlcv["low"]) & (ohlcv["volume"] > 0)]
        locked_bars = {
            (row.ticker, row.date.date() if hasattr(row.date, "date") else row.date)
            for row in locked.itertuples(index=False)
        }

    return {
        "trading_days": trading_days, "price_map": price_map, "ticker_dates": ticker_dates,
        "adtv_panel": adtv_panel, "locked_bars": locked_bars,
    }


def _build_config(
    ohlcv: pd.DataFrame, sector_map: Dict[str, str], top_n_by_adtv: Optional[int] = None,
    block_circuit_fills: bool = False, max_blackout_sessions: Optional[int] = None,
) -> OrchestratorConfig:
    # A87 Stage 1: trading_days / price_map / ticker_dates are pure functions
    # of the OHLCV frame, so a sweep sharing one frame shares these too. The
    # cache key is the frame's own identity plus the two options below that
    # change what is derived from it -- never the strategy, which must not be
    # able to influence the universe it is simulated against.
    from backtest import shared_panels

    _artifacts = shared_panels.get_artifacts(
        shared_panels.artifact_key(id(ohlcv), top_n_by_adtv, block_circuit_fills),
        lambda: _build_config_artifacts(ohlcv, top_n_by_adtv, block_circuit_fills),
    )
    trading_days = _artifacts["trading_days"]
    price_map = _artifacts["price_map"]
    ticker_dates = _artifacts["ticker_dates"]
    adtv_panel = _artifacts["adtv_panel"]
    locked_bars = _artifacts["locked_bars"]

    # [2026-08-13] Point-in-time ADTV ranking. The universe was ALREADY being
    # truncated to "top 800 by ADTV" via config.universe.get_top_adtv_tickers,
    # so this looked correct — but that helper ranks on the adtv_cr column of
    # TODAY'S universe CSV, a single static present-day snapshot, and applies
    # it to the whole 2009-2026 window. That is lookahead of the worst kind:
    # a name that became liquid BECAUSE of a rally is admitted to the universe
    # for the years before the rally, i.e. precisely when it was untradeable.
    #
    # INDOTECH is the worked example. Static CSV rank 671 -> inside the top
    # 800 -> tradeable from 2009 in every run. Its real trailing-21-session
    # ADTV rank on its 2023-04-25 entry date was 1,554th. It produced the
    # single largest trade in the entire history (+1,493.95%, replicated
    # across six templates). JAIBALAJI (static 726, PIT 1,305) and SERVOTECH
    # (static 792, PIT 1,253) are the same story.
    #
    # None preserves the previous behaviour exactly for callers that do not
    # opt in, so this is inert until a run asks for it.
    # (built in _build_config_artifacts above, shared across the sweep)

    def universe_provider(as_of: date_type) -> List[str]:
        as_of_ordinal: np.int64 = np.datetime64(as_of, "D").astype(np.int64)
        lower_bound = as_of_ordinal - UNIVERSE_STALENESS_TOLERANCE_DAYS
        candidates = []
        for ticker, dates in ticker_dates.items():
            # Index of the last real bar on/before as_of (searchsorted
            # "right" - 1); a ticker not yet listed at as_of has no such
            # bar at all (idx < 0), and one delisted/suspended too long
            # ago has its last bar older than lower_bound — both excluded.
            idx = np.searchsorted(dates, as_of_ordinal, side="right") - 1
            if idx >= 0 and dates[idx] >= lower_bound:
                candidates.append(ticker)

        if adtv_panel is None:
            return candidates

        as_of_ts = pd.Timestamp(as_of)
        rows = adtv_panel.index[adtv_panel.index <= as_of_ts]
        if len(rows) == 0:
            # Before any ADTV history exists there is no basis to rank on.
            # Returning the unranked candidates would silently disable the
            # filter for the start of the window, so return nothing and let
            # the run show zero trades there instead of untradeable ones.
            return []
        ranked = adtv_panel.loc[rows[-1]].dropna()
        top = set(ranked.nlargest(top_n_by_adtv).index)
        return [t for t in candidates if t in top]

    # Circuit-locked bars, identified as high == low on a day that actually
    # traded. That is the unambiguous signature: a real session with literally
    # no intraday range means the band was hit and held. The pre-existing
    # circuit_band_pct adapter option is a close-to-close PROXY for the same
    # thing and was None in all 195 unconstrained Technical runs, so no run to
    # date has excluded a single locked fill.
    #
    # volume > 0 is required: a flat bar with no volume is a carried-forward
    # price on a non-trading day, not a lock, and treating those as locks
    # would withhold dormant small caps for the wrong reason.
    # (locked_bars built in _build_config_artifacts above, shared across the sweep)

    return OrchestratorConfig(
        trading_days=trading_days,
        universe_provider=universe_provider,
        price_lookup=lambda ticker, as_of: price_map.get((ticker, as_of)),
        sector_lookup=lambda ticker: sector_map.get(ticker, "Unknown"),
        circuit_locked_lookup=(
            (lambda ticker, as_of: (ticker, as_of) in locked_bars)
            if block_circuit_fills else None
        ),
        max_blackout_sessions=max_blackout_sessions,
    )


def _momentum_descriptor(
    top_n: int, lookback_months: int, min_adtv_cr: Optional[float],
    downtrend_filter_pct: Optional[float], circuit_band_pct: Optional[float],
    exit_policy_variant: Optional[str],
    *, rank_band_id: Optional[int] = None, rebalance_cadence_days: Optional[int] = None,
    quality_gate: Optional[Dict[str, float]] = None,
    disable_buys_in_regime: Optional[List[str]] = None,
    orthogonalize_vs_size_beta: bool = False,
    rank_method: str = "trailing_return", skip_months: int = 0,
    top_sectors: int = 5,
    strategy_family: str = "M",
) -> str:
    """Momentum's identity string, covering every parameter that changes the
    signals it emits.

    Technical and Fundamental name themselves with a template or a preset.
    Momentum has no such name, so this was `f"top{top_n}_{lookback_months}m"`
    -- two parameters out of seven. Measured 2026-08-14: a control run and a
    filtered run of the same lookback produced the SAME strategy_key, so both
    wrote to strategy_signals under one identity and the ledger could no
    longer say which variant emitted a given buy. That is the one question
    the ledger exists to answer.

    Only non-default components are appended, so an unfiltered run keeps the
    exact key it had before this change and existing rows stay addressable.

    This is a stopgap, not the destination. The 1,680 registry rows already
    carry real names (`balanced_b3_101-150_lb3mo_monthly_top10`), where the
    category prefix IS the filter set; once momentum runs are addressable as
    registry rows (ML40), identity comes from there and this generated string
    goes away. Encoding identity in a parsed string is the mess A89 exists to
    end -- but a key that collides is worse than a key that is ugly.
    """
    # [ML40-2.4, 2026-08-15] Prefer the name of the strategy_registry row that
    # DECLARES this run. ML41 registered the real grid as
    # {category}_b{band}_{ranks}_lb{N}mo_{rebalance}_top{N}, and this generated
    # string never matched it -- measured 2026-08-14, 10 of 93 ledger
    # strategy_keys resolved to no row, and all 10 were momentum.
    #
    # exit_variant is deliberately absent from the resolved
    # name. They are run parameters, not strategy identity: strategy_signals'
    # PK carries run_id, so two runs of one strategy under different exit
    # policies never collide and stay distinguishable by joining backtest_runs.
    # The FILTERS, which e6c97160 rightly added, survive -- they select the
    # category, which is in the name.
    #
    # None means this parameter combination declares no strategy (an ad-hoc
    # filter mix, an unswept cadence). That falls through to the legacy
    # descriptor below, leaving the run visibly undeclared rather than handing
    # it a plausible key that resolves to nothing -- the original defect.
    resolved = registry_name(
        rank_band_id=rank_band_id,
        lookback_months=lookback_months,
        rebalance_cadence_days=rebalance_cadence_days,
        top_n=top_n,
        min_adtv_cr=min_adtv_cr,
        circuit_band_pct=circuit_band_pct,
        quality_gate=quality_gate,
        disable_buys_in_regime=disable_buys_in_regime,
        orthogonalize_vs_size_beta=orthogonalize_vs_size_beta,
        strategy_family=strategy_family,
        skip_months=skip_months,
    )
    if resolved is not None:
        return str(resolved)

    parts = [f"top{top_n}", f"{lookback_months}m"]
    if skip_months > 0:
        parts.append(f"skip{skip_months}m")
    if rank_method != "trailing_return":
        parts.append(f"rank_{rank_method}")
    if rank_method == "industry_momentum" and top_sectors != 5:
        parts.append(f"sectors{top_sectors}")
    if min_adtv_cr is not None:
        parts.append(f"adtv{min_adtv_cr:g}")
    if downtrend_filter_pct is not None:
        parts.append(f"dt{downtrend_filter_pct:g}")
    if circuit_band_pct is not None:
        parts.append(f"cb{circuit_band_pct:g}")
    # The exit policy changes which sells are emitted, so two runs differing
    # only in it are genuinely different strategies.
    if exit_policy_variant and exit_policy_variant != "risk_managed":
        parts.append(str(exit_policy_variant))
    return "_".join(parts)


def _exit_policy_cadence_for(channel: str) -> Literal["daily", "rebalance"]:
    """Whether this channel's exit policy is evaluated daily or only on
    rebalance dates. See OrchestratorConfig.exit_policy_cadence.

    Momentum is a periodic strategy: it ranks the universe every N days and
    holds the selection until the next ranking. Its entries therefore exist
    ONLY on rebalance dates, so a daily exit emptied a slot that could not be
    refilled for up to a full cadence. Measured on 2026-08-14 at top_n=10:
    lb6 held a median of 1 position, lb3 a mean of 3.86.

    Technical and ML stay daily because their signals genuinely are daily —
    a technical stop is part of the strategy, not an accident of cadence.

    Fundamental is structurally periodic in the same way momentum is (its
    adapter also cuts a ranked list at top_n on rebalance dates) and is left
    on "daily" here pending its own measurement rather than being changed as
    a side effect of the momentum fix.
    """
    return "rebalance" if channel == "momentum" else "daily"


def _sizing_overrides_for(channel: str, top_n: int) -> Optional[Dict[str, Any]]:
    """Per-channel overrides onto the horizon bucket's HorizonSizingPolicy.

    [2026-08-18, explicit user decision] Momentum runs:

      * FULLY INVESTED (investable_pct = 1.0). Its equal-weight slot is
        therefore 1/top_n of the book -- 6.67% at top_n=15 -- which the D21
        bucket's 3% ceiling would clip to less than half. The ceiling is
        raised to exactly the slot size, so the equal-weight rule is what
        sizes a position and the cap never silently becomes the real rule.
      * WITHOUT sector diversification. Momentum's risk control is the
        universe itself: the top 800 names by ADTV. A 20% sector cap is a
        different strategy's risk model, and applying it here rejects buys
        the strategy's own rules selected.

    Every other channel keeps its bucket defaults -- these limits exist for
    good reasons there, and this is deliberately not a global change.
    """
    if channel != "momentum":
        return None
    return {
        # 1.0 = "no cap beyond the equal-weight slot"; expressed as the slot
        # size itself so a future top_n change carries the ceiling with it.
        "max_position_pct": 1.0 / max(int(top_n), 1),
        "max_sector_pct": 1.0,
    }


def build_technical_feature_lookup(engine: Optional[Any] = None) -> TechnicalFeatureLookup:
    """Callable[[ticker, as_of_date], Dict[str, float]] returning that
    ticker's real technical indicator snapshot (sma_200_ratio, rsi_14,
    adx_14, etc. — the full daily feature Parquet row) as of a date —
    used as BacktestOrchestrator's technical_feature_lookup so
    ConditionBasedExitPolicy sees the SAME indicator values ScreenerEngine/
    TechnicalAdapter already read for entry screening (features/technical.
    py::compute_technical_features, materialized into config.settings.
    FEATURES_DAILY_DIR), never recomputed.

    Caches only the SINGLE most-recently-loaded date's feature Parquet, not
    every date ever seen. This closure is built once per run and its
    `lookup` is called by BacktestOrchestrator._apply_exit_policy() — which
    runs every trading day, for every open position — for the entire life
    of the run, walking `as_of` strictly forward in calendar order (see
    core/engine.py's `for as_of_date in config.trading_days:` loop). A plain
    `dict` keyed by date string (the original implementation) therefore
    accumulated one full ~2,300-ticker DataFrame PER TRADING DAY with no
    eviction — confirmed live via py-spy on a 10-year/800-ticker run
    (2026-07-25, FeatureBacklog.md): ~2,500 trading days' worth of
    never-freed DataFrames drove memory from a few hundred MB up past a 6GB
    cgroup cap over ~1.5 hours, independent of exit-policy variant (this
    lookup fires for every technical-channel run, not just
    regime_conditional — that variant's OWN unrelated performance bug,
    fixed the same session in regime_conditional_exit_policy.py, is what
    surfaced this one by making a run run long enough to observe it).
    Bounding the cache to size 1 is correct (not just a size limit that
    happens to work): a date once passed is never looked up again within a
    single run, so evicting anything but the current date loses nothing.

    [PERF, 2026-08-02] `engine` is now an optional shared ScreenerEngine —
    when the caller passes the SAME instance it also gives TechnicalAdapter
    for entry screening (see _run_immediate/_run_deferred below),
    ScreenerEngine._load_df's own size-1 cache (added this session) is
    shared too, so a rebalance date's feature Parquet — read once for
    entry screening AND checked again here for exit conditions — is read
    from disk exactly once instead of twice. None (default) preserves
    prior behavior exactly (a fresh, unshared engine)."""
    from systems.technical_analysis.screener.engine import ScreenerEngine

    engine = engine or ScreenerEngine()

    def lookup(ticker: str, as_of: date_type) -> Dict[str, float]:
        date_str = as_of.isoformat() if hasattr(as_of, "isoformat") else str(as_of)
        df = engine._load_df(date_str)
        if df is None or "ticker" not in df.columns:
            return {}
        row = df.loc[df["ticker"] == ticker]
        if row.empty:
            return {}
        values: Dict[str, float] = row.iloc[0].to_dict()
        return values

    return lookup


def _resolve_horizon_bucket(
    channel: str, horizon_bucket: Optional[str], template_name: Optional[str], preset: Optional[str],
    lookback_months: int,
) -> HorizonBucket:
    """Resolves the run's HorizonBucket: an explicit --horizon-bucket wins
    (full HorizonBucket value or a strategy_id short code, e.g. "21d");
    omitted, it defaults per channel — Technical from the template's declared
    style, read from its strategy_registry row (A95-R1) and mapped through the
    Explainer's published style->horizon table; Fundamental from its preset,
    Momentum from its lookback_months (see backtest/strategy_id.py's
    default_horizon_for_* docstrings).

    For a combo run (--combo-templates), template_name is the caller's
    already-resolved "first template in the combo" convenience value (see
    run_orchestrator_backtest's combo branch) — same registry lookup, just
    resolved off one representative template rather than requiring an explicit
    --horizon-bucket for every combo.
    """
    if horizon_bucket:
        if horizon_bucket in HORIZON_BUCKET_MAP:
            return HORIZON_BUCKET_MAP[horizon_bucket]
        if horizon_bucket in CODE_TO_HORIZON:
            return CODE_TO_HORIZON[horizon_bucket]
        raise ValueError(
            f"unknown horizon_bucket {horizon_bucket!r}; must be one of {list(HORIZON_BUCKET_MAP)} "
            f"or a short code {list(CODE_TO_HORIZON)}"
        )

    if channel == "technical":
        if not template_name:
            raise ValueError("cannot default horizon_bucket: --template-name is required for channel=technical")
        # [A95-R1, 2026-08-14] The style comes from the strategy_registry row,
        # not from an imported TEMPLATE_STYLE dict. Verified at switch-over:
        # all 63 templates present, zero style mismatches between the two.
        # A missing row raises rather than falling back to the dict — see
        # strategies/definitions.py's NO SILENT FALLBACK note.
        try:
            style = technical_template_style(template_name)
        except DefinitionNotFound as exc:
            raise ValueError(
                f"cannot default horizon_bucket: unknown --template-name {template_name!r} ({exc})"
            ) from exc
        return default_horizon_for_technical(style)
    if channel == "fundamental":
        if not preset:
            raise ValueError("cannot default horizon_bucket: --preset is required for channel=fundamental")
        return default_horizon_for_fundamental(preset)
    if channel == "momentum":
        return default_horizon_for_momentum(lookback_months)
    raise ValueError(f"unsupported channel {channel!r} — must be technical, fundamental, or momentum")


@contextlib.contextmanager
def _no_regime_conn() -> Iterator[None]:
    yield None


def _build_drawdown_regime_labels(
    conn: Any, index_name: str, bear_drawdown_pct: float,
) -> Dict[date_type, str]:
    """date -> "bear"/"bull" from a running-peak drawdown on `index_name`'s
    real closes in index_ohlcv (2026-08-09, see --bear-drawdown-pct).

    Raises rather than returning empty when the index has no rows: a
    disable-buys-in-bear run whose labels are all missing would silently
    degrade into an unfiltered run and report itself as bear-gated, which is
    the exact silent-no-op failure mode that made the first Category-T sweep
    untrustworthy.
    """
    import pandas as pd

    from systems.regime.market_regime import bear_by_running_peak_drawdown

    rows = conn.execute(
        "SELECT date, close FROM index_ohlcv WHERE index_name = ? AND close IS NOT NULL ORDER BY date",
        [index_name],
    ).fetchall()
    if not rows:
        raise ValueError(
            f"--bear-drawdown-pct was requested but index_ohlcv has no rows for {index_name!r} — "
            "the buy gate would match nothing and the run would misreport itself as bear-gated."
        )
    prices = pd.Series([r[1] for r in rows], index=pd.to_datetime([r[0] for r in rows]), dtype=float)
    labels = bear_by_running_peak_drawdown(prices, threshold_pct=bear_drawdown_pct)
    return {ts.date(): label for ts, label in labels.items()}


def _persist_run_result(
    conn: Any, run_id: str, channel: str, descriptor: str, run_date: date_type, result: Any,
    template_name: Optional[str], preset: Optional[str], top_n: int, lookback_months: int,
    saved_exit_policy_variant: Optional[str], regime_method: Optional[str], report_suffix: Optional[str],
) -> None:
    """The DB-writing tail shared by both run_orchestrator_backtest() code
    paths (immediate and defer_db_writes) — save_run_result + strategy_catalog
    upsert + enriched trade-book export. Extracted (2026-08-02, Technical
    sweep parallelization) so this write-sensitive logic is written and
    tested exactly once rather than risking the two paths drifting apart.
    """
    queue_id = re.sub(r"_job\d+$", "", report_suffix) if report_suffix else None
    save_run_result(conn, result, queue_id=queue_id)

    # strategy_catalog upsert (2026-07-24 addition, additive only — does
    # not affect run/simulation logic): one row per distinct strategy
    # CONFIGURATION, keyed on channel+descriptor+params so re-running the
    # same config updates the existing row instead of duplicating.
    create_strategy_catalog_schema(
        BACKTEST_DUCKDB_PATH,
        retry_attempts=DUCKDB_WRITE_LOCK_RETRY_ATTEMPTS,
        retry_base_delay_s=DUCKDB_WRITE_LOCK_RETRY_BASE_DELAY_S,
        retry_max_delay_s=DUCKDB_WRITE_LOCK_RETRY_MAX_DELAY_S,
    )
    catalog_params = {
        "template_name": template_name, "preset": preset, "top_n": top_n,
        "lookback_months": lookback_months, "exit_policy_variant": saved_exit_policy_variant,
        "regime_method": regime_method,
    }
    catalog_params_json = json.dumps(catalog_params, default=str)
    strategy_key = hashlib.sha1(f"{channel}|{descriptor}|{catalog_params_json}".encode()).hexdigest()
    conn.execute(
        """
        INSERT INTO strategy_catalog
            (strategy_key, channel, descriptor, params_json, latest_run_id, first_run_at, last_run_at, n_runs)
        VALUES (?, ?, ?, ?, ?, ?, ?, 1)
        ON CONFLICT (strategy_key) DO UPDATE SET
            latest_run_id = excluded.latest_run_id,
            last_run_at = excluded.last_run_at,
            n_runs = strategy_catalog.n_runs + 1
        """,
        [strategy_key, channel, descriptor, catalog_params_json, run_id, run_date, run_date],
    )
    conn.commit()

    # Enriched trade-book CSV (entry/exit reason + indicator values) — pure
    # post-processing over data already written above. Reuses `conn`
    # (still open here) rather than opening a second connection to the
    # same file, which DuckDB rejects while a read-write connection is
    # already held.
    if result.trade_log_path:
        # write_html=True only for the technical channel (2026-08-01 user
        # request, scoped to Technical — Momentum's own separate
        # trade-book writer in scripts/run_momentum_*.py is intentionally
        # left CSV-only).
        export_trade_book(run_id, Path(result.trade_log_path), conn=conn, write_html=channel == "technical")


def _run_immediate(
    channel: str, start_date: date_type, end_date: date_type, run_id: str, strategy_id: str,
    horizon: HorizonBucket, descriptor: str, run_date: date_type,
    capital_mode: str, initial_capital: float, sip_amount: Optional[float],
    universe_spec: str, max_tickers: Optional[int], min_history_days: int,
    template_name: Optional[str], preset: Optional[str], top_n: int, lookback_months: int,
    rank_method: str = "trailing_return", skip_months: int = 0, top_sectors: int = 5,
    report_suffix: Optional[str] = None, regime_index_name: Optional[str] = None,
    exit_policy_variant: str = "baseline", regime_method: Optional[str] = None, regime_type: Optional[str] = None, max_hold_days: Optional[int] = None,
    min_adtv_cr: Optional[float] = None, quality_gate_min_f_score: Optional[float] = None,
    quality_gate_max_m_score: Optional[float] = None, downtrend_filter_pct: Optional[float] = None,
    circuit_band_pct: Optional[float] = None, disable_buys_in_regime: Optional[List[str]] = None,
    bear_drawdown_pct: Optional[float] = None,
    combo_templates: Optional[List[str]] = None, precomputed_matches_dir: Optional[Any] = None,
    prefetch_feature_parquets: bool = False, rank_band_id: Optional[int] = None, ohlcv_snapshot_dir: Optional[Any] = None,
    # Point-in-time top-N-by-ADTV universe. Keyword-only and defaulting to
    # None so every existing caller is byte-identical until it opts in.
    *, rebalance_cadence_days: Optional[int] = None,
    defer_feature_log: bool = False,
    annual_reset_spec: Optional[Dict[str, Any]] = None, pit_adtv_top_n: Optional[int] = None,
    block_circuit_fills: bool = False,
    max_blackout_sessions: Optional[int] = None,
    # A98: the index this run is COMPARED against. None keeps the historical
    # behaviour of reusing the regime index, so no existing caller changes.
    benchmark_index_name: Optional[str] = None,
    temp_results_dir: Optional[str] = None,
) -> BacktestRunResult:
    """defer_db_writes=False path — today's existing, unmodified behavior:
    the whole run (OHLCV fetch through the final DB save) holds
    exclusive_backtest_lock and one live BACKTEST_DUCKDB_PATH connection
    throughout. Extracted verbatim from run_orchestrator_backtest() only to
    make room for _run_deferred() as a sibling — no logic changed."""
    with exclusive_backtest_lock(label=f"orchestrator[{run_id}]"):
        ohlcv = _fetch_real_ohlcv(
            max_tickers, min_history_days, start_date, end_date, ohlcv_snapshot_dir, pit_adtv_top_n,
        )
        sector_map = _real_sector_map()
        config = _build_config(
            ohlcv, sector_map, top_n_by_adtv=pit_adtv_top_n,
            block_circuit_fills=block_circuit_fills,
            max_blackout_sessions=max_blackout_sessions,
        )
        config.exit_policy_cadence = _exit_policy_cadence_for(channel)
        config.sizing_overrides = _sizing_overrides_for(channel, top_n)
        # None keeps engine.py's horizon-derived default; an explicit value
        # is the only way to reach a cadence the HORIZON_SIZING table cannot
        # express (biweekly=10, bimonthly=42) -- see the CLI flag's help.
        config.rebalance_cadence_days = rebalance_cadence_days
        # [BUG FIX, 4th fundamental-strategies review, item 2] real wide
        # price/volume panels from the same ohlcv pull momentum's branch
        # below already uses — passed to Technical/Fundamental too so their
        # emitted Signals carry a real Signal.adtv_cr (previously always
        # None outside Momentum, silently no-op'ing check_06_liquidity's
        # MIN_ADT_INR floor for these two channels).
        _price_panel_for_adtv = ohlcv.pivot(index="date", columns="ticker", values="close")
        _volume_panel_for_adtv = ohlcv.pivot(index="date", columns="ticker", values="volume")
        # Hoisted out of the technical branch (2026-08-05, Momentum engine
        # consolidation Phase 1) — the momentum branch now takes the same
        # quality-gate thresholds, so both channels read one construction.
        quality_gate = {}
        if quality_gate_min_f_score is not None:
            quality_gate["min_f_score"] = quality_gate_min_f_score
        if quality_gate_max_m_score is not None:
            quality_gate["max_m_score"] = quality_gate_max_m_score
        # Momentum rank-band universe (2026-08-05 Phase 2). Only when a
        # band was explicitly requested — omitting --rank-band-id keeps
        # every existing momentum job on _build_config's generic universe,
        # unchanged.
        momentum_band = (
            _momentum_rank_band_wiring(rank_band_id, start_date, end_date)
            if channel == "momentum" and rank_band_id is not None
            else None
        )
        if momentum_band:
            config.universe_provider = momentum_band["universe_provider"]

        if channel == "technical":
            if not template_name and not combo_templates:
                raise ValueError("channel=technical requires --template-name or --combo-templates")
            # [PERF, 2026-08-02] ONE ScreenerEngine shared across every
            # TechnicalAdapter/sub-adapter this job constructs AND the
            # exit-check lookup built below — ScreenerEngine._load_df's
            # own size-1 cache is keyed only by date (a Parquet snapshot is
            # per-DATE, not per-template), so sharing it means a given
            # date's feature Parquet is read from disk at most once per
            # job total, instead of once per (template, purpose) pairing —
            # previously every sub-adapter in a combo AND the separate
            # exit-lookup engine each maintained their own unshared cache
            # (or, for entry screening, no cache at all).
            from systems.technical_analysis.screener.engine import ScreenerEngine

            _shared_screener_engine = ScreenerEngine()
            if prefetch_feature_parquets:
                _shared_screener_engine.preload_dates([d.date().isoformat() for d in config.trading_days])
            # 2026-08-09: point-in-time bear labels for --bear-drawdown-pct. Built
            # here (own short-lived read-only connection — DUCKDB_PATH allows many
            # concurrent readers) because the adapter kwargs below are assembled
            # before regime_cm opens. None when the flag wasn't passed, which leaves
            # TechnicalAdapter on the segment-store path unchanged.
            _drawdown_regime_labels = None
            if bear_drawdown_pct is not None:
                with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as _dd_conn:
                    _drawdown_regime_labels = _build_drawdown_regime_labels(
                        _dd_conn, regime_index_name or "Nifty 500", bear_drawdown_pct,
                    )
                logger.info(
                    f"bear gate: running-peak drawdown >= {bear_drawdown_pct:.0%} on "
                    f"{regime_index_name or 'Nifty 500'} — {sum(1 for v in _drawdown_regime_labels.values() if v == 'bear')}"
                    f"/{len(_drawdown_regime_labels)} dates labelled bear"
                )
            _shared_adapter_kwargs = dict(
                top_n=top_n, sector_lookup=sector_map, screener_engine=_shared_screener_engine,
                price_panel=_price_panel_for_adtv, volume_panel=_volume_panel_for_adtv,
                min_adtv_cr=min_adtv_cr,
                quality_gate=quality_gate or None,
                downtrend_filter_pct=downtrend_filter_pct,
                circuit_band_pct=circuit_band_pct,
                disable_buys_in_regime=set(disable_buys_in_regime) if disable_buys_in_regime else None,
                regime_labels=_drawdown_regime_labels,
                regime_index_name=regime_index_name or "Nifty 500",
                regime_method=regime_method,
                precomputed_matches_dir=precomputed_matches_dir,
                # regime_conn wired post-construction below, same deferred
                # pattern as _screener_cache_conn — the connection doesn't
                # exist yet at this point in the function.
            )
            if combo_templates:
                # 2026-08-01 (Momentum-parity "combination of strategies")
                # — one TechnicalAdapter per combo template, same filter
                # kwargs on each, pooled by TechnicalComboAdapter. Each
                # sub-adapter's top_n is generous (5x the combo's own
                # top_n) since the COMBO applies the real top_n cut after
                # pooling — an individual sub-adapter must not silently
                # truncate a candidate away before pooling gets to see it.
                sub_kwargs = {**_shared_adapter_kwargs, "top_n": top_n * 5}
                sub_adapters = [
                    TechnicalAdapter(template_name=name, **sub_kwargs) for name in combo_templates
                ]
                adapter = TechnicalComboAdapter(sub_adapters, top_n=top_n)
            else:
                adapter = TechnicalAdapter(template_name=template_name, **_shared_adapter_kwargs)
        elif channel == "fundamental":
            if not preset:
                raise ValueError("channel=fundamental requires --preset")
            # [2026-07-28 third model-review, item 8] backtest/live parity
            # gap: this market_cap_lookup wiring applies the
            # LIQUIDITY_FLOOR_MARKET_CAP_CR gate (small_cap_compounders/
            # smile/under_followed only — see fundamental_adapter.py's
            # _PRESETS_NEEDING_LIQUIDITY_FLOOR) to BACKTESTS only. The live
            # GET /screener endpoint (datastore/api/routers/fundamentals.py)
            # has no equivalent — it only applies the separate ADTV-based
            # filter_recommendable() gate. A backtest result for these 3
            # presets can therefore differ from what a live screener call
            # would show for the same date, purely due to this extra floor.
            # Documented, not unified, in this pass — see the matching note
            # in datastore/api/routers/fundamentals.py.
            # 2026-08-14: the regime gate needs the deferred _regime_conn
            # wiring TechnicalAdapter/MomentumAdapter get below, which this
            # adapter has no equivalent of. Refuse rather than accept-and-
            # ignore: silently dropping it is precisely the defect being
            # fixed here, and a run that reports a bear gate it never
            # applied is worse than one that won't start.
            if disable_buys_in_regime:
                raise ValueError(
                    "--disable-buys-in-regime is not supported for channel=fundamental: "
                    "FundamentalAdapter has no regime connection, so the gate would be "
                    "silently inert. Drop the flag, or use the technical/momentum channel."
                )
            adapter = FundamentalAdapter(
                preset=preset, top_n=top_n, sector_lookup=sector_map,
                market_cap_lookup=_real_market_cap_map(),
                price_panel=_price_panel_for_adtv, volume_panel=_volume_panel_for_adtv,
                # Previously omitted entirely — see panel_filters.py. Without
                # these, --min-adtv-cr/--circuit-band-pct/--downtrend-filter-pct
                # were accepted, recorded in config_json, shown in the report,
                # and applied to nothing.
                min_adtv_cr=min_adtv_cr,
                circuit_band_pct=circuit_band_pct,
                downtrend_filter_pct=downtrend_filter_pct,
            )
        elif channel == "momentum":
            adapter = MomentumAdapter(
                price_panel=_price_panel_for_adtv, volume_panel=_volume_panel_for_adtv,
                top_n=top_n, lookback_months=lookback_months,
                sector_lookup=sector_map,
                min_adtv_cr=min_adtv_cr,
                quality_gate=quality_gate or None,
                downtrend_filter_pct=downtrend_filter_pct,
                circuit_band_pct=circuit_band_pct,
                disable_buys_in_regime=set(disable_buys_in_regime) if disable_buys_in_regime else None,
                regime_index_name=regime_index_name or "Nifty 500",
                regime_method=regime_method,
                # Rank-band wiring (Phase 2 approximation_flags, Phase 3
                # sticky promotion) — all None without --rank-band-id.
                rank_start=(momentum_band or {}).get("rank_start"),
                approximation_flags=(momentum_band or {}).get("approximation_flags"),
                yearly_rank_lookup=(momentum_band or {}).get("yearly_rank_lookup"),
                # Phase 0: R-family strategy dispatch (all default-off for M-family compatibility).
                rank_method=rank_method,
                skip_months=skip_months,
                top_sectors=top_sectors,
                # regime_conn wired post-construction below, same deferred
                # pattern as the technical branch (the connection doesn't
                # exist yet at this point in the function).
            )
        else:
            raise ValueError(f"unsupported channel {channel!r} — must be technical, fundamental, or momentum")

        run = BacktestRun(
            run_id=run_id, channel=channel, strategy_id=strategy_id, horizon_bucket=horizon,
            mode="backtest", universe_spec=universe_spec, start_date=start_date, end_date=end_date,
            capital_mode=capital_mode, initial_capital=initial_capital, sip_amount=sip_amount,
            annual_reset_ltcg_rate=(annual_reset_spec or {}).get("ltcg_rate"),
            annual_reset_ltcg_exemption=(annual_reset_spec or {}).get("ltcg_exemption"),
            annual_reset_regime_label=(annual_reset_spec or {}).get("regime_label"),
            annual_reset_top_up_after_loss=(annual_reset_spec or {}).get("top_up_after_loss", True),
            config={
                "template_name": template_name, "preset": preset, "top_n": top_n, "lookback_months": lookback_months,
                "rank_method": rank_method, "skip_months": skip_months,
                "max_tickers": max_tickers, "min_history_days": min_history_days, "exit_variant": exit_policy_variant,
                "max_hold_days": max_hold_days, "min_adtv_cr": min_adtv_cr,
                "strategy_name": descriptor,  # [ML40-2.4] the DECLARED name; canonical_strategy_key prefers it
                "quality_gate_min_f_score": quality_gate_min_f_score, "quality_gate_max_m_score": quality_gate_max_m_score,
                "downtrend_filter_pct": downtrend_filter_pct, "circuit_band_pct": circuit_band_pct,
                "disable_buys_in_regime": disable_buys_in_regime,
                "combo_templates": combo_templates,
                # The cadence the engine ACTUALLY slices on (core/engine.py:1007),
                # not the override -- which is None whenever the horizon default
                # was taken. Recorded because until 2026-08-19 cadence survived
                # only as a WORD inside strategy_id ("..._bimonthly_..."), so the
                # stored config could not answer the one knob the momentum sweep
                # varies most.
                "rebalance_cadence_days": (
                    config.rebalance_cadence_days
                    or sizing_for(horizon).default_rebalance_cadence_days
                ),
            },
        )

        create_backtest_schema(
            BACKTEST_DUCKDB_PATH,
            retry_attempts=DUCKDB_WRITE_LOCK_RETRY_ATTEMPTS,
            retry_base_delay_s=DUCKDB_WRITE_LOCK_RETRY_BASE_DELAY_S,
            retry_max_delay_s=DUCKDB_WRITE_LOCK_RETRY_MAX_DELAY_S,
        )
        # market_regimes lives in the normalised-schema DB (DUCKDB_PATH), a
        # separate file from BACKTEST_DUCKDB_PATH — a second, read-only
        # connection, opened only when a regime breakdown was actually
        # requested (regime_index_name=None skips it entirely).
        regime_cm = (
            get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False)
            if regime_index_name
            else _no_regime_conn()
        )
        # [2026-07-25 fix] piotroski_on_value/margin_of_safety/net_net need
        # raw fundamentals_history + ohlcv_adjusted from the normalised-
        # schema DB (DUCKDB_PATH) — FundamentalAdapter previously required
        # this at construction time (before any DB connection exists here),
        # so orchestrator CLI runs of these 3 presets raised ValueError
        # immediately. Same read-only-second-connection shape as regime_cm.
        fundamentals_cm = (
            get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False)
            if channel == "fundamental" and preset in BESPOKE_PRESETS
            else _no_regime_conn()
        )
        # 2026-07-26 fix: a wider lock-retry budget than the API's default
        # (DUCKDB_WRITE_LOCK_RETRY_ATTEMPTS/_BASE_DELAY_S vs.
        # DUCKDB_LOCK_RETRY_ATTEMPTS/_BASE_DELAY_S) — this job has no outer
        # timeout, unlike the API's read-only requests, so it can afford to
        # wait out sustained read-lock churn from frontend status polling
        # rather than hard-failing after ~15.5s. Reviewed by
        # ml-rigor-reviewer + backtest-reviewer (see FeatureBacklog.md).
        with get_duckdb_connection(
            BACKTEST_DUCKDB_PATH, read_only=False, persist=False,
            retry_attempts=DUCKDB_WRITE_LOCK_RETRY_ATTEMPTS,
            retry_base_delay_s=DUCKDB_WRITE_LOCK_RETRY_BASE_DELAY_S,
            retry_max_delay_s=DUCKDB_WRITE_LOCK_RETRY_MAX_DELAY_S,
        ) as conn, regime_cm as regime_conn, fundamentals_cm as fundamentals_conn:
            if channel == "technical":
                # Wired post-construction (conn doesn't exist yet when
                # `adapter` is built above) — shares entry-signal candidates
                # across every exit-variant job for the same template via
                # technical_screener_cache (backtest/core/screener_cache.py,
                # 2026-07-25 fix — see FeatureBacklog.md). For a combo run,
                # every underlying sub-adapter needs this wired individually
                # (TechnicalComboAdapter itself has no screener/regime state
                # of its own — it only pools its sub-adapters' output).
                _technical_sub_adapters = adapter.adapters if combo_templates else [adapter]
                for _sub in _technical_sub_adapters:
                    _sub._screener_cache_conn = conn
                    # Same deferred-wiring reason as _screener_cache_conn
                    # above — only meaningful when disable_buys_in_regime
                    # was actually requested; regime_conn is always a real
                    # (possibly no-op _no_regime_conn()) context manager either way.
                    _sub._regime_conn = regime_conn
            elif channel == "momentum":
                # Same deferred-wiring pattern as the technical branch above —
                # MomentumAdapter self-fetches regime segments through this
                # connection when disable_buys_in_regime was requested.
                adapter._regime_conn = regime_conn
            elif channel == "fundamental" and preset in BESPOKE_PRESETS:
                # Same deferred-wiring pattern as the technical branch above.
                adapter._db_conn = fundamentals_conn
            feature_log_writer = FeatureLogWriter(conn)
            exit_model = build_exit_model_for_variant(
                exit_policy_variant, regime_conn=regime_conn, regime_index_name=regime_index_name or "Nifty 500",
                max_hold_days=max_hold_days,
            )
            # ConditionBasedExitPolicy (and CompositeExitPolicy wrapping it)
            # need live technical indicator values per ticker/day — only
            # wired for channel=="technical" (the only channel with a real
            # screener-template `template` to re-check); other channels'
            # exit_ctx rows simply won't carry these columns, which
            # ConditionBasedExitPolicy already treats as "never triggers".
            technical_feature_lookup = (
                build_technical_feature_lookup(engine=_shared_screener_engine) if channel == "technical" else None
            )
            # Tag the saved exit_policy_variant with a non-default regime
            # method so experiments comparing 5/10/15/20% thresholds against
            # the SAME strategy+variant stay distinguishable in backtest_runs
            # without a schema migration (regime_method itself isn't a
            # persisted column — see BacktestOrchestrator.__init__ docstring).
            from systems.regime.market_regime import METHOD_NAME as _DEFAULT_REGIME_METHOD
            saved_exit_policy_variant = exit_policy_variant
            if regime_method and regime_method != _DEFAULT_REGIME_METHOD:
                saved_exit_policy_variant = f"{exit_policy_variant}__{regime_method}"

            result = BacktestOrchestrator(
                feature_log_writer=feature_log_writer, regime_conn=regime_conn,
                regime_index_name=regime_index_name or "Nifty 500",
                benchmark_index_name=benchmark_index_name, exit_model=exit_model,
                technical_feature_lookup=technical_feature_lookup, exit_policy_variant=saved_exit_policy_variant,
                regime_method=regime_method,
            ).run(run, adapter, config)
            feature_log_writer.flush()
            # 2026-07-26 (REV6 wiring): report_suffix from run_strategy_queue.py
            # is "{queue_suffix}_job{N}" (see _job_to_cmd's --report-suffix
            # construction) — strip the trailing "_jobN" to recover the
            # queue-level id every job in the same sweep shares, so DSR's
            # n_trials can be counted per sweep. A standalone (non-queue)
            # CLI invocation's report_suffix has no "_jobN" suffix and is
            # used as its own queue_id unchanged (a queue_id of 1 doesn't
            # break anything downstream — DSR with n_trials=1 is simply
            # uncorrected, the mathematically correct answer for a lone run).
            _persist_run_result(
                conn, run_id, channel, descriptor, run_date, result,
                template_name, preset, top_n, lookback_months,
                saved_exit_policy_variant, regime_method, report_suffix,
            )
    return result


def _run_deferred(
    channel: str, start_date: date_type, end_date: date_type, run_id: str, strategy_id: str,
    horizon: HorizonBucket, descriptor: str, run_date: date_type,
    capital_mode: str, initial_capital: float, sip_amount: Optional[float],
    universe_spec: str, max_tickers: Optional[int], min_history_days: int,
    template_name: Optional[str], preset: Optional[str], top_n: int, lookback_months: int,
    rank_method: str = "trailing_return", skip_months: int = 0, top_sectors: int = 5,
    report_suffix: Optional[str] = None, regime_index_name: Optional[str] = None,
    exit_policy_variant: str = "baseline", regime_method: Optional[str] = None, regime_type: Optional[str] = None, max_hold_days: Optional[int] = None,
    min_adtv_cr: Optional[float] = None, quality_gate_min_f_score: Optional[float] = None,
    quality_gate_max_m_score: Optional[float] = None, downtrend_filter_pct: Optional[float] = None,
    circuit_band_pct: Optional[float] = None, disable_buys_in_regime: Optional[List[str]] = None,
    bear_drawdown_pct: Optional[float] = None,
    combo_templates: Optional[List[str]] = None, precomputed_matches_dir: Optional[Any] = None,
    prefetch_feature_parquets: bool = False, rank_band_id: Optional[int] = None, ohlcv_snapshot_dir: Optional[Any] = None,
    # Point-in-time top-N-by-ADTV universe. Keyword-only and defaulting to
    # None so every existing caller is byte-identical until it opts in.
    *, rebalance_cadence_days: Optional[int] = None,
    defer_feature_log: bool = False,
    annual_reset_spec: Optional[Dict[str, Any]] = None, pit_adtv_top_n: Optional[int] = None,
    block_circuit_fills: bool = False,
    max_blackout_sessions: Optional[int] = None,
    # A98: the index this run is COMPARED against. None keeps the historical
    # behaviour of reusing the regime index, so no existing caller changes.
    benchmark_index_name: Optional[str] = None,
    temp_results_dir: Optional[str] = None,
) -> BacktestRunResult:
    """defer_db_writes=True path (2026-08-02, Technical sweep
    parallelization) — see run_orchestrator_backtest's docstring for the
    full rationale. OHLCV fetch through simulation runs with NO
    exclusive_backtest_lock and NO live BACKTEST_DUCKDB_PATH connection;
    FeatureLogWriter spills to a local temp file and TechnicalAdapter's
    screener_cache is skipped. Only the short tail (schema + spill-load +
    save + catalog + trade-book) reacquires the lock."""
    from backtest.core.feature_log import load_spill_file

    ohlcv = _fetch_real_ohlcv(
        max_tickers, min_history_days, start_date, end_date, ohlcv_snapshot_dir, pit_adtv_top_n,
    )
    sector_map = _real_sector_map()
    config = _build_config(
            ohlcv, sector_map, top_n_by_adtv=pit_adtv_top_n,
            block_circuit_fills=block_circuit_fills,
            max_blackout_sessions=max_blackout_sessions,
        )
    # A103 readiness is OFF on the deferred path, deliberately. The point of
    # defer_db_writes is that no live BACKTEST_DUCKDB_PATH connection is held
    # between the OHLCV fetch and the short tail, so parallel workers never
    # contend. The readiness check reads strategy_registry, and doing that per
    # job put a registry read in the middle of every worker -- which collided
    # with other workers' write-lock tails and failed jobs outright (observed
    # at --max-workers 3). Readiness belongs ONCE at queue level, before any
    # worker starts, not inside each job. Tracked as A105.
    config.enforce_readiness = False
    config.exit_policy_cadence = _exit_policy_cadence_for(channel)
    config.sizing_overrides = _sizing_overrides_for(channel, top_n)
    config.rebalance_cadence_days = rebalance_cadence_days
    _price_panel_for_adtv = ohlcv.pivot(index="date", columns="ticker", values="close")
    _volume_panel_for_adtv = ohlcv.pivot(index="date", columns="ticker", values="volume")
    # Hoisted out of the technical branch — see the matching note in
    # _run_immediate (the momentum branch takes the same thresholds).
    quality_gate = {}
    if quality_gate_min_f_score is not None:
        quality_gate["min_f_score"] = quality_gate_min_f_score
    if quality_gate_max_m_score is not None:
        quality_gate["max_m_score"] = quality_gate_max_m_score
    # See the matching note in _run_immediate.
    momentum_band = (
        _momentum_rank_band_wiring(rank_band_id, start_date, end_date)
        if channel == "momentum" and rank_band_id is not None
        else None
    )
    if momentum_band:
        config.universe_provider = momentum_band["universe_provider"]

    if channel == "technical":
        if not template_name and not combo_templates:
            raise ValueError("channel=technical requires --template-name or --combo-templates")
        # [PERF, 2026-08-02] see the matching comment in _run_immediate —
        # one ScreenerEngine shared across every sub-adapter and the
        # exit-check lookup built below, so a given date's feature Parquet
        # is read from disk at most once per job.
        from systems.technical_analysis.screener.engine import ScreenerEngine

        _shared_screener_engine = ScreenerEngine()
        if prefetch_feature_parquets:
            _shared_screener_engine.preload_dates([d.date().isoformat() for d in config.trading_days])
        # 2026-08-09: point-in-time bear labels for --bear-drawdown-pct. Built
        # here (own short-lived read-only connection — DUCKDB_PATH allows many
        # concurrent readers) because the adapter kwargs below are assembled
        # before regime_cm opens. None when the flag wasn't passed, which leaves
        # TechnicalAdapter on the segment-store path unchanged.
        _drawdown_regime_labels = None
        if bear_drawdown_pct is not None:
            with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as _dd_conn:
                _drawdown_regime_labels = _build_drawdown_regime_labels(
                    _dd_conn, regime_index_name or "Nifty 500", bear_drawdown_pct,
                )
            logger.info(
                f"bear gate: running-peak drawdown >= {bear_drawdown_pct:.0%} on "
                f"{regime_index_name or 'Nifty 500'} — {sum(1 for v in _drawdown_regime_labels.values() if v == 'bear')}"
                f"/{len(_drawdown_regime_labels)} dates labelled bear"
            )
        _shared_adapter_kwargs = dict(
            top_n=top_n, sector_lookup=sector_map, screener_engine=_shared_screener_engine,
            price_panel=_price_panel_for_adtv, volume_panel=_volume_panel_for_adtv,
            min_adtv_cr=min_adtv_cr,
            quality_gate=quality_gate or None,
            downtrend_filter_pct=downtrend_filter_pct,
            circuit_band_pct=circuit_band_pct,
            disable_buys_in_regime=set(disable_buys_in_regime) if disable_buys_in_regime else None,
            regime_labels=_drawdown_regime_labels,
            regime_index_name=regime_index_name or "Nifty 500",
            regime_method=regime_method,
            precomputed_matches_dir=precomputed_matches_dir,
        )
        if combo_templates:
            sub_kwargs = {**_shared_adapter_kwargs, "top_n": top_n * 5}
            sub_adapters = [TechnicalAdapter(template_name=name, **sub_kwargs) for name in combo_templates]
            adapter = TechnicalComboAdapter(sub_adapters, top_n=top_n)
        else:
            adapter = TechnicalAdapter(template_name=template_name, **_shared_adapter_kwargs)
    elif channel == "fundamental":
        if not preset:
            raise ValueError("channel=fundamental requires --preset")
        # Same fix as the immediate path above (2026-08-14). This is the
        # branch the parallel queue actually runs (defer_db_writes=True), so
        # every filtered fundamental sweep to date went through here
        # unfiltered.
        if disable_buys_in_regime:
            raise ValueError(
                "--disable-buys-in-regime is not supported for channel=fundamental: "
                "FundamentalAdapter has no regime connection, so the gate would be "
                "silently inert. Drop the flag, or use the technical/momentum channel."
            )
        adapter = FundamentalAdapter(
            preset=preset, top_n=top_n, sector_lookup=sector_map,
            market_cap_lookup=_real_market_cap_map(),
            price_panel=_price_panel_for_adtv, volume_panel=_volume_panel_for_adtv,
            min_adtv_cr=min_adtv_cr,
            circuit_band_pct=circuit_band_pct,
            downtrend_filter_pct=downtrend_filter_pct,
        )
    elif channel == "momentum":
        adapter = MomentumAdapter(
            price_panel=_price_panel_for_adtv, volume_panel=_volume_panel_for_adtv,
            top_n=top_n, lookback_months=lookback_months,
            sector_lookup=sector_map,
            min_adtv_cr=min_adtv_cr,
            quality_gate=quality_gate or None,
            downtrend_filter_pct=downtrend_filter_pct,
            circuit_band_pct=circuit_band_pct,
            disable_buys_in_regime=set(disable_buys_in_regime) if disable_buys_in_regime else None,
            regime_index_name=regime_index_name or "Nifty 500",
            regime_method=regime_method,
            # See the matching note in _run_immediate.
            rank_start=(momentum_band or {}).get("rank_start"),
            approximation_flags=(momentum_band or {}).get("approximation_flags"),
            yearly_rank_lookup=(momentum_band or {}).get("yearly_rank_lookup"),
            # Phase 0: R-family strategy dispatch (all default-off for M-family compatibility).
            rank_method=rank_method,
            skip_months=skip_months,
            top_sectors=top_sectors,
        )
    else:
        raise ValueError(f"unsupported channel {channel!r} — must be technical, fundamental, or momentum")

    run = BacktestRun(
        run_id=run_id, channel=channel, strategy_id=strategy_id, horizon_bucket=horizon,
        mode="backtest", universe_spec=universe_spec, start_date=start_date, end_date=end_date,
        capital_mode=capital_mode, initial_capital=initial_capital, sip_amount=sip_amount,
        annual_reset_ltcg_rate=(annual_reset_spec or {}).get("ltcg_rate"),
        annual_reset_ltcg_exemption=(annual_reset_spec or {}).get("ltcg_exemption"),
        annual_reset_regime_label=(annual_reset_spec or {}).get("regime_label"),
        annual_reset_top_up_after_loss=(annual_reset_spec or {}).get("top_up_after_loss", True),
        config={
            "template_name": template_name, "preset": preset, "top_n": top_n, "lookback_months": lookback_months,
            "max_tickers": max_tickers, "min_history_days": min_history_days, "exit_variant": exit_policy_variant,
            "max_hold_days": max_hold_days, "min_adtv_cr": min_adtv_cr,
            "strategy_name": descriptor,  # [ML40-2.4] the DECLARED name; canonical_strategy_key prefers it
            "quality_gate_min_f_score": quality_gate_min_f_score, "quality_gate_max_m_score": quality_gate_max_m_score,
            "downtrend_filter_pct": downtrend_filter_pct, "circuit_band_pct": circuit_band_pct,
            "disable_buys_in_regime": disable_buys_in_regime,
            "combo_templates": combo_templates,
            # The cadence the engine ACTUALLY slices on (core/engine.py:1007),
            # not the override -- which is None whenever the horizon default
            # was taken. Recorded because until 2026-08-19 cadence survived
            # only as a WORD inside strategy_id ("..._bimonthly_..."), so the
            # stored config could not answer the one knob the momentum sweep
            # varies most.
            "rebalance_cadence_days": (
                config.rebalance_cadence_days
                or sizing_for(horizon).default_rebalance_cadence_days
            ),
        },
    )

    spill_path = _feature_log_spill_dir() / f"backtest_feature_log_spill_{run_id}.jsonl"
    feature_log_writer = FeatureLogWriter(spill_path=spill_path)

    # regime_conn/fundamentals_conn are READ-ONLY connections to DUCKDB_PATH
    # (the normalised-schema DB — a different file from BACKTEST_DUCKDB_PATH),
    # opened unguarded here: multiple concurrent read-only connections to
    # the same DuckDB file don't conflict with each other, only a writer
    # conflicts with anything else — this app already relies on that
    # elsewhere (e.g. the API server's own read-only connections) without
    # needing exclusive_backtest_lock.
    regime_cm = (
        get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False)
        if regime_index_name else _no_regime_conn()
    )
    fundamentals_cm = (
        get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False)
        if channel == "fundamental" and preset in BESPOKE_PRESETS
        else _no_regime_conn()
    )
    with regime_cm as regime_conn, fundamentals_cm as fundamentals_conn:
        if channel == "technical":
            # _screener_cache_conn intentionally left unset (None) — no
            # live BACKTEST_DUCKDB_PATH connection during this compute
            # phase, so each parallel job re-screens live via ScreenerEngine
            # instead of sharing the cross-exit-variant cache. regime_conn
            # (read-only) is still wired since disable_buys_in_regime needs it.
            for _sub in (adapter.adapters if combo_templates else [adapter]):
                _sub._regime_conn = regime_conn
        elif channel == "momentum":
            adapter._regime_conn = regime_conn
        elif channel == "fundamental" and preset in BESPOKE_PRESETS:
            adapter._db_conn = fundamentals_conn

        exit_model = build_exit_model_for_variant(
            exit_policy_variant, regime_conn=regime_conn, regime_index_name=regime_index_name or "Nifty 500",
            max_hold_days=max_hold_days,
        )
        technical_feature_lookup = (
            build_technical_feature_lookup(engine=_shared_screener_engine) if channel == "technical" else None
        )

        from systems.regime.market_regime import METHOD_NAME as _DEFAULT_REGIME_METHOD
        saved_exit_policy_variant = exit_policy_variant
        if regime_method and regime_method != _DEFAULT_REGIME_METHOD:
            saved_exit_policy_variant = f"{exit_policy_variant}__{regime_method}"

        result = BacktestOrchestrator(
            feature_log_writer=feature_log_writer, regime_conn=regime_conn,
            regime_index_name=regime_index_name or "Nifty 500",
            benchmark_index_name=benchmark_index_name, exit_model=exit_model,
            technical_feature_lookup=technical_feature_lookup, exit_policy_variant=saved_exit_policy_variant,
            regime_method=regime_method, regime_type=regime_type,
        ).run(run, adapter, config)
        feature_log_writer.flush()

    # Short, serialized tail — the only part of this run touching
    # BACKTEST_DUCKDB_PATH's single read-write connection.
    with exclusive_backtest_lock(label=f"orchestrator[{run_id}]"):
        create_backtest_schema(
            BACKTEST_DUCKDB_PATH,
            retry_attempts=DUCKDB_WRITE_LOCK_RETRY_ATTEMPTS,
            retry_base_delay_s=DUCKDB_WRITE_LOCK_RETRY_BASE_DELAY_S,
            retry_max_delay_s=DUCKDB_WRITE_LOCK_RETRY_MAX_DELAY_S,
        )
        with get_duckdb_connection(
            BACKTEST_DUCKDB_PATH, read_only=False, persist=False,
            retry_attempts=DUCKDB_WRITE_LOCK_RETRY_ATTEMPTS,
            retry_base_delay_s=DUCKDB_WRITE_LOCK_RETRY_BASE_DELAY_S,
            retry_max_delay_s=DUCKDB_WRITE_LOCK_RETRY_MAX_DELAY_S,
        ) as conn:
            # [2026-08-19] defer_feature_log: leave the spill file on disk for
            # a sweep-level bulk load instead of inserting it here.
            #
            # MEASURED in a warm job: this insert was 7.6s of duckdb
            # executemany plus 3.9s re-reading the spill -- 11.5s of a 31.2s
            # job, for a per-decision DEBUG log, not results.
            #
            # Durability is unaffected, which is the point: the spill file is
            # already written and fsynced by FeatureLogWriter, so deferring
            # moves rows that are on disk into the DB later rather than
            # holding anything in memory. An OOM or a crash loses no feature
            # rows -- the files are still there to load. backtest_runs and
            # strategy_signals (the actual RESULTS) are still written per job
            # by _persist_run_result below, unchanged.
            if not defer_feature_log:
                load_spill_file(conn, spill_path)
            _persist_run_result(
                conn, run_id, channel, descriptor, run_date, result,
                template_name, preset, top_n, lookback_months,
                saved_exit_policy_variant, regime_method, report_suffix,
            )
    return result


def run_orchestrator_backtest(
    channel: str, start_date: date_type, end_date: date_type, strategy_id: Optional[str] = None,
    horizon_bucket: Optional[str] = None,
    # Point-in-time top-N-by-ADTV universe (2026-08-13). None = today's
    # behaviour unchanged, i.e. the static present-day CSV ranking.
    # A84/A85: the module contract's defaults, so a programmatic caller gets
    # the same universe and fill rules as the CLI rather than quietly weaker
    # ones.
    pit_adtv_top_n: Optional[int] = DEFAULT_PIT_ADTV_TOP_N,
    # Refuse fills on circuit-locked bars. False = prior behaviour.
    block_circuit_fills: bool = True,
    # Force-close an open position after this many consecutive no-bar sessions.
    max_blackout_sessions: Optional[int] = None,
    capital_mode: str = "lump", initial_capital: float = 1_000_000.0, sip_amount: Optional[float] = None,
    universe_spec: str = "curated", max_tickers: Optional[int] = None, min_history_days: int = 60,
    template_name: Optional[str] = None, preset: Optional[str] = None, top_n: int = 10,
    lookback_months: int = 6, rank_method: str = "trailing_return", skip_months: int = 0,
    top_sectors: int = 5, strategy_family: str = "M",
    run_id: Optional[str] = None, report_suffix: Optional[str] = None,
    regime_index_name: Optional[str] = "Nifty 500",
    # A98: separate from regime_index_name. None means "compare against the
    # regime index", which is the historical behaviour.
    benchmark_index_name: Optional[str] = None,
    exit_policy_variant: str = "baseline",
    regime_method: Optional[str] = None,
    regime_type: Optional[str] = None,
    max_hold_days: Optional[int] = None,
    min_adtv_cr: Optional[float] = None,
    quality_gate_min_f_score: Optional[float] = None,
    quality_gate_max_m_score: Optional[float] = None,
    downtrend_filter_pct: Optional[float] = None,
    circuit_band_pct: Optional[float] = None,
    disable_buys_in_regime: Optional[List[str]] = None,
    bear_drawdown_pct: Optional[float] = None,
    combo_templates: Optional[List[str]] = None,
    defer_db_writes: bool = False,
    precomputed_matches_dir: Optional[str] = None,
    prefetch_feature_parquets: bool = False,
    rank_band_id: Optional[int] = None,
    rebalance_cadence_days: Optional[int] = None,
    defer_feature_log: bool = False,
    ohlcv_snapshot_dir: Optional[str] = None,
    temp_results_dir: Optional[str] = None,
    # capital_mode="annual_reset" only — the LTCG regime is a run-level input
    # because it changes the FY withdrawal and therefore the trades taken.
    annual_reset_ltcg_rate: Optional[float] = None,
    # False = withdraw surplus as usual but never inject after a losing year.
    annual_reset_top_up_after_loss: bool = True,
    annual_reset_ltcg_exemption: Optional[float] = None,
    annual_reset_regime_label: Optional[str] = None,
) -> Dict[str, Any]:
    """
    ohlcv_snapshot_dir : (2026-08-05, FeatureBacklog A73 remaining gap —
        batch-sweep OHLCV reuse) — when set, _fetch_real_ohlcv reads/writes
        a shared Parquet snapshot (backtest/core/ohlcv_prewarm.py) instead
        of always issuing its own GET /ohlcv/_bulk call. Intended to be set
        by a queue driver (backtest/run_strategy_queue.py) that has already
        prewarmed the snapshot once for the whole sweep's [start_date,
        end_date] — every job then hits the cache instead of independently
        re-fetching the same bulk data. None (default) preserves today's
        always-live-fetch behavior exactly.

    rank_band_id : (2026-08-05, Momentum engine consolidation Phase 2 —
        momentum channel only) run against one of
        features/momentum_universe.py's RANK_BANDS market-cap bands
        (1 = rank 1-50, 2 = 51-100, 3 = 100-150, 4 = 150-200,
        5 = 100-200) instead of the generic recent-OHLCV-bar universe:
        band membership is fixed on the first real trading day of each
        calendar year, built from real PIT market cap with
        include_delisted=True. Also supplies the adapter's
        approximation_flags and the Phase 3 sticky-promotion rank lookup.
        None (default) leaves every existing momentum job's universe
        exactly as it is today. Ignored for other channels.

    precomputed_matches_dir : (2026-08-02, sweep-scale entry-signal reuse)
        directory of scripts/precompute_technical_screener_matches.py
        output — see TechnicalAdapter's precomputed_matches_dir docstring.
        None (default) preserves today's always-live screening exactly.

    prefetch_feature_parquets : (2026-08-02, per-job exit-check speedup —
        technical channel only) — eagerly reads every trading day's
        feature Parquet up front via ScreenerEngine.preload_dates()
        (ThreadPoolExecutor, ~2.2x faster than the lazy one-at-a-time
        reads _apply_exit_policy's daily feature lookup otherwise does —
        confirmed the actual dominant per-job cost via profiling, unlike
        precomputed_matches_dir above which targets entry-signal
        generation, a much smaller cost). Trades memory (every requested
        date's DataFrame held simultaneously — measured ~0.6MB/date, so
        ~1.5GB for a full 10-year/~2,500-day window) for speed. Default
        False preserves today's lazy single-slot-cache behavior exactly.

    defer_db_writes : (2026-08-02, Technical sweep parallelization) —
        default False preserves today's behavior EXACTLY: the whole run
        (OHLCV fetch through the final DB save) holds
        batch_common.exclusive_backtest_lock, a system-wide mutex added
        after a real incident (two concurrently-running queues starved
        each other on DuckDB single-writer lock contention and started
        failing outright — see that lock's docstring). Every existing
        caller omits this param and is completely unaffected.

        True runs the OHLCV-fetch-through-simulation phase with NO
        exclusive_backtest_lock and NO live BACKTEST_DUCKDB_PATH
        connection at all: FeatureLogWriter spills its rows to a local
        temp file instead of writing to DuckDB as it goes (bounded
        memory — see backtest/core/feature_log.py's 2026-08-02 addition),
        and TechnicalAdapter's screener_cache is skipped (each candidate
        template is screened live instead of sharing the cross-exit-
        variant cache — a small recomputation cost, not a correctness
        change). Only the short tail — schema creation, loading the spill
        file, save_run_result, strategy_catalog upsert, trade-book export
        — reacquires the lock and opens the real connection. This is what
        makes it safe for multiple such jobs to run truly concurrently:
        the only genuinely-exclusive resource (BACKTEST_DUCKDB_PATH's
        single read-write connection) is now held for seconds, not the
        job's full multi-minute runtime.
    """
    if combo_templates and len(combo_templates) < 2:
        raise ValueError("combo_templates needs at least 2 templates — use --template-name for a single one")
    horizon = _resolve_horizon_bucket(
        channel, horizon_bucket, combo_templates[0] if combo_templates else template_name, preset, lookback_months,
    )

    run_started = time.monotonic()
    run_date = now_ist()
    run_id = run_id or f"orch_{channel}_{run_date.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

    # descriptor is the channel-specific "what strategy is this" bit: the
    # template name, the preset name, or a top-N/lookback summary for
    # momentum (which has no single named descriptor of its own). Computed
    # unconditionally (not just when strategy_id is auto-built) since
    # strategy_catalog needs it regardless of whether the caller passed an
    # explicit strategy_id.
    # [2026-08-20, user decision] Technical carries its exit variant IN the
    # descriptor ("E6_unconstrained"), so the same template under two exit
    # policies is distinguishable by name and not only by joining
    # backtest_runs.exit_policy_variant. Momentum deliberately does NOT: its
    # variant_name is the registry's declared identity, and exit policy is a
    # run parameter there, not part of what strategy was run (see
    # strategies/momentum_identity.py::registry_name).
    _technical_descriptor = "+".join(combo_templates) if combo_templates else template_name
    if _technical_descriptor and exit_policy_variant:
        _technical_descriptor = f"{_technical_descriptor}_{exit_policy_variant}"
    descriptor = {
        "technical": _technical_descriptor,
        "fundamental": preset,
        "momentum": _momentum_descriptor(
            top_n, lookback_months, min_adtv_cr, downtrend_filter_pct,
            circuit_band_pct, exit_policy_variant,
            rank_band_id=rank_band_id,
            # [ML40-2.4] Momentum has no --rebalance-cadence-days flag: its
            # cadence IS the resolved horizon bucket's default (see
            # core/engine.py:928, which takes config.rebalance_cadence_days or
            # falls back to exactly this). Taken from the same place the run
            # will take it, so the name can never describe a cadence the run
            # did not use.
            rebalance_cadence_days=(
                rebalance_cadence_days
                if rebalance_cadence_days is not None
                else sizing_for(horizon).default_rebalance_cadence_days
            ),
            # Built the same way _run_immediate/_run_deferred build it (from
            # the two score params) — the dict itself is assembled inside those
            # functions and does not exist in this scope. Only its TRUTHINESS
            # matters to the category match, but it is constructed rather than
            # passed as a bool so the two sites cannot diverge on what counts
            # as "a quality gate is set".
            quality_gate={
                k: v for k, v in (
                    ("min_f_score", quality_gate_min_f_score),
                    ("max_m_score", quality_gate_max_m_score),
                ) if v is not None
            },
            disable_buys_in_regime=disable_buys_in_regime,
            # Phase 0: R-family strategy dispatch params for identity string.
            rank_method=rank_method,
            skip_months=skip_months,
            top_sectors=top_sectors,
            strategy_family=strategy_family,
        ),
    }[channel]
    if descriptor is None:
        # technical without a template, or fundamental without a preset. Every
        # caller path validates this earlier; the guard is here because a None
        # descriptor would otherwise reach build_strategy_id and name the run
        # "None", which is unrecoverable once it is in the registry.
        raise ValueError(
            f"channel={channel!r} produced no descriptor — a technical run needs "
            "--template-name/--combo-templates and a fundamental run needs --preset"
        )
    # Bound to its own name rather than reassigned: the parameter is
    # Optional[str], and every path below needs the resolved, definitely-present
    # id. Reassigning left it Optional to a type checker that cannot see
    # build_strategy_id's return type (the pre-commit hook's isolated env).
    resolved_strategy_id: str = strategy_id or build_strategy_id(
        channel, descriptor, horizon, as_of=run_date.date(),  # codified id, backtest/strategy_id.py
    )

    logger.info(
        f"orchestrator backtest starting: channel={channel} strategy_id={resolved_strategy_id} "
        f"run_id={run_id} horizon_bucket={horizon.value}"
    )
    # exclusive_backtest_lock (user-confirmed requirement: backtests run
    # strictly sequentially, never concurrently — see that lock's
    # docstring) is held either across the WHOLE run (_run_immediate,
    # defer_db_writes=False, today's default/unchanged behavior) or only
    # across the short final save (_run_deferred, defer_db_writes=True —
    # 2026-08-02 Technical sweep parallelization, see this function's
    # docstring above).
    # capital_mode="annual_reset" (2026-08-12): bundle the LTCG regime into one
    # optional spec rather than threading three more positionals through two
    # already-long signatures. None for every other capital mode.
    annual_reset_spec = None
    if capital_mode == "annual_reset":
        annual_reset_spec = {
            "ltcg_rate": annual_reset_ltcg_rate,
            "top_up_after_loss": annual_reset_top_up_after_loss,
            "ltcg_exemption": annual_reset_ltcg_exemption,
            "regime_label": annual_reset_regime_label,
        }

    run_fn = _run_deferred if defer_db_writes else _run_immediate
    result = run_fn(
        channel, start_date, end_date, run_id, resolved_strategy_id, horizon, descriptor, run_date,
        capital_mode, initial_capital, sip_amount, universe_spec, max_tickers, min_history_days,
        template_name, preset, top_n, lookback_months,
        rank_method=rank_method, skip_months=skip_months,
        report_suffix=report_suffix, regime_index_name=regime_index_name,
        exit_policy_variant=exit_policy_variant, regime_method=regime_method, regime_type=regime_type, max_hold_days=max_hold_days,
        min_adtv_cr=min_adtv_cr, quality_gate_min_f_score=quality_gate_min_f_score,
        quality_gate_max_m_score=quality_gate_max_m_score, downtrend_filter_pct=downtrend_filter_pct,
        circuit_band_pct=circuit_band_pct, disable_buys_in_regime=disable_buys_in_regime,
        bear_drawdown_pct=bear_drawdown_pct,
        combo_templates=combo_templates, precomputed_matches_dir=precomputed_matches_dir,
        prefetch_feature_parquets=prefetch_feature_parquets, rank_band_id=rank_band_id, ohlcv_snapshot_dir=ohlcv_snapshot_dir,
        rebalance_cadence_days=rebalance_cadence_days,
        defer_feature_log=defer_feature_log,
        annual_reset_spec=annual_reset_spec, pit_adtv_top_n=pit_adtv_top_n,
        block_circuit_fills=block_circuit_fills, max_blackout_sessions=max_blackout_sessions,
        benchmark_index_name=benchmark_index_name,
        temp_results_dir=temp_results_dir,
    )

    runtime_seconds = time.monotonic() - run_started
    logger.info(f"orchestrator backtest finished in {runtime_seconds:.1f}s: {json.dumps(result.metrics, default=str)}")

    report: Dict[str, Any] = result.to_dict()
    report["runtime_seconds"] = runtime_seconds
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    suffix = report_suffix or run_id
    report_path = REPORTS_DIR / f"orchestrator_{suffix}.json"
    with open(report_path, "w") as fh:
        json.dump(report, fh, indent=2, default=str)
    print(f"\nReport written to {report_path}")
    print(f"run_id={run_id}")

    return report


def build_arg_parser() -> argparse.ArgumentParser:
    """The CLI surface, split out of main() so the module contract's defaults
    (A84/A85) can be asserted without executing a backtest."""
    parser = argparse.ArgumentParser(description="Run backtest/core/engine.py's BacktestOrchestrator against real data")
    parser.add_argument("--channel", required=True, choices=["technical", "fundamental", "momentum"])
    parser.add_argument(
        "--strategy-id", default=None,
        help="Defaults to the codified {channel}_{descriptor}_{horizon}_{YYYYMMDD} form (backtest/strategy_id.py)",
    )
    parser.add_argument(
        "--horizon-bucket", default=None, choices=list(HORIZON_BUCKET_MAP) + list(CODE_TO_HORIZON),
        help="Defaults per channel/template per the Explainer's published style table (backtest/strategy_id.py)",
    )
    # A96: the window is a first-class parameter. "N years" existed only as a
    # years_back local inside sweep scripts and was never reachable from the
    # CLI or the API, so every caller hand-computed dates and they drifted.
    # --window is resolved against --end-date (or the latest trading date) so
    # "10y" means the same thing to every caller.
    parser.add_argument(
        "--window", default=None,
        help=(
            "Backtest window as 3y/5y/10y/15y/max, resolved backwards from "
            "--end-date (default: today). Mutually exclusive with --start-date. "
            "Windows reaching before "
            f"{EARLIEST_RELIABLE_START} are clamped, because earlier data crosses "
            "the 2007-04-02 legacy/Fyers seam and the unrepaired corporate "
            "actions in A99-A102 -- returns from that period are not trustworthy."
        ),
    )
    parser.add_argument("--start-date", required=False, default=None, type=date_type.fromisoformat)
    parser.add_argument("--end-date", required=False, default=None, type=date_type.fromisoformat)
    parser.add_argument("--capital-mode", default="lump", choices=["lump", "sip", "annual_reset"])
    # capital_mode="annual_reset" only (2026-08-12). Unlike the lump run — where
    # the report applies whichever LTCG regime it likes to a single trade book —
    # the regime here changes the FY withdrawal, hence next year's capital, hence
    # which trades execute. One run per regime; the engine refuses to guess.
    parser.add_argument(
        "--annual-reset-no-top-up", action="store_true",
        help=(
            "capital_mode=annual_reset only. Withdraw surplus at each FY boundary exactly "
            "as usual, but after a LOSING year inject nothing — the strategy continues on "
            "the capital it has left and must earn its way back before any further "
            "withdrawal. Without this flag a losing year is topped back up to the base, "
            "which means the run is refunded annually and can never report ruin however "
            "badly it performs. The withdrawal threshold stays at the base either way, so "
            "recovery is measured against the original capital."
        ),
    )
    parser.add_argument(
        "--annual-reset-ltcg-rate", type=float, default=None,
        help="LTCG rate for the annual-reset withdrawal, e.g. 0.10 or 0.125. Required for --capital-mode annual_reset.",
    )
    parser.add_argument(
        "--annual-reset-ltcg-exemption", type=float, default=None,
        help="Per-FY LTCG exemption in INR, e.g. 100000 or 125000. Applied to the year's NET long-term gain.",
    )
    parser.add_argument(
        "--annual-reset-regime-label", default=None,
        help="Regime name stamped on the run and its FY ledger, e.g. ltcg_10pct_1L. Required for annual_reset.",
    )
    parser.add_argument("--initial-capital", type=float, default=1_000_000.0)
    parser.add_argument("--sip-amount", type=float, default=None)
    parser.add_argument("--universe-spec", default="curated")
    parser.add_argument("--max-tickers", type=int, default=None)
    parser.add_argument("--min-history-days", type=int, default=60)
    parser.add_argument("--template-name", default=None, help="technical channel: one of the 42 screener templates")
    parser.add_argument(
        "--preset", default=None,
        help=(
            "fundamental channel: any of the 26 features.fundamental_composites.STRATEGY_CATALOG "
            "keys — a SCREENER_PRESETS threshold, a BESPOKE_PRESETS raw-financials strategy "
            "(piotroski_on_value/margin_of_safety/net_net), or a SCORE_FUNCTIONS composite "
            "score (QGLP, Moat, Owner Earnings, etc. — ranked top-N by score, not binary pass/fail)"
        ),
    )
    parser.add_argument("--top-n", type=int, default=10)
    parser.add_argument("--lookback-months", type=int, default=6, help="momentum channel only")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--report-suffix", default=None)
    parser.add_argument(
        "--benchmark-index", default=None,
        help=(
            "Index this strategy's returns are COMPARED against. Separate from "
            "--regime-index on purpose (A98): changing what you compare a "
            "strategy to must not change which regimes it was allowed to "
            "trade in. Defaults to --regime-index, the historical behaviour."
        ),
    )
    parser.add_argument(
        "--regime-index", default="Nifty 500",
        help="Index (in market_regimes) for the per-Bull/Bear/Sideways performance breakdown. Pass '' to skip it.",
    )
    parser.add_argument(
        "--bear-drawdown-pct", type=float, default=None,
        help=(
            "Use a point-in-time running-peak drawdown rule for --disable-buys-in-regime instead "
            "of the market_regimes segment store: a date is 'bear' iff --regime-index closed at "
            "least this fraction (e.g. 0.12) below its highest close UP TO that date. Unlike the "
            "segment classifier this has no confirmation lag, so the identical rule runs in "
            "backtest and live — see systems/regime/market_regime.py::bear_by_running_peak_drawdown."
        ),
    )
    parser.add_argument(
        "--max-blackout-sessions", type=int, default=None,
        help=(
            "Force-close an open position after this many consecutive trading days with "
            "no bar, at its LAST KNOWN price. Without it a position is carried at that "
            "price for as long as the data is missing — INDOTECH's 601-day trade spans a "
            "209-day hole and was marked at whatever price appeared when data resumed. "
            "No stop or target can fire on a day with no bar, so holding through a "
            "blackout is not a decision the strategy made."
        ),
    )
    parser.add_argument(
        "--block-circuit-fills", action=argparse.BooleanOptionalAction, default=True,
        help=(
            "Refuse to fill a buy or sell on a circuit-locked bar (high == low with "
            "volume > 0). There is no opposing side at a locked price, so a fill there "
            "is money the simulation grants itself. Distinct from the adapter's "
            "--circuit-band-pct, which is a close-to-close proxy. Blocked fills are "
            "recorded as data_gaps, not dropped silently. "
            "[A85, 2026-08-13] NOW ON BY DEFAULT — it was off in all 195 unconstrained "
            "Technical runs, which therefore contain fills that could not have happened. "
            "Pass --no-block-circuit-fills to reproduce a historical run exactly."
        ),
    )
    parser.add_argument(
        "--pit-adtv-top-n", type=int, default=DEFAULT_PIT_ADTV_TOP_N,
        help=(
            "Restrict the tradeable universe to the top N by POINT-IN-TIME ADTV, ranked on "
            "a trailing 21-session window ending strictly before each date. The alternative "
            "is --max-tickers ranking on config/universe.py's adtv_cr column — a static "
            "present-day snapshot applied to the whole history, which admits names that "
            "only became liquid later (INDOTECH: static rank 671, actual rank 1,554 on its "
            "2023 entry date, and the single largest trade in the run history). That is "
            "lookahead: a stock enters the universe BECAUSE of the rally the backtest then "
            f"claims to capture. [A84, 2026-08-13] Defaults to {DEFAULT_PIT_ADTV_TOP_N}; "
            "pass 0 to disable and rank statically, which reproduces pre-A84 runs."
        ),
    )
    parser.add_argument(
        "--exit-variant", default="risk_managed", choices=list(ALL_EXIT_POLICY_VARIANTS),
        help=(
            "Exit policy variant (backtest/core/engine.py::build_exit_model_for_variant). "
            "Carried grid: unconstrained (no barrier — the reference), risk_managed "
            "(per-template stop/target/max-hold, all reachable; the DEFAULT), condition "
            "(exit when the entry thesis breaks), combined (risk_managed OR condition), "
            "trailing, atr_adaptive. Retired but still selectable so historical runs stay "
            "reproducible: baseline (three of its four triggers were unreachable — 0.00%% "
            "time exits over 108,762 model-driven exits; use risk_managed instead), "
            "regime_conditional."
        ),
    )
    parser.add_argument(
        "--regime-method", default=None,
        help=(
            "Which market_regimes classification threshold to use for the exit-policy `regime` "
            "column AND regime_breakdown (systems/regime/market_regime.py): e.g. '20pct_threshold_v1' "
            "(default, matches METHOD_NAME), '15pct_threshold_v1', '10pct_threshold_v1', "
            "'5pct_threshold_v1'. Only matters for --exit-variant regime_conditional and for "
            "per-regime performance breakdown; other variants ignore it."
        ),
    )
    parser.add_argument(
        "--regime-type", default=None, choices=["ema_rsi_v1"],
        help=(
            "Phase 2: EMA-RSI regime integration. When set, loads regime data from the feature store "
            "and applies regime exposure to signals. Enriches trade log with regime columns "
            "(regime, regime_exposure, nifty_rsi_14, nifty_ema_5, nifty_ema_10). "
            "Currently supports: 'ema_rsi_v1' (Nifty 50 EMA-RSI regime, 2009-2026)."
        ),
    )
    # 2026-08-01 Momentum-parity additions — all default to None/off, no
    # behavior change for any existing caller that omits them.
    parser.add_argument(
        "--max-hold-days", type=int, default=None,
        help=(
            "Override the exit policy's day-count barrier (backtest/core/engine.py::"
            "build_exit_model_for_variant) for --exit-variant baseline/combined/trailing/"
            "atr_adaptive/regime_conditional. Default (omit) preserves today's behavior — "
            "day-count barrier effectively off. Ignored for condition/unconstrained."
        ),
    )
    parser.add_argument(
        "--min-adtv-cr", type=float, default=None,
        help="technical channel: liquidity floor (crores trailing ADTV) — a candidate below this is dropped before ranking.",
    )
    parser.add_argument(
        "--quality-gate-min-f-score", type=float, default=None,
        help="technical channel: drop a candidate whose Piotroski F-score is below this (only affects tickers with a real score on record).",
    )
    parser.add_argument(
        "--quality-gate-max-m-score", type=float, default=None,
        help="technical channel: drop a candidate whose Beneish M-score exceeds this (only affects tickers with a real score on record).",
    )
    parser.add_argument(
        "--downtrend-filter-pct", type=float, default=None,
        help="technical channel: drop a candidate whose trailing 20-day price return is <= -this fraction.",
    )
    parser.add_argument(
        "--circuit-band-pct", type=float, default=None,
        help="technical channel: drop a candidate whose latest 1-day return meets/exceeds this magnitude (circuit-lock proxy).",
    )
    parser.add_argument(
        "--disable-buys-in-regime", default=None,
        help=(
            "technical channel: comma-separated regime label(s) (bull/bear/sideways) to disable NEW "
            "buys in, e.g. 'bear' or 'bear,sideways' — a single string (not repeatable) so this also "
            "serializes cleanly as one job-dict field for run_strategy_queue.py."
        ),
    )
    parser.add_argument(
        "--combo-templates", default=None,
        help=(
            "technical channel: comma-separated template names (2+) to pool into ONE combined "
            "TechnicalComboAdapter strategy instead of a single --template-name, e.g. 'A1,D4'. "
            "Mutually exclusive with --template-name."
        ),
    )
    parser.add_argument(
        "--defer-db-writes", action="store_true",
        help=(
            "2026-08-02 Technical sweep parallelization: run OHLCV-fetch-through-simulation with NO "
            "exclusive_backtest_lock and no live BACKTEST_DUCKDB_PATH connection (FeatureLogWriter spills "
            "to a local temp file, screener_cache is skipped) — only the short final save reacquires the "
            "lock. Makes this job safe to run concurrently with other such jobs. Default (omit) is today's "
            "unchanged fully-serialized behavior."
        ),
    )
    parser.add_argument(
        "--precomputed-matches-dir", default=None,
        help=(
            "2026-08-02 sweep-scale entry-signal reuse: directory of "
            "scripts/precompute_technical_screener_matches.py output. A date "
            "inside the manifest's covered range skips live screener_engine.screen() "
            "entirely; a date outside it falls back to live screening. Default (omit) "
            "is today's unchanged always-live behavior."
        ),
    )
    parser.add_argument(
        "--prefetch-feature-parquets", action="store_true",
        help=(
            "2026-08-02 per-job exit-check speedup (technical channel only): eagerly reads every "
            "trading day's feature Parquet up front via a thread pool (~2.2x faster than the lazy "
            "single-slot cache for this — the ACTUAL dominant per-job cost, unlike --precomputed-matches-dir "
            "above). Trades memory (~0.6MB/day held simultaneously, ~1.5GB for a full 10-year window) for "
            "speed. Default (omit) is today's unchanged lazy-loading behavior."
        ),
    )
    parser.add_argument(
        "--rebalance-cadence-days", type=int, default=None,
        help=(
            "trading days between rebalances. Omit to use the resolved horizon bucket's "
            "default (backtest/core/horizon.py::HORIZON_SIZING), which is what every run "
            "did before 2026-08-19. Needed because that table only expresses 5/21/63/252 "
            "days, while the momentum registry grid declares FIVE cadences — weekly=5, "
            "biweekly=10, monthly=21, bimonthly=42, quarterly=63. Without this flag the "
            "biweekly and bimonthly rows (672 of the 1,680) were unreachable from the CLI: "
            "they resolved to no registry name and fell back to a generated descriptor."
        ),
    )
    # Phase 0: R-family strategy dispatch parameters (all optional, default-off).
    parser.add_argument(
        "--strategy-family", type=str, default="M", choices=["M", "R"],
        help=(
            "momentum channel only (Phase 0): strategy family to use. 'M' (default) "
            "runs M-family strategies (M1-M12); 'R' runs R-family strategies (R1, R3, etc.) "
            "which are registered separately in strategy_registry with distinct names."
        ),
    )
    parser.add_argument(
        "--rank-method", type=str, default="trailing_return",
        choices=["trailing_return", "pct_of_52wk_high", "risk_adjusted", "industry_momentum"],
        help=(
            "momentum channel only (Phase 0): ranking signal to use. 'trailing_return' (default) "
            "is the canonical momentum score. Other methods enable R-family strategies: "
            "'pct_of_52wk_high' for 52-week-high momentum (7.5), 'risk_adjusted' for the "
            "volatility-adjusted composite (section 8), 'industry_momentum' for sector momentum (7.4)."
        ),
    )
    parser.add_argument(
        "--skip-months", type=int, default=0,
        help=(
            "momentum channel only (Phase 0, R3): skip this many recent months when computing "
            "the lookback momentum window. Default 0 uses the full lookback. Set to 1 for the "
            "canonical 12-1 or 6-1 skip-month variants (e.g., --lookback-months 12 --skip-months 1)."
        ),
    )
    parser.add_argument(
        "--top-sectors", type=int, default=5,
        help=(
            "momentum channel only (Phase 4, R4): when using --rank-method industry_momentum, "
            "select this many top-ranked sectors by average momentum, then rank constituents "
            "within those sectors. Default 5 sectors. No effect unless --rank-method is industry_momentum."
        ),
    )
    parser.add_argument(
        "--rank-band-id", type=int, default=None, choices=[b[0] for b in RANK_BANDS],
        help=(
            "momentum channel only (2026-08-05): select from one of features/momentum_universe.py's "
            "RANK_BANDS market-cap rank bands — 1=rank 1-50, 2=51-100, 3=100-150, 4=150-200, "
            "5=100-200 — instead of the generic 'every ticker with a recent OHLCV bar' universe. "
            "Membership is fixed on the first real trading day of each calendar year from real PIT "
            "market cap (include_delisted=True). Also enables the sticky-promotion rule: a held "
            "position promoted to a higher-market-cap band stays rankable instead of being "
            "force-sold at the year boundary. Default (omit) is today's unchanged behavior."
        ),
    )
    parser.add_argument(
        "--ohlcv-snapshot-dir", default=None,
        help=(
            "2026-08-05 (FeatureBacklog A73 remaining gap): directory of a shared OHLCV Parquet "
            "snapshot (backtest/core/ohlcv_prewarm.py), prewarmed once by a queue driver "
            "(backtest/run_strategy_queue.py) before launching a sweep's jobs. When set, this job "
            "reads OHLCV from the snapshot instead of issuing its own GET /ohlcv/_bulk call. "
            "Default (omit) is today's unchanged always-live-fetch behavior."
        ),
    )
    parser.add_argument(
        "--temp-results-dir", default=None,
        help=(
            "2026-08-23 (queue batch-write optimization): directory to write backtest results as JSON "
            "files instead of directly to DuckDB. Used by run_strategy_queue.py to eliminate DB write "
            "lock contention — all jobs write to temp files, queue merges them once at the end. "
            "Default (omit) uses immediate DuckDB writes."
        ),
    )
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    # A96: one resolution point for the window, so --window and explicit dates
    # cannot disagree and every downstream consumer sees concrete dates.
    args.start_date, args.end_date = resolve_window(
        args.window, args.start_date, args.end_date
    )

    run_orchestrator_backtest(
        channel=args.channel, strategy_id=args.strategy_id, horizon_bucket=args.horizon_bucket,
        start_date=args.start_date, end_date=args.end_date, capital_mode=args.capital_mode,
        initial_capital=args.initial_capital, sip_amount=args.sip_amount, universe_spec=args.universe_spec,
        max_tickers=args.max_tickers, min_history_days=args.min_history_days, template_name=args.template_name,
        preset=args.preset, top_n=args.top_n, lookback_months=args.lookback_months,
        rank_method=args.rank_method, skip_months=args.skip_months, top_sectors=args.top_sectors, strategy_family=args.strategy_family,
        run_id=args.run_id,
        report_suffix=args.report_suffix, regime_index_name=args.regime_index or None,
        benchmark_index_name=args.benchmark_index or None,
        exit_policy_variant=args.exit_variant, regime_method=args.regime_method, regime_type=args.regime_type,
        # 0 is the explicit "rank statically" escape hatch; downstream treats
        # None as disabled, so translate here rather than teaching every
        # consumer a second sentinel.
        pit_adtv_top_n=args.pit_adtv_top_n or None,
        block_circuit_fills=args.block_circuit_fills,
        max_blackout_sessions=args.max_blackout_sessions,
        max_hold_days=args.max_hold_days, min_adtv_cr=args.min_adtv_cr,
        quality_gate_min_f_score=args.quality_gate_min_f_score,
        quality_gate_max_m_score=args.quality_gate_max_m_score,
        downtrend_filter_pct=args.downtrend_filter_pct, circuit_band_pct=args.circuit_band_pct,
        disable_buys_in_regime=args.disable_buys_in_regime.split(",") if args.disable_buys_in_regime else None,
        bear_drawdown_pct=args.bear_drawdown_pct,
        combo_templates=args.combo_templates.split(",") if args.combo_templates else None,
        defer_db_writes=args.defer_db_writes,
        annual_reset_ltcg_rate=args.annual_reset_ltcg_rate,
        annual_reset_top_up_after_loss=not args.annual_reset_no_top_up,
        annual_reset_ltcg_exemption=args.annual_reset_ltcg_exemption,
        annual_reset_regime_label=args.annual_reset_regime_label,
        precomputed_matches_dir=args.precomputed_matches_dir,
        prefetch_feature_parquets=args.prefetch_feature_parquets,
        rank_band_id=args.rank_band_id,
        rebalance_cadence_days=args.rebalance_cadence_days,
        ohlcv_snapshot_dir=args.ohlcv_snapshot_dir,
    )


if __name__ == "__main__":
    main()
