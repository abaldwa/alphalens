"""
Pre-Built Momentum Rank Cache — every strategy's per-ticker momentum
score and band-scoped rank, for every rebalance date, computed ONCE and
reused. Sibling of universe_cache.py (same pattern, same rationale): "the
set of stocks for a band on a rebalance cycle" was pre-built there; this
is "the momentum rank of each of those stocks."

WHY A SEPARATE TABLE FROM band_universe_snapshots: band membership
(who's IN the band) and momentum rank (how they score once selected) are
different questions with different cache keys — membership doesn't
depend on lookback_months, rank does. Folding rank into the universe
table would mean 5x duplication of every row (one per lookback) for a
column most queries wouldn't need.

WHY NOT PER-STRATEGY like the legacy momentum_rankings table (176M rows,
keyed by strategy_id — see project_ml_signals_availability /
project_native_orchestrator_and_data_wiring memory for that history):
TrailingMomentumSignal's docstring (common/signals.py) establishes that
R01/R03/R07/R08/R09/R10/R12/R14-R17 ALL rank via the exact same formula,
differing only in lookback_months and post-ranking logic (skip-month
offset, crash overlay, vol-scaling, sector filter, weighting). The raw
per-ticker momentum_return is therefore a function of (ticker, date,
lookback_months, band) alone — never of strategy_id — so one shared
table keyed that way serves every consumer, no per-strategy duplication.

KEY OPTIMIZATION (mirrors build_universe_cache.py's ranked_liquid_universe
call-once-slice-7-ways trick): momentum_return itself doesn't depend on
band either — it's computed once per (date, lookback) over band_id=13's
(M13, the full ~800-ticker ADTV universe) superset, then SLICED per band
using the already-built band_universe_snapshots membership, with rank
computed fresh within each band's slice. This avoids recomputing the
same OHLCV-derived return 7 times per (date, lookback).

Storage: momentum_framework/cache/universe_cache.duckdb (same file as
the universe cache — one local cache DB, two tables, never the
production DB). Table: momentum_rank_snapshots
(band_id, as_of_date, lookback_months, ticker, momentum_return, rank).
"""

from pathlib import Path
from typing import Any, Dict, Optional
import logging
import os
import threading

import duckdb

logger = logging.getLogger(__name__)

CACHE_DIR = Path(__file__).resolve().parent.parent / "cache"
CACHE_DB_PATH = CACHE_DIR / "universe_cache.duckdb"

# Every lookback_months value any ported strategy actually sweeps — see
# strategies/r01_trailing_momentum.py, r03/r07/r08/r09/r10's LOOKBACK_MONTHS
# ([3, 6, 9, 12]) and r12_reversal_1mo.py's fixed REVERSAL_LOOKBACK_MONTHS
# (1). Not a guess — grepped from every strategy file 2026-09-04.
LOOKBACK_MONTHS_GRID = [1, 3, 6, 9, 12]

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS momentum_rank_snapshots (
    band_id INTEGER NOT NULL,
    as_of_date DATE NOT NULL,
    lookback_months INTEGER NOT NULL,
    ticker VARCHAR NOT NULL,
    momentum_return DOUBLE NOT NULL,
    rank INTEGER NOT NULL,
    PRIMARY KEY (band_id, as_of_date, lookback_months, ticker)
);
CREATE INDEX IF NOT EXISTS idx_momentum_rank_lookup
    ON momentum_rank_snapshots (band_id, as_of_date, lookback_months);
"""


def get_cache_connection(read_only: bool = True) -> duckdb.DuckDBPyConnection:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    if not CACHE_DB_PATH.exists() and read_only:
        raise FileNotFoundError(
            f"Momentum rank cache not built yet at {CACHE_DB_PATH} — run "
            f"scripts/build_momentum_rank_cache.py first. See common/momentum_rank_cache.py."
        )
    conn = duckdb.connect(str(CACHE_DB_PATH), read_only=read_only)
    if not read_only:
        conn.execute(SCHEMA_SQL)
    return conn


_thread_local = threading.local()


def get_thread_cache_connection() -> duckdb.DuckDBPyConnection:
    """
    One read-only connection to the (1GB+) cache file per THREAD, reused
    for the life of that thread rather than opened fresh per call.

    [2026-09-05, explicit user instruction — diagnosing the M13 sweep's
    R03 stall a second time] The stall was NOT (only) R03's date-matching
    — that was real and is fixed by get_cached_ranking()'s floor lookup —
    but the dominant cost turned out to be `duckdb.connect()` itself
    against this file: ~2.9s PER CALL, measured directly, regardless of
    read_only mode or cache hit/miss. signals.py's `_try_cache()` used to
    open-then-close a connection on every single rebalance call (hundreds
    per backtest), so even a perfect cache hit paid ~2.9s of connection
    overhead every time — the sweep was CPU-bound on opening its own
    read-only cache file, not on any actual computation. Caching one
    connection per thread (there are exactly PASS2_MAX_WORKERS of them,
    a small fixed number) turns that into a one-time cost per worker
    thread for the entire sweep.

    Safe to share across every job run on this thread: DuckDB connections
    are safe for sequential (not concurrent) use from a single thread,
    which is exactly the ThreadPoolExecutor access pattern here — one
    thread runs one job's calls to completion before picking up the next.
    Never shared ACROSS threads (that would need real synchronization);
    threading.local() gives each worker thread its own connection object.
    """
    conn = getattr(_thread_local, "conn", None)
    if conn is None:
        conn = get_cache_connection(read_only=True)
        # Same PRAGMA threads cap as run_campaign.py::run_pass2's prod_conn,
        # same reasoning (see that call site's comment): uncapped, this
        # connection's own DuckDB-internal parallelism stacks on top of
        # every other worker thread's connections and oversubscribes the
        # physical cores once per-call overhead stops masking it.
        worker_count = int(os.environ.get("M13_MAX_WORKERS", 4))
        conn.execute(f"PRAGMA threads={max(1, (os.cpu_count() or worker_count) // worker_count)}")
        _thread_local.conn = conn
    return conn


def get_cached_ranking(
    band_id: int,
    as_of_date: str,
    lookback_months: int,
    cache_conn: Optional[Any] = None,
    min_date: Optional[str] = None,
) -> Optional[Dict[str, Dict[str, float]]]:
    """
    Returns {ticker: {"momentum_return": ..., "rank": ...}} for
    (band_id, lookback_months) at the LATEST pre-built date <= as_of_date
    (not >= min_date, if given), or None on a genuine cache miss (nothing
    at or before as_of_date exists for this band+lookback at all).

    FLOOR LOOKUP, NOT EXACT MATCH (2026-09-05, explicit user instruction,
    fixing the M13 top_n sweep's R03 stall): the pre-built grid only ever
    covers the UNSHIFTED rebalance-date union (calendar[::5/10/21]).
    strategies/r03_jt_skipmonth.py ranks at as_of_date minus a 21-trading-
    day skip, which almost never lands exactly on that grid — an exact
    match missed on every R03 rebalance and fell through to a live,
    GIL-holding pandas computation, which is what stalled the sweep.

    Floor-matching is a strict superset of the old exact-match behaviour
    for every OTHER caller (R01/R07/R08/R09/R10/R12/R14-R17): they always
    pass an as_of_date that IS already on the grid, so "the latest cached
    date <= as_of_date" is as_of_date itself — same row, same answer, zero
    behaviour change. For R03, it resolves to the nearest already-tabulated
    snapshot at or before the true skip target: still skips AT LEAST the
    intended month (never uses data newer than the true target, so the
    short-term-reversal contamination J&T's skip-month rule exists to
    avoid is never reintroduced), typically off by at most a few trading
    days given the grid's density.

    `min_date` (pass the backtest's floor_date) additionally refuses to
    snap PAST that boundary — protects the narrow warm-up window right
    after a backtest's start where floor-snapping could otherwise reach
    for a real cached date that existed before the backtest is supposed
    to have any history at all.
    """
    owns_conn = cache_conn is None
    conn = cache_conn or get_cache_connection(read_only=True)
    try:
        min_clause = " AND as_of_date >= ?" if min_date else ""
        min_params = [min_date] if min_date else []
        resolved = conn.execute(
            f"""
            SELECT MAX(as_of_date) FROM momentum_rank_snapshots
            WHERE band_id = ? AND lookback_months = ? AND as_of_date <= ?{min_clause}
            """,
            [band_id, lookback_months, as_of_date] + min_params,
        ).fetchone()
        resolved_date = resolved[0] if resolved else None
        if resolved_date is None:
            return None  # nothing at or before as_of_date (and after min_date) for this band+lookback
        rows = conn.execute(
            """
            SELECT ticker, momentum_return, rank FROM momentum_rank_snapshots
            WHERE band_id = ? AND as_of_date = ? AND lookback_months = ?
            ORDER BY rank
            """,
            [band_id, resolved_date, lookback_months],
        ).fetchall()
        if not rows:
            return None
        return {ticker: {"momentum_return": ret, "rank": rank} for ticker, ret, rank in rows}
    finally:
        if owns_conn:
            conn.close()


def is_floor_eligible(
    normalised_conn: Any, floor_date: Optional[str], as_of_date: str, lookback_days: int,
) -> bool:
    """
    Does a cached (unbounded) momentum_return for `as_of_date` equal what
    a FLOORED live computation (MomentumSignal.floor_date, see
    common/signals.py) would have produced for the same date?

    KEY INSIGHT (2026-09-04, explicit user direction after discussing the
    two-cache-per-floor problem): the cache is built UNBOUNDED (no floor
    at all — the correct convention for production reuse, which never
    wants an artificial start-date floor). A floored and an unbounded
    computation read the EXACT SAME underlying rows — and so produce the
    EXACT SAME value — whenever the lookback window already fits entirely
    on/after floor_date. They only diverge in the narrow warm-up window
    right after floor_date, where floored has no signal yet (not enough
    real history since the floor) but unbounded reaches further back and
    produces a real number anyway.

    So eligibility reduces to: has at least `lookback_days + 1` trading
    sessions elapsed between floor_date and as_of_date? If yes, the cached
    value is safe to use as-is for ANY backtest's floor_date, no matter
    what start_date that backtest uses — one shared unbounded cache serves
    every floor. If no, the cache must NOT be used — treat as unranked,
    matching what a real floored computation would return (empty), not
    what the cache happens to hold.

    `floor_date=None` (unbounded backtest / production) is always eligible
    — there is no warm-up gap to protect against.
    """
    if floor_date is None:
        return True
    count = normalised_conn.execute(
        "SELECT COUNT(DISTINCT date) FROM ohlcv_adjusted WHERE date >= ? AND date <= ?",
        [floor_date, as_of_date],
    ).fetchone()[0]
    return bool(count >= lookback_days + 1)


def cache_coverage_summary(cache_conn: Optional[Any] = None) -> Dict[Any, Dict[str, Any]]:
    """Per-(band, lookback) summary of what's cached — date range and
    count, for sanity-checking after a build."""
    owns_conn = cache_conn is None
    conn = cache_conn or get_cache_connection(read_only=True)
    try:
        rows = conn.execute("""
            SELECT band_id, lookback_months, MIN(as_of_date), MAX(as_of_date), COUNT(DISTINCT as_of_date)
            FROM momentum_rank_snapshots GROUP BY band_id, lookback_months ORDER BY band_id, lookback_months
        """).fetchall()
        return {
            (band_id, lookback): {"first_date": str(first), "last_date": str(last), "date_count": count}
            for band_id, lookback, first, last, count in rows
        }
    finally:
        if owns_conn:
            conn.close()
