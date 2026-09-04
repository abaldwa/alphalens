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


def get_cached_ranking(
    band_id: int, as_of_date: str, lookback_months: int, cache_conn: Optional[Any] = None
) -> Optional[Dict[str, Dict[str, float]]]:
    """
    Returns {ticker: {"momentum_return": ..., "rank": ...}} for
    (band_id, as_of_date, lookback_months), or None on a cache MISS (this
    exact combination not in the pre-built grid). None, not an empty
    dict, distinguishes "not cached" from "cached as genuinely empty" —
    same convention as universe_cache.py::get_cached_universe(), for the
    same reason (lets a caller fall back to live computation instead of
    silently treating a miss as "no tickers ranked").
    """
    owns_conn = cache_conn is None
    conn = cache_conn or get_cache_connection(read_only=True)
    try:
        rows = conn.execute(
            """
            SELECT ticker, momentum_return, rank FROM momentum_rank_snapshots
            WHERE band_id = ? AND as_of_date = ? AND lookback_months = ?
            ORDER BY rank
            """,
            [band_id, as_of_date, lookback_months],
        ).fetchall()
        if not rows:
            any_row = conn.execute(
                "SELECT 1 FROM momentum_rank_snapshots WHERE band_id = ? AND lookback_months = ? LIMIT 1",
                [band_id, lookback_months],
            ).fetchone()
            if any_row is None:
                return None  # this band+lookback combination never built at all
            return None  # this specific date not in the pre-built grid
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
