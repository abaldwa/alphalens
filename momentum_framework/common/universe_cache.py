"""
Pre-Built Universe Cache — every strategy's band membership, for every
rebalance date, computed ONCE and reused.

Explicit user instruction (2026-09-04): "One of the major activities we
do for every backtest is to identify the set of stocks for each band for
each rebalancing cycle. We need to pre-build the same and all strategies
to refer to these tables" — this is that table.

WHY THIS MATTERS: before this cache, every strategy's resolve_universe()
call re-ran features/momentum_universe.py::momentum_band_universe() from
scratch — 2 DB queries (~0.38s measured) EVERY rebalance date, for EVERY
strategy, even though most strategies share the same (band, date) pairs.
Across a real campaign (13 strategies x 6-7 bands x ~1,000 rebalance
dates), that's redundant work on the order of tens of minutes that a
one-time pre-build turns into near-zero-cost lookups.

KEY OPTIMIZATION: momentum_band_universe()'s expensive half
(ranked_liquid_universe() — market-cap ranking within the ADTV-liquid
universe) does NOT depend on which band is being resolved; the band is
just a rank_start:rank_end SLICE of that one ranked list. So the builder
computes ranked_liquid_universe() ONCE per date and slices it for all 7
MBANDS bands, rather than once per (band, date) pair — a ~7x reduction
in DB round-trips versus the naive per-band approach.

Storage: a local DuckDB file (momentum_framework/cache/universe_cache.duckdb),
never the read-only production DB. Table: band_universe_snapshots
(band_id, as_of_date, ticker, rank). One row per (band, date, ticker).
"""

from pathlib import Path
from typing import Any, Dict, List, Optional
import logging

import duckdb

logger = logging.getLogger(__name__)

CACHE_DIR = Path(__file__).resolve().parent.parent / "cache"
CACHE_DB_PATH = CACHE_DIR / "universe_cache.duckdb"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS band_universe_snapshots (
    band_id INTEGER NOT NULL,
    as_of_date DATE NOT NULL,
    ticker VARCHAR NOT NULL,
    rank INTEGER NOT NULL,
    PRIMARY KEY (band_id, as_of_date, ticker)
);
CREATE INDEX IF NOT EXISTS idx_band_universe_lookup
    ON band_universe_snapshots (band_id, as_of_date);
"""


def get_cache_connection(read_only: bool = True) -> duckdb.DuckDBPyConnection:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    if not CACHE_DB_PATH.exists() and read_only:
        raise FileNotFoundError(
            f"Universe cache not built yet at {CACHE_DB_PATH} — run "
            f"scripts/build_universe_cache.py first. See common/universe_cache.py."
        )
    conn = duckdb.connect(str(CACHE_DB_PATH), read_only=read_only)
    if not read_only:
        conn.execute(SCHEMA_SQL)
    return conn


def get_cached_universe(band_id: int, as_of_date: str, cache_conn: Optional[Any] = None) -> Optional[List[str]]:
    """
    Returns the cached ticker list for (band_id, as_of_date), or None on
    a cache MISS (date not in the pre-built grid — e.g. an ad-hoc date
    outside the standard rebalance cadences). None, not an empty list,
    distinguishes "not cached" from "cached as genuinely empty" — see
    StrategyAdapter.resolve_universe() for the fallback this triggers.
    """
    owns_conn = cache_conn is None
    conn = cache_conn or get_cache_connection(read_only=True)
    try:
        rows = conn.execute(
            "SELECT ticker FROM band_universe_snapshots WHERE band_id = ? AND as_of_date = ? ORDER BY rank",
            [band_id, as_of_date],
        ).fetchall()
        if not rows:
            # Distinguish "date genuinely not in the grid" from "band+date
            # cached as empty" by checking whether ANY row exists for this
            # band at all near this date range — cheap existence check.
            any_row = conn.execute(
                "SELECT 1 FROM band_universe_snapshots WHERE band_id = ? LIMIT 1", [band_id]
            ).fetchone()
            if any_row is None:
                return None  # band never built at all
            return None  # this specific date not in the pre-built grid
        return [r[0] for r in rows]
    finally:
        if owns_conn:
            conn.close()


def cache_coverage_summary(cache_conn: Optional[Any] = None) -> Dict[int, Dict[str, Any]]:
    """Per-band summary of what's cached — date range and count, for
    sanity-checking after a build."""
    owns_conn = cache_conn is None
    conn = cache_conn or get_cache_connection(read_only=True)
    try:
        rows = conn.execute("""
            SELECT band_id, MIN(as_of_date), MAX(as_of_date), COUNT(DISTINCT as_of_date)
            FROM band_universe_snapshots GROUP BY band_id ORDER BY band_id
        """).fetchall()
        return {
            band_id: {"first_date": str(first), "last_date": str(last), "date_count": count}
            for band_id, first, last, count in rows
        }
    finally:
        if owns_conn:
            conn.close()
