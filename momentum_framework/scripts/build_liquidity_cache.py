"""
momentum_framework/scripts/build_liquidity_cache.py

One-time build of circuit_lock_snapshots and adtv_snapshots into
momentum_framework/cache/universe_cache.duckdb (the SAME file
common/momentum_rank_cache.py already uses for the momentum-rank cache —
one cache file, one get_thread_cache_connection() helper, three tables).

WHY THIS EXISTS (2026-09-06): common/liquidity.py::get_circuit_locked_tickers()
and compute_adtv_cr() are called live on EVERY rebalance date for EVERY
campaign job (added this session — Category B8's circuit-lock exclusion,
both buy- and sell-side, and E5's ADTV-based cost/slippage basis). Measured
directly: ~7ms/call and ~42ms/call respectively. For a 5-day-cadence job
over the full 2009-2026 window (~880 rebalances) that is real, avoidable
wall-clock time repeated identically across a 1,116-job campaign, since
"was ticker X locked on date D" and "what was ticker X's trailing ADTV on
date D" never change between jobs — same principle as the existing
momentum-rank cache, applied to these two functions.

Both tables are cheap to build in full: circuit-lock is a filtered scan
(171,643 locked ticker-days found in ~0.5s over 8.1M ohlcv_adjusted rows);
ADTV is one window-function pass over the full table (~1s for 8.1M output
rows). Verified against the live functions to floating-point precision on
real spot-checked tickers/dates before switching any call site over —
see common/liquidity.py::get_circuit_locked_tickers_cached() and
compute_adtv_cr_cached()'s docstrings.

Re-run this whenever ohlcv_adjusted changes materially (new ingestion,
corporate-action backfill, etc.) — these tables are a point-in-time
snapshot, not auto-refreshing.

Run: PYTHONPATH=. python3 momentum_framework/scripts/build_liquidity_cache.py
"""

import time

import duckdb

from momentum_framework.common.momentum_rank_cache import CACHE_DB_PATH

PROD_DB_PATH = "/home/amit/projects/AlphaLens/datastore/normalised/alphalens.duckdb"


def build() -> None:
    conn = duckdb.connect(str(CACHE_DB_PATH), read_only=False)
    conn.execute(f"ATTACH '{PROD_DB_PATH}' AS src (READ_ONLY)")
    try:
        t0 = time.time()
        conn.execute("DROP TABLE IF EXISTS circuit_lock_snapshots")
        conn.execute(
            """
            CREATE TABLE circuit_lock_snapshots AS
            SELECT ticker, date FROM src.ohlcv_adjusted
            WHERE high = low AND volume > 0
            """
        )
        conn.execute("CREATE INDEX idx_circuit_lock_date ON circuit_lock_snapshots(date)")
        n_locked = conn.execute("SELECT COUNT(*) FROM circuit_lock_snapshots").fetchone()[0]
        print(f"circuit_lock_snapshots: {n_locked} rows in {time.time() - t0:.1f}s")

        t0 = time.time()
        conn.execute("DROP TABLE IF EXISTS adtv_snapshots")
        conn.execute(
            """
            CREATE TABLE adtv_snapshots AS
            SELECT ticker, date,
                   AVG(close * volume) OVER (
                       PARTITION BY ticker ORDER BY date
                       ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                   ) / 1e7 AS adtv_cr
            FROM src.ohlcv_adjusted
            WHERE volume > 0
            """
        )
        conn.execute("CREATE INDEX idx_adtv_ticker_date ON adtv_snapshots(ticker, date)")
        n_adtv = conn.execute("SELECT COUNT(*) FROM adtv_snapshots").fetchone()[0]
        print(f"adtv_snapshots: {n_adtv} rows in {time.time() - t0:.1f}s")
    finally:
        conn.execute("DETACH src")
        conn.close()


if __name__ == "__main__":
    build()
