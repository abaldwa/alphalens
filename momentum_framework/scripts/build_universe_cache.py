"""
momentum_framework/scripts/build_universe_cache.py

Pre-builds common/universe_cache.py's band_universe_snapshots table —
every M-band's ticker membership, for every date any strategy's
rebalance_cadence_days [5, 10, 21] would actually land on, computed once.

Run: PYTHONPATH=. python3 momentum_framework/scripts/build_universe_cache.py

KEY OPTIMIZATION (see common/universe_cache.py's module docstring):
features.momentum_universe.ranked_liquid_universe() — the expensive half
of band resolution — does NOT depend on which band is being resolved.
Called ONCE per date here, then sliced by each of the 7 MBANDS'
rank_start:rank_end, instead of once per (band, date) pair. Measured
~0.38s/call for the naive per-band approach; this script's approach is
~7x fewer calls for the same coverage.
"""

import time
from typing import List

import duckdb

PROD_DB_PATH = "/home/amit/projects/AlphaLens/datastore/normalised/alphalens.duckdb"
START_DATE = "2009-01-01"
END_DATE = "2026-06-30"
REBALANCE_CADENCES = [5, 10, 21]  # every cadence any ported strategy uses


def _trading_calendar(conn) -> List[str]:
    rows = conn.execute(
        "SELECT DISTINCT date FROM ohlcv_adjusted WHERE date >= ? AND date <= ? ORDER BY date",
        [START_DATE, END_DATE],
    ).fetchall()
    return [str(r[0]) for r in rows]


def _rebalance_date_grid(calendar: List[str]) -> List[str]:
    """Union of every cadence's rebalance dates — the ONLY dates that
    ever actually get looked up by a real backtest, so the only dates
    worth pre-building."""
    dates = set()
    for cadence in REBALANCE_CADENCES:
        dates.update(calendar[::cadence])
    return sorted(dates)


def build() -> None:
    from momentum_framework.common.universe import MBANDS
    from momentum_framework.common.universe_cache import get_cache_connection
    from features.momentum_universe import ranked_liquid_universe

    prod_conn = duckdb.connect(PROD_DB_PATH, read_only=True)
    calendar = _trading_calendar(prod_conn)
    rebalance_dates = _rebalance_date_grid(calendar)

    print(f"Trading calendar: {len(calendar)} days [{START_DATE}, {END_DATE}]")
    print(f"Rebalance date grid (union of cadences {REBALANCE_CADENCES}): {len(rebalance_dates)} dates")
    print(f"Bands: {sorted(MBANDS.keys())}")
    print(f"Estimated time: ~{len(rebalance_dates) * 0.4 / 60:.1f} minutes")

    cache_conn = get_cache_connection(read_only=False)
    cache_conn.execute("DELETE FROM band_universe_snapshots")  # idempotent full rebuild

    t0 = time.time()
    total_rows = 0
    for i, as_of_date in enumerate(rebalance_dates):
        ranked = ranked_liquid_universe(prod_conn, as_of_date)
        if ranked.empty:
            continue

        rows_this_date = []
        for band_id, band in MBANDS.items():
            band_slice = ranked[(ranked["rank"] >= band.rank_start) & (ranked["rank"] <= band.rank_end)]
            for _, row in band_slice.iterrows():
                rows_this_date.append((band_id, as_of_date, row["ticker"], int(row["rank"])))

        if rows_this_date:
            cache_conn.executemany(
                "INSERT INTO band_universe_snapshots (band_id, as_of_date, ticker, rank) VALUES (?, ?, ?, ?)",
                rows_this_date,
            )
            total_rows += len(rows_this_date)

        if (i + 1) % 100 == 0 or (i + 1) == len(rebalance_dates):
            elapsed = time.time() - t0
            rate = (i + 1) / elapsed
            eta = (len(rebalance_dates) - i - 1) / rate if rate > 0 else 0
            print(f"  {i+1}/{len(rebalance_dates)} dates ({elapsed:.0f}s elapsed, ETA {eta:.0f}s) — {total_rows} rows so far")

    prod_conn.close()
    cache_conn.close()

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.0f}s ({elapsed/60:.1f} min). Total rows: {total_rows}")

    # Sanity summary
    from momentum_framework.common.universe_cache import cache_coverage_summary
    summary = cache_coverage_summary()
    print("\nCoverage summary:")
    for band_id, info in summary.items():
        print(f"  band_id={band_id}: {info['date_count']} dates, {info['first_date']} to {info['last_date']}")


if __name__ == "__main__":
    build()
