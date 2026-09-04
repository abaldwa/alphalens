"""
momentum_framework/scripts/build_momentum_rank_cache.py

Pre-builds common/momentum_rank_cache.py's momentum_rank_snapshots table
— every ticker's momentum_return and band-scoped rank, for every
(band, rebalance date, lookback_months) combination any ported strategy
would actually look up.

Run: PYTHONPATH=. python3 momentum_framework/scripts/build_momentum_rank_cache.py

Requires the universe cache (scripts/build_universe_cache.py) to already
be built — this script slices its band_universe_snapshots to know which
tickers belong to which band on which date, and specifically relies on
band_id=13 (M13, the full ~800-ticker ADTV universe) as the superset to
compute momentum_return over ONCE per (date, lookback_months), sliced
into all 7 bands afterward — see common/momentum_rank_cache.py's module
docstring for why this avoids 7x redundant OHLCV queries.
"""

import time
from typing import Dict, List

import duckdb

PROD_DB_PATH = "/home/amit/projects/AlphaLens/datastore/normalised/alphalens.duckdb"
SUPERSET_BAND_ID = 13  # M13 — full ADTV universe, superset of every other band


def build() -> None:
    from momentum_framework.common.momentum_rank_cache import (
        LOOKBACK_MONTHS_GRID,
        get_cache_connection,
    )
    from momentum_framework.common.signals import TrailingMomentumSignal
    from momentum_framework.common.universe import MBANDS

    prod_conn = duckdb.connect(PROD_DB_PATH, read_only=True)
    # ONE read-write connection for the whole cache file — band_universe_snapshots
    # and momentum_rank_snapshots are both tables in universe_cache.duckdb, and
    # DuckDB refuses a second connection to the same file with a different
    # (read-only vs read-write) configuration. universe_cache.py's own
    # get_cache_connection() would open a SEPARATE read-only connection to the
    # same file, which is what caused this script's first run to fail.
    cache_conn = get_cache_connection(read_only=False)
    universe_conn = cache_conn

    dates_df = universe_conn.execute(
        "SELECT DISTINCT as_of_date FROM band_universe_snapshots WHERE band_id = ? ORDER BY as_of_date",
        [SUPERSET_BAND_ID],
    ).fetch_df()
    rebalance_dates = [str(d) for d in dates_df["as_of_date"]]

    print(f"Rebalance dates (from universe cache, band={SUPERSET_BAND_ID}): {len(rebalance_dates)}")
    print(f"Lookback grid: {LOOKBACK_MONTHS_GRID}")
    print(f"Bands: {sorted(MBANDS.keys())}")
    print(f"Total (date x lookback) signal computations: {len(rebalance_dates) * len(LOOKBACK_MONTHS_GRID)}")

    cache_conn.execute("DELETE FROM momentum_rank_snapshots")  # idempotent full rebuild

    signals_by_lookback = {lb: TrailingMomentumSignal(lookback_months=lb) for lb in LOOKBACK_MONTHS_GRID}

    t0 = time.time()
    total_rows = 0
    for i, as_of_date in enumerate(rebalance_dates):
        superset_tickers = universe_conn.execute(
            "SELECT ticker FROM band_universe_snapshots WHERE band_id = ? AND as_of_date = ?",
            [SUPERSET_BAND_ID, as_of_date],
        ).fetchall()
        superset_tickers = [t[0] for t in superset_tickers]
        if not superset_tickers:
            continue

        # Per-band membership for this date, fetched once and reused across
        # all lookbacks — band membership doesn't depend on lookback_months.
        band_membership: Dict[int, List[str]] = {}
        for band_id in MBANDS:
            rows = universe_conn.execute(
                "SELECT ticker FROM band_universe_snapshots WHERE band_id = ? AND as_of_date = ?",
                [band_id, as_of_date],
            ).fetchall()
            band_membership[band_id] = [r[0] for r in rows]

        rows_this_date = []
        for lookback, signal in signals_by_lookback.items():
            momentum = signal.compute(prod_conn, superset_tickers, as_of_date)
            if momentum.empty:
                continue

            for band_id, tickers in band_membership.items():
                band_scores = momentum[momentum.index.isin(tickers)]
                if band_scores.empty:
                    continue
                ranked = band_scores.sort_values(ascending=False)
                for rank, (ticker, ret) in enumerate(ranked.items(), start=1):
                    rows_this_date.append((band_id, as_of_date, lookback, ticker, float(ret), rank))

        if rows_this_date:
            cache_conn.executemany(
                """INSERT INTO momentum_rank_snapshots
                   (band_id, as_of_date, lookback_months, ticker, momentum_return, rank)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                rows_this_date,
            )
            total_rows += len(rows_this_date)

        if (i + 1) % 50 == 0 or (i + 1) == len(rebalance_dates):
            elapsed = time.time() - t0
            rate = (i + 1) / elapsed
            eta = (len(rebalance_dates) - i - 1) / rate if rate > 0 else 0
            print(f"  {i+1}/{len(rebalance_dates)} dates ({elapsed:.0f}s elapsed, ETA {eta:.0f}s) — {total_rows} rows so far")

    prod_conn.close()
    universe_conn.close()
    cache_conn.close()

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.0f}s ({elapsed/60:.1f} min). Total rows: {total_rows}")

    from momentum_framework.common.momentum_rank_cache import cache_coverage_summary
    summary = cache_coverage_summary()
    print("\nCoverage summary:")
    for (band_id, lookback), info in summary.items():
        print(f"  band_id={band_id}, lookback={lookback}mo: {info['date_count']} dates, {info['first_date']} to {info['last_date']}")


if __name__ == "__main__":
    build()
