"""
Universe cache tests — the pre-built band_universe_snapshots table
(common/universe_cache.py). Real production DB only: this cache exists
specifically to avoid re-querying the DB per strategy per rebalance, so
testing it against synthetic data would miss the actual thing being
verified (that live and cached resolution agree on real data).
"""

import pytest

from momentum_framework.common.universe_cache import (
    CACHE_DB_PATH,
    cache_coverage_summary,
    get_cached_universe,
)
from momentum_framework.common.band_universe import resolve_band_universe

pytestmark = pytest.mark.real_data

CACHE_BUILT = CACHE_DB_PATH.exists()
skip_if_not_built = pytest.mark.skipif(
    not CACHE_BUILT, reason="Universe cache not built — run scripts/build_universe_cache.py first"
)


@skip_if_not_built
def test_cache_matches_live_computation(prod_conn):
    """
    The definitive correctness check: for a date actually in the
    pre-built grid, the cached universe must be IDENTICAL (same tickers,
    same order/rank) to what a fresh live query would compute — the
    cache is a performance layer, never a source of divergent truth.
    """
    summary = cache_coverage_summary()
    band_id = 2
    if band_id not in summary:
        pytest.skip(f"band_id={band_id} not in the built cache")

    sample_date = summary[band_id]["first_date"]
    cached = get_cached_universe(band_id, sample_date)
    live = resolve_band_universe(band_id, sample_date, prod_conn)

    assert cached is not None
    assert cached == live, "Cached universe must exactly match a live recomputation, including order"


@skip_if_not_built
def test_cache_covers_all_seven_bands():
    summary = cache_coverage_summary()
    assert set(summary.keys()) == {2, 4, 7, 9, 10, 12, 13}


@skip_if_not_built
def test_cache_miss_on_unbuilt_date_returns_none():
    """A date genuinely outside the pre-built grid (e.g. far in the
    future) must return None, not raise or silently return []."""
    result = get_cached_universe(2, "2099-01-01")
    assert result is None


@skip_if_not_built
def test_strategy_adapter_uses_cache_transparently(prod_conn):
    """StrategyAdapter.resolve_universe() must prefer the cache when
    available — every strategy gets this automatically, no per-strategy
    wiring. Verified via R01 as a representative strategy."""
    from momentum_framework.strategies.r01_trailing_momentum import R01TrailingMomentum

    summary = cache_coverage_summary()
    sample_date = summary[2]["first_date"]

    strategy = R01TrailingMomentum(band_id=2, top_n=5, lookback_months=6, rebalance_cadence_days=21)
    universe = strategy.resolve_universe(sample_date, prod_conn)

    cached = get_cached_universe(2, sample_date)
    assert universe == cached


def test_cache_gracefully_absent_falls_back_to_live(prod_conn, tmp_path, monkeypatch):
    """If the cache file doesn't exist at all, resolve_universe() must
    still work correctly via the live-query fallback — the cache is
    optional infrastructure, not a hard precondition."""
    import momentum_framework.common.universe_cache as uc

    fake_path = tmp_path / "does_not_exist.duckdb"
    monkeypatch.setattr(uc, "CACHE_DB_PATH", fake_path)

    from momentum_framework.strategies.r01_trailing_momentum import R01TrailingMomentum
    strategy = R01TrailingMomentum(band_id=2, top_n=5, lookback_months=6, rebalance_cadence_days=21)
    universe = strategy.resolve_universe("2020-01-01", prod_conn)

    assert len(universe) > 0
