"""
Regime detection + band-attached benchmark tests. Two layers:
1. Synthetic in-memory data — exact, controlled crash/calm scenarios.
2. Real production DB — the COVID-crash regression check performed
   manually during porting (2026-09-04), now persisted so it can't
   silently break on a future edit.
"""

import numpy as np
import pandas as pd
import pytest

from momentum_framework.common.benchmark import (
    UNRESOLVED_BANDS,
    load_benchmark_equity_curve,
    resolve_benchmark_index,
)
from momentum_framework.common.crash_regime import crash_regime_detector
from momentum_framework.common.regime_detection import detect_ensemble_regime


def test_m7_benchmark_unresolved_by_design():
    """M7 has no matching index in index_ohlcv (only Midcap 50/100/150
    exist, not 'Midcap 250') — this must keep raising until a user
    decision picks a real substitute. See common/benchmark.py."""
    assert 7 in UNRESOLVED_BANDS
    with pytest.raises(ValueError, match="no resolved benchmark index"):
        resolve_benchmark_index(7)


def test_m13_resolves_to_nifty_500():
    """Explicit user instruction, 2026-09-04."""
    assert resolve_benchmark_index(13) == "Nifty 500"


def test_m2_resolves_to_nifty_50():
    assert resolve_benchmark_index(2) == "Nifty 50"


def test_crash_regime_detects_synthetic_drawdown(memory_conn):
    dates = pd.date_range("2020-01-01", periods=300, freq="B")
    np.random.seed(0)
    values = [1000.0]
    for i in range(1, 300):
        ret = np.random.normal(-0.02, 0.04) if 260 <= i <= 280 else np.random.normal(0.0005, 0.005)
        values.append(values[-1] * (1 + ret))
    equity = pd.Series(values, index=dates)

    crash_series = crash_regime_detector(
        equity, drawdown_threshold=-0.15, vol_percentile_threshold=0.75,
        lookback_days=252, vol_lookback_days=20,
    )
    assert crash_series.any(), "A synthetic sharp drawdown+vol-spike window must be flagged"


def test_crash_regime_calm_market_never_flagged(memory_conn):
    dates = pd.date_range("2020-01-01", periods=300, freq="B")
    np.random.seed(1)
    values = [1000.0]
    for _ in range(299):
        values.append(values[-1] * (1 + np.random.normal(0.0003, 0.003)))  # low, steady drift
    equity = pd.Series(values, index=dates)

    crash_series = crash_regime_detector(equity)
    assert not crash_series.any(), "A calm, low-vol market must never be flagged as crash regime"


def test_ensemble_regime_majority_vote_on_synthetic_bull(memory_conn):
    """A steadily, strongly rising series should classify mostly Bull, never mostly Bear."""
    dates = pd.date_range("2020-01-01", periods=100, freq="B")
    prices = pd.Series([100 * (1.003 ** i) for i in range(100)], index=dates)  # steady uptrend

    regimes = detect_ensemble_regime(prices)
    counts = regimes.value_counts()
    assert counts.get("Bull", 0) > counts.get("Bear", 0)


@pytest.mark.real_data
def test_covid_crash_detected_in_real_nifty50_data(prod_conn):
    """
    Regression test for the manual verification performed 2026-09-04
    during porting: the ensemble regime detector must classify the
    March 2020 COVID crash window as majority 'Bear' using REAL Nifty 50
    index data — not synthetic. If this ever fails, either the regime
    detector broke or the underlying index_ohlcv data changed.
    """
    close = load_benchmark_equity_curve(
        band_id=2, conn=prod_conn, start_date="2019-06-01", end_date="2020-06-30",
    )
    assert len(close) > 200, "Expected substantial real Nifty 50 history in this window"

    regimes = detect_ensemble_regime(close)
    covid_window = regimes[(regimes.index >= "2020-03-01") & (regimes.index <= "2020-04-15")]

    bear_fraction = (covid_window == "Bear").mean()
    assert bear_fraction > 0.5, (
        f"Expected majority-Bear classification during the real COVID crash window, "
        f"got {bear_fraction:.0%} Bear"
    )


@pytest.mark.real_data
def test_real_benchmark_data_available_for_resolved_bands(prod_conn):
    """Every band with a resolved benchmark mapping must have actual
    rows in index_ohlcv — catches a stale mapping pointing at a real but
    now-renamed/removed index name."""
    for band_id in [2, 4, 9, 10, 12, 13]:
        close = load_benchmark_equity_curve(band_id, prod_conn, start_date="2020-01-01", end_date="2020-12-31")
        assert len(close) > 50, f"band_id={band_id}'s benchmark has too little real data in 2020"
