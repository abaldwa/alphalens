"""
Windowed/"slider" analysis tests — real production DB. The exact
scenario in the user's request (2026-09-04: capital infused 2020-01-01,
run through 2024-12-31, ₹10L) is pinned as a named regression.
"""

import pytest

from momentum_framework.backtesting.windowed_analysis import rolling_window_scan, run_window
from momentum_framework.strategies.r01_trailing_momentum import R01TrailingMomentum

pytestmark = pytest.mark.real_data


def test_run_window_arbitrary_dates(prod_conn):
    """The exact scenario from the user's request, pinned as a smoke
    test — must run without error and produce a plausible CAGR."""
    strategy = R01TrailingMomentum(band_id=2, top_n=5, lookback_months=6, rebalance_cadence_days=21)
    result = run_window(strategy, "2020-01-01", "2024-12-31", 1_000_000, prod_conn)

    assert result.integrity_passed
    assert result.trade_count > 100, "5 years of monthly rebalancing should produce well over 100 trades"
    assert -0.5 < result.cagr() < 1.0, f"CAGR={result.cagr():.1%} outside a plausible range for this window"


def test_run_window_capital_amount_does_not_affect_cagr(prod_conn):
    """CAGR is a rate of return — it must be capital-amount-invariant.
    ₹1L and ₹10L over the identical window/strategy should produce the
    same CAGR (same trades, same % moves, different absolute rupees)."""
    strategy_a = R01TrailingMomentum(band_id=2, top_n=5, lookback_months=6, rebalance_cadence_days=21)
    strategy_b = R01TrailingMomentum(band_id=2, top_n=5, lookback_months=6, rebalance_cadence_days=21)

    result_1l = run_window(strategy_a, "2023-01-01", "2023-12-31", 100_000, prod_conn)
    result_10l = run_window(strategy_b, "2023-01-01", "2023-12-31", 1_000_000, prod_conn)

    assert result_1l.cagr() == pytest.approx(result_10l.cagr(), abs=0.01)


def test_rolling_scan_uses_fresh_strategy_per_window(prod_conn):
    """CRITICAL correctness test: rolling_window_scan must construct a
    NEW strategy instance per window via strategy_factory, never reuse
    one — reusing would leak _held/_equity_history state between windows
    (the exact class of bug found in R07/R09 earlier this session,
    applied here to a different mechanism)."""
    constructed_instances = []

    def factory():
        s = R01TrailingMomentum(band_id=2, top_n=5, lookback_months=6, rebalance_cadence_days=21)
        constructed_instances.append(s)
        return s

    df = rolling_window_scan(
        strategy_factory=factory, conn=prod_conn, window_years=1,
        initial_capital=1_000_000, full_start="2022-01-01", full_end="2023-12-31", step_months=12,
    )

    assert len(constructed_instances) == len(df) == 2
    assert constructed_instances[0] is not constructed_instances[1]


def test_rolling_scan_windows_never_exceed_full_end(prod_conn):
    df = rolling_window_scan(
        strategy_factory=lambda: R01TrailingMomentum(
            band_id=2, top_n=5, lookback_months=6, rebalance_cadence_days=21),
        conn=prod_conn, window_years=1, initial_capital=1_000_000,
        full_start="2022-01-01", full_end="2023-06-30", step_months=12,
    )
    import pandas as pd
    for window_end in df["window_end"]:
        assert pd.Timestamp(window_end) <= pd.Timestamp("2023-06-30")


def test_rolling_scan_invalid_params_rejected(prod_conn):
    with pytest.raises(ValueError, match="window_years must be positive"):
        rolling_window_scan(
            strategy_factory=lambda: None, conn=prod_conn, window_years=0,
            initial_capital=1_000_000,
        )
    with pytest.raises(ValueError, match="step_months must be positive"):
        rolling_window_scan(
            strategy_factory=lambda: None, conn=prod_conn, window_years=1,
            initial_capital=1_000_000, step_months=0,
        )
