"""
Native orchestrator end-to-end tests — real production DB, real
multi-day simulation runs. This is the highest-value part of the suite:
the R01 rotation bug (2026-09-04) was invisible to every other layer of
testing (compile, imports, nomenclature, isolated signal correctness) —
it only showed up when a FULL multi-day backtest actually ran and a
human looked at whether the resulting number was plausible. These tests
encode that lesson as permanent regressions.
"""

import pytest

from momentum_framework.backtesting.orchestrator import BacktestConfig, BacktestOrchestrator
from momentum_framework.strategies.r01_trailing_momentum import R01TrailingMomentum
from momentum_framework.strategies.r07_crash_aware import R07CrashAware
from momentum_framework.strategies.r09_mm_volscale import R09MMVolScale

pytestmark = pytest.mark.real_data


def test_r01_portfolio_actually_rotates(prod_conn):
    """
    Regression test for the 2026-09-04 bug: Portfolio.execute() (the
    pre-fix version) never sold anything, so a "monthly rebalance"
    backtest was actually "buy once in month 1, do nothing for the rest
    of the year." Caught because the user found a 30.5% CAGR implausible
    for a 5-stock Nifty 50 basket in 2023 — confirmed by inspecting real
    rebalance() output (which correctly showed new picks each month) and
    tracing the bug to Portfolio's execution logic, not the strategy.

    Assertion: over a full year with monthly rebalancing (12 periods,
    top_n=5), trade count must be well above the ~5 a "buy once" bug
    would produce — real rotation sells old names and buys new ones.
    """
    strategy = R01TrailingMomentum(band_id=2, top_n=5, lookback_months=6, rebalance_cadence_days=21)
    config = BacktestConfig(start_date="2023-01-01", end_date="2023-12-31", initial_capital=1_000_000)
    result = BacktestOrchestrator(strategy, config).run_native(prod_conn)

    assert result.trade_count > 20, (
        f"Only {result.trade_count} trades over a full year of monthly rebalancing — "
        f"this is the exact signature of the 2026-09-04 no-rotation bug"
    )


def test_r01_cagr_is_plausible_vs_nifty50(prod_conn):
    """
    Regression test for the same bug, from the other direction: the
    pre-fix number (30.5% CAGR) was ~1.5x Nifty 50's actual 2023 return
    for a 5-stock subset of it — implausible concentration-adjusted
    outperformance. Post-fix (11.9%) is a modest, plausible result. This
    test pins a generous but bounded plausible range so a future
    regression trips it without needing the exact number to stay fixed.
    """
    strategy = R01TrailingMomentum(band_id=2, top_n=5, lookback_months=6, rebalance_cadence_days=21)
    config = BacktestConfig(start_date="2023-01-01", end_date="2023-12-31", initial_capital=1_000_000)
    result = BacktestOrchestrator(strategy, config).run_native(prod_conn)

    cagr = result.cagr()
    assert -0.5 < cagr < 1.0, (
        f"R01 CAGR={cagr:.1%} is outside a plausible range for a 5-stock Nifty 50 "
        f"momentum basket over one year — investigate before trusting this number "
        f"(the 2026-09-04 bug produced 0.305, itself inside this generous band, so "
        f"this alone would not have caught it — see test_r01_portfolio_actually_rotates)"
    )


def test_r01_equity_curve_has_no_gaps(prod_conn):
    strategy = R01TrailingMomentum(band_id=2, top_n=5, lookback_months=6, rebalance_cadence_days=21)
    config = BacktestConfig(start_date="2023-01-01", end_date="2023-06-30", initial_capital=1_000_000)
    result = BacktestOrchestrator(strategy, config).run_native(prod_conn)

    assert result.integrity_passed
    assert result.integrity_detail["trading_days"] > 100


def test_r07_crash_overlay_runs_through_covid_without_error(prod_conn):
    """R07's benchmark auto-resolution + crash detection + held-state
    tracking must complete a real run spanning the COVID crash."""
    strategy = R07CrashAware(band_id=2, top_n=5, lookback_months=6, rebalance_cadence_days=21)
    config = BacktestConfig(start_date="2020-01-01", end_date="2020-06-30", initial_capital=1_000_000)
    result = BacktestOrchestrator(strategy, config).run_native(prod_conn)

    assert result.integrity_passed
    assert strategy._benchmark_equity is not None and not strategy._benchmark_equity.empty


def test_r09_equity_history_and_regime_populate_through_native_run(prod_conn):
    """Regression test for the update_portfolio_equity() wiring verified
    manually 2026-09-04: real daily equity accumulation and real regime
    detection must both actually fire during a native run, not just be
    theoretically callable in isolation."""
    strategy = R09MMVolScale(
        band_id=2, top_n=5, lookback_months=6, rebalance_cadence_days=21,
        vol_scaling_mode="inverse_volatility", regime_switching_enabled=True,
    )
    config = BacktestConfig(start_date="2019-09-01", end_date="2020-03-31", initial_capital=1_000_000)
    BacktestOrchestrator(strategy, config).run_native(prod_conn)

    assert strategy._equity_history is not None and len(strategy._equity_history) > 100
    assert strategy._regime_series is not None and len(strategy._regime_series) > 1000


def test_r09_top_n_sensitivity_is_monotonic(prod_conn):
    """
    Regression test for the concentration-risk finding (2026-09-04): CAGR
    should decay as top_n widens (less concentration = less single-stock
    fragility), not increase or swing unpredictably. Confirms the pattern
    found manually (top_n 5->10->20: 50.8%->25.9%->12.7% over the COVID
    window) holds as a monotonic direction, without pinning exact values.
    """
    results = {}
    for top_n in [5, 10, 20]:
        strategy = R09MMVolScale(
            band_id=2, top_n=top_n, lookback_months=6, rebalance_cadence_days=21,
            vol_scaling_mode="inverse_volatility", regime_switching_enabled=True,
        )
        config = BacktestConfig(start_date="2019-09-01", end_date="2020-09-30", initial_capital=1_000_000)
        results[top_n] = BacktestOrchestrator(strategy, config).run_native(prod_conn).cagr()

    assert results[5] > results[10] > results[20], (
        f"Expected CAGR to decay monotonically as top_n widens (less concentration risk), "
        f"got {results}"
    )
