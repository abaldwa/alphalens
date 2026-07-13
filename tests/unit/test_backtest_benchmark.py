"""
tests/unit/test_backtest_benchmark.py

ML17a — real Nifty 500 benchmark equity curve in backtest/engine.py.
compute_fold_metrics()'s benchmark_cagr/benchmark_sharpe/excess_return
extension and BacktestEngine._build_benchmark_curve() (tested via a
lightweight stand-in object exposing just the two attributes that method
reads — benchmark_index, initial_capital — since building a full
BacktestEngine requires a trained PnD/exit model + real technical-feature
computation unrelated to what this method does).
"""

from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from backtest.engine import BacktestEngine, compute_fold_metrics


def _equity_curve(values):
    return pd.DataFrame({"date": pd.date_range("2026-01-01", periods=len(values)), "equity": values})


class TestComputeFoldMetricsBenchmark:
    def test_no_benchmark_leaves_fields_none(self):
        curve = _equity_curve([1_000_000, 1_010_000, 1_020_000])
        trades = pd.DataFrame(columns=["pnl_inr"])
        metrics = compute_fold_metrics(curve, trades, 1_000_000.0)
        assert metrics["benchmark_cagr"] is None
        assert metrics["benchmark_sharpe"] is None
        assert metrics["excess_return"] is None

    def test_empty_equity_curve_leaves_benchmark_none_too(self):
        metrics = compute_fold_metrics(pd.DataFrame(), pd.DataFrame(), 1_000_000.0, benchmark_equity_curve=_equity_curve([100, 110]))
        assert metrics["benchmark_cagr"] is None

    def test_strategy_beats_benchmark_positive_excess_return(self):
        # Strategy doubles over 252 days; benchmark up 10%.
        strategy_curve = _equity_curve(np.linspace(1_000_000, 2_000_000, 252))
        benchmark_curve = _equity_curve(np.linspace(1_000_000, 1_100_000, 252))
        trades = pd.DataFrame(columns=["pnl_inr"])

        metrics = compute_fold_metrics(strategy_curve, trades, 1_000_000.0, benchmark_equity_curve=benchmark_curve)

        assert metrics["benchmark_cagr"] is not None
        assert metrics["benchmark_cagr"] == pytest.approx(0.10, abs=0.01)
        assert metrics["cagr"] > metrics["benchmark_cagr"]
        assert metrics["excess_return"] == pytest.approx(metrics["cagr"] - metrics["benchmark_cagr"], abs=1e-9)
        assert metrics["excess_return"] > 0

    def test_benchmark_normalised_from_its_own_first_value_not_initial_capital(self):
        # Benchmark curve starting at an index level (e.g. 22000), not INR
        # capital — compute_fold_metrics must normalise CAGR off the
        # benchmark curve's own first value, not the strategy's
        # initial_capital, since the two series have unrelated scales.
        strategy_curve = _equity_curve(np.linspace(1_000_000, 1_100_000, 30))
        benchmark_curve = _equity_curve(np.linspace(22000, 24200, 30))  # +10% in index points
        trades = pd.DataFrame(columns=["pnl_inr"])

        metrics = compute_fold_metrics(strategy_curve, trades, 1_000_000.0, benchmark_equity_curve=benchmark_curve)
        # Both curves are +10% over ~30 days -> similarly-scaled CAGR, not
        # wildly wrong because of the ~22x scale mismatch between series.
        assert metrics["benchmark_cagr"] == pytest.approx(metrics["cagr"], rel=0.05)


class TestBuildBenchmarkCurve:
    def _fake_engine(self, benchmark_index, initial_capital=1_000_000.0):
        return SimpleNamespace(benchmark_index=benchmark_index, initial_capital=initial_capital)

    def test_none_benchmark_index_returns_none(self):
        fake = self._fake_engine(None)
        test_fold = pd.DataFrame({"date": pd.date_range("2026-01-01", periods=5)})
        result = BacktestEngine._build_benchmark_curve(fake, test_fold)
        assert result is None

    def test_no_overlap_returns_none(self):
        bm = pd.DataFrame({"date": pd.date_range("2020-01-01", periods=5), "close": [100, 101, 102, 103, 104]})
        fake = self._fake_engine(bm)
        test_fold = pd.DataFrame({"date": pd.date_range("2026-01-01", periods=5)})
        result = BacktestEngine._build_benchmark_curve(fake, test_fold)
        assert result is None

    def test_real_overlap_builds_normalised_curve(self):
        dates = pd.date_range("2026-01-01", periods=10)
        bm = pd.DataFrame({"date": dates, "close": np.linspace(1000, 1100, 10)})
        fake = self._fake_engine(bm, initial_capital=1_000_000.0)
        test_fold = pd.DataFrame({"date": dates})

        result = BacktestEngine._build_benchmark_curve(fake, test_fold)

        assert result is not None
        assert len(result) == 10
        # First equity value == initial_capital (shares bought at entry price).
        assert result["equity"].iloc[0] == pytest.approx(1_000_000.0)
        # Final equity reflects the same +10% the index itself moved.
        assert result["equity"].iloc[-1] == pytest.approx(1_100_000.0, rel=0.01)

    def test_test_fold_narrower_than_benchmark_history_slices_correctly(self):
        full_dates = pd.date_range("2026-01-01", periods=30)
        bm = pd.DataFrame({"date": full_dates, "close": np.linspace(1000, 1300, 30)})
        fake = self._fake_engine(bm)
        # Fold only covers the last 5 days.
        test_fold = pd.DataFrame({"date": full_dates[-5:]})

        result = BacktestEngine._build_benchmark_curve(fake, test_fold)

        assert result is not None
        assert len(result) == 5
        assert result["date"].min() == full_dates[-5]
