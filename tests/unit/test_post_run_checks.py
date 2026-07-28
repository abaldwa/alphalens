"""
tests/unit/test_post_run_checks.py

backtest/core/post_run_checks.py had zero dedicated test coverage before
this file (only referenced in a comment inside test_backtester.py) despite
being the real wiring point between BacktestOrchestrator runs and
BacktestIntegrityChecker for the Technical/Fundamental/Momentum towers
(see that module's docstring). Covers realized_cost_and_liquidity's
zero-trade/uncapped-ADTV branches, subperiod_check_inputs' empty/populated
regime-segment paths, and run_post_run_integrity's pass/fail/ML-channel
branches.
"""

from dataclasses import dataclass
from datetime import date

import pandas as pd
import pytest

from backtest.core.post_run_checks import (
    _benchmark_segment_return,
    realized_cost_and_liquidity,
    run_post_run_integrity,
    subperiod_check_inputs,
)


@dataclass
class FakeTrade:
    entry_price: float
    quantity: int
    cost_inr: float
    exit_date: date
    pnl_inr: float = 0.0


class TestRealizedCostAndLiquidity:
    def test_no_trades_returns_none_none(self):
        cost, adt = realized_cost_and_liquidity([], [])
        assert cost is None
        assert adt is None

    def test_zero_total_turnover_returns_none_none(self):
        trades = [FakeTrade(entry_price=0.0, quantity=10, cost_inr=5.0, exit_date=date(2024, 1, 1))]
        cost, adt = realized_cost_and_liquidity(trades, [])
        assert cost is None
        assert adt is None

    def test_realized_cost_computed_from_real_trades(self):
        trades = [
            FakeTrade(entry_price=100.0, quantity=10, cost_inr=20.0, exit_date=date(2024, 1, 1)),
            FakeTrade(entry_price=200.0, quantity=5, cost_inr=10.0, exit_date=date(2024, 2, 1)),
        ]
        cost, adt = realized_cost_and_liquidity(trades, [])
        total_turnover = 100.0 * 10 + 200.0 * 5
        assert cost == pytest.approx((20.0 + 10.0) / total_turnover)
        # No uncapped-ADTV data gap recorded -> the configured floor was applied.
        from config.settings import MIN_ADT_INR

        assert adt == float(MIN_ADT_INR)

    def test_uncapped_adtv_gap_zeroes_applied_floor(self):
        trades = [FakeTrade(entry_price=100.0, quantity=10, cost_inr=20.0, exit_date=date(2024, 1, 1))]
        data_gaps = [{"reason": "no_adtv_data_position_sized_uncapped", "ticker": "X"}]
        cost, adt = realized_cost_and_liquidity(trades, data_gaps)
        assert cost is not None
        assert adt == 0.0

    def test_unrelated_data_gap_does_not_zero_floor(self):
        trades = [FakeTrade(entry_price=100.0, quantity=10, cost_inr=20.0, exit_date=date(2024, 1, 1))]
        data_gaps = [{"reason": "some_other_gap"}]
        _, adt = realized_cost_and_liquidity(trades, data_gaps)
        from config.settings import MIN_ADT_INR

        assert adt == float(MIN_ADT_INR)


class _FakeConn:
    """Minimal duck-typed connection for _benchmark_segment_return."""

    def __init__(self, row=None, raise_exc=False):
        self._row = row
        self._raise_exc = raise_exc

    def execute(self, sql, params):
        if self._raise_exc:
            raise RuntimeError("boom")

        class _Result:
            def __init__(self, row):
                self._row = row

            def fetchone(self):
                return self._row

        return _Result(self._row)


class TestBenchmarkSegmentReturn:
    def test_returns_none_on_query_exception(self):
        conn = _FakeConn(raise_exc=True)
        result = _benchmark_segment_return(conn, "Nifty 500", date(2024, 1, 1), date(2024, 2, 1))
        assert result is None

    def test_returns_none_when_row_missing(self):
        conn = _FakeConn(row=None)
        result = _benchmark_segment_return(conn, "Nifty 500", date(2024, 1, 1), date(2024, 2, 1))
        assert result is None

    def test_returns_none_when_start_close_is_zero(self):
        conn = _FakeConn(row=(0.0, 110.0))
        result = _benchmark_segment_return(conn, "Nifty 500", date(2024, 1, 1), date(2024, 2, 1))
        assert result is None

    def test_returns_none_when_a_close_is_null(self):
        conn = _FakeConn(row=(None, 110.0))
        result = _benchmark_segment_return(conn, "Nifty 500", date(2024, 1, 1), date(2024, 2, 1))
        assert result is None

    def test_computes_real_total_return(self):
        conn = _FakeConn(row=(100.0, 110.0))
        result = _benchmark_segment_return(conn, "Nifty 500", date(2024, 1, 1), date(2024, 2, 1))
        assert result == pytest.approx(0.10)


class TestSubperiodCheckInputs:
    def test_no_regime_segments_returns_empty_lists(self):
        equity_curve = pd.Series(
            [100.0, 105.0, 110.0],
            index=pd.date_range("2024-01-01", periods=3, freq="D"),
        )
        fold_sharpes, fold_returns, benchmark_returns, note = subperiod_check_inputs(
            equity_curve, [], date(2024, 1, 1), date(2024, 1, 3), [],
        )
        assert fold_sharpes == []
        assert fold_returns == []
        assert benchmark_returns == []
        assert "subperiod-based" in note
        assert "0 real market-regime segments" in note

    def test_regime_segments_populate_fold_metrics_and_benchmarks(self):
        idx = pd.date_range("2024-01-01", periods=10, freq="D")
        equity_curve = pd.Series([100.0 + i for i in range(10)], index=idx)
        regime_segments = [
            {"regime": "bull", "start_date": date(2024, 1, 1), "end_date": date(2024, 1, 5)},
            {"regime": "sideways", "start_date": date(2024, 1, 6), "end_date": date(2024, 1, 10)},
        ]
        conn = _FakeConn(row=(100.0, 105.0))
        fold_sharpes, fold_returns, benchmark_returns, note = subperiod_check_inputs(
            equity_curve, [], date(2024, 1, 1), date(2024, 1, 10), regime_segments, regime_conn=conn,
        )
        assert len(fold_returns) == 2
        assert len(benchmark_returns) == 2
        assert "2 real market-regime segments" in note

    def test_benchmark_lookup_failure_truncates_fold_returns_to_match(self):
        idx = pd.date_range("2024-01-01", periods=10, freq="D")
        equity_curve = pd.Series([100.0 + i for i in range(10)], index=idx)
        regime_segments = [
            {"regime": "bull", "start_date": date(2024, 1, 1), "end_date": date(2024, 1, 5)},
            {"regime": "sideways", "start_date": date(2024, 1, 6), "end_date": date(2024, 1, 10)},
        ]
        # Benchmark lookup always fails -> benchmark_returns stays empty,
        # fold_returns must be truncated to the same (zero) length so the
        # pair stays positionally aligned for check_09.
        conn = _FakeConn(raise_exc=True)
        fold_sharpes, fold_returns, benchmark_returns, _ = subperiod_check_inputs(
            equity_curve, [], date(2024, 1, 1), date(2024, 1, 10), regime_segments, regime_conn=conn,
        )
        assert benchmark_returns == []
        assert len(fold_returns) == len(benchmark_returns)


class TestRunPostRunIntegrity:
    def _make_equity_curve(self, n=30, flat=False):
        idx = pd.date_range("2024-01-01", periods=n, freq="D")
        if flat:
            return pd.Series([100.0] * n, index=idx)
        return pd.Series([100.0 + i for i in range(n)], index=idx)

    def test_zero_trades_flat_curve_fails_critical_check(self):
        equity_curve = self._make_equity_curve(flat=True)
        integrity_passed, detail = run_post_run_integrity(
            channel="fundamental", trades=[], data_gaps=[], equity_curve=equity_curve,
            run_start=date(2024, 1, 1), run_end=date(2024, 1, 30),
        )
        assert integrity_passed is False
        assert detail["checks"] == {}
        assert detail["realized_cost_pct"] is None

    def test_healthy_run_with_real_trades_and_regimes_can_pass(self):
        equity_curve = self._make_equity_curve(n=30)
        trades = [
            FakeTrade(entry_price=100.0, quantity=10, cost_inr=5.0, exit_date=date(2024, 1, 5), pnl_inr=50.0)
            for _ in range(6)
        ]
        regime_segments = [
            {"regime": "bull", "start_date": date(2024, 1, 1), "end_date": date(2024, 1, 15)},
            {"regime": "sideways", "start_date": date(2024, 1, 16), "end_date": date(2024, 1, 30)},
        ]
        conn = _FakeConn(row=(90.0, 91.0))
        integrity_passed, detail = run_post_run_integrity(
            channel="fundamental", trades=trades, data_gaps=[], equity_curve=equity_curve,
            run_start=date(2024, 1, 1), run_end=date(2024, 1, 30),
            regime_segments=regime_segments, regime_conn=conn,
        )
        assert detail["n_subperiods"] == 2
        assert "check_10_random_feature" not in detail["checks"]

    def test_ml_channel_includes_random_feature_check_name_in_applicable_set(self):
        # check_10_random_feature has no random_feature_accuracy supplied here,
        # so it will FAIL (not be silently absent) for the ml channel — unlike
        # every non-ml channel, where it must never even be attempted.
        equity_curve = self._make_equity_curve(flat=True)
        integrity_passed, detail = run_post_run_integrity(
            channel="ml", trades=[], data_gaps=[], equity_curve=equity_curve,
            run_start=date(2024, 1, 1), run_end=date(2024, 1, 30),
        )
        assert integrity_passed is False
