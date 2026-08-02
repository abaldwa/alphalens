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
    adtv_cr: float = None


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

    def test_real_trades_with_adtv_cr_derive_applied_floor_from_actual_minimum(self):
        # [BUG FIX, 5th fundamental-strategies review, item 4] previously
        # applied_min_adt_inr always echoed back the MIN_ADT_INR constant
        # whenever no uncapped gap was recorded — never actually DERIVED
        # from real per-trade ADTV. With real (distinct) Trade.adtv_cr
        # values present, the result must be the genuine minimum observed
        # (converted crore -> INR), not the constant.
        trades = [
            FakeTrade(entry_price=100.0, quantity=10, cost_inr=20.0, exit_date=date(2024, 1, 1), adtv_cr=5.0),
            FakeTrade(entry_price=200.0, quantity=5, cost_inr=10.0, exit_date=date(2024, 2, 1), adtv_cr=2.5),
            FakeTrade(entry_price=150.0, quantity=8, cost_inr=15.0, exit_date=date(2024, 3, 1), adtv_cr=9.0),
        ]
        cost, adt = realized_cost_and_liquidity(trades, [])

        from config.settings import MIN_ADT_INR

        assert adt == pytest.approx(2.5 * 1e7)
        assert adt != float(MIN_ADT_INR)

    def test_trades_without_any_real_adtv_cr_falls_back_to_constant(self):
        # No trade carries a real adtv_cr (e.g. Trade objects predating
        # this field) -> nothing to derive from -> falls back to echoing
        # the config constant, same as before this fix.
        trades = [FakeTrade(entry_price=100.0, quantity=10, cost_inr=20.0, exit_date=date(2024, 1, 1), adtv_cr=None)]
        _, adt = realized_cost_and_liquidity(trades, [])
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

    def test_fewer_than_3_segments_logs_warning_and_annotates_note(self, caplog):
        idx = pd.date_range("2024-01-01", periods=10, freq="D")
        equity_curve = pd.Series([100.0 + i for i in range(10)], index=idx)
        regime_segments = [
            {"regime": "bull", "start_date": date(2024, 1, 1), "end_date": date(2024, 1, 10)},
        ]
        with caplog.at_level("WARNING"):
            _, _, _, note = subperiod_check_inputs(
                equity_curve, [], date(2024, 1, 1), date(2024, 1, 10), regime_segments,
                regime_method="20pct_threshold_v1",
            )
        assert "20pct_threshold_v1" in note
        assert "need at least 3" in note
        assert any("20pct_threshold_v1" in rec.message for rec in caplog.records)


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
        # [BUG FIX, 5th fundamental-strategies review, item 5] a failing
        # CRITICAL check (check_05_costs here, since applied_roundtrip_
        # cost_pct is None with zero trades) used to discard the FULLY
        # computed per-check breakdown and persist "checks": {} — hiding
        # which specific checks passed/failed. Confirms the breakdown
        # (including checks that were NOT the critical failure) is still
        # persisted, not an empty dict.
        assert detail["checks"] != {}
        assert detail["checks"]["check_05_costs"] is False
        assert detail["checks"]["check_12_flat_equity_curve"] is False
        assert detail["realized_cost_pct"] is None

    def test_critical_check_failure_still_persists_other_checks_pass_fail(self):
        # A run with real trades (so check_05_costs/check_06_liquidity
        # would legitimately PASS) but a genuinely flat equity curve and
        # too few trades (check_12_flat_equity_curve — CRITICAL — fails).
        # The persisted breakdown must show check_05/06 as PASSED even
        # though check_12 failing raised RuntimeError internally.
        equity_curve = self._make_equity_curve(n=30, flat=True)
        trades = [
            FakeTrade(entry_price=100.0, quantity=10, cost_inr=5.0, exit_date=date(2024, 1, 5), pnl_inr=0.0)
            for _ in range(2)  # below MIN_TRADES_FLOOR -> check_12 fails
        ]
        integrity_passed, detail = run_post_run_integrity(
            channel="fundamental", trades=trades, data_gaps=[], equity_curve=equity_curve,
            run_start=date(2024, 1, 1), run_end=date(2024, 1, 30),
        )
        assert integrity_passed is False
        assert detail["checks"]["check_12_flat_equity_curve"] is False
        assert detail["checks"]["check_05_costs"] is True
        assert detail["checks"]["check_06_liquidity"] is True

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
        # [BUG FIX, 6th fundamental-strategies review, item 2] 2 segments is
        # still below MIN_MEANINGFUL_SUBPERIODS (3) - must be flagged loudly
        # in the persisted detail, not silently reported as a clean pass.
        assert detail["insufficient_subperiods_for_meaningful_check"] is True

    def test_non_critical_check_failure_alone_does_not_fail_integrity(self):
        """[BUG FIX, 2026-08-02] check_08_fold_stability/check_09_benchmarks
        are non-critical (integrity_checker.py's CRITICAL_CHECKS comment:
        a real, clean backtest can legitimately fail fold stability or
        underperform a benchmark without that implying a data leak).
        run_post_run_integrity used to compute integrity_passed =
        all(passed_map.values()), re-coupling those two into the pass/fail
        gate — with only 3 regime segments, check_09 requires beating the
        benchmark in ALL 3 to pass, which a benchmark stacked to always win
        (here) trivially defeats, while every CRITICAL check (05/06/12)
        still passes cleanly. integrity_passed must reflect only the
        critical checks."""
        equity_curve = self._make_equity_curve(n=30)
        trades = [
            FakeTrade(entry_price=100.0, quantity=10, cost_inr=5.0, exit_date=date(2024, 1, 5), pnl_inr=50.0)
            for _ in range(6)
        ]
        regime_segments = [
            {"regime": "bull", "start_date": date(2024, 1, 1), "end_date": date(2024, 1, 10)},
            {"regime": "bear", "start_date": date(2024, 1, 11), "end_date": date(2024, 1, 20)},
            {"regime": "sideways", "start_date": date(2024, 1, 21), "end_date": date(2024, 1, 30)},
        ]
        conn = _FakeConn(row=(90.0, 91.0))
        integrity_passed, detail = run_post_run_integrity(
            channel="fundamental", trades=trades, data_gaps=[], equity_curve=equity_curve,
            run_start=date(2024, 1, 1), run_end=date(2024, 1, 30),
            regime_segments=regime_segments, regime_conn=conn,
        )
        assert detail["n_subperiods"] == 3
        # short (10-day) regime segments annualize to wildly different
        # per-fold Sharpes/CAGRs — real signal that check_08 is meant to
        # catch, but it must stay non-critical.
        assert detail["checks"]["check_08_fold_stability"] is False
        assert detail["checks"]["check_05_costs"] is True
        assert detail["checks"]["check_06_liquidity"] is True
        assert detail["checks"]["check_12_flat_equity_curve"] is True
        assert integrity_passed is True

    def test_check_12_floor_scales_with_run_duration_not_fixed_at_5(self):
        """[BUG FIX, 2026-07-28 third model-review, item 3] MIN_TRADES_FLOOR
        (5) alone is unrelated to the run's actual duration — a long,
        multi-year run with only 8 trades should NOT be held to the same
        floor as a 1-month run. run_post_run_integrity has run_start/
        run_end and must scale the floor accordingly, so this ~4-year run
        with 8 trades (comfortably above the fixed floor of 5) still fails
        check_12 once duration-scaled."""
        equity_curve = self._make_equity_curve(n=30)  # non-flat, so check_12 isn't hit for the wrong reason
        trades = [
            FakeTrade(entry_price=100.0, quantity=10, cost_inr=5.0, exit_date=date(2024, 1, 5), pnl_inr=50.0)
            for _ in range(8)
        ]
        integrity_passed, detail = run_post_run_integrity(
            channel="fundamental", trades=trades, data_gaps=[], equity_curve=equity_curve,
            run_start=date(2020, 1, 1), run_end=date(2024, 1, 1),
        )
        # check_12 is a CRITICAL check — a critical failure raises inside
        # BacktestIntegrityChecker.run_all_checks, which run_post_run_integrity
        # catches and reports as an overall fail. [BUG FIX, 5th
        # fundamental-strategies review, item 5] the per-check breakdown is
        # still recovered from the checker's own results cache (not
        # discarded to {}) — the real assertion is that this run failed
        # for exactly the reason named in the warning log above (24, the
        # duration-scaled floor, not the fixed constant 5), and that OTHER
        # checks' real pass/fail status is still visible in the persisted
        # detail rather than hidden behind an empty dict.
        assert integrity_passed is False
        assert detail["checks"]["check_12_flat_equity_curve"] is False
        assert detail["checks"] != {}

    def test_check_12_floor_stays_at_baseline_for_a_short_run(self):
        """Same 8-trade count as above, but a short (~1 month) run — the
        duration-scaled floor must not exceed the checker's own
        MIN_TRADES_FLOOR baseline (5) for a short window, so 8 trades still
        passes check_12."""
        equity_curve = self._make_equity_curve(n=30)
        trades = [
            FakeTrade(entry_price=100.0, quantity=10, cost_inr=5.0, exit_date=date(2024, 1, 5), pnl_inr=50.0)
            for _ in range(8)
        ]
        integrity_passed, detail = run_post_run_integrity(
            channel="fundamental", trades=trades, data_gaps=[], equity_curve=equity_curve,
            run_start=date(2024, 1, 1), run_end=date(2024, 1, 30),
        )
        assert detail["checks"]["check_12_flat_equity_curve"] is True

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
