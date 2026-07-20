"""tests/unit/test_ml_adapter.py — backtest/adapters/ml_adapter.py.

Tests the RESULT-SCHEMA TRANSLATION only, per this module's design (it
never constructs or drives a BacktestEngine itself — see
ml_adapter.py's module docstring). Running a real BacktestEngine end to
end requires a full ohlcv panel, a real benchmark, a P&D detector, an
exit model, and a signal model class — that's exercised by
tests/unit/test_backtester.py already; duplicating it here would just
mock the ML training pipeline, which the No-Mock-Data Policy reserves
for genuine data-layer concerns, not for unit-testing a dict mapping.
A BacktestResults instance built directly (its own dataclass, not
DB-backed) is a real object of the real type ml_adapter.py consumes.
"""

from datetime import date

import pytest

from backtest.adapters.ml_adapter import channel, wrap_ml_backtest_result
from backtest.core.horizon import HorizonBucket
from backtest.core.run_context import BacktestRun
from backtest.engine import BacktestResults


def _run(**overrides):
    defaults = dict(
        channel="ml", strategy_id="ml_signal_5d", horizon_bucket=HorizonBucket.D5,
        mode="backtest", universe_spec="nifty500", start_date=date(2015, 1, 1), end_date=date(2020, 1, 1),
        capital_mode="lump", initial_capital=1_000_000.0,
    )
    defaults.update(overrides)
    return BacktestRun(**defaults)


def _engine_results(**aggregate_overrides):
    aggregate = {
        "cagr": 0.18, "sharpe": 1.1, "max_drawdown": -0.22, "win_rate": 0.55,
        "profit_factor": 1.6, "n_trades": 340, "final_equity": 2_100_000.0,
        "benchmark_cagr": 0.12, "benchmark_sharpe": 0.8, "excess_return": 0.06,
    }
    aggregate.update(aggregate_overrides)
    return BacktestResults(
        model_name="signal_5d", from_date="2015-01-01", to_date="2020-01-01",
        fold_results=[], aggregate=aggregate, integrity_passed=True, integrity_detail={},
    )


class TestChannel:
    def test_channel_is_ml(self):
        assert channel() == "ml"


class TestWrapMlBacktestResult:
    def test_rejects_non_ml_run(self):
        run = _run(channel="technical")
        with pytest.raises(ValueError, match="channel='ml'"):
            wrap_ml_backtest_result(run, _engine_results())

    def test_maps_aggregate_fields_into_shared_metrics_shape(self):
        run = _run()
        result = wrap_ml_backtest_result(run, _engine_results())
        assert result.metrics["final_capital"] == pytest.approx(2_100_000.0)
        assert result.metrics["win_rate"] == pytest.approx(0.55)
        assert result.metrics["n_trades"] == 340
        assert result.metrics["cagr_trading_day_legacy"] == pytest.approx(0.18)

    def test_unavailable_fields_are_none_not_fabricated(self):
        run = _run()
        result = wrap_ml_backtest_result(run, _engine_results())
        assert result.metrics["cagr"] is None  # not faithfully recomputable, see module docstring
        assert result.metrics["xirr"] is None
        assert result.metrics["sortino"] is None
        assert result.metrics["calmar"] is None
        assert result.metrics["turnover_ratio"] is None

    def test_benchmark_status_ok_when_benchmark_cagr_present(self):
        run = _run()
        result = wrap_ml_backtest_result(run, _engine_results())
        assert result.metrics["benchmark_status"] == "ok"

    def test_benchmark_status_flagged_when_benchmark_cagr_missing(self):
        run = _run()
        result = wrap_ml_backtest_result(run, _engine_results(benchmark_cagr=None))
        assert result.metrics["benchmark_status"] == "insufficient_benchmark_history"

    def test_integrity_fields_passed_through_unchanged(self):
        run = _run()
        engine_results = _engine_results()
        engine_results.integrity_passed = False
        engine_results.integrity_detail = {"critical_failures": ["check_04_survivorship failed"]}
        result = wrap_ml_backtest_result(run, engine_results)
        assert result.integrity_passed is False
        assert result.integrity_detail == {"critical_failures": ["check_04_survivorship failed"]}

    def test_data_gaps_always_empty_since_engine_raises_instead(self):
        run = _run()
        result = wrap_ml_backtest_result(run, _engine_results())
        assert result.data_gaps == []

    def test_run_is_carried_through_unchanged(self):
        run = _run(strategy_id="ml_signal_21d")
        result = wrap_ml_backtest_result(run, _engine_results())
        assert result.run.strategy_id == "ml_signal_21d"
