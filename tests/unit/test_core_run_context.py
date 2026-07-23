"""tests/unit/test_core_run_context.py — backtest/core/run_context.py."""

from datetime import date

import pytest

from backtest.core.horizon import HorizonBucket
from backtest.core.run_context import BacktestRun, BacktestRunResult, config_hash


def _run(**overrides):
    defaults = dict(
        channel="technical", strategy_id="ta_5d_breakout", horizon_bucket=HorizonBucket.D5,
        mode="backtest", universe_spec="nifty500", start_date=date(2015, 1, 1), end_date=date(2020, 1, 1),
        capital_mode="lump", initial_capital=10_000_000.0,
    )
    defaults.update(overrides)
    return BacktestRun(**defaults)


class TestStrategyIdRequired:
    def test_empty_strategy_id_rejected(self):
        with pytest.raises(ValueError, match="strategy_id"):
            _run(strategy_id="")


class TestFundamentalMinStartDate:
    def test_fundamental_run_before_2020_rejected(self):
        with pytest.raises(ValueError, match="2020"):
            _run(channel="fundamental", start_date=date(2015, 1, 1))

    def test_fundamental_run_on_2020_01_01_allowed(self):
        run = _run(channel="fundamental", start_date=date(2020, 1, 1), strategy_id="fund_1y_value")
        assert run.start_date == date(2020, 1, 1)

    def test_non_fundamental_channel_unaffected_by_the_2020_floor(self):
        run = _run(channel="technical", start_date=date(2007, 1, 1))
        assert run.start_date == date(2007, 1, 1)


class TestConfigHashDeterminism:
    def test_same_config_produces_same_hash(self):
        cfg = {"a": 1, "b": [1, 2, 3]}
        assert config_hash(cfg) == config_hash(cfg)

    def test_different_config_produces_different_hash(self):
        assert config_hash({"a": 1}) != config_hash({"a": 2})

    def test_key_order_does_not_affect_hash(self):
        assert config_hash({"a": 1, "b": 2}) == config_hash({"b": 2, "a": 1})

    def test_run_config_hash_property_matches_module_function(self):
        run = _run(config={"x": 1})
        assert run.config_hash == config_hash({"x": 1})


class TestBacktestRunResultToDict:
    def test_serializes_horizon_bucket_as_plain_string(self):
        run = _run()
        result = BacktestRunResult(run=run, metrics={"cagr": 0.15})
        d = result.to_dict()
        assert d["run"]["horizon_bucket"] == "5_day"
        assert d["metrics"]["cagr"] == 0.15

    def test_data_gaps_default_to_empty_list_not_none(self):
        result = BacktestRunResult(run=_run(), metrics={})
        assert result.data_gaps == []
