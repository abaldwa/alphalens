"""tests/unit/test_ml_dual_write.py — backtest/adapters/ml_dual_write.py.

Uses a real BacktestResults instance (its own dataclass, not DB-backed —
same convention as test_ml_adapter.py) and a real in-memory
backtest_runs table (create_backtest_schema(in_memory=True)), never a
mock of the DB layer or a fabricated result shape.
"""

import pandas as pd
import pytest

from backtest.adapters import ml_dual_write
from backtest.core.run_store import list_runs
from backtest.engine import BacktestResults
from datastore.api.db import get_duckdb_connection
from datastore.schema import create_backtest


@pytest.fixture
def conn(monkeypatch):
    create_backtest.create_backtest_schema(in_memory=True)
    monkeypatch.setattr(ml_dual_write, "BACKTEST_DUCKDB_PATH", None)
    with get_duckdb_connection(None) as c:
        c.execute("DELETE FROM backtest_runs")
        yield c


def _real_ohlcv():
    return pd.DataFrame(
        {
            "date": pd.to_datetime(["2018-01-01", "2018-06-01", "2019-01-01"]),
            "ticker": ["RELIANCE", "RELIANCE", "RELIANCE"],
            "close": [900.0, 950.0, 1100.0],
        }
    )


def _engine_results(model_name="signal_5d", **aggregate_overrides):
    aggregate = {
        "cagr": 0.18, "sharpe": 1.1, "max_drawdown": -0.22, "win_rate": 0.55,
        "profit_factor": 1.6, "n_trades": 340, "final_equity": 2_100_000.0,
        "benchmark_cagr": 0.12, "benchmark_sharpe": 0.8, "excess_return": 0.06,
    }
    aggregate.update(aggregate_overrides)
    return BacktestResults(
        model_name=model_name, from_date="2018-01-01", to_date="2019-01-01",
        fold_results=[], aggregate=aggregate, integrity_passed=True, integrity_detail={},
    )


class TestDualWriteMlRun:
    def test_writes_a_real_run_queryable_via_list_runs(self, conn):
        run_id = ml_dual_write.dual_write_ml_run(
            _engine_results(), strategy_id="signal_5d", horizon_days=5,
            ohlcv=_real_ohlcv(), initial_capital=1_000_000.0, random_seed=42,
        )
        assert run_id is not None
        runs = list_runs(conn, channel="ml")
        assert len(runs) == 1
        assert runs[0]["run_id"] == run_id
        assert runs[0]["strategy_id"] == "signal_5d"
        assert runs[0]["horizon_bucket"] == "5_day"
        assert runs[0]["metrics"]["final_capital"] == pytest.approx(2_100_000.0)

    def test_horizon_days_maps_to_correct_bucket(self, conn):
        ml_dual_write.dual_write_ml_run(
            _engine_results(model_name="signal_21d"), strategy_id="signal_21d", horizon_days=21,
            ohlcv=_real_ohlcv(), initial_capital=1_000_000.0, random_seed=42,
        )
        runs = list_runs(conn, strategy_id="signal_21d")
        assert runs[0]["horizon_bucket"] == "21_day"

    def test_unmapped_horizon_days_returns_none_and_writes_nothing(self, conn):
        run_id = ml_dual_write.dual_write_ml_run(
            _engine_results(), strategy_id="signal_9d", horizon_days=9,
            ohlcv=_real_ohlcv(), initial_capital=1_000_000.0, random_seed=42,
        )
        assert run_id is None
        assert list_runs(conn, strategy_id="signal_9d") == []

    def test_start_end_date_derived_from_real_ohlcv_range(self, conn):
        run_id = ml_dual_write.dual_write_ml_run(
            _engine_results(), strategy_id="signal_5d", horizon_days=5,
            ohlcv=_real_ohlcv(), initial_capital=1_000_000.0, random_seed=42,
        )
        runs = list_runs(conn, strategy_id="signal_5d")
        assert runs[0]["start_date"] == "2018-01-01"
        assert runs[0]["end_date"] == "2019-01-01"
        assert run_id == runs[0]["run_id"]

    def test_never_raises_on_internal_failure(self, conn, monkeypatch):
        def _boom(*args, **kwargs):
            raise RuntimeError("simulated failure")

        monkeypatch.setattr(ml_dual_write, "wrap_ml_backtest_result", _boom)
        run_id = ml_dual_write.dual_write_ml_run(
            _engine_results(), strategy_id="signal_5d", horizon_days=5,
            ohlcv=_real_ohlcv(), initial_capital=1_000_000.0, random_seed=42,
        )
        assert run_id is None
