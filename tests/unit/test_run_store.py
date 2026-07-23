"""tests/unit/test_run_store.py — backtest/core/run_store.py."""

from datetime import date

import pytest

from backtest.core.horizon import HorizonBucket
from backtest.core.run_context import BacktestRun, BacktestRunResult
from backtest.core.run_store import get_run, get_run_lineage, list_runs, save_run_result
from datastore.api.db import get_duckdb_connection
from datastore.schema import create_backtest


def _run(**overrides):
    defaults = dict(
        channel="technical", strategy_id="ta_5d_breakout", horizon_bucket=HorizonBucket.D5,
        mode="backtest", universe_spec="nifty500", start_date=date(2015, 1, 1), end_date=date(2020, 1, 1),
        capital_mode="lump", initial_capital=10_000_000.0,
    )
    defaults.update(overrides)
    return BacktestRun(**defaults)


def _result(run, **overrides):
    defaults = dict(run=run, metrics={"cagr": 0.15, "final_capital": 20_000_000.0}, data_gaps=[])
    defaults.update(overrides)
    return BacktestRunResult(**defaults)


@pytest.fixture
def conn():
    create_backtest.create_backtest_schema(in_memory=True)
    with get_duckdb_connection(None) as c:
        c.execute("DELETE FROM backtest_runs")
        c.execute("DELETE FROM backtest_feature_log")
        yield c


class TestSaveAndGetRun:
    def test_round_trips_a_run(self, conn):
        run = _run()
        save_run_result(conn, _result(run))
        fetched = get_run(conn, run.run_id)
        assert fetched is not None
        assert fetched["channel"] == "technical"
        assert fetched["strategy_id"] == "ta_5d_breakout"
        assert fetched["horizon_bucket"] == "5_day"
        assert fetched["metrics"]["cagr"] == 0.15

    def test_get_run_returns_none_for_unknown_id(self, conn):
        assert get_run(conn, "does-not-exist") is None

    def test_live_eligible_defaults_false_and_is_never_set_by_save(self, conn):
        run = _run()
        save_run_result(conn, _result(run))
        row = conn.execute("SELECT live_eligible FROM backtest_runs WHERE run_id = ?", [run.run_id]).fetchone()
        assert row[0] is False

    def test_upsert_on_rerun_of_same_run_id_updates_metrics(self, conn):
        run = _run()
        save_run_result(conn, _result(run, metrics={"cagr": 0.10}))
        save_run_result(conn, _result(run, metrics={"cagr": 0.25}))
        fetched = get_run(conn, run.run_id)
        assert fetched["metrics"]["cagr"] == 0.25
        count = conn.execute("SELECT COUNT(*) FROM backtest_runs WHERE run_id = ?", [run.run_id]).fetchone()[0]
        assert count == 1  # upsert, not a duplicate row

    def test_data_gaps_round_trip(self, conn):
        run = _run()
        gaps = [{"ticker": "GHOST", "as_of_date": "2020-01-01", "reason": "no_price_for_buy_signal"}]
        save_run_result(conn, _result(run, data_gaps=gaps))
        fetched = get_run(conn, run.run_id)
        assert fetched["data_gaps"] == gaps


class TestListRuns:
    def test_filters_by_channel(self, conn):
        save_run_result(conn, _result(_run(channel="technical", strategy_id="s1")))
        save_run_result(conn, _result(_run(channel="momentum", strategy_id="s2")))
        results = list_runs(conn, channel="momentum")
        assert len(results) == 1
        assert results[0]["channel"] == "momentum"

    def test_filters_by_mode(self, conn):
        save_run_result(conn, _result(_run(mode="backtest", strategy_id="s1")))
        save_run_result(conn, _result(_run(mode="walk_forward", strategy_id="s2")))
        results = list_runs(conn, mode="walk_forward")
        assert len(results) == 1
        assert results[0]["mode"] == "walk_forward"

    def test_filters_by_strategy_id(self, conn):
        save_run_result(conn, _result(_run(strategy_id="s1")))
        save_run_result(conn, _result(_run(strategy_id="s2")))
        results = list_runs(conn, strategy_id="s1")
        assert len(results) == 1

    def test_no_filters_returns_all(self, conn):
        save_run_result(conn, _result(_run(strategy_id="s1")))
        save_run_result(conn, _result(_run(strategy_id="s2")))
        assert len(list_runs(conn)) == 2

    def test_respects_limit(self, conn):
        for i in range(5):
            save_run_result(conn, _result(_run(strategy_id=f"s{i}")))
        assert len(list_runs(conn, limit=2)) == 2


class TestRunLineage:
    def test_single_run_with_no_parent_returns_itself_only(self, conn):
        run = _run()
        save_run_result(conn, _result(run))
        lineage = get_run_lineage(conn, run.run_id)
        assert [r["run_id"] for r in lineage] == [run.run_id]

    def test_parent_chain_returned_oldest_first(self, conn):
        root = _run(strategy_id="root")
        save_run_result(conn, _result(root))
        child = _run(strategy_id="child", parent_run_id=root.run_id)
        save_run_result(conn, _result(child))
        grandchild = _run(strategy_id="grandchild", parent_run_id=child.run_id)
        save_run_result(conn, _result(grandchild))

        lineage = get_run_lineage(conn, grandchild.run_id)
        assert [r["strategy_id"] for r in lineage] == ["root", "child", "grandchild"]

    def test_missing_parent_id_does_not_crash_lineage_walk(self, conn):
        run = _run(parent_run_id="does-not-exist")
        save_run_result(conn, _result(run))
        lineage = get_run_lineage(conn, run.run_id)
        assert [r["run_id"] for r in lineage] == [run.run_id]
