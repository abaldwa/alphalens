"""tests/unit/test_backtest_runs_router.py — datastore/api/routers/backtest_runs.py."""

from datetime import date

import pytest
from fastapi.testclient import TestClient

from backtest.core.horizon import HorizonBucket
from backtest.core.run_context import BacktestRun, BacktestRunResult
from backtest.core.run_store import save_run_result
from datastore.api.db import close_all_connections, get_duckdb_connection
from datastore.api.main import app
from datastore.api.routers import backtest_runs as backtest_runs_router
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
def client(tmp_path, monkeypatch):
    # A real on-disk file, not :memory: — the router opens its connections
    # with read_only=True (SPEC-SCHED-013 concurrency discipline), and DuckDB
    # refuses read_only=True for in-memory databases outright.
    db_path = tmp_path / "backtest_runs_test.duckdb"
    monkeypatch.setattr(backtest_runs_router, "BACKTEST_DUCKDB_PATH", db_path)
    create_backtest.create_backtest_schema(db_path=db_path)
    close_all_connections()
    test_client = TestClient(app)
    test_client.db_path = db_path
    return test_client


class TestListRuns:
    def test_empty_store_returns_empty_list(self, client):
        response = client.get("/api/v1/backtest/runs")
        assert response.status_code == 200
        assert response.json()["runs"] == []

    def test_lists_saved_runs(self, client):
        run = _run()
        with get_duckdb_connection(client.db_path, persist=False) as conn:
            save_run_result(conn, _result(run))
        response = client.get("/api/v1/backtest/runs")
        assert response.status_code == 200
        runs = response.json()["runs"]
        assert len(runs) == 1
        assert runs[0]["run_id"] == run.run_id
        assert runs[0]["metrics"]["cagr"] == 0.15

    def test_filters_by_channel(self, client):
        with get_duckdb_connection(client.db_path, persist=False) as conn:
            save_run_result(conn, _result(_run(channel="technical", strategy_id="s1")))
            save_run_result(conn, _result(_run(channel="momentum", strategy_id="s2")))
        response = client.get("/api/v1/backtest/runs", params={"channel": "momentum"})
        runs = response.json()["runs"]
        assert len(runs) == 1
        assert runs[0]["channel"] == "momentum"

    def test_live_eligible_always_false_by_default_in_response(self, client):
        with get_duckdb_connection(client.db_path, persist=False) as conn:
            save_run_result(conn, _result(_run()))
        response = client.get("/api/v1/backtest/runs")
        assert response.json()["runs"][0]["live_eligible"] is False


class TestGetRun:
    def test_returns_404_for_unknown_run(self, client):
        response = client.get("/api/v1/backtest/runs/does-not-exist")
        assert response.status_code == 404

    def test_returns_run_by_id(self, client):
        run = _run()
        with get_duckdb_connection(client.db_path, persist=False) as conn:
            save_run_result(conn, _result(run))
        response = client.get(f"/api/v1/backtest/runs/{run.run_id}")
        assert response.status_code == 200
        assert response.json()["strategy_id"] == "ta_5d_breakout"


class TestLineage:
    def test_returns_404_for_unknown_run(self, client):
        response = client.get("/api/v1/backtest/runs/does-not-exist/lineage")
        assert response.status_code == 404

    def test_returns_chain_oldest_first(self, client):
        root = _run(strategy_id="root")
        child = _run(strategy_id="child", parent_run_id=root.run_id)
        with get_duckdb_connection(client.db_path, persist=False) as conn:
            save_run_result(conn, _result(root))
            save_run_result(conn, _result(child))
        response = client.get(f"/api/v1/backtest/runs/{child.run_id}/lineage")
        assert response.status_code == 200
        lineage = response.json()["lineage"]
        assert [r["strategy_id"] for r in lineage] == ["root", "child"]


class TestFeatureLog:
    def test_returns_404_for_unknown_run(self, client):
        response = client.get("/api/v1/backtest/runs/does-not-exist/feature_log")
        assert response.status_code == 404

    def test_returns_empty_rows_for_run_with_no_logged_decisions(self, client):
        run = _run()
        with get_duckdb_connection(client.db_path, persist=False) as conn:
            save_run_result(conn, _result(run))
        response = client.get(f"/api/v1/backtest/runs/{run.run_id}/feature_log")
        assert response.status_code == 200
        assert response.json()["rows"] == []

    def test_returns_logged_feature_vectors(self, client):
        from backtest.core.feature_log import FeatureLogWriter

        run = _run()
        with get_duckdb_connection(client.db_path, persist=False) as conn:
            save_run_result(conn, _result(run))
            writer = FeatureLogWriter(conn, flush_batch_size=100)
            writer.record(
                run_id=run.run_id, ticker="RELIANCE", as_of_date=date(2020, 1, 1),
                horizon_bucket=HorizonBucket.D5, feature_vector={"rsi_14": 55.0},
                decision_taken="bought", signal_output="buy",
            )
            writer.flush()
        response = client.get(f"/api/v1/backtest/runs/{run.run_id}/feature_log")
        rows = response.json()["rows"]
        assert len(rows) == 1
        assert rows[0]["ticker"] == "RELIANCE"
        assert rows[0]["feature_vector"] == {"rsi_14": 55.0}
