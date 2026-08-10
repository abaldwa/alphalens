"""
tests/unit/test_backtest_trades_router.py

Covers GET /api/v1/backtest/trades and /trades/summary.

[2026-08-10] Added with the backtest_trades table. Before it, individual
trades were not queryable at all — runs were in backtest_runs but the trades
themselves lived only as ~3,800 loose CSVs, so "every trade across all
strategies" meant globbing files.
"""

import duckdb
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from datastore.api.routers import backtest_runs as backtest_runs_router


@pytest.fixture
def db(tmp_path):
    path = tmp_path / "backtest.duckdb"
    conn = duckdb.connect(str(path))
    conn.execute(
        """CREATE TABLE backtest_trades (
               run_id VARCHAR, strategy_id VARCHAR, template_name VARCHAR,
               exit_variant VARCHAR, ticker VARCHAR, qty DOUBLE,
               buy_date DATE, buy_price DOUBLE, sale_date DATE, sale_price DOUBLE,
               stock_rank INTEGER, pnl_inr DOUBLE, pnl_pct DOUBLE,
               exit_reason VARCHAR, holding_days INTEGER,
               buy_value DOUBLE, sale_value DOUBLE, financial_year VARCHAR)"""
    )
    conn.executemany(
        "INSERT INTO backtest_trades VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            ("r1", "ta_a1", "A1", "unconstrained", "RELIANCE", 10, "2007-05-02", 100.0,
             "2007-06-02", 110.0, 1, 100.0, 0.10, "signal", 31, 1000.0, 1100.0, "FY2007-08"),
            ("r1", "ta_a1", "A1", "unconstrained", "TCS", 5, "2008-05-02", 200.0,
             "2009-08-02", 180.0, 2, -100.0, -0.10, "stop", 457, 1000.0, 900.0, "FY2009-10"),
            ("r2", "ta_b2", "B2", "unconstrained", "INFY", 8, "2010-01-02", 50.0,
             "2010-03-02", 60.0, 3, 80.0, 0.20, "signal", 59, 400.0, 480.0, "FY2009-10"),
        ],
    )
    conn.close()
    return path


@pytest.fixture
def client(db, monkeypatch):
    monkeypatch.setattr(backtest_runs_router, "BACKTEST_DUCKDB_PATH", db)
    app = FastAPI()
    app.include_router(backtest_runs_router.router)
    return TestClient(app)


class TestListTrades:
    def test_returns_all_requested_fields(self, client):
        body = client.get("/api/v1/backtest/trades").json()
        assert body["total"] == 3
        t = body["trades"][0]
        # Exactly the fields the user asked to be recorded.
        for field in ("strategy_id", "template_name", "ticker", "buy_date",
                      "buy_price", "qty", "sale_date", "sale_price"):
            assert field in t, f"trade row missing `{field}`"

    def test_strategy_is_on_the_row_not_joined(self, client):
        """Denormalised on purpose — a missing backtest_runs row must not make
        the strategy NULL."""
        body = client.get("/api/v1/backtest/trades").json()
        assert all(t["strategy_id"] for t in body["trades"])

    @pytest.mark.parametrize(
        "param,value,expected",
        [
            ("strategy_id", "ta_a1", 2),
            ("template_name", "B2", 1),
            ("ticker", "INFY", 1),
            ("financial_year", "FY2009-10", 2),
            ("run_id", "r1", 2),
            ("exit_reason", "stop", 1),
        ],
    )
    def test_filters(self, client, param, value, expected):
        body = client.get("/api/v1/backtest/trades", params={param: value}).json()
        assert body["total"] == expected

    def test_pagination_reports_unpaginated_total(self, client):
        body = client.get("/api/v1/backtest/trades", params={"limit": 1}).json()
        assert len(body["trades"]) == 1
        assert body["total"] == 3, "total must be the unpaginated count"

    def test_missing_table_returns_empty_not_500(self, tmp_path, monkeypatch):
        empty = tmp_path / "empty.duckdb"
        duckdb.connect(str(empty)).close()
        monkeypatch.setattr(backtest_runs_router, "BACKTEST_DUCKDB_PATH", empty)
        app = FastAPI()
        app.include_router(backtest_runs_router.router)
        r = TestClient(app).get("/api/v1/backtest/trades")
        assert r.status_code == 200
        assert r.json() == {"trades": [], "total": 0, "limit": 500, "offset": 0}


class TestTradesSummary:
    def test_group_by_strategy(self, client):
        rows = client.get("/api/v1/backtest/trades/summary",
                          params={"group_by": "strategy"}).json()["rows"]
        by_key = {r["key"]: r for r in rows}
        assert by_key["ta_a1"]["n_trades"] == 2
        assert by_key["ta_a1"]["pnl_inr"] == 0.0
        assert by_key["ta_b2"]["pnl_inr"] == 80.0

    def test_group_by_financial_year(self, client):
        rows = client.get("/api/v1/backtest/trades/summary",
                          params={"group_by": "financial_year"}).json()["rows"]
        assert {r["key"] for r in rows} == {"FY2007-08", "FY2009-10"}

    def test_win_rate_computed(self, client):
        rows = client.get("/api/v1/backtest/trades/summary",
                          params={"group_by": "strategy"}).json()["rows"]
        assert {r["key"]: r["win_rate"] for r in rows}["ta_a1"] == 0.5

    def test_invalid_group_by_rejected(self, client):
        """group_by is interpolated into SQL, so it must be pattern-locked."""
        assert client.get("/api/v1/backtest/trades/summary",
                          params={"group_by": "ticker; DROP TABLE backtest_trades"}).status_code == 422
