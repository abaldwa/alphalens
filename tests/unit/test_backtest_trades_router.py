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
               exit_variant VARCHAR, channel VARCHAR,
               backtest_run_at TIMESTAMP, backtest_start_date DATE, backtest_end_date DATE,
               ticker VARCHAR, qty DOUBLE,
               buy_date DATE, buy_price DOUBLE, sale_date DATE, sale_price DOUBLE,
               stock_rank INTEGER, pnl_inr DOUBLE, pnl_pct DOUBLE,
               exit_reason VARCHAR, holding_days INTEGER,
               buy_value DOUBLE, sale_value DOUBLE, financial_year VARCHAR)"""
    )
    # A momentum row rides along so channel filtering is exercised against a
    # genuinely mixed table, not a single-channel one where any filter passes.
    conn.executemany(
        "INSERT INTO backtest_trades VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            ("r1", "ta_a1", "A1", "unconstrained", "technical",
             "2026-08-11 03:23:44", "2007-04-01", "2026-08-10",
             "RELIANCE", 10, "2007-05-02", 100.0,
             "2007-06-02", 110.0, 1, 100.0, 0.10, "signal", 31, 1000.0, 1100.0, "FY2007-08"),
            ("r1", "ta_a1", "A1", "unconstrained", "technical",
             "2026-08-11 03:23:44", "2007-04-01", "2026-08-10",
             "TCS", 5, "2008-05-02", 200.0,
             "2009-08-02", 180.0, 2, -100.0, -0.10, "stop", 457, 1000.0, 900.0, "FY2009-10"),
            ("r2", "ta_b2", "B2", "unconstrained", "technical",
             "2026-08-11 03:45:31", "2007-04-01", "2026-08-10",
             "INFY", 8, "2010-01-02", 50.0,
             "2010-03-02", 60.0, 3, 80.0, 0.20, "signal", 59, 400.0, 480.0, "FY2009-10"),
            ("r3", "mom_1", None, "unconstrained", "momentum",
             "2026-08-05 08:35:14", "2016-04-01", "2026-08-04",
             "HDFCBANK", 4, "2020-01-02", 300.0,
             "2020-06-02", 330.0, 1, 120.0, 0.10, "signal", 152, 1200.0, 1320.0, "FY2020-21"),
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
        # 4 rows: 3 technical + 1 momentum (mixed channels on purpose)
        assert body["total"] == 4
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
        assert body["total"] == 4, "total must be the unpaginated count"

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
        assert {r["key"] for r in rows} == {"FY2007-08", "FY2009-10", "FY2020-21"}

    def test_win_rate_computed(self, client):
        rows = client.get("/api/v1/backtest/trades/summary",
                          params={"group_by": "strategy"}).json()["rows"]
        assert {r["key"]: r["win_rate"] for r in rows}["ta_a1"] == 0.5

    def test_invalid_group_by_rejected(self, client):
        """group_by is interpolated into SQL, so it must be pattern-locked."""
        assert client.get("/api/v1/backtest/trades/summary",
                          params={"group_by": "ticker; DROP TABLE backtest_trades"}).status_code == 422


class TestChannelAndBacktestDateColumns:
    """[2026-08-11] A trade row must say which ENGINE produced it and when the
    backtest ran, without joining backtest_runs.

    backtest_trades recorded what was traded and by which strategy, but not
    the channel (technical/momentum/fundamental/ml) nor the backtest's own
    date. Both required a join — the exact dependency this table's
    denormalisation exists to avoid, and one that silently drops trades whose
    parent run row has been purged (7,925 such orphans were found on the day
    this was added).

    Three date questions are distinct and all get asked:
      buy_date/sale_date    when the TRADE happened
      backtest_run_at       when the BACKTEST executed
      backtest_start/end    what PERIOD the backtest covered
    """

    def test_trade_columns_include_channel_and_backtest_dates(self):
        from datastore.api.routers.backtest_runs import _TRADE_COLUMNS

        for col in (
            "channel",
            "backtest_run_at",
            "backtest_start_date",
            "backtest_end_date",
        ):
            assert col in _TRADE_COLUMNS, f"{col} missing from the trades projection"

    def test_backtest_dates_are_distinct_from_trade_dates(self):
        """Guards against collapsing the three date concepts into one."""
        from datastore.api.routers.backtest_runs import _TRADE_COLUMNS

        for col in ("buy_date", "sale_date", "backtest_run_at"):
            assert col in _TRADE_COLUMNS

    def test_channel_filter_discriminates_between_engines(self, client):
        """The point of the column: Technical trades separable from Momentum."""
        tech = client.get("/api/v1/backtest/trades", params={"channel": "technical"}).json()
        mom = client.get("/api/v1/backtest/trades", params={"channel": "momentum"}).json()
        assert tech["total"] == 3
        assert mom["total"] == 1
        assert {t["channel"] for t in tech["trades"]} == {"technical"}
        assert mom["trades"][0]["ticker"] == "HDFCBANK"

    def test_backtest_run_date_is_on_the_row(self, client):
        """When the BACKTEST ran, not when the trade did."""
        row = client.get("/api/v1/backtest/trades", params={"ticker": "HDFCBANK"}).json()["trades"][0]
        assert row["backtest_run_at"].startswith("2026-08-05")
        assert row["backtest_start_date"].startswith("2016-04-01")
        # ...and distinct from the trade's own dates.
        assert row["buy_date"].startswith("2020-01-02")

    def test_summary_can_group_by_channel(self, client):
        rows = client.get("/api/v1/backtest/trades/summary",
                          params={"group_by": "channel"}).json()["rows"]
        assert {r["key"]: r["n_trades"] for r in rows} == {"technical": 3, "momentum": 1}

    def test_unknown_channel_returns_empty_not_error(self, client):
        body = client.get("/api/v1/backtest/trades", params={"channel": "nonsense"}).json()
        assert body["total"] == 0
