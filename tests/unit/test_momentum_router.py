"""
tests/unit/test_momentum_router.py

ML38 — real seeded-DuckDB (tmp_path, never the production
datastore/normalised/alphalens.duckdb) TestClient(app) tests for
datastore/api/routers/momentum.py's CRUD + summary endpoints. No mocks
over the DB layer, matching test_holdings_router.py's convention.
"""

import pytest
from fastapi.testclient import TestClient

from datastore.api.db import close_all_connections, get_duckdb_connection
from datastore.api.main import app
from datastore.api.routers import momentum as momentum_router
from datastore.schema import create_normalised


@pytest.fixture
def db_path(tmp_path):
    path = tmp_path / "normalised_test.duckdb"
    create_normalised.create_schema(db_path=path)
    close_all_connections()
    return path


@pytest.fixture
def client(db_path, monkeypatch):
    monkeypatch.setattr(momentum_router, "DUCKDB_PATH", db_path)
    return TestClient(app)


def _seed_ohlcv(db_path, ticker, date_str, close):
    with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
        conn.execute(
            """
            INSERT INTO ohlcv_adjusted (date, ticker, open, high, low, close, volume, delivery_qty, delivery_pct)
            VALUES (?, ?, ?, ?, ?, ?, 1000, 500, 50.0)
            ON CONFLICT DO NOTHING
            """,
            [date_str, ticker, close, close, close, close],
        )


class TestMomentumSchema:
    def test_tables_created_with_expected_columns(self, db_path):
        with get_duckdb_connection(db_path, persist=False, read_only=True) as conn:
            for table, expected in [
                ("momentum_trades", {"id", "ticker", "purchase_date", "qty", "sale_date", "grace_remaining"}),
                ("momentum_contributions", {"id", "contribution_date", "amount"}),
                ("momentum_rankings", {"date", "ticker", "momentum_rank", "in_top_n"}),
                ("momentum_rebalance_suggestions", {"id", "rebalance_date", "ticker", "action", "status"}),
                ("momentum_rebalance_state", {"strategy_id", "next_rebalance_date"}),
            ]:
                cols = {
                    r[0] for r in conn.execute(
                        f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}'"
                    ).fetchall()
                }
                assert expected <= cols, f"{table} missing columns: {expected - cols}"


class TestUniverse:
    def test_falls_back_to_live_compute_when_no_snapshot_row(self, client, db_path, monkeypatch):
        monkeypatch.setattr(momentum_router.momentum_live, "rank_band_tickers", lambda *a, **kw: ["AAA", "BBB"])
        monkeypatch.setattr(momentum_router.momentum_live, "LOOKBACK_MONTHS", 1)
        import pandas as pd
        dates = pd.bdate_range("2026-01-01", periods=30)
        for d in dates:
            _seed_ohlcv(db_path, "AAA", str(d.date()), 100)
            _seed_ohlcv(db_path, "BBB", str(d.date()), 100)

        resp = client.get("/api/v1/momentum/universe", params={"as_of_date": str(dates[-1].date())})
        assert resp.status_code == 200, resp.text
        rows = resp.json()
        assert {r["ticker"] for r in rows} == {"AAA", "BBB"}

    def test_reads_precomputed_snapshot_when_present(self, client, db_path):
        with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
            conn.execute(
                "INSERT INTO momentum_rankings (date, strategy_id, ticker, momentum_return, momentum_rank, in_top_n, band_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                ["2026-02-01", momentum_router.DEFAULT_STRATEGY_ID, "ZZZ", 0.1, 1, True, 3],
            )
        resp = client.get("/api/v1/momentum/universe", params={"as_of_date": "2026-02-01"})
        assert resp.status_code == 200, resp.text
        rows = resp.json()
        assert len(rows) == 1
        row = rows[0]
        assert row["ticker"] == "ZZZ"
        assert row["momentum_return"] == 0.1
        assert row["momentum_rank"] == 1
        assert row["in_top_n"] is True
        # No stock_master/ohlcv_adjusted row seeded for ZZZ — real absence, not fabricated.
        assert row["company_name"] is None
        assert row["price"] is None
        assert row["sparkline"] == []
        assert row["return_20d"] is None

    def test_enriches_with_company_name_price_and_sparkline(self, client, db_path):
        import pandas as pd
        with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
            conn.execute(
                "INSERT INTO momentum_rankings (date, strategy_id, ticker, momentum_return, momentum_rank, in_top_n, band_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                ["2026-02-01", momentum_router.DEFAULT_STRATEGY_ID, "ZZZ", 0.1, 1, True, 3],
            )
            conn.execute(
                "INSERT INTO stock_master (ticker, company_name, nse_series) VALUES (?, ?, ?)",
                ["ZZZ", "ZZZ Industries Ltd", "EQ"],
            )
        dates = pd.bdate_range("2025-12-01", "2026-02-01")  # >30 business days, exercises the trailing-30 cap
        for i, d in enumerate(dates):
            _seed_ohlcv(db_path, "ZZZ", str(d.date()), 100 + i)

        resp = client.get("/api/v1/momentum/universe", params={"as_of_date": "2026-02-01"})
        row = resp.json()[0]
        assert row["company_name"] == "ZZZ Industries Ltd"
        assert row["price"] == 100 + len(dates) - 1
        assert len(row["sparkline"]) == momentum_router._SPARKLINE_TRADING_DAYS
        assert row["sparkline"][-1] == row["price"]
        # 44 real seeded trading days -> enough history for a real 20-trading-day
        # return; close 20 trading days back was (100 + len(dates)-1-20).
        expected_20d_return = (row["price"] / (100 + len(dates) - 1 - 20)) - 1.0
        assert row["return_20d"] == pytest.approx(expected_20d_return)


class TestRebalanceState:
    def test_next_rebalance_defaults_to_nulls_when_no_state(self, client):
        resp = client.get("/api/v1/momentum/rebalance/next")
        assert resp.status_code == 200
        assert resp.json() == {"last_rebalance_date": None, "next_rebalance_date": None}

    def test_next_rebalance_reads_state(self, client, db_path):
        with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
            conn.execute(
                "INSERT INTO momentum_rebalance_state (strategy_id, next_rebalance_date) VALUES (?, ?)",
                [momentum_router.DEFAULT_STRATEGY_ID, "2026-03-02"],
            )
        resp = client.get("/api/v1/momentum/rebalance/next")
        assert resp.json()["next_rebalance_date"] == "2026-03-02"


class TestRebalanceSuggestions:
    def test_list_and_dismiss(self, client, db_path):
        with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
            new_id = conn.execute(
                "INSERT INTO momentum_rebalance_suggestions "
                "(strategy_id, rebalance_date, ticker, action, momentum_rank, grace_remaining) "
                "VALUES (?, ?, ?, ?, ?, ?) RETURNING id",
                [momentum_router.DEFAULT_STRATEGY_ID, "2026-03-02", "AAA", "add", 1, None],
            ).fetchone()[0]

        listed = client.get("/api/v1/momentum/rebalance/suggestions").json()
        assert len(listed) == 1
        assert listed[0]["status"] == "pending"

        resp = client.post(f"/api/v1/momentum/rebalance/suggestions/{new_id}/dismiss")
        assert resp.status_code == 200

        listed_after = client.get(
            "/api/v1/momentum/rebalance/suggestions", params={"rebalance_date": "2026-03-02"}
        ).json()
        assert listed_after[0]["status"] == "dismissed"

    def test_dismiss_nonexistent_404s(self, client):
        resp = client.post("/api/v1/momentum/rebalance/suggestions/99999/dismiss")
        assert resp.status_code == 404


class TestTradesCrud:
    def test_create_and_list_trade(self, client):
        resp = client.post(
            "/api/v1/momentum/trades/",
            json={
                "strategy_id": momentum_router.DEFAULT_STRATEGY_ID,
                "ticker": "reliance", "purchase_date": "2026-01-05", "qty": 10, "purchase_price": 2500.0,
            },
        )
        assert resp.status_code == 200, resp.text
        created = resp.json()
        assert created["ticker"] == "RELIANCE"
        assert created["strategy_id"] == momentum_router.DEFAULT_STRATEGY_ID
        assert created["sale_date"] is None

        listed = client.get("/api/v1/momentum/trades/", params={"strategy_id": momentum_router.DEFAULT_STRATEGY_ID}).json()
        assert len(listed) == 1
        assert listed[0]["id"] == created["id"]

    def test_create_trade_rejects_unknown_strategy(self, client):
        resp = client.post(
            "/api/v1/momentum/trades/",
            json={"strategy_id": "not_a_real_strategy", "ticker": "AAA", "purchase_date": "2026-01-05", "qty": 10},
        )
        assert resp.status_code == 400

    def test_list_trades_across_all_strategies_when_unfiltered(self, client):
        for band in momentum_router.momentum_live.STRATEGIES[:2]:
            client.post(
                "/api/v1/momentum/trades/",
                json={"strategy_id": band["strategy_id"], "ticker": "AAA", "purchase_date": "2026-01-01", "qty": 1},
            )
        listed = client.get("/api/v1/momentum/trades/").json()
        assert len(listed) == 2
        assert {r["strategy_id"] for r in listed} == {s["strategy_id"] for s in momentum_router.momentum_live.STRATEGIES[:2]}

    def test_create_trade_linked_to_suggestion_marks_it_acted(self, client, db_path):
        with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
            suggestion_id = conn.execute(
                "INSERT INTO momentum_rebalance_suggestions "
                "(strategy_id, rebalance_date, ticker, action, momentum_rank) "
                "VALUES (?, ?, ?, ?, ?) RETURNING id",
                [momentum_router.DEFAULT_STRATEGY_ID, "2026-03-02", "AAA", "add", 1],
            ).fetchone()[0]

        client.post(
            "/api/v1/momentum/trades/",
            json={
                "strategy_id": momentum_router.DEFAULT_STRATEGY_ID,
                "ticker": "AAA", "purchase_date": "2026-03-02", "qty": 10, "purchase_price": 100.0,
                "suggestion_id": suggestion_id,
            },
        )
        suggestions = client.get(
            "/api/v1/momentum/rebalance/suggestions", params={"rebalance_date": "2026-03-02"}
        ).json()
        assert suggestions[0]["status"] == "acted"

    def test_open_only_filter_excludes_sold_positions(self, client):
        strategy_id = momentum_router.DEFAULT_STRATEGY_ID
        open_pos = client.post(
            "/api/v1/momentum/trades/",
            json={"strategy_id": strategy_id, "ticker": "TCS", "purchase_date": "2026-01-01", "qty": 5},
        ).json()
        client.post(
            "/api/v1/momentum/trades/",
            json={
                "strategy_id": strategy_id, "ticker": "INFY", "purchase_date": "2026-01-01", "qty": 5,
                "sale_date": None, "sell_price": None,
            },
        )
        client.put(
            f"/api/v1/momentum/trades/{client.get('/api/v1/momentum/trades/', params={'strategy_id': strategy_id}).json()[0]['id']}",
            json={"sale_date": "2026-02-01", "sell_price": 1600.0},
        )

        open_only = client.get(
            "/api/v1/momentum/trades/", params={"strategy_id": strategy_id, "open_only": True}
        ).json()
        assert open_pos["id"] in [r["id"] for r in open_only]

    def test_update_records_a_sale(self, client):
        created = client.post(
            "/api/v1/momentum/trades/",
            json={
                "strategy_id": momentum_router.DEFAULT_STRATEGY_ID,
                "ticker": "HDFCBANK", "purchase_date": "2026-01-01", "qty": 3, "purchase_price": 1500.0,
            },
        ).json()

        resp = client.put(
            f"/api/v1/momentum/trades/{created['id']}",
            json={"sale_date": "2026-03-01", "sell_price": 1650.0, "exit_rank": 30, "sell_rationale": "Grace exhausted"},
        )
        assert resp.status_code == 200, resp.text
        updated = resp.json()
        assert updated["sale_date"] == "2026-03-01"
        assert updated["sell_price"] == 1650.0
        assert updated["exit_rank"] == 30
        assert updated["ticker"] == "HDFCBANK"

    def test_update_nonexistent_trade_404s(self, client):
        resp = client.put("/api/v1/momentum/trades/99999", json={"sell_price": 100.0})
        assert resp.status_code == 404

    def test_delete_trade(self, client):
        strategy_id = momentum_router.DEFAULT_STRATEGY_ID
        created = client.post(
            "/api/v1/momentum/trades/",
            json={"strategy_id": strategy_id, "ticker": "WIPRO", "purchase_date": "2026-01-01", "qty": 1},
        ).json()
        resp = client.delete(f"/api/v1/momentum/trades/{created['id']}")
        assert resp.status_code == 200
        assert client.get("/api/v1/momentum/trades/", params={"strategy_id": strategy_id}).json() == []

    def test_delete_nonexistent_trade_404s(self, client):
        resp = client.delete("/api/v1/momentum/trades/99999")
        assert resp.status_code == 404


class TestContributions:
    def test_create_and_list(self, client):
        strategy_id = momentum_router.DEFAULT_STRATEGY_ID
        resp = client.post(
            "/api/v1/momentum/contributions/",
            json={"strategy_id": strategy_id, "contribution_date": "2026-01-01", "amount": 1_000_000.0, "note": "Initial capital"},
        )
        assert resp.status_code == 200, resp.text
        listed = client.get("/api/v1/momentum/contributions/", params={"strategy_id": strategy_id}).json()
        assert len(listed) == 1
        assert listed[0]["amount"] == 1_000_000.0


class TestStrategies:
    def test_list_strategies_returns_5_rank_bands(self, client):
        resp = client.get("/api/v1/momentum/strategies")
        assert resp.status_code == 200, resp.text
        strategies = resp.json()
        assert len(strategies) == 5
        assert momentum_router.DEFAULT_STRATEGY_ID in {s["strategy_id"] for s in strategies}


class TestSummary:
    def test_summary_with_one_closed_and_one_open_position(self, client, db_path):
        strategy_id = momentum_router.DEFAULT_STRATEGY_ID
        _seed_ohlcv(db_path, "AAA", "2026-06-01", 120.0)  # today's mark-to-market for the open position

        client.post(
            "/api/v1/momentum/contributions/",
            json={"strategy_id": strategy_id, "contribution_date": "2026-01-01", "amount": 100_000.0},
        )
        # Closed trade: bought 10 @ 100 on Jan 1, sold @ 110 on Feb 1 (STCG, <365 days).
        client.post(
            "/api/v1/momentum/trades/",
            json={"strategy_id": strategy_id, "ticker": "BBB", "purchase_date": "2026-01-01", "qty": 10, "purchase_price": 100.0},
        )
        bbb_id = client.get("/api/v1/momentum/trades/", params={"strategy_id": strategy_id}).json()[0]["id"]
        client.put(f"/api/v1/momentum/trades/{bbb_id}", json={"sale_date": "2026-02-01", "sell_price": 110.0})

        # Open trade: bought 5 @ 100 on Jan 1, currently marked at 120 (seeded above).
        client.post(
            "/api/v1/momentum/trades/",
            json={"strategy_id": strategy_id, "ticker": "AAA", "purchase_date": "2026-01-01", "qty": 5, "purchase_price": 100.0},
        )

        resp = client.get("/api/v1/momentum/summary")
        assert resp.status_code == 200, resp.text
        summary = resp.json()

        # Realized gain on BBB: 10*(110-100)=100, STCG 20% => 20.
        # Unrealized gain on AAA (marked @120): 5*(120-100)=100, STCG 20% => 20.
        assert summary["total_tax_due"] == pytest.approx(40.0)
        assert summary["current_holdings_value"] == pytest.approx(5 * 120.0)
        assert summary["total_contributed"] == pytest.approx(100_000.0)
        # idle_cash = 100_000 contributed - (1000 + 500) deployed + 1100 recovered
        assert summary["idle_cash"] == pytest.approx(100_000.0 - 1500.0 + 1100.0)
        assert summary["xirr"] is not None

    def test_summary_with_no_data_returns_nulls_not_error(self, client):
        resp = client.get("/api/v1/momentum/summary")
        assert resp.status_code == 200
        summary = resp.json()
        assert summary["xirr"] is None
        assert summary["total_tax_due"] == 0.0
        assert summary["current_holdings_value"] == 0.0
