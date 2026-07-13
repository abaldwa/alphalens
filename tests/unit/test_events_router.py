"""
tests/unit/test_events_router.py

A72 (partial) — real seeded-DuckDB TestClient(app) tests for
datastore/api/routers/events.py: corporate_actions/bulk_deal_positions
reuse (existing tables) + recommendation_trigger detection (buy-signal
crossing) from real ml_signals rows. No mocks over the DB layer.
"""

import pytest
from fastapi.testclient import TestClient

from datastore.api.db import close_all_connections, get_duckdb_connection
from datastore.api.main import app
from datastore.api.routers import events as events_router
from datastore.schema import create_normalised, create_signals


@pytest.fixture
def client(tmp_path, monkeypatch):
    normalised_path = tmp_path / "normalised_test.duckdb"
    signals_path = tmp_path / "signals_test.duckdb"
    create_normalised.create_schema(db_path=normalised_path)
    create_signals.create_signal_tables_schema(db_path=signals_path)
    close_all_connections()

    monkeypatch.setattr(events_router, "DUCKDB_PATH", normalised_path)
    monkeypatch.setattr(events_router, "SIGNALS_DUCKDB_PATH", signals_path)
    return TestClient(app), normalised_path, signals_path


def _seed_corp_action(db_path):
    with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
        conn.execute(
            """
            INSERT INTO corporate_actions (ticker, ex_date, action_type, ratio, details)
            VALUES ('RELIANCE', '2026-01-10', 'BONUS', 1.0, '1:1 bonus')
            """
        )


def _seed_bulk_deal(db_path):
    with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
        conn.execute(
            """
            INSERT INTO bulk_deal_positions
                (family_id, ticker, trade_date, deal_type, net_transaction_type, net_quantity,
                 exchange, cumulative_position_est, is_new_entry, is_full_exit)
            VALUES ('FAM1', 'RELIANCE', '2026-01-12', 'bulk', 'BUY', 100000, 'NSE', 100000, TRUE, FALSE)
            """
        )


def _seed_signals(db_path, direction_by_date):
    with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
        for d, direction in direction_by_date.items():
            conn.execute(
                """
                INSERT OR REPLACE INTO ml_signals
                (date, ticker, model_name, model_version, signal_direction)
                VALUES (?, 'RELIANCE', 'signal_5d', 'v1', ?)
                """,
                [d, direction],
            )


class TestEventsEndpoint:
    def test_corporate_action_and_bulk_deal_events_included(self, client):
        app_client, normalised_path, signals_path = client
        _seed_corp_action(normalised_path)
        _seed_bulk_deal(normalised_path)

        resp = app_client.get("/api/v1/events/RELIANCE")
        assert resp.status_code == 200, resp.text
        events = resp.json()
        types = {e["event_type"] for e in events}
        assert "corporate_action" in types
        assert "bulk_deal" in types
        ca = next(e for e in events if e["event_type"] == "corporate_action")
        assert ca["date"] == "2026-01-10"
        assert "BONUS" in ca["description"]

    def test_recommendation_trigger_only_on_crossing_into_buy(self, client):
        app_client, normalised_path, signals_path = client
        _seed_signals(signals_path, {
            "2026-01-01": "hold",
            "2026-01-02": "buy",   # crossing — event
            "2026-01-03": "buy",   # persists — no new event
            "2026-01-04": "sell",
            "2026-01-05": "buy",   # crossing again — event
        })

        resp = app_client.get("/api/v1/events/RELIANCE")
        assert resp.status_code == 200, resp.text
        triggers = [e for e in resp.json() if e["event_type"] == "recommendation_trigger"]
        assert [t["date"] for t in triggers] == ["2026-01-02", "2026-01-05"]

    def test_date_range_filters_events(self, client):
        app_client, normalised_path, signals_path = client
        _seed_corp_action(normalised_path)
        resp = app_client.get(
            "/api/v1/events/RELIANCE",
            params={"from_date": "2026-02-01", "to_date": "2026-03-01"},
        )
        assert resp.status_code == 200
        assert resp.json() == []

    def test_no_events_returns_empty_list(self, client):
        app_client, _, _ = client
        resp = app_client.get("/api/v1/events/NOTAREALTICKER")
        assert resp.status_code == 200
        assert resp.json() == []
