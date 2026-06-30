"""
tests/unit/test_fno_api.py

Phase: 2.3 (F&O Features + Signal63D + Full Phase 2 Feature Matrix)
Specs: SPEC-DS-001, SPEC-DS-002, SPEC-PIPE-001
Owner: Platform / QA
Consumers: CI, pytest

Exercises the real FastAPI app + a real on-disk DuckDB file (not mocks),
same pattern as tests/unit/test_corporate_actions_api.py.
"""

from datetime import date

import pytest
from fastapi.testclient import TestClient

from datastore.api.db import close_all_connections
from datastore.api.main import app
from datastore.api.routers import fno as fno_router
from datastore.schema import create_normalised


@pytest.fixture
def client(tmp_path, monkeypatch):
    db_path = tmp_path / "fno_test.duckdb"
    create_normalised.create_schema(db_path=db_path)
    close_all_connections()
    monkeypatch.setattr(fno_router, "DUCKDB_PATH", db_path)
    return TestClient(app)


def _insert_contract(
    db_path, ticker, trade_date, instrument, expiry, strike=None, option_type=None,
    oi=0, oi_change=0, volume=0, settle_price=0.0, close_price=0.0, underlying_price=0.0,
):
    import duckdb

    conn = duckdb.connect(str(db_path))
    conn.execute(
        "INSERT INTO fno_data (trade_date, ticker, instrument, expiry, strike, option_type, "
        "oi, oi_change, volume, settle_price, close_price, underlying_price) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [trade_date, ticker, instrument, expiry, strike, option_type,
         oi, oi_change, volume, settle_price, close_price, underlying_price],
    )
    conn.close()


class TestGetFNOChain:
    def test_returns_rows_ascending_by_trade_date(self, client, tmp_path):
        db_path = tmp_path / "fno_test.duckdb"
        _insert_contract(
            db_path, "RELIANCE", date(2026, 6, 22), "STF", date(2026, 6, 30),
            oi=10000, settle_price=1303.0, underlying_price=1300.0,
        )
        _insert_contract(
            db_path, "RELIANCE", date(2026, 6, 19), "STF", date(2026, 6, 30),
            oi=9500, settle_price=1290.0, underlying_price=1288.0,
        )

        response = client.get(
            "/api/v1/fno/RELIANCE", params={"from": "2026-06-01", "to": "2026-06-30"}
        )
        assert response.status_code == 200
        data = response.json()["data"]
        assert len(data) == 2
        assert data[0]["trade_date"].startswith("2026-06-19")  # ascending
        assert data[1]["trade_date"].startswith("2026-06-22")

    def test_from_to_filters_by_trade_date_window(self, client, tmp_path):
        db_path = tmp_path / "fno_test.duckdb"
        _insert_contract(db_path, "TCS", date(2026, 5, 1), "STF", date(2026, 5, 28), oi=100)
        _insert_contract(db_path, "TCS", date(2026, 6, 22), "STF", date(2026, 6, 30), oi=200)

        response = client.get(
            "/api/v1/fno/TCS", params={"from": "2026-06-01", "to": "2026-06-30"}
        )
        data = response.json()["data"]
        assert len(data) == 1
        assert data[0]["oi"] == 200

    def test_futures_rows_have_null_strike_and_option_type(self, client, tmp_path):
        db_path = tmp_path / "fno_test.duckdb"
        _insert_contract(db_path, "INFY", date(2026, 6, 22), "STF", date(2026, 6, 30), oi=500)

        response = client.get("/api/v1/fno/INFY", params={"from": "2026-06-01", "to": "2026-06-30"})
        row = response.json()["data"][0]
        assert row["strike"] is None
        assert row["option_type"] is None

    def test_from_after_to_returns_400(self, client):
        response = client.get(
            "/api/v1/fno/TCS", params={"from": "2026-12-31", "to": "2026-01-01"}
        )
        assert response.status_code == 400

    def test_no_rows_returns_empty_list_not_error(self, client):
        response = client.get(
            "/api/v1/fno/NOTFNOELIGIBLE", params={"from": "2026-01-01", "to": "2026-12-31"}
        )
        assert response.status_code == 200
        assert response.json()["data"] == []
        assert response.json()["record_count"] == 0
