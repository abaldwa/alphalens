"""
tests/unit/test_ohlcv_index_endpoint.py

ML17a — GET /api/v1/ohlcv/index/{index_name} (datastore/api/routers/ohlcv.py),
real seeded index_ohlcv rows via TestClient(app). Feeds
backtest/run_phase1_backtest.py::_fetch_real_benchmark_index() /
datastore/client.py::DataStoreClient.get_index_ohlcv().
"""


import pytest
from fastapi.testclient import TestClient

from datastore.api.db import close_all_connections, get_duckdb_connection
from datastore.api.main import app
from datastore.api.routers import ohlcv as ohlcv_router
from datastore.schema import create_normalised


@pytest.fixture
def client(tmp_path, monkeypatch):
    db_path = tmp_path / "normalised_test.duckdb"
    create_normalised.create_schema(db_path=db_path)
    close_all_connections()
    monkeypatch.setattr(ohlcv_router, "DUCKDB_PATH", db_path)
    return TestClient(app)


def _seed_index(db_path, index_name, rows):
    with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
        for d, close in rows:
            conn.execute(
                """
                INSERT INTO index_ohlcv (date, index_name, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT DO NOTHING
                """,
                [d, index_name, close, close, close, close, 100000],
            )


class TestGetIndexOhlcv:
    def test_no_data_returns_empty_array(self, client):
        r = client.get(
            "/api/v1/ohlcv/index/Nifty 500", params={"from": "2026-01-01", "to": "2026-07-01"}
        )
        assert r.status_code == 200
        assert r.json() == []

    def test_real_seeded_rows_round_trip(self, client):
        rows = [("2026-06-01", 22000.0), ("2026-06-02", 22050.0), ("2026-06-03", 22100.0)]
        _seed_index(ohlcv_router.DUCKDB_PATH, "Nifty 500", rows)

        r = client.get(
            "/api/v1/ohlcv/index/Nifty 500", params={"from": "2026-01-01", "to": "2026-07-01"}
        )
        assert r.status_code == 200, r.text
        body = r.json()
        assert len(body) == 3
        assert body[0]["index_name"] == "Nifty 500"
        assert body[-1]["close"] == 22100.0

    def test_index_name_with_ampersand_encoded_url(self, client):
        rows = [("2026-06-01", 15000.0)]
        _seed_index(ohlcv_router.DUCKDB_PATH, "Nifty Oil & Gas", rows)

        r = client.get(
            "/api/v1/ohlcv/index/Nifty Oil %26 Gas", params={"from": "2026-01-01", "to": "2026-07-01"}
        )
        assert r.status_code == 200, r.text
        body = r.json()
        assert len(body) == 1
        assert body[0]["index_name"] == "Nifty Oil & Gas"

    def test_from_after_to_is_400(self, client):
        r = client.get(
            "/api/v1/ohlcv/index/Nifty 500", params={"from": "2026-07-01", "to": "2026-01-01"}
        )
        assert r.status_code == 400

    def test_different_index_not_returned(self, client):
        _seed_index(ohlcv_router.DUCKDB_PATH, "Nifty 500", [("2026-06-01", 22000.0)])
        _seed_index(ohlcv_router.DUCKDB_PATH, "Nifty IT", [("2026-06-01", 35000.0)])

        r = client.get(
            "/api/v1/ohlcv/index/Nifty 500", params={"from": "2026-01-01", "to": "2026-07-01"}
        )
        body = r.json()
        assert len(body) == 1
        assert body[0]["close"] == 22000.0
