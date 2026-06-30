"""
tests/unit/test_corporate_actions_api.py

Phase: 2.2 (AMFI MF Holdings + Corporate Action Features)
Specs: SPEC-DS-001, SPEC-DS-002, SPEC-PIPE-002
Owner: Platform / QA
Consumers: CI, pytest

Exercises the real FastAPI app + a real on-disk DuckDB file (not mocks),
same pattern as tests/unit/test_pit_alignment.py / test_shareholding_api.py.
"""

from datetime import date

import pytest
from fastapi.testclient import TestClient

from datastore.api.db import close_all_connections
from datastore.api.main import app
from datastore.api.routers import corporate_actions as corporate_actions_router
from datastore.schema import create_normalised


@pytest.fixture
def client(tmp_path, monkeypatch):
    db_path = tmp_path / "corp_actions_test.duckdb"
    create_normalised.create_schema(db_path=db_path)
    close_all_connections()
    monkeypatch.setattr(corporate_actions_router, "DUCKDB_PATH", db_path)
    return TestClient(app)


def _insert_action(db_path, ticker, ex_date, action_type, ratio, announcement_date=None, record_date=None):
    import duckdb

    conn = duckdb.connect(str(db_path))
    conn.execute(
        "INSERT INTO corporate_actions (ticker, ex_date, action_type, ratio, announcement_date, record_date) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        [ticker, ex_date, action_type, ratio, announcement_date, record_date],
    )
    conn.close()


class TestGetCorporateActions:
    def test_returns_rows_ascending_by_ex_date(self, client, tmp_path, monkeypatch):
        db_path = tmp_path / "corp_actions_test.duckdb"
        _insert_action(db_path, "RELIANCE", date(2025, 6, 1), "SPLIT", 2.0, date(2025, 5, 1), date(2025, 6, 5))
        _insert_action(db_path, "RELIANCE", date(2025, 1, 1), "BONUS", 1.0, date(2024, 12, 1), date(2025, 1, 5))

        response = client.get("/api/v1/corporate_actions/RELIANCE")
        assert response.status_code == 200
        data = response.json()["data"]
        assert len(data) == 2
        assert data[0]["ex_date"].startswith("2025-01-01")  # ascending
        assert data[1]["ex_date"].startswith("2025-06-01")

    def test_from_to_filters_by_ex_date_window(self, client, tmp_path):
        db_path = tmp_path / "corp_actions_test.duckdb"
        _insert_action(db_path, "TCS", date(2025, 1, 1), "BONUS", 1.0)
        _insert_action(db_path, "TCS", date(2025, 6, 1), "SPLIT", 2.0)

        response = client.get(
            "/api/v1/corporate_actions/TCS", params={"from": "2025-03-01", "to": "2025-12-31"}
        )
        data = response.json()["data"]
        assert len(data) == 1
        assert data[0]["action_type"] == "SPLIT"

    def test_from_after_to_returns_400(self, client):
        response = client.get(
            "/api/v1/corporate_actions/TCS", params={"from": "2025-12-31", "to": "2025-01-01"}
        )
        assert response.status_code == 400

    def test_no_rows_returns_empty_list_not_error(self, client):
        response = client.get("/api/v1/corporate_actions/NEWCO")
        assert response.status_code == 200
        assert response.json()["data"] == []
        assert response.json()["record_count"] == 0
