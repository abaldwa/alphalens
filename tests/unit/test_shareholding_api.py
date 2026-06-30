"""
tests/unit/test_shareholding_api.py

Phase: 2.1 (Fundamental Data Ingestion + PIT Validation)
Specs: SPEC-PIPE-003 (CRITICAL), SPEC-DS-003
Owner: Platform / QA
Consumers: CI, pytest

SPEC-PIPE-003: shareholding PIT key is filing_date, NEVER quarter_end_date
— exercised against the real FastAPI app and a real on-disk DuckDB file,
mirroring tests/unit/test_pit_alignment.py's fundamentals coverage.
"""

from datetime import date, datetime

import pytest
from fastapi.testclient import TestClient

from datastore.api.db import close_all_connections
from datastore.api.main import app
from datastore.api.routers import shareholding as shareholding_router
from datastore.schema import create_normalised


@pytest.fixture
def client(tmp_path, monkeypatch):
    db_path = tmp_path / "shareholding_test.duckdb"
    create_normalised.create_schema(db_path=db_path)
    close_all_connections()
    monkeypatch.setattr(shareholding_router, "DUCKDB_PATH", db_path)
    return TestClient(app)


class TestShareholdingPIT:
    """SPEC-PIPE-003: filing_date is the PIT key, never quarter_end_date."""

    def test_row_excluded_while_filing_is_future_even_though_quarter_end_is_in_window(self, client):
        response = client.post(
            "/api/v1/shareholding/write",
            json={
                "ticker": "RELIANCE",
                "quarter_end_date": "2025-03-31",
                "filing_date": "2025-04-21",
                "promoter_pct": 50.3,
            },
        )
        assert response.status_code == 200, response.text

        before = client.get(
            "/api/v1/shareholding/RELIANCE",
            params={"start_date": "2025-01-01", "end_date": "2025-06-30", "as_of": "2025-04-01"},
        )
        assert before.json()["data"] == []

        after = client.get(
            "/api/v1/shareholding/RELIANCE",
            params={"start_date": "2025-01-01", "end_date": "2025-06-30", "as_of": "2025-04-21"},
        )
        rows = after.json()["data"]
        assert len(rows) == 1
        assert rows[0]["promoter_pct"] == 50.3

    def test_write_rejects_filing_date_before_quarter_end_date(self, client):
        response = client.post(
            "/api/v1/shareholding/write",
            json={"ticker": "BADCO", "quarter_end_date": "2025-03-31", "filing_date": "2025-03-01"},
        )
        assert response.status_code == 400

    def test_no_returned_row_has_filing_date_after_as_of(self, client):
        as_of = date(2025, 6, 1)
        rows_to_write = [
            ("2024-06-30", "2024-07-21"),
            ("2024-09-30", "2024-10-21"),
            ("2024-12-31", "2025-01-21"),
            ("2025-03-31", "2025-04-21"),
            ("2025-06-30", "2025-07-21"),  # future relative to as_of — must be excluded
        ]
        for qed, fd in rows_to_write:
            r = client.post(
                "/api/v1/shareholding/write",
                json={"ticker": "TCS", "quarter_end_date": qed, "filing_date": fd, "promoter_pct": 72.0},
            )
            assert r.status_code == 200

        response = client.get(
            "/api/v1/shareholding/TCS",
            params={"start_date": "2024-01-01", "end_date": "2025-12-31", "as_of": as_of.isoformat()},
        )
        rows = response.json()["data"]
        assert len(rows) == 4
        for row in rows:
            assert datetime.fromisoformat(row["filing_date"]).date() <= as_of

    def test_write_upserts_same_quarter(self, client):
        """SPEC-DS-004: same (ticker, quarter_end_date) replaces, never duplicates."""
        for promoter_pct in (50.0, 51.5):
            r = client.post(
                "/api/v1/shareholding/write",
                json={
                    "ticker": "INFY", "quarter_end_date": "2025-03-31", "filing_date": "2025-04-21",
                    "promoter_pct": promoter_pct,
                },
            )
            assert r.status_code == 200

        response = client.get(
            "/api/v1/shareholding/INFY",
            params={"start_date": "2025-01-01", "end_date": "2025-06-30", "as_of": "2025-04-21"},
        )
        rows = response.json()["data"]
        assert len(rows) == 1
        assert rows[0]["promoter_pct"] == 51.5
