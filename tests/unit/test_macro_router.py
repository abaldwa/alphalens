"""
tests/unit/test_macro_router.py

A27 (2026-07-10): exercises GET/POST /api/v1/macro/indicators against the
real FastAPI app, with an isolated tmp_path parquet file (never the
production macro_real_economy.parquet).
"""

import pytest
from fastapi.testclient import TestClient

from datastore.api.main import app


@pytest.fixture
def client(tmp_path, monkeypatch):
    import datastore.api.routers.macro as macro_router

    monkeypatch.setattr(macro_router, "_MACRO_REAL_ECONOMY_PATH", tmp_path / "macro_real_economy.parquet")
    return TestClient(app)


class TestWriteMacroIndicator:
    def test_writes_and_reads_back_a_manual_entry(self, client):
        resp = client.post(
            "/api/v1/macro/indicators",
            json={"feature_name": "pmi_manufacturing", "reference_month_end": "2026-06-30", "value": 54.2},
        )
        assert resp.status_code == 200
        assert resp.json()["written"] is True

        get_resp = client.get("/api/v1/macro/indicators", params={"feature_name": "pmi_manufacturing"})
        rows = get_resp.json()["rows"]
        assert len(rows) == 1
        assert rows[0]["value"] == 54.2
        assert rows[0]["reference_month_end"] == "2026-06-30"

    def test_rejects_a_feature_with_a_real_automated_source(self, client):
        resp = client.post(
            "/api/v1/macro/indicators",
            json={"feature_name": "cement_dispatches_growth", "reference_month_end": "2026-06-30", "value": 5.0},
        )
        assert resp.status_code == 400
        assert "not manually enterable" in resp.json()["detail"]

    def test_rejects_an_unknown_feature_name(self, client):
        resp = client.post(
            "/api/v1/macro/indicators",
            json={"feature_name": "made_up_indicator", "reference_month_end": "2026-06-30", "value": 1.0},
        )
        assert resp.status_code == 400

    def test_upsert_overwrites_not_duplicates_same_month(self, client):
        client.post(
            "/api/v1/macro/indicators",
            json={"feature_name": "gst_collection_growth", "reference_month_end": "2026-05-15", "value": 10.0},
        )
        client.post(
            "/api/v1/macro/indicators",
            json={"feature_name": "gst_collection_growth", "reference_month_end": "2026-05-20", "value": 12.0},
        )
        get_resp = client.get("/api/v1/macro/indicators", params={"feature_name": "gst_collection_growth"})
        rows = get_resp.json()["rows"]
        assert len(rows) == 1  # same month (05) -> overwrite, not append
        assert rows[0]["value"] == 12.0

    def test_different_months_produce_separate_rows(self, client):
        client.post(
            "/api/v1/macro/indicators",
            json={"feature_name": "bank_credit_growth", "reference_month_end": "2026-04-30", "value": 8.0},
        )
        client.post(
            "/api/v1/macro/indicators",
            json={"feature_name": "bank_credit_growth", "reference_month_end": "2026-05-31", "value": 9.0},
        )
        get_resp = client.get("/api/v1/macro/indicators", params={"feature_name": "bank_credit_growth"})
        rows = get_resp.json()["rows"]
        assert len(rows) == 2


class TestGetMacroIndicators:
    def test_no_file_yet_returns_empty(self, client):
        resp = client.get("/api/v1/macro/indicators")
        assert resp.status_code == 200
        assert resp.json()["rows"] == []

    def test_most_recent_first(self, client):
        for month, value in [("2026-01-31", 1.0), ("2026-02-28", 2.0), ("2026-03-31", 3.0)]:
            client.post(
                "/api/v1/macro/indicators",
                json={"feature_name": "upi_transaction_growth", "reference_month_end": month, "value": value},
            )
        resp = client.get("/api/v1/macro/indicators", params={"feature_name": "upi_transaction_growth"})
        rows = resp.json()["rows"]
        assert [r["value"] for r in rows] == [3.0, 2.0, 1.0]
