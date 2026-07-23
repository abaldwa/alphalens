"""
tests/unit/test_features_router.py

A65: router-level tests for `datastore/api/routers/features.py` (SPEC-FEAT-001/
SPEC-DS-006), previously untested (39.29% coverage, no test file). Real
Parquet files on tmp_path via TestClient(app), same pattern as
tests/unit/test_feature_store_utils.py/test_technical_router.py — no mocks.
"""

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from datastore.api.main import app
from datastore.api.utils import feature_store as feature_store_module


@pytest.fixture
def features_dir(tmp_path, monkeypatch):
    d = tmp_path / "features_daily"
    d.mkdir()
    monkeypatch.setattr(feature_store_module, "FEATURES_DAILY_DIR", d)
    return d


def _write_day(features_dir, date_str, rows):
    pd.DataFrame(rows).to_parquet(features_dir / f"{date_str}.parquet")


class TestGetFeatures:
    def test_no_feature_data_returns_404(self, features_dir):
        client = TestClient(app)
        resp = client.get(
            "/api/v1/features/RELIANCE",
            params={"start_date": "2026-01-01", "end_date": "2026-01-05"},
        )
        assert resp.status_code == 404

    def test_end_before_start_returns_400(self, features_dir):
        client = TestClient(app)
        resp = client.get(
            "/api/v1/features/RELIANCE",
            params={"start_date": "2026-01-05", "end_date": "2026-01-01"},
        )
        assert resp.status_code == 400

    def test_returns_all_features_for_ticker_in_range(self, features_dir):
        _write_day(
            features_dir, "2026-06-01",
            [{"ticker": "RELIANCE", "date": "2026-06-01", "rsi_14": 55.0, "adx_14": 20.0}],
        )
        _write_day(
            features_dir, "2026-06-02",
            [{"ticker": "RELIANCE", "date": "2026-06-02", "rsi_14": 60.0, "adx_14": 22.0}],
        )
        client = TestClient(app)
        resp = client.get(
            "/api/v1/features/RELIANCE",
            params={"start_date": "2026-06-01", "end_date": "2026-06-02"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["record_count"] == 2
        assert body["data"][0]["feature_values"]["rsi_14"] == 55.0
        assert body["data"][1]["feature_values"]["adx_14"] == 22.0

    def test_subset_of_feature_names_returned(self, features_dir):
        _write_day(
            features_dir, "2026-06-01",
            [{"ticker": "RELIANCE", "date": "2026-06-01", "rsi_14": 55.0, "adx_14": 20.0}],
        )
        client = TestClient(app)
        resp = client.get(
            "/api/v1/features/RELIANCE",
            params={"start_date": "2026-06-01", "end_date": "2026-06-01", "feature_names": ["rsi_14"]},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert set(body["data"][0]["feature_values"].keys()) == {"rsi_14"}

    def test_unknown_feature_name_returns_400(self, features_dir):
        _write_day(
            features_dir, "2026-06-01",
            [{"ticker": "RELIANCE", "date": "2026-06-01", "rsi_14": 55.0}],
        )
        client = TestClient(app)
        resp = client.get(
            "/api/v1/features/RELIANCE",
            params={"start_date": "2026-06-01", "end_date": "2026-06-01", "feature_names": ["not_a_feature"]},
        )
        assert resp.status_code == 400

    def test_nan_feature_value_becomes_null_and_counted_missing(self, features_dir):
        _write_day(
            features_dir, "2026-06-01",
            [{"ticker": "RELIANCE", "date": "2026-06-01", "rsi_14": None, "adx_14": 20.0}],
        )
        client = TestClient(app)
        resp = client.get(
            "/api/v1/features/RELIANCE",
            params={"start_date": "2026-06-01", "end_date": "2026-06-01"},
        )
        assert resp.status_code == 200
        body = resp.json()
        row = body["data"][0]
        assert row["feature_values"]["rsi_14"] is None
        assert row["missing_feature_count"] == 1

    def test_other_ticker_data_not_returned(self, features_dir):
        _write_day(
            features_dir, "2026-06-01",
            [
                {"ticker": "RELIANCE", "date": "2026-06-01", "rsi_14": 55.0},
                {"ticker": "TCS", "date": "2026-06-01", "rsi_14": 45.0},
            ],
        )
        client = TestClient(app)
        resp = client.get(
            "/api/v1/features/TCS",
            params={"start_date": "2026-06-01", "end_date": "2026-06-01"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["record_count"] == 1
        assert body["data"][0]["ticker"] == "TCS"
        assert body["data"][0]["feature_values"]["rsi_14"] == 45.0
