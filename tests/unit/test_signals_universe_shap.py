"""
tests/unit/test_signals_universe_shap.py

Phase: 5 (Dashboard backlog ML23)
Specs: SPEC-DS-004
Owner: Platform / DataStore
Consumers: CI, pytest

ML23: GET /api/v1/signals/ml/universe/{date} now also returns each row's
signal_5d shap_top5_json (already persisted per ML3/ML8) so the dashboard's
Full Universe table can render a short "Basis" summary without a per-ticker
detail fetch. Exercised end-to-end through the real FastAPI app and a real
on-disk DuckDB fixture (no mocks, per the no-stub/synthetic-data policy).
"""

import json
from datetime import date

import pytest
from fastapi.testclient import TestClient

from datastore.api.db import close_all_connections
from datastore.api.main import app
from datastore.api.routers import signals as signals_router
from datastore.schema import create_signals


@pytest.fixture
def client(tmp_path, monkeypatch):
    signals_path = tmp_path / "signals_test.duckdb"
    create_signals.create_signal_tables_schema(db_path=signals_path)
    close_all_connections()
    monkeypatch.setattr(signals_router, "SIGNALS_DUCKDB_PATH", signals_path)
    return TestClient(app)


def test_universe_row_carries_shap_top5_json(client):
    d = str(date(2026, 6, 1))
    shap = json.dumps([{"feature": "sma_50_ratio", "value": 0.12}])
    resp = client.post(
        "/api/v1/signals/ml/write",
        json={
            "date": d,
            "ticker": "TESTTICK",
            "model_name": "signal_5d",
            "model_version": "v1",
            "buy_prob": 0.8,
            "q50_return": 0.05,
            "shap_top5_json": shap,
        },
    )
    assert resp.status_code == 200, resp.text

    resp = client.get(f"/api/v1/signals/ml/universe/{d}")
    assert resp.status_code == 200, resp.text
    rows = resp.json()
    assert len(rows) == 1
    assert rows[0]["ticker"] == "TESTTICK"
    assert rows[0]["shap_top5_json"] == shap


def test_universe_row_shap_null_when_not_written(client):
    d = str(date(2026, 6, 2))
    resp = client.post(
        "/api/v1/signals/ml/write",
        json={
            "date": d,
            "ticker": "NOSHAP",
            "model_name": "signal_5d",
            "model_version": "v1",
            "buy_prob": 0.6,
        },
    )
    assert resp.status_code == 200, resp.text

    resp = client.get(f"/api/v1/signals/ml/universe/{d}")
    assert resp.status_code == 200, resp.text
    rows = resp.json()
    assert len(rows) == 1
    assert rows[0]["shap_top5_json"] is None
