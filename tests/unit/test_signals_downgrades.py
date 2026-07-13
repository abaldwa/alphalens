"""
tests/unit/test_signals_downgrades.py

Phase: 5 (Dashboard backlog T12)
Specs: SPEC-DS-004
Owner: Platform / DataStore
Consumers: CI, pytest

T12: GET /api/v1/signals/ml/downgrades/{date} — stocks previously flagged
"buy" by AlphaLens.ML (signal_5d's own `signal_direction` classification)
that have since downgraded to "sell". Read-only aggregation over existing
`ml_signals` history, no new model logic. Exercised end-to-end through the
real FastAPI app and a real on-disk DuckDB fixture (no mocks, per the
no-stub/synthetic-data policy).
"""

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


def _write_signal(client, *, d, ticker, direction, buy_prob, sell_prob=None):
    resp = client.post(
        "/api/v1/signals/ml/write",
        json={
            "date": d,
            "ticker": ticker,
            "model_name": "signal_5d",
            "model_version": "v1",
            "signal_direction": direction,
            "buy_prob": buy_prob,
            "sell_prob": sell_prob if sell_prob is not None else 1.0 - buy_prob,
        },
    )
    assert resp.status_code == 200, resp.text


class TestSignalDowngrades:
    def test_no_rows_returns_empty(self, client):
        r = client.get("/api/v1/signals/ml/downgrades/2026-06-01")
        assert r.status_code == 200
        assert r.json()["count"] == 0

    def test_buy_then_sell_flagged_as_downgrade(self, client):
        _write_signal(client, d="2026-06-01", ticker="DOWNGRADED", direction="buy", buy_prob=0.8)
        _write_signal(client, d="2026-06-05", ticker="DOWNGRADED", direction="sell", buy_prob=0.1, sell_prob=0.85)
        r = client.get("/api/v1/signals/ml/downgrades/2026-06-05")
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["count"] == 1
        row = body["rows"][0]
        assert row["ticker"] == "DOWNGRADED"
        assert row["prior_buy_date"].startswith("2026-06-01")
        assert row["prior_buy_prob"] == pytest.approx(0.8)
        assert row["current_date"].startswith("2026-06-05")
        assert row["current_sell_prob"] == pytest.approx(0.85)

    def test_always_sell_ticker_not_flagged(self, client):
        _write_signal(client, d="2026-06-01", ticker="ALWAYSSELL", direction="sell", buy_prob=0.1)
        _write_signal(client, d="2026-06-05", ticker="ALWAYSSELL", direction="sell", buy_prob=0.1)
        r = client.get("/api/v1/signals/ml/downgrades/2026-06-05")
        assert r.status_code == 200
        assert r.json()["count"] == 0

    def test_buy_then_hold_not_flagged(self, client):
        _write_signal(client, d="2026-06-01", ticker="HOLDER", direction="buy", buy_prob=0.8)
        _write_signal(client, d="2026-06-05", ticker="HOLDER", direction="hold", buy_prob=0.4)
        r = client.get("/api/v1/signals/ml/downgrades/2026-06-05")
        assert r.status_code == 200
        assert r.json()["count"] == 0

    def test_prior_buy_outside_lookback_window_excluded(self, client):
        _write_signal(client, d="2026-01-01", ticker="STALE", direction="buy", buy_prob=0.8)
        _write_signal(client, d="2026-06-05", ticker="STALE", direction="sell", buy_prob=0.1)
        r = client.get(
            "/api/v1/signals/ml/downgrades/2026-06-05", params={"lookback_days": 30}
        )
        assert r.status_code == 200
        assert r.json()["count"] == 0

    def test_carry_forward_resolves_latest_date(self, client):
        _write_signal(client, d="2026-06-01", ticker="CF", direction="buy", buy_prob=0.8)
        _write_signal(client, d="2026-06-05", ticker="CF", direction="sell", buy_prob=0.1)
        r = client.get("/api/v1/signals/ml/downgrades/2026-06-30")
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["date"] == str(date(2026, 6, 5))
        assert body["count"] == 1
