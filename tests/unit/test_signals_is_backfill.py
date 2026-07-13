"""
tests/unit/test_signals_is_backfill.py

Phase: 5 (Ops / Backlog A43)
Specs: SPEC-DS-004, SPEC-MODEL-006, SPEC-SCHED-006 (A30 is_backfill)
Owner: Platform / DataStore
Consumers: CI, pytest

A43: GET /api/v1/signals/ml/* rows should carry an is_backfill flag so the
Daily Insights / ML signal dashboard screens can tell an operator whether a
given day's signal was produced live or by a later catch-up run. ml_signals
(DuckDB, Store 4) and pipeline_checkpoints (SQLite, the scheduler's own log)
are different databases with no foreign key, so this is a Python-side join
keyed on (date, step_name='write_signals') via
ingestion.scheduler.checkpoint.CheckpointManager.get_step_is_backfill,
exercised here end-to-end through the real FastAPI app and a real on-disk
DuckDB fixture (no mocks, per the no-stub/synthetic-data policy) plus a real
in-memory SQLite CheckpointManager.
"""

from datetime import date

import pytest
from fastapi.testclient import TestClient

from datastore.api.db import close_all_connections
from datastore.api.main import app
from datastore.api.routers import signals as signals_router
from datastore.schema import create_signals
from ingestion.scheduler.checkpoint import CheckpointManager


@pytest.fixture
def client(tmp_path, monkeypatch):
    signals_path = tmp_path / "signals_test.duckdb"
    create_signals.create_signal_tables_schema(db_path=signals_path)
    close_all_connections()

    monkeypatch.setattr(signals_router, "SIGNALS_DUCKDB_PATH", signals_path)

    cm = CheckpointManager(in_memory=True)
    monkeypatch.setattr(signals_router, "_checkpoint_manager", cm)

    return TestClient(app), cm


def _write_signal(client, d, ticker, model_name="signal_5d", buy_prob=0.5):
    resp = client.post(
        "/api/v1/signals/ml/write",
        json={
            "date": d,
            "ticker": ticker,
            "model_name": model_name,
            "model_version": "v1",
            "buy_prob": buy_prob,
        },
    )
    assert resp.status_code == 200, resp.text


class TestIsBackfillJoin:
    def test_ml_signals_endpoint_reports_live_and_backfilled(self, client):
        http, cm = client
        live_date = date(2026, 7, 3)
        backfilled_date = date(2026, 7, 6)
        cm.save_checkpoint(live_date, "write_signals", "success", is_backfill=False)
        cm.save_checkpoint(backfilled_date, "write_signals", "success", is_backfill=True)

        _write_signal(http, "2026-07-03", "RELIANCE")
        _write_signal(http, "2026-07-06", "RELIANCE")

        row_live = http.get("/api/v1/signals/ml/RELIANCE/2026-07-03").json()[0]
        row_backfilled = http.get("/api/v1/signals/ml/RELIANCE/2026-07-06").json()[0]

        assert row_live["is_backfill"] is False
        assert row_backfilled["is_backfill"] is True

    def test_no_checkpoint_row_yields_none_not_false(self, client):
        http, cm = client
        _write_signal(http, "2026-07-09", "TCS")

        row = http.get("/api/v1/signals/ml/TCS/2026-07-09").json()[0]
        assert row["is_backfill"] is None

    def test_top_buys_endpoint_carries_flag_per_row(self, client):
        http, cm = client
        cm.save_checkpoint(date(2026, 7, 6), "write_signals", "success", is_backfill=True)
        _write_signal(http, "2026-07-06", "HIGHCO", buy_prob=0.9)

        body = http.get("/api/v1/signals/ml/top_buys/2026-07-06").json()
        assert len(body) == 1
        assert body[0]["is_backfill"] is True

    def test_history_endpoint_caches_one_lookup_per_distinct_date(self, client):
        """Two tickers sharing a date should still resolve to the same
        cached is_backfill value without a second checkpoint query per row."""
        http, cm = client
        cm.save_checkpoint(date(2026, 7, 6), "write_signals", "success", is_backfill=True)
        _write_signal(http, "2026-07-06", "TCS")
        _write_signal(http, "2026-07-01", "TCS")

        body = http.get("/api/v1/signals/ml/history/TCS?n=10").json()
        by_date = {row["date"][:10]: row["is_backfill"] for row in body}
        assert by_date["2026-07-06"] is True
        assert by_date["2026-07-01"] is None


def test_close_all_connections_after_module():
    close_all_connections()
