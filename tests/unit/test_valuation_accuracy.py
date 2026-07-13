"""
tests/unit/test_valuation_accuracy.py

F6 — Valuation Accuracy backtest endpoint
(GET /api/v1/valuation/accuracy/backtest), datastore/api/routers/valuation.py.

Real seeded DuckDB fixtures (normalised alphalens.duckdb for ohlcv_adjusted,
signals.duckdb for valuation_signals) via TestClient(app) — no mocks, per
this repo's no-stub/synthetic-data testing policy.
"""

from datetime import date, timedelta

import pytest
from fastapi.testclient import TestClient

from datastore.api.db import close_all_connections, get_duckdb_connection
from datastore.api.main import app
from datastore.api.routers import valuation as valuation_router
from datastore.schema import create_normalised, create_signals


@pytest.fixture
def client(tmp_path, monkeypatch):
    normalised_path = tmp_path / "normalised_test.duckdb"
    signals_path = tmp_path / "signals_test.duckdb"
    create_normalised.create_schema(db_path=normalised_path)
    create_signals.create_signal_tables_schema(db_path=signals_path)
    close_all_connections()

    monkeypatch.setattr(valuation_router, "DUCKDB_PATH", normalised_path)
    monkeypatch.setattr(valuation_router, "SIGNALS_DUCKDB_PATH", signals_path)
    return TestClient(app)


def _seed_ohlcv(db_path, ticker, rows):
    """rows: list of (date_str, close)."""
    with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
        for d, close in rows:
            conn.execute(
                """
                INSERT INTO ohlcv_adjusted (ticker, date, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT DO NOTHING
                """,
                [ticker, d, close, close, close, close, 1000],
            )


def _seed_valuation_signal(db_path, ticker, sig_date, mos, gap_pct=None, intrinsic=None):
    with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS valuation_signals (
                date DATE NOT NULL, ticker VARCHAR NOT NULL, lifecycle_stage VARCHAR,
                intrinsic_value FLOAT, valuation_gap_pct FLOAT, margin_of_safety FLOAT,
                wacc FLOAT, cost_of_equity FLOAT, terminal_value_pct FLOAT,
                dcf_model_type VARCHAR, scenario_bull FLOAT, scenario_base FLOAT,
                scenario_bear FLOAT, mc_probability_undervalued FLOAT, relative_pe_gap FLOAT,
                PRIMARY KEY (date, ticker)
            )
            """
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO valuation_signals
            (date, ticker, lifecycle_stage, intrinsic_value, valuation_gap_pct, margin_of_safety)
            VALUES (?, ?, 'mature', ?, ?, ?)
            """,
            [sig_date, ticker, intrinsic, gap_pct, mos],
        )


class TestValuationAccuracyBacktest:
    def test_no_valuation_signals_returns_empty(self, client):
        r = client.get("/api/v1/valuation/accuracy/backtest")
        assert r.status_code == 200
        body = r.json()
        assert body["count"] == 0
        assert body["hit_rate"] is None

    def test_undervalued_call_that_went_up_is_a_hit(self, client, monkeypatch):
        base = date(2026, 6, 1)
        sig_date = base.isoformat()
        entry_date = (base - timedelta(days=1)).isoformat()
        realized_date = (base + timedelta(days=5)).isoformat()

        _seed_ohlcv(
            valuation_router.DUCKDB_PATH, "UPCO",
            [(entry_date, 100.0), (realized_date, 120.0)],
        )
        _seed_valuation_signal(valuation_router.SIGNALS_DUCKDB_PATH, "UPCO", sig_date, mos=0.25)

        # min_age_days=0 so "today" doesn't gate out this synthetic-but-real-shape fixture date.
        r = client.get(
            "/api/v1/valuation/accuracy/backtest",
            params={"horizon_days": 5, "min_age_days": 0},
        )
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["scored"] == 1
        row = body["rows"][0]
        assert row["ticker"] == "UPCO"
        assert row["predicted_undervalued"] is True
        assert row["hit"] is True
        assert row["realized_return_pct"] == pytest.approx(20.0, abs=0.01)

    def test_overvalued_call_that_went_up_is_a_miss(self, client):
        base = date(2026, 6, 1)
        sig_date = base.isoformat()
        entry_date = (base - timedelta(days=1)).isoformat()
        realized_date = (base + timedelta(days=5)).isoformat()

        _seed_ohlcv(
            valuation_router.DUCKDB_PATH, "DOWNCO",
            [(entry_date, 100.0), (realized_date, 110.0)],
        )
        _seed_valuation_signal(valuation_router.SIGNALS_DUCKDB_PATH, "DOWNCO", sig_date, mos=-0.15)

        r = client.get(
            "/api/v1/valuation/accuracy/backtest",
            params={"horizon_days": 5, "min_age_days": 0},
        )
        body = r.json()
        row = [x for x in body["rows"] if x["ticker"] == "DOWNCO"][0]
        assert row["predicted_undervalued"] is False
        assert row["hit"] is False

    def test_no_forward_price_excludes_row_from_scoring(self, client):
        """No fabricated forward price — a signal with no later OHLCV bar
        at all must be excluded from `rows`/`scored`, not guessed at."""
        base = date(2026, 6, 1)
        sig_date = base.isoformat()
        entry_date = (base - timedelta(days=1)).isoformat()

        _seed_ohlcv(valuation_router.DUCKDB_PATH, "NOFWD", [(entry_date, 100.0)])
        _seed_valuation_signal(valuation_router.SIGNALS_DUCKDB_PATH, "NOFWD", sig_date, mos=0.1)

        r = client.get(
            "/api/v1/valuation/accuracy/backtest",
            params={"horizon_days": 5, "min_age_days": 0},
        )
        body = r.json()
        assert all(row["ticker"] != "NOFWD" for row in body["rows"])
