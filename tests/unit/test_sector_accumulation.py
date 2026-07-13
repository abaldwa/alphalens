"""
tests/unit/test_sector_accumulation.py

ML29 — features/sector_accumulation.py, datastore/api/routers/sector_accumulation.py.

Real seeded DuckDB fixtures (normalised alphalens.duckdb: ohlcv_adjusted +
fundamentals) via TestClient(app) — no mocks over the DB layer, per this
repo's no-stub/synthetic-data testing policy. config.universe.load_universe()
is monkeypatched to a small controlled DataFrame (not a DB access) so
sector membership is deterministic, matching test_sector_rotation.py's
existing convention.
"""

from datetime import date

import pandas as pd
import pytest
from fastapi.testclient import TestClient

import features.sector_accumulation as sector_accum_mod
from datastore.api.db import close_all_connections, get_duckdb_connection
from datastore.api.main import app
from datastore.api.routers import sector_accumulation as sector_accum_router
from datastore.schema import create_normalised


def _seed_ohlcv(db_path, ticker, rows):
    """rows: list of (date_str, volume, delivery_pct)."""
    with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
        for d, volume, delivery_pct in rows:
            conn.execute(
                """
                INSERT INTO ohlcv_adjusted (date, ticker, open, high, low, close, volume, delivery_qty, delivery_pct)
                VALUES (?, ?, 100, 100, 100, 100, ?, ?, ?)
                ON CONFLICT DO NOTHING
                """,
                [d, ticker, volume, int(volume * delivery_pct / 100), delivery_pct],
            )


def _seed_fundamentals(db_path, ticker, announcement_date, shares_outstanding):
    with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
        conn.execute(
            """
            INSERT INTO fundamentals (ticker, fiscal_year, quarter, quarter_end_date, announcement_date, shares_outstanding)
            VALUES (?, 2026, 1, ?, ?, ?)
            """,
            [ticker, announcement_date, announcement_date, shares_outstanding],
        )


@pytest.fixture
def normalised_db(tmp_path):
    db_path = tmp_path / "normalised_test.duckdb"
    create_normalised.create_schema(db_path=db_path)
    close_all_connections()
    return db_path


class TestComputeSectorAccumulation:
    def test_no_data_returns_empty(self, normalised_db, monkeypatch):
        universe = pd.DataFrame({"ticker": ["AAA"], "sector": ["Information Technology"]})
        monkeypatch.setattr(sector_accum_mod, "load_universe", lambda: universe)
        with get_duckdb_connection(normalised_db, persist=False, read_only=True) as conn:
            result = sector_accum_mod.compute_sector_accumulation(conn)
        assert result.empty

    def test_sector_shares_outstanding_is_simple_sum(self, normalised_db, monkeypatch):
        """2026-07-13 user decision: sector total outstanding shares = simple
        sum of each constituent's own shares_outstanding (not weighted)."""
        universe = pd.DataFrame({"ticker": ["AAA", "BBB"], "sector": ["Information Technology"] * 2})
        monkeypatch.setattr(sector_accum_mod, "load_universe", lambda: universe)

        d = date(2026, 1, 10).isoformat()
        _seed_fundamentals(normalised_db, "AAA", "2025-12-01", 100_000)
        _seed_fundamentals(normalised_db, "BBB", "2025-12-01", 200_000)
        _seed_ohlcv(normalised_db, "AAA", [(d, 10_000, 50.0)])
        _seed_ohlcv(normalised_db, "BBB", [(d, 20_000, 25.0)])

        with get_duckdb_connection(normalised_db, persist=False, read_only=True) as conn:
            result = sector_accum_mod.compute_sector_accumulation(conn, start_date=d, end_date=d)

        assert len(result) == 1
        row = result.iloc[0]
        assert row["sector"] == "Information Technology"
        assert row["sector_shares_outstanding"] == pytest.approx(300_000)
        expected_delivery_volume = (0.5 * 10_000) + (0.25 * 20_000)  # 5000 + 5000 = 10000
        assert row["delivery_volume"] == pytest.approx(expected_delivery_volume)
        assert row["accumulation_score"] == pytest.approx(expected_delivery_volume / 300_000)
        assert row["n_stocks_included"] == 2

    def test_pit_correctness_uses_announcement_date_not_future_fundamentals(self, normalised_db, monkeypatch):
        """A fundamentals row announced AFTER the ohlcv date must not be
        used for that date's shares_outstanding (no look-ahead)."""
        universe = pd.DataFrame({"ticker": ["AAA"], "sector": ["Information Technology"]})
        monkeypatch.setattr(sector_accum_mod, "load_universe", lambda: universe)

        early_d = date(2026, 1, 5).isoformat()
        _seed_fundamentals(normalised_db, "AAA", "2026-02-01", 999_999)  # announced AFTER early_d
        _seed_ohlcv(normalised_db, "AAA", [(early_d, 10_000, 50.0)])

        with get_duckdb_connection(normalised_db, persist=False, read_only=True) as conn:
            result = sector_accum_mod.compute_sector_accumulation(conn, start_date=early_d, end_date=early_d)

        # No PIT-eligible shares_outstanding as of early_d -> stock excluded,
        # sector has zero fully-known constituents that day -> no row.
        assert result.empty

    def test_stock_missing_shares_outstanding_excluded_no_guess(self, normalised_db, monkeypatch):
        universe = pd.DataFrame({"ticker": ["AAA", "BBB"], "sector": ["Information Technology"] * 2})
        monkeypatch.setattr(sector_accum_mod, "load_universe", lambda: universe)

        d = date(2026, 1, 10).isoformat()
        _seed_fundamentals(normalised_db, "AAA", "2025-12-01", 100_000)
        # BBB has no fundamentals row at all.
        _seed_ohlcv(normalised_db, "AAA", [(d, 10_000, 50.0)])
        _seed_ohlcv(normalised_db, "BBB", [(d, 20_000, 25.0)])

        with get_duckdb_connection(normalised_db, persist=False, read_only=True) as conn:
            result = sector_accum_mod.compute_sector_accumulation(conn, start_date=d, end_date=d)

        assert len(result) == 1
        row = result.iloc[0]
        assert row["n_stocks_included"] == 1
        assert row["sector_shares_outstanding"] == pytest.approx(100_000)


class TestSectorAccumulationDrilldown:
    def test_drilldown_breaks_down_by_stock(self, normalised_db, monkeypatch):
        universe = pd.DataFrame({"ticker": ["AAA", "BBB"], "sector": ["Information Technology"] * 2})
        monkeypatch.setattr(sector_accum_mod, "load_universe", lambda: universe)

        d = date(2026, 1, 10).isoformat()
        _seed_fundamentals(normalised_db, "AAA", "2025-12-01", 100_000)
        _seed_fundamentals(normalised_db, "BBB", "2025-12-01", 200_000)
        _seed_ohlcv(normalised_db, "AAA", [(d, 10_000, 50.0)])
        _seed_ohlcv(normalised_db, "BBB", [(d, 20_000, 25.0)])

        with get_duckdb_connection(normalised_db, persist=False, read_only=True) as conn:
            drilldown = sector_accum_mod.sector_accumulation_drilldown(conn, "Information Technology", d)

        assert len(drilldown) == 2
        assert set(drilldown["ticker"]) == {"AAA", "BBB"}
        assert drilldown["contribution_pct"].sum() == pytest.approx(100.0)

    def test_empty_sector_returns_empty(self, normalised_db, monkeypatch):
        universe = pd.DataFrame({"ticker": [], "sector": []})
        monkeypatch.setattr(sector_accum_mod, "load_universe", lambda: universe)
        with get_duckdb_connection(normalised_db, persist=False, read_only=True) as conn:
            drilldown = sector_accum_mod.sector_accumulation_drilldown(conn, "Information Technology", "2026-01-10")
        assert drilldown.empty


# ===== datastore/api/routers/sector_accumulation.py =====
@pytest.fixture
def client(tmp_path, monkeypatch):
    normalised_path = tmp_path / "normalised_test.duckdb"
    create_normalised.create_schema(db_path=normalised_path)
    close_all_connections()

    monkeypatch.setattr(sector_accum_router, "DUCKDB_PATH", normalised_path)
    return TestClient(app), normalised_path


class TestSectorAccumulationEndpoints:
    def test_daily_endpoint_real_data(self, client, monkeypatch):
        test_client, db_path = client
        universe = pd.DataFrame({"ticker": ["AAA", "BBB"], "sector": ["Information Technology"] * 2})
        monkeypatch.setattr(sector_accum_mod, "load_universe", lambda: universe)

        d = date(2026, 1, 10).isoformat()
        _seed_fundamentals(db_path, "AAA", "2025-12-01", 100_000)
        _seed_fundamentals(db_path, "BBB", "2025-12-01", 200_000)
        _seed_ohlcv(db_path, "AAA", [(d, 10_000, 50.0)])
        _seed_ohlcv(db_path, "BBB", [(d, 20_000, 25.0)])

        resp = test_client.get("/api/v1/sector_accumulation/daily", params={"start_date": d, "end_date": d})
        assert resp.status_code == 200
        rows = resp.json()
        assert len(rows) == 1
        assert rows[0]["sector"] == "Information Technology"
        assert rows[0]["sector_shares_outstanding"] == pytest.approx(300_000)

    def test_daily_endpoint_no_data(self, client, monkeypatch):
        test_client, _ = client
        universe = pd.DataFrame({"ticker": ["AAA"], "sector": ["Information Technology"]})
        monkeypatch.setattr(sector_accum_mod, "load_universe", lambda: universe)
        resp = test_client.get("/api/v1/sector_accumulation/daily")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_drilldown_endpoint(self, client, monkeypatch):
        test_client, db_path = client
        universe = pd.DataFrame({"ticker": ["AAA"], "sector": ["Information Technology"]})
        monkeypatch.setattr(sector_accum_mod, "load_universe", lambda: universe)

        d = date(2026, 1, 10).isoformat()
        _seed_fundamentals(db_path, "AAA", "2025-12-01", 100_000)
        _seed_ohlcv(db_path, "AAA", [(d, 10_000, 50.0)])

        resp = test_client.get("/api/v1/sector_accumulation/drilldown", params={"sector": "Information Technology", "date": d})
        assert resp.status_code == 200
        rows = resp.json()
        assert len(rows) == 1
        assert rows[0]["ticker"] == "AAA"
