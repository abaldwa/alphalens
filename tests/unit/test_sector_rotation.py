"""
tests/unit/test_sector_rotation.py

ML12 steps 4-6 — config/sector_index_map.py, features/sector_rotation.py,
datastore/api/routers/sector_rotation.py.

Real seeded DuckDB fixtures (normalised alphalens.duckdb for index_ohlcv,
signals.duckdb for ml_signals/ml_multibagger) via TestClient(app) — no
mocks over the DB layer, per this repo's no-stub/synthetic-data testing
policy. config.universe.load_universe() is monkeypatched to a small
controlled DataFrame (not a DB access) so sector-membership is
deterministic in the test without depending on the real, large
nifty500_universe.csv contents.
"""

from datetime import date, timedelta

import pandas as pd
import pytest
from fastapi.testclient import TestClient

import features.sector_rotation as sector_rotation_mod
from config.sector_index_map import (
    EXPLICITLY_EXCLUDED_SECTORS,
    SECTOR_INDEX_MAP,
    get_index_for_sector,
    sectors_for_index,
)
from config.universe import load_universe_raw
from datastore.api.db import close_all_connections, get_duckdb_connection
from datastore.api.main import app
from datastore.api.routers import sector_rotation as sector_rotation_router
from datastore.schema import create_normalised, create_signals


# ===== config/sector_index_map.py =====
class TestSectorIndexMap:
    def test_mapped_and_excluded_sectors_dont_overlap(self):
        assert set(SECTOR_INDEX_MAP.keys()).isdisjoint(EXPLICITLY_EXCLUDED_SECTORS)

    def test_real_universe_sectors_all_accounted_for(self):
        """Every distinct sector value in the real universe CSV is either
        mapped to a real index or explicitly excluded — nothing silently
        falls through the cracks."""
        raw = load_universe_raw()
        real_sectors = set(raw["sector"].dropna().unique()) - {""}
        accounted = set(SECTOR_INDEX_MAP.keys()) | set(EXPLICITLY_EXCLUDED_SECTORS)
        missing = real_sectors - accounted
        assert not missing, f"sector(s) not accounted for in sector_index_map.py: {missing}"

    def test_get_index_for_sector(self):
        assert get_index_for_sector("Information Technology") == "Nifty IT"
        assert get_index_for_sector("Power") is None
        assert get_index_for_sector("NotARealSector") is None

    def test_sectors_for_index_reverse_lookup(self):
        oil_gas_sectors = sectors_for_index("Nifty Oil & Gas")
        assert "Oil Gas & Consumable Fuels" in oil_gas_sectors
        assert "Oil, Gas & Consumable Fuels" in oil_gas_sectors

    def test_only_about_eight_distinct_semantic_sectors_mapped(self):
        # Punctuation-variant duplicates collapse to the same index.
        distinct_indices = set(SECTOR_INDEX_MAP.values())
        assert 5 <= len(distinct_indices) <= 10


# ===== features/sector_rotation.py =====
def _seed_index_ohlcv(db_path, index_name, closes_by_date):
    with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
        for d, close in closes_by_date.items():
            conn.execute(
                """
                INSERT INTO index_ohlcv (date, index_name, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT DO NOTHING
                """,
                [d, index_name, close, close, close, close, 1_000_000],
            )


def _trading_days(start: date, n: int):
    return [(start + timedelta(days=i)).isoformat() for i in range(n)]


@pytest.fixture
def normalised_db(tmp_path):
    db_path = tmp_path / "normalised_test.duckdb"
    create_normalised.create_schema(db_path=db_path)
    close_all_connections()
    return db_path


@pytest.fixture
def signals_db(tmp_path):
    db_path = tmp_path / "signals_test.duckdb"
    create_signals.create_signal_tables_schema(db_path=db_path)
    close_all_connections()
    return db_path


class TestComputeIndexRelativeStrength:
    def test_no_data_returns_empty(self, normalised_db):
        with get_duckdb_connection(normalised_db, persist=False, read_only=True) as conn:
            result = sector_rotation_mod.compute_index_relative_strength(conn)
        assert result.empty

    def test_it_outperforms_nifty500_ranks_first(self, normalised_db):
        days = _trading_days(date(2026, 1, 1), 25)
        # Nifty 500 flat (1000 every day); Nifty IT rallies +10% over the window.
        nifty500_closes = {d: 1000.0 for d in days}
        it_closes = {d: 1000.0 + i * 4.0 for i, d in enumerate(days)}  # steady climb
        fmcg_closes = {d: 1000.0 - i * 1.0 for i, d in enumerate(days)}  # steady decline

        _seed_index_ohlcv(normalised_db, "Nifty 500", nifty500_closes)
        _seed_index_ohlcv(normalised_db, "Nifty IT", it_closes)
        _seed_index_ohlcv(normalised_db, "Nifty FMCG", fmcg_closes)

        with get_duckdb_connection(normalised_db, persist=False, read_only=True) as conn:
            result = sector_rotation_mod.compute_index_relative_strength(conn)

        assert not result.empty
        it_row = result[result["sector"] == "Information Technology"].iloc[0]
        fmcg_row = result[result["sector"] == "Fast Moving Consumer Goods"].iloc[0]
        assert it_row["relative_strength"] > 0
        assert fmcg_row["relative_strength"] < 0
        assert it_row["rank"] < fmcg_row["rank"]

    def test_insufficient_history_excludes_sector_no_guess(self, normalised_db):
        days = _trading_days(date(2026, 1, 1), 25)
        nifty500_closes = {d: 1000.0 for d in days}
        _seed_index_ohlcv(normalised_db, "Nifty 500", nifty500_closes)
        # Only 5 days of Nifty Auto history — below MIN_INDEX_ROWS (22).
        _seed_index_ohlcv(normalised_db, "Nifty Auto", {d: 900.0 for d in days[:5]})

        with get_duckdb_connection(normalised_db, persist=False, read_only=True) as conn:
            result = sector_rotation_mod.compute_index_relative_strength(conn)

        assert "Automobile and Auto Components" not in result["sector"].tolist()


def _seed_ml_signal(db_path, ticker, sig_date, buy_prob):
    with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO ml_signals
            (date, ticker, model_name, model_version, signal_direction, buy_prob)
            VALUES (?, ?, 'signal_5d', 'v1', 'buy', ?)
            """,
            [sig_date, ticker, buy_prob],
        )


class TestTopStocksForSector:
    def test_ranks_by_buy_prob(self, signals_db, monkeypatch):
        universe = pd.DataFrame(
            {"ticker": ["AAA", "BBB", "CCC"], "sector": ["Information Technology"] * 3}
        )
        monkeypatch.setattr(sector_rotation_mod, "load_universe", lambda: universe)

        _seed_ml_signal(signals_db, "AAA", "2026-07-09", 0.9)
        _seed_ml_signal(signals_db, "BBB", "2026-07-09", 0.5)
        _seed_ml_signal(signals_db, "CCC", "2026-07-09", 0.7)

        with get_duckdb_connection(signals_db, persist=False, read_only=True) as conn:
            top = sector_rotation_mod.top_stocks_for_sector(conn, "Information Technology", top_n=2)

        assert list(top["ticker"]) == ["AAA", "CCC"]

    def test_no_tickers_in_sector_returns_empty(self, signals_db, monkeypatch):
        universe = pd.DataFrame({"ticker": [], "sector": []})
        monkeypatch.setattr(sector_rotation_mod, "load_universe", lambda: universe)
        with get_duckdb_connection(signals_db, persist=False, read_only=True) as conn:
            top = sector_rotation_mod.top_stocks_for_sector(conn, "Information Technology")
        assert top.empty


class TestComputeSectorRotationReport:
    def test_full_report_joins_ranking_and_top_stocks(self, normalised_db, signals_db, monkeypatch):
        days = _trading_days(date(2026, 1, 1), 25)
        _seed_index_ohlcv(normalised_db, "Nifty 500", {d: 1000.0 for d in days})
        _seed_index_ohlcv(normalised_db, "Nifty IT", {d: 1000.0 + i * 4.0 for i, d in enumerate(days)})

        universe = pd.DataFrame({"ticker": ["TCS", "INFY"], "sector": ["Information Technology"] * 2})
        monkeypatch.setattr(sector_rotation_mod, "load_universe", lambda: universe)
        _seed_ml_signal(signals_db, "TCS", days[-1], 0.8)
        _seed_ml_signal(signals_db, "INFY", days[-1], 0.6)

        with get_duckdb_connection(normalised_db, persist=False, read_only=True) as nconn:
            with get_duckdb_connection(signals_db, persist=False, read_only=True) as sconn:
                report = sector_rotation_mod.compute_sector_rotation_report(nconn, sconn)

        assert report["as_of_date"] == days[-1]
        it_sector = [s for s in report["sectors"] if s["sector"] == "Information Technology"][0]
        assert it_sector["rank"] == 1
        assert [s["ticker"] for s in it_sector["top_stocks"]] == ["TCS", "INFY"]


# ===== datastore/api/routers/sector_rotation.py =====
@pytest.fixture
def client(tmp_path, monkeypatch):
    normalised_path = tmp_path / "normalised_test.duckdb"
    signals_path = tmp_path / "signals_test.duckdb"
    create_normalised.create_schema(db_path=normalised_path)
    create_signals.create_signal_tables_schema(db_path=signals_path)
    close_all_connections()

    monkeypatch.setattr(sector_rotation_router, "DUCKDB_PATH", normalised_path)
    monkeypatch.setattr(sector_rotation_router, "SIGNALS_DUCKDB_PATH", signals_path)
    return TestClient(app)


class TestSectorRotationEndpoint:
    def test_no_data_returns_empty_sectors(self, client):
        r = client.get("/api/v1/sector_rotation/report")
        assert r.status_code == 200
        body = r.json()
        assert body["sectors"] == []

    def test_real_seeded_data_round_trips(self, client, monkeypatch):
        days = _trading_days(date(2026, 1, 1), 25)
        _seed_index_ohlcv(sector_rotation_router.DUCKDB_PATH, "Nifty 500", {d: 1000.0 for d in days})
        _seed_index_ohlcv(
            sector_rotation_router.DUCKDB_PATH, "Nifty IT", {d: 1000.0 + i * 4.0 for i, d in enumerate(days)}
        )
        universe = pd.DataFrame({"ticker": ["TCS"], "sector": ["Information Technology"]})
        monkeypatch.setattr(sector_rotation_mod, "load_universe", lambda: universe)
        _seed_ml_signal(sector_rotation_router.SIGNALS_DUCKDB_PATH, "TCS", days[-1], 0.8)

        r = client.get("/api/v1/sector_rotation/report", params={"top_n_stocks": 3})
        assert r.status_code == 200, r.text
        body = r.json()
        assert len(body["sectors"]) == 1
        assert body["sectors"][0]["sector"] == "Information Technology"
        assert body["sectors"][0]["top_stocks"][0]["ticker"] == "TCS"
