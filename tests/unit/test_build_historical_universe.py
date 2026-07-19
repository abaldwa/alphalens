"""
tests/unit/test_build_historical_universe.py

2026-07-19 full-codebase-review Fix A4: config.build_universe.
build_historical_universe_from_delisted() unions today's active universe
with delisted_companies, closing momentum_universe.py's survivorship-bias
gap. Real seeded DuckDB + real universe CSV on disk, no mocks over the
DB/CSV layer (per this project's no-stub/no-synthetic-data policy).
"""

from datetime import date

import pandas as pd
import pytest

from config.build_universe import OUTPUT_COLUMNS, build_historical_universe_from_delisted
from datastore.api.db import get_duckdb_connection
from datastore.schema import create_normalised


@pytest.fixture
def db_path(tmp_path):
    p = tmp_path / "normalised_test.duckdb"
    create_normalised.create_schema(db_path=p)
    return p


def _write_universe_csv(tmp_path, tickers, monkeypatch):
    rows = [{
        "ticker": t, "company_name": f"{t} Ltd", "sector": "Industrials", "tier": 5,
        "market_cap_cr": 0.0, "adtv_cr": 0.0, "is_fno_eligible": False,
        "is_nifty500": True, "isin": f"INE{t}0001",
    } for t in tickers]
    path = tmp_path / "universe.csv"
    pd.DataFrame(rows)[OUTPUT_COLUMNS].to_csv(path, index=False)
    monkeypatch.setattr("config.universe.UNIVERSE_CSV_PATH", path)
    return path


def _insert_delisted(db_path, ticker, delisting_date):
    with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
        conn.execute(
            """
            INSERT INTO delisted_companies (ticker, company_name, delisting_date, delisting_type, source_url)
            VALUES (?, ?, ?, 'Compulsory', 'https://example.com')
            ON CONFLICT DO NOTHING
            """,
            [ticker, f"{ticker} Ltd", delisting_date],
        )


class TestBuildHistoricalUniverseFromDelisted:
    def test_unions_active_and_delisted_tickers(self, tmp_path, db_path, monkeypatch):
        _write_universe_csv(tmp_path, ["ACTIVE1", "ACTIVE2"], monkeypatch)
        _insert_delisted(db_path, "DELISTED1", date(2018, 5, 1))

        result = build_historical_universe_from_delisted(db_path=db_path)

        assert set(result) == {"ACTIVE1", "ACTIVE2", "DELISTED1"}

    def test_no_delisted_table_data_falls_back_to_active_only(self, tmp_path, db_path, monkeypatch):
        _write_universe_csv(tmp_path, ["ACTIVE1"], monkeypatch)
        # delisted_companies table exists (schema created) but empty.

        result = build_historical_universe_from_delisted(db_path=db_path)

        assert result == ["ACTIVE1"]

    def test_include_since_year_filters_older_delistings(self, tmp_path, db_path, monkeypatch):
        _write_universe_csv(tmp_path, ["ACTIVE1"], monkeypatch)
        _insert_delisted(db_path, "OLDDELIST", date(2010, 1, 1))
        _insert_delisted(db_path, "RECENTDELIST", date(2022, 1, 1))

        result = build_historical_universe_from_delisted(db_path=db_path, include_since_year=2016)

        assert "RECENTDELIST" in result
        assert "OLDDELIST" not in result

    def test_ticker_already_active_not_duplicated(self, tmp_path, db_path, monkeypatch):
        _write_universe_csv(tmp_path, ["RELISTED"], monkeypatch)
        _insert_delisted(db_path, "RELISTED", date(2015, 1, 1))

        result = build_historical_universe_from_delisted(db_path=db_path)

        assert result.count("RELISTED") == 1

    def test_missing_db_falls_back_to_active_only_not_crash(self, tmp_path, monkeypatch):
        _write_universe_csv(tmp_path, ["ACTIVE1"], monkeypatch)
        nonexistent_db = tmp_path / "does_not_exist.duckdb"

        result = build_historical_universe_from_delisted(db_path=nonexistent_db)

        assert result == ["ACTIVE1"]
