"""
tests/unit/test_build_universe_recompute.py

Coverage for config/build_universe.py's DB-driven recompute passes
(compute_adtv_from_ohlcv, compute_market_cap_from_fundamentals), which
were previously untested (0% coverage). Uses a real seeded DuckDB
(create_normalised schema) and a real universe CSV on disk, per this
project's no-stub/no-synthetic-data policy — no mocked business logic.
"""

from datetime import date, timedelta

import pandas as pd
import pytest

from config.build_universe import (
    OUTPUT_COLUMNS,
    compute_adtv_from_ohlcv,
    compute_market_cap_from_fundamentals,
)
from datastore.api.db import get_duckdb_connection
from datastore.schema import create_normalised


@pytest.fixture
def db_path(tmp_path):
    p = tmp_path / "normalised_test.duckdb"
    create_normalised.create_schema(db_path=p)
    return p


def _write_universe_csv(tmp_path, tickers):
    rows = []
    for t in tickers:
        rows.append(
            {
                "ticker": t,
                "company_name": f"{t} Ltd",
                "sector": "Industrials",
                "tier": 5,
                "market_cap_cr": 0.0,
                "adtv_cr": 0.0,
                "is_fno_eligible": False,
                "is_nifty500": True,
                "isin": f"INE{t}0001",
            }
        )
    df = pd.DataFrame(rows)[OUTPUT_COLUMNS]
    path = tmp_path / "universe.csv"
    df.to_csv(path, index=False)
    return path


def _insert_ohlcv(db_path, ticker, closes):
    with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
        d0 = date(2026, 6, 1)
        for i, close in enumerate(closes):
            conn.execute(
                """
                INSERT INTO ohlcv_adjusted (ticker, date, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT DO NOTHING
                """,
                [ticker, d0 + timedelta(days=i), close, close, close, close, 100000],
            )


def _insert_fundamentals(db_path, ticker, shares_outstanding, announcement_date):
    with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
        conn.execute(
            """
            INSERT INTO fundamentals
                (ticker, announcement_date, fiscal_year, quarter, quarter_end_date, shares_outstanding)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT DO NOTHING
            """,
            [ticker, announcement_date, announcement_date.year, 4, announcement_date, shares_outstanding],
        )


class TestComputeAdtvFromOhlcv:
    def test_missing_universe_csv_raises(self, tmp_path, db_path):
        missing = tmp_path / "does_not_exist.csv"
        with pytest.raises(FileNotFoundError):
            compute_adtv_from_ohlcv(universe_csv_path=missing, db_path=db_path)

    def test_real_ohlcv_history_updates_adtv(self, tmp_path, db_path):
        csv_path = _write_universe_csv(tmp_path, ["RELIANCE", "TCS"])
        _insert_ohlcv(db_path, "RELIANCE", [2500.0] * 25)
        # TCS left with no OHLCV history -> stays at 0.

        result = compute_adtv_from_ohlcv(universe_csv_path=csv_path, db_path=db_path, window_days=20)

        rel_row = result[result["ticker"] == "RELIANCE"].iloc[0]
        tcs_row = result[result["ticker"] == "TCS"].iloc[0]
        expected_adtv = (2500.0 * 100000) / 1e7
        assert rel_row["adtv_cr"] == pytest.approx(expected_adtv)
        assert tcs_row["adtv_cr"] == 0.0

        # File on disk was rewritten in place.
        reread = pd.read_csv(csv_path)
        assert reread.loc[reread["ticker"] == "RELIANCE", "adtv_cr"].iloc[0] == pytest.approx(expected_adtv)

    def test_window_days_limits_to_recent_rows(self, tmp_path, db_path):
        csv_path = _write_universe_csv(tmp_path, ["INFY"])
        # 30 days of cheap prices then last 5 days of expensive prices.
        _insert_ohlcv(db_path, "INFY", [100.0] * 25 + [10000.0] * 5)

        result = compute_adtv_from_ohlcv(universe_csv_path=csv_path, db_path=db_path, window_days=5)
        row = result[result["ticker"] == "INFY"].iloc[0]
        assert row["adtv_cr"] == pytest.approx((10000.0 * 100000) / 1e7)


class TestComputeMarketCapFromFundamentals:
    def test_missing_universe_csv_raises(self, tmp_path, db_path):
        missing = tmp_path / "does_not_exist.csv"
        with pytest.raises(FileNotFoundError):
            compute_market_cap_from_fundamentals(universe_csv_path=missing, db_path=db_path)

    def test_real_fundamentals_and_close_join_computes_market_cap(self, tmp_path, db_path):
        csv_path = _write_universe_csv(tmp_path, ["HDFCBANK", "WIPRO"])
        _insert_ohlcv(db_path, "HDFCBANK", [1500.0])
        _insert_fundamentals(db_path, "HDFCBANK", 550_00_00_000, date(2026, 3, 31))
        # WIPRO has no fundamentals row -> stays at 0.

        result = compute_market_cap_from_fundamentals(universe_csv_path=csv_path, db_path=db_path)

        hdfc_row = result[result["ticker"] == "HDFCBANK"].iloc[0]
        wipro_row = result[result["ticker"] == "WIPRO"].iloc[0]
        expected = (550_00_00_000 * 1500.0) / 1e7
        assert hdfc_row["market_cap_cr"] == pytest.approx(expected)
        assert wipro_row["market_cap_cr"] == 0.0

    def test_uses_latest_announcement_date_when_multiple_rows(self, tmp_path, db_path):
        csv_path = _write_universe_csv(tmp_path, ["ITC"])
        _insert_ohlcv(db_path, "ITC", [400.0])
        _insert_fundamentals(db_path, "ITC", 1_000_000_000, date(2025, 12, 31))
        _insert_fundamentals(db_path, "ITC", 1_200_000_000, date(2026, 3, 31))

        result = compute_market_cap_from_fundamentals(universe_csv_path=csv_path, db_path=db_path)
        row = result[result["ticker"] == "ITC"].iloc[0]
        # Latest announcement (1.2B shares) should win, not the older 1.0B row.
        assert row["market_cap_cr"] == pytest.approx((1_200_000_000 * 400.0) / 1e7)
