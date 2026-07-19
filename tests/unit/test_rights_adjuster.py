"""
tests/unit/test_rights_adjuster.py

2026-07-19 full-codebase-review Fix 2: ingestion/adjust/rights_adjuster.py
computes RIGHTS-action price adjustment factors from Fyers' own price
series (not a locally recomputed formula — see that module's docstring
for why price_adjuster.py's SPLIT/BONUS-style ratio formula can't work
for rights issues). Real seeded DuckDB (ohlcv_adjusted), a stub Fyers
client (dependency injection per compute_rights_adjustment_factor's
docstring) — no real Fyers auth/network dependency in unit tests.
"""

import pandas as pd
import pytest

from datastore.api.db import close_all_connections, get_duckdb_connection
from datastore.schema import create_normalised
from ingestion.adjust.rights_adjuster import compute_rights_adjustment_factor


class _StubFyersClient:
    """download_history(ticker, from_date, to_date) stub — same signature
    as ingestion.scrapers.fyers_backfill.FYERSBackfill.download_history."""

    def __init__(self, history_df: pd.DataFrame):
        self._df = history_df

    def download_history(self, ticker, from_date, to_date):
        mask = (self._df["date"] >= from_date) & (self._df["date"] <= to_date)
        return self._df[mask].copy()


def _seed_ohlcv(db_path, ticker, rows):
    with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
        for date_str, close in rows:
            conn.execute(
                """
                INSERT INTO ohlcv_adjusted (date, ticker, open, high, low, close, volume, delivery_qty, delivery_pct)
                VALUES (?, ?, ?, ?, ?, ?, 1000, 500, 50.0)
                ON CONFLICT DO NOTHING
                """,
                [date_str, ticker, close, close, close, close],
            )


@pytest.fixture
def normalised_db(tmp_path):
    db_path = tmp_path / "normalised_test.duckdb"
    create_normalised.create_schema(db_path=db_path)
    close_all_connections()
    return db_path


class TestComputeRightsAdjustmentFactor:
    def test_detects_unadjusted_gap_via_fyers_comparison(self, normalised_db):
        """Our series has NO rights adjustment applied (flat 100 before
        and after ex_date), but Fyers' series shows the true 20% dilution
        drop — the ratio (ours/fyers) should jump from ~1.0 pre to ~1.25
        post, giving price_factor ~1.25 (the correction our series needs)."""
        ex_date = "2026-03-10"
        dates_pre = ["2026-03-05", "2026-03-06", "2026-03-07", "2026-03-08", "2026-03-09"]
        dates_post = ["2026-03-10", "2026-03-11", "2026-03-12", "2026-03-13", "2026-03-14"]

        # Our unadjusted series: flat 100 throughout (the documented gap).
        _seed_ohlcv(normalised_db, "RIGHTSCO", [(d, 100.0) for d in dates_pre + dates_post])

        # Fyers' correctly-adjusted series: 100 pre, 80 post (20% dilution).
        fyers_rows = [(d, 100.0) for d in dates_pre] + [(d, 80.0) for d in dates_post]
        fyers_df = pd.DataFrame(fyers_rows, columns=["date", "close"])
        stub = _StubFyersClient(fyers_df)

        with get_duckdb_connection(normalised_db, persist=False, read_only=True) as conn:
            result = compute_rights_adjustment_factor(conn, stub, "RIGHTSCO", ex_date, window_days=10)

        assert result is not None
        assert result.ratio_pre == pytest.approx(1.0, abs=0.01)
        assert result.ratio_post == pytest.approx(1.25, abs=0.01)
        assert result.price_factor == pytest.approx(1.25, abs=0.01)
        assert result.n_pre == 5
        assert result.n_post == 5

    def test_already_adjusted_series_gives_factor_near_one(self, normalised_db):
        """If our series already matches Fyers' adjusted series, the
        implied correction factor should be ~1.0 (no correction needed)."""
        ex_date = "2026-03-10"
        dates = ["2026-03-08", "2026-03-09", "2026-03-10", "2026-03-11"]

        _seed_ohlcv(normalised_db, "OKCO", [(d, 80.0) for d in dates])
        fyers_df = pd.DataFrame([(d, 80.0) for d in dates], columns=["date", "close"])
        stub = _StubFyersClient(fyers_df)

        with get_duckdb_connection(normalised_db, persist=False, read_only=True) as conn:
            result = compute_rights_adjustment_factor(conn, stub, "OKCO", ex_date, window_days=10)

        assert result is not None
        assert result.price_factor == pytest.approx(1.0, abs=0.01)

    def test_returns_none_when_no_data_before_ex_date(self, normalised_db):
        ex_date = "2026-03-10"
        dates = ["2026-03-10", "2026-03-11"]
        _seed_ohlcv(normalised_db, "NEWLIST", [(d, 100.0) for d in dates])
        fyers_df = pd.DataFrame([(d, 100.0) for d in dates], columns=["date", "close"])
        stub = _StubFyersClient(fyers_df)

        with get_duckdb_connection(normalised_db, persist=False, read_only=True) as conn:
            result = compute_rights_adjustment_factor(conn, stub, "NEWLIST", ex_date, window_days=10)

        assert result is None

    def test_returns_none_when_fyers_has_no_data(self, normalised_db):
        ex_date = "2026-03-10"
        _seed_ohlcv(normalised_db, "NODATA", [("2026-03-09", 100.0), ("2026-03-11", 100.0)])
        stub = _StubFyersClient(pd.DataFrame(columns=["date", "close"]))

        with get_duckdb_connection(normalised_db, persist=False, read_only=True) as conn:
            result = compute_rights_adjustment_factor(conn, stub, "NODATA", ex_date, window_days=10)

        assert result is None
