"""
tests/unit/test_baseline_runner.py

Phase: 0.6 (Data Quality & Observability)
Specs: SPEC-PIPE-005, SPEC-DS-007
Owner: Platform / Ingestion
Consumers: CI, pytest

Unit tests for ingestion/quality/baseline_runner.py, against a file-based
DuckDB instance created via datastore/schema/create_normalised.py.
"""

from datetime import date, timedelta

import pandas as pd
import pytest

from datastore.api.db import get_duckdb_connection
from datastore.schema import create_normalised
from ingestion.quality import baseline_runner


@pytest.fixture
def db_path(tmp_path):
    path = tmp_path / "test.duckdb"
    create_normalised.create_schema(db_path=path)
    return path


def _insert_ohlcv(db_path, ticker, rows):
    """rows: list of (date, close, volume, delivery_pct)."""
    with get_duckdb_connection(db_path) as conn:
        conn.executemany(
            "INSERT INTO ohlcv_adjusted (date, ticker, open, high, low, close, volume, delivery_pct, adj_factor) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (d, ticker, c, c, c, c, v, dp, 1.0)
                for d, c, v, dp in rows
            ],
        )


# ===== load_ohlcv_history =====


def test_load_ohlcv_history_returns_expected_columns_and_rows(db_path):
    rows = [(date(2026, 1, 1) + timedelta(days=i), 100.0 + i, 1000 + i, 50.0) for i in range(5)]
    _insert_ohlcv(db_path, "AAA", rows)

    df = baseline_runner.load_ohlcv_history(db_path=db_path, end_date=date(2026, 1, 10), years=2)

    assert list(df.columns) == ["date", "ticker", "close", "volume", "delivery_pct"]
    assert len(df) == 5
    assert set(df["ticker"]) == {"AAA"}


def test_load_ohlcv_history_filters_to_window(db_path):
    in_window = date(2026, 1, 1)
    out_of_window = date(2020, 1, 1)
    _insert_ohlcv(db_path, "AAA", [(in_window, 100.0, 1000, 50.0), (out_of_window, 90.0, 900, 40.0)])

    df = baseline_runner.load_ohlcv_history(db_path=db_path, end_date=date(2026, 1, 10), years=2)

    assert len(df) == 1
    assert pd.Timestamp(df.iloc[0]["date"]) == pd.Timestamp(in_window)


def test_load_ohlcv_history_raises_file_not_found_when_empty(db_path):
    """SPEC-PIPE-005: an empty ohlcv_adjusted table must raise a clear, actionable error."""
    with pytest.raises(FileNotFoundError, match="Run the OHLCV backfill first"):
        baseline_runner.load_ohlcv_history(db_path=db_path, end_date=date(2026, 1, 10), years=2)


# ===== _derive_baseline_features =====


def test_derive_baseline_features_computes_return_1d():
    ohlcv = pd.DataFrame(
        {
            "date": [date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3)],
            "ticker": ["AAA", "AAA", "AAA"],
            "close": [100.0, 110.0, 99.0],
            "volume": [1000, 1100, 900],
            "delivery_pct": [50.0, 55.0, 45.0],
        }
    )

    result = baseline_runner._derive_baseline_features(ohlcv)

    assert list(result.columns) == ["return_1d", "volume", "delivery_pct"]
    # First row per ticker has no prior close -> dropped (return_1d undefined).
    assert len(result) == 2
    assert result["return_1d"].iloc[0] == pytest.approx(0.10)  # (110-100)/100
    assert result["return_1d"].iloc[1] == pytest.approx((99.0 - 110.0) / 110.0)


def test_derive_baseline_features_handles_multiple_tickers_independently():
    ohlcv = pd.DataFrame(
        {
            "date": [date(2026, 1, 1), date(2026, 1, 2)] * 2,
            "ticker": ["AAA", "AAA", "BBB", "BBB"],
            "close": [100.0, 105.0, 50.0, 45.0],
            "volume": [1000, 1000, 500, 500],
            "delivery_pct": [50.0, 50.0, 30.0, 30.0],
        }
    )

    result = baseline_runner._derive_baseline_features(ohlcv)

    # One return_1d row survives per ticker (the second observation).
    assert len(result) == 2
    assert sorted(result["return_1d"].round(4).tolist()) == [-0.1, 0.05]


# ===== run() =====


def test_run_computes_and_persists_baseline(monkeypatch, db_path, tmp_path):
    rows = [(date(2026, 1, 1) + timedelta(days=i), 100.0 + i, 1000 + i, 50.0) for i in range(10)]
    _insert_ohlcv(db_path, "AAA", rows)

    baseline_pkl = tmp_path / "stats_baseline.pkl"
    monkeypatch.setattr("ingestion.quality.drift_monitor.PSI_BASELINE_PATH", baseline_pkl)

    result = baseline_runner.run(db_path=db_path, end_date=date(2026, 1, 20), years=2)

    assert set(result.keys()) == {"return_1d", "volume", "delivery_pct"}
    assert baseline_pkl.exists()
