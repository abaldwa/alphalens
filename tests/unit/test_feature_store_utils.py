"""
tests/unit/test_feature_store_utils.py

Coverage for datastore/api/utils/feature_store.py's Parquet read helpers
(previously ~31% covered), using real Parquet files written to a tmp_path
and monkeypatched FEATURES_DAILY_DIR — no mocked business logic, real
pandas/DuckDB reads over real files.
"""

from datetime import datetime

import pandas as pd
import pytest

from datastore.api.utils import feature_store


@pytest.fixture
def features_dir(tmp_path, monkeypatch):
    d = tmp_path / "features_daily"
    d.mkdir()
    monkeypatch.setattr(feature_store, "FEATURES_DAILY_DIR", d)
    return d


def _write_day(features_dir, date_str, rows):
    df = pd.DataFrame(rows)
    df.to_parquet(features_dir / f"{date_str}.parquet")


class TestLatestFeatureDay:
    def test_no_dir_returns_none(self, tmp_path, monkeypatch):
        monkeypatch.setattr(feature_store, "FEATURES_DAILY_DIR", tmp_path / "does_not_exist")
        assert feature_store.latest_feature_day() is None

    def test_no_files_returns_none(self, features_dir):
        assert feature_store.latest_feature_day() is None

    def test_returns_lexicographically_last_date(self, features_dir):
        _write_day(features_dir, "2026-06-01", [{"ticker": "A", "close": 1.0}])
        _write_day(features_dir, "2026-06-15", [{"ticker": "A", "close": 2.0}])
        _write_day(features_dir, "2026-06-02", [{"ticker": "A", "close": 1.5}])
        assert feature_store.latest_feature_day() == "2026-06-15"


class TestReadFeatureDay:
    def test_missing_day_returns_none(self, features_dir):
        assert feature_store.read_feature_day("2026-01-01") is None

    def test_real_file_round_trips(self, features_dir):
        _write_day(features_dir, "2026-06-01", [{"ticker": "A", "close": 10.0}, {"ticker": "B", "close": 20.0}])
        df = feature_store.read_feature_day("2026-06-01")
        assert len(df) == 2
        assert set(df["ticker"]) == {"A", "B"}


class TestReadFeatureRow:
    def test_missing_day_returns_none(self, features_dir):
        assert feature_store.read_feature_row("A", "2026-01-01") is None

    def test_missing_ticker_returns_none(self, features_dir):
        _write_day(features_dir, "2026-06-01", [{"ticker": "A", "close": 10.0}])
        assert feature_store.read_feature_row("ZZZ", "2026-06-01") is None

    def test_real_row_found(self, features_dir):
        _write_day(features_dir, "2026-06-01", [{"ticker": "A", "close": 10.0}, {"ticker": "B", "close": 20.0}])
        row = feature_store.read_feature_row("B", "2026-06-01")
        assert row["close"] == 20.0


class TestResolveDate:
    def test_explicit_date_passed_through(self, features_dir):
        assert feature_store.resolve_date("2026-05-01") == "2026-05-01"

    def test_none_falls_back_to_latest(self, features_dir):
        _write_day(features_dir, "2026-06-10", [{"ticker": "A", "close": 1.0}])
        assert feature_store.resolve_date(None) == "2026-06-10"


class TestReadFeatureRange:
    def test_no_parquet_files_returns_empty_correctly_columned(self, features_dir):
        df = feature_store.read_feature_range("A", datetime(2026, 1, 1), datetime(2026, 12, 31))
        assert list(df.columns) == ["date", "ticker"]
        assert df.empty

    def test_real_range_query_filters_by_ticker_and_date(self, features_dir):
        _write_day(features_dir, "2026-06-01", [
            {"date": "2026-06-01", "ticker": "A", "close": 1.0},
            {"date": "2026-06-01", "ticker": "B", "close": 100.0},
        ])
        _write_day(features_dir, "2026-06-02", [
            {"date": "2026-06-02", "ticker": "A", "close": 1.1},
        ])
        _write_day(features_dir, "2026-07-01", [
            {"date": "2026-07-01", "ticker": "A", "close": 5.0},
        ])

        df = feature_store.read_feature_range("A", datetime(2026, 6, 1), datetime(2026, 6, 30))
        assert len(df) == 2
        assert set(df["date"].astype(str)) == {"2026-06-01", "2026-06-02"}
        assert (df["ticker"] == "A").all()
