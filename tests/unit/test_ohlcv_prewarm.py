"""
tests/unit/test_ohlcv_prewarm.py

Unit tests for backtest/core/ohlcv_prewarm.py — the shared OHLCV Parquet
snapshot cache batch/queue drivers use to avoid every subprocess job in a
technical sweep independently re-fetching the same GET /ohlcv/_bulk data
(FeatureBacklog A73).
"""

from datetime import date

import pandas as pd

from backtest.core.ohlcv_prewarm import get_or_fetch_ohlcv_bulk, read_snapshot, write_snapshot


class _CountingClient:
    """Records every get_ohlcv_bulk() call — used to assert a snapshot hit
    never re-invokes the underlying (expensive) live fetch."""

    def __init__(self, df: pd.DataFrame):
        self._df = df
        self.calls = []

    def get_ohlcv_bulk(self, from_dt, to_dt):
        self.calls.append((from_dt, to_dt))
        return self._df


def _sample_df() -> pd.DataFrame:
    return pd.DataFrame({
        "date": [pd.Timestamp("2023-01-03"), pd.Timestamp("2023-01-03")],
        "ticker": ["AAA", "BBB"],
        "close": [100.0, 200.0],
        "volume": [1000, 2000],
    })


class TestGetOrFetchOhlcvBulk:
    def test_miss_fetches_and_caches(self, tmp_path):
        client = _CountingClient(_sample_df())
        result = get_or_fetch_ohlcv_bulk(client, date(2023, 1, 1), date(2023, 1, 5), tmp_path)
        assert len(client.calls) == 1
        assert set(result["ticker"]) == {"AAA", "BBB"}

    def test_hit_does_not_refetch(self, tmp_path):
        client = _CountingClient(_sample_df())
        get_or_fetch_ohlcv_bulk(client, date(2023, 1, 1), date(2023, 1, 5), tmp_path)
        get_or_fetch_ohlcv_bulk(client, date(2023, 1, 1), date(2023, 1, 5), tmp_path)
        get_or_fetch_ohlcv_bulk(client, date(2023, 1, 1), date(2023, 1, 5), tmp_path)
        assert len(client.calls) == 1

    def test_hit_from_a_second_client_instance_still_does_not_refetch(self, tmp_path):
        # Simulates a second subprocess job: same snapshot dir, a DIFFERENT
        # client instance — proves the cache genuinely lives on disk, not
        # in any one process's memory.
        client1 = _CountingClient(_sample_df())
        client2 = _CountingClient(_sample_df())
        get_or_fetch_ohlcv_bulk(client1, date(2023, 1, 1), date(2023, 1, 5), tmp_path)
        result = get_or_fetch_ohlcv_bulk(client2, date(2023, 1, 1), date(2023, 1, 5), tmp_path)
        assert client1.calls == [(pd.Timestamp("2023-01-01"), pd.Timestamp("2023-01-05"))]
        assert client2.calls == []
        assert set(result["ticker"]) == {"AAA", "BBB"}

    def test_different_date_ranges_are_cached_independently(self, tmp_path):
        client = _CountingClient(_sample_df())
        get_or_fetch_ohlcv_bulk(client, date(2023, 1, 1), date(2023, 1, 5), tmp_path)
        get_or_fetch_ohlcv_bulk(client, date(2023, 2, 1), date(2023, 2, 5), tmp_path)
        assert len(client.calls) == 2

    def test_snapshot_round_trips_data_identically_to_a_live_fetch(self, tmp_path):
        df = _sample_df()
        client = _CountingClient(df)
        live = get_or_fetch_ohlcv_bulk(client, date(2023, 1, 1), date(2023, 1, 5), tmp_path)
        cached = read_snapshot(date(2023, 1, 1), date(2023, 1, 5), tmp_path)
        pd.testing.assert_frame_equal(live.reset_index(drop=True), cached.reset_index(drop=True))

    def test_missing_snapshot_read_returns_none(self, tmp_path):
        assert read_snapshot(date(2023, 1, 1), date(2023, 1, 5), tmp_path) is None

    def test_corrupt_manifest_is_treated_as_a_miss_not_raised(self, tmp_path):
        write_snapshot(_sample_df(), date(2023, 1, 1), date(2023, 1, 5), tmp_path)
        manifest_path = tmp_path / "2023-01-01_2023-01-05.manifest.json"
        manifest_path.write_text("not valid json")
        assert read_snapshot(date(2023, 1, 1), date(2023, 1, 5), tmp_path) is None
