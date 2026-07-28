"""
tests/unit/test_feature_backfill_manifest.py

[BUG FIX, 2026-07-28 model-review item 6] scripts/feature_backfill.py's
per-date loop used to catch and log a failure with no other record — for a
20-80 hour unattended run, a human had no way to find exactly which dates
failed without re-scanning a potentially enormous log file (and this
project has real precedent for a backfill silently failing almost every
date, only discovered after the fact). This test exercises main() end to
end with every external dependency faked, asserting the failed-dates
manifest file is written incrementally and contains exactly the dates that
raised.
"""
import sys
from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def patched_backfill_env(monkeypatch, tmp_path):
    """Fakes every external dependency scripts/feature_backfill.py's main()
    imports, so the test never touches the real DataStore/DuckDB/API."""
    import scripts.feature_backfill as fb

    features_dir = tmp_path / "features_daily"
    features_dir.mkdir()
    logs_dir = tmp_path / "logs"

    fake_settings = SimpleNamespace(
        DUCKDB_PATH=tmp_path / "fake.duckdb", FEATURES_DAILY_DIR=features_dir, LOGS_DIR=logs_dir,
    )
    monkeypatch.setitem(sys.modules, "config.settings", fake_settings)

    fake_universe = SimpleNamespace(get_tickers=lambda: ["AAA", "BBB"])
    monkeypatch.setitem(sys.modules, "config.universe", fake_universe)

    class _FakeConn:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, *_a, **_kw):
            return self

        def fetchall(self):
            return [(d.isoformat(),) for d in self._dates]

    fake_conn = _FakeConn()
    fake_conn._dates = [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)]
    fake_db = SimpleNamespace(get_duckdb_connection=lambda *a, **kw: fake_conn)
    monkeypatch.setitem(sys.modules, "datastore.api.db", fake_db)

    # step_compute_features fails for 2024-01-03, succeeds otherwise.
    def fake_step_compute_features(d, compute_hmm=True, data_cache=None):
        if d == date(2024, 1, 3):
            raise RuntimeError("simulated feature-build failure")
        return None

    fake_pipeline = SimpleNamespace(step_compute_features=fake_step_compute_features)
    monkeypatch.setitem(sys.modules, "ingestion.scheduler.daily_pipeline", fake_pipeline)

    fake_client_module = SimpleNamespace(DataStoreClient=MagicMock())
    monkeypatch.setitem(sys.modules, "datastore.client", fake_client_module)

    fake_cache_module = SimpleNamespace(BackfillDataCache=MagicMock())
    monkeypatch.setitem(sys.modules, "features.backfill_cache", fake_cache_module)

    monkeypatch.setattr(fb, "logger", fb.logging.getLogger("test_feature_backfill"))

    return fb, tmp_path


def test_failed_date_is_written_to_the_manifest(patched_backfill_env, monkeypatch):
    fb, tmp_path = patched_backfill_env
    monkeypatch.setattr(
        sys, "argv",
        ["feature_backfill.py", "--from-date", "2024-01-02", "--to-date", "2024-01-04",
         "--run-id", "unittest123", "--no-hmm"],
    )
    manifest_path = tmp_path / "logs" / "feature_backfill_failed_unittest123.txt"
    fb.main()
    assert manifest_path.exists()
    failed_dates = manifest_path.read_text().splitlines()
    assert failed_dates == ["2024-01-03"]


def test_no_failures_leaves_no_manifest_entries(patched_backfill_env, monkeypatch):
    fb, tmp_path = patched_backfill_env

    # Redefine the fake pipeline so nothing fails this time.
    fake_pipeline = SimpleNamespace(step_compute_features=lambda d, compute_hmm=True, data_cache=None: None)
    monkeypatch.setitem(sys.modules, "ingestion.scheduler.daily_pipeline", fake_pipeline)

    monkeypatch.setattr(
        sys, "argv",
        ["feature_backfill.py", "--from-date", "2024-01-02", "--to-date", "2024-01-04",
         "--run-id", "unittest456", "--no-hmm"],
    )
    manifest_path = tmp_path / "logs" / "feature_backfill_failed_unittest456.txt"
    fb.main()
    # A manifest that's never appended to is fine to not exist at all.
    if manifest_path.exists():
        assert manifest_path.read_text() == ""
