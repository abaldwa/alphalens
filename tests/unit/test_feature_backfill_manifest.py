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

    fake_universe = SimpleNamespace(get_tickers_for_feature_engineering=lambda: ["AAA", "BBB"])
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
    # **kwargs tolerates step_compute_features/main() growing new forwarded
    # kwargs over time (e.g. panel_workers, staged_panel) without this fake
    # needing to track every one of them — this test only cares about the
    # manifest-writing behavior around success/failure, not the call shape.
    def fake_step_compute_features(d, compute_hmm=True, data_cache=None, **kwargs):
        if d == date(2024, 1, 3):
            raise RuntimeError("simulated feature-build failure")
        return None

    fake_pipeline = SimpleNamespace(step_compute_features=fake_step_compute_features)
    monkeypatch.setitem(sys.modules, "ingestion.scheduler.daily_pipeline", fake_pipeline)

    # enable/disable_bulk_ohlcv_cache are imported by feature_backfill's
    # per-date loop (2026-08-12 bulk OHLCV window cache). The real functions are
    # module-level because step_compute_features builds a fresh DataStoreClient
    # per date, so the cache cannot live on the instance. This fake must expose
    # them or the import fails outright and every test in this file errors —
    # which is exactly what happened when the cache landed.
    fake_client_module = SimpleNamespace(
        DataStoreClient=MagicMock(),
        enable_bulk_ohlcv_cache=MagicMock(return_value=0),
        disable_bulk_ohlcv_cache=MagicMock(),
    )
    monkeypatch.setitem(sys.modules, "datastore.client", fake_client_module)

    fake_cache_module = SimpleNamespace(BackfillDataCache=MagicMock())
    monkeypatch.setitem(sys.modules, "features.backfill_cache", fake_cache_module)

    # main() reads LOOKBACK_CALENDAR_DAYS to size the bulk OHLCV cache window.
    # Faked rather than imported for real: features.matrix_builder pulls a large
    # dependency graph (it needs DELIVERY_PCT_RANGE and more from config.settings,
    # which is itself stubbed above), and this file's contract is that nothing
    # here touches the real DataStore, DuckDB or API. The value only has to be a
    # positive int — the manifest behaviour under test does not depend on it.
    fake_matrix_builder = SimpleNamespace(LOOKBACK_CALENDAR_DAYS=760)
    monkeypatch.setitem(sys.modules, "features.matrix_builder", fake_matrix_builder)

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


def test_force_is_only_honored_once_per_run_id(patched_backfill_env, monkeypatch):
    """--force on the first invocation of a run_id recomputes every date
    (including ones with an existing parquet); a SECOND invocation with the
    SAME run_id and --force again (simulating an auto-restart-supervisor
    that always passes --force) must NOT re-force — it should fall back to
    normal skip-if-exists behavior instead, so an unattended restart loop
    converges instead of redoing already-finished work forever."""
    fb, tmp_path = patched_backfill_env
    features_dir = tmp_path / "features_daily"

    computed_dates = []

    def fake_step_compute_features(d, compute_hmm=True, data_cache=None, **kwargs):
        computed_dates.append(d)
        # Simulate this date's parquet now existing (as the real
        # step_compute_features would produce).
        (features_dir / f"{d.isoformat()}.parquet").write_bytes(b"")
        return None

    fake_pipeline = SimpleNamespace(step_compute_features=fake_step_compute_features)
    monkeypatch.setitem(sys.modules, "ingestion.scheduler.daily_pipeline", fake_pipeline)

    # Pre-seed one date with an existing (stale) parquet so --force's
    # "recompute even if it exists" behavior is actually exercised.
    (features_dir / "2024-01-02.parquet").write_bytes(b"stale")

    argv = [
        "feature_backfill.py", "--from-date", "2024-01-02", "--to-date", "2024-01-04",
        "--run-id", "force_once_test", "--no-hmm", "--force",
    ]
    monkeypatch.setattr(sys, "argv", argv)

    # --- First invocation: --force should be honored, recomputing ALL 3
    # dates (including 2024-01-02, which already had a stale parquet). ---
    fb.main()
    assert sorted(computed_dates) == [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)]
    sentinel = features_dir / ".force_once_test.force_applied"
    assert sentinel.exists()

    # --- Second invocation, SAME run_id, --force passed again (simulating
    # an auto-restart supervisor): must NOT recompute dates that already
    # have a parquet from the first invocation. ---
    computed_dates.clear()
    monkeypatch.setattr(sys, "argv", argv)
    fb.main()
    assert computed_dates == [], (
        f"expected no dates recomputed on restart (all 3 already have parquets from "
        f"attempt 1, --force should be ignored the 2nd time), got {computed_dates}"
    )


def test_no_failures_leaves_no_manifest_entries(patched_backfill_env, monkeypatch):
    fb, tmp_path = patched_backfill_env

    # Redefine the fake pipeline so nothing fails this time.
    fake_pipeline = SimpleNamespace(
        step_compute_features=lambda d, compute_hmm=True, data_cache=None, **kwargs: None
    )
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
