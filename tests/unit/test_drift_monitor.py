"""
tests/unit/test_drift_monitor.py

Phase: 0.6 (Data Quality & Observability)
Specs: SPEC-PIPE-005, SPEC-ALERT-001, SPEC-SCHED-010
Owner: Platform / Ingestion
Consumers: CI, pytest

Unit tests for ingestion/quality/drift_monitor.py's PSIMonitor baseline
persistence and check_drift workflow (compute_psi/classify are covered in
tests/unit/test_validator.py).
"""

import numpy as np
import pandas as pd
import pytest

from ingestion.quality.drift_monitor import PSIMonitor


def _feature_matrix(n=200, seed=0, **overrides):
    rng = np.random.default_rng(seed)
    data = {"f1": rng.normal(0, 1, n), "f2": rng.normal(5, 2, n)}
    data.update(overrides)
    return pd.DataFrame(data)


# ===== compute_baseline =====


def test_compute_baseline_returns_bin_edges_and_proportions_per_feature(tmp_path):
    monitor = PSIMonitor(baseline_path=tmp_path / "baseline.pkl")
    matrix = _feature_matrix()

    baseline = monitor.compute_baseline(matrix, save=False)

    assert set(baseline.keys()) == {"f1", "f2"}
    for entry in baseline.values():
        assert "bin_edges" in entry and "baseline_pct" in entry
        assert entry["bin_edges"][0] == -np.inf
        assert entry["bin_edges"][-1] == np.inf
        assert np.isclose(entry["baseline_pct"].sum(), 1.0, atol=1e-2)


def test_compute_baseline_skips_entirely_nan_columns(tmp_path):
    monitor = PSIMonitor(baseline_path=tmp_path / "baseline.pkl")
    matrix = _feature_matrix(f3=[np.nan] * 200)

    baseline = monitor.compute_baseline(matrix, save=False)

    assert "f3" not in baseline
    assert "f1" in baseline and "f2" in baseline


def test_compute_baseline_handles_near_constant_feature(tmp_path):
    """A near-constant feature degenerates to a single bin covering everything."""
    monitor = PSIMonitor(baseline_path=tmp_path / "baseline.pkl")
    matrix = pd.DataFrame({"flat": [1.0] * 100})

    baseline = monitor.compute_baseline(matrix, save=False)

    assert list(baseline["flat"]["bin_edges"]) == [-np.inf, np.inf]


def test_compute_baseline_save_true_writes_pickle_atomically(tmp_path):
    baseline_path = tmp_path / "nested" / "baseline.pkl"
    monitor = PSIMonitor(baseline_path=baseline_path)
    matrix = _feature_matrix()

    monitor.compute_baseline(matrix, save=True)

    assert baseline_path.exists()
    assert not baseline_path.with_suffix(baseline_path.suffix + ".tmp").exists()


def test_compute_baseline_save_false_does_not_write(tmp_path):
    baseline_path = tmp_path / "baseline.pkl"
    monitor = PSIMonitor(baseline_path=baseline_path)
    matrix = _feature_matrix()

    monitor.compute_baseline(matrix, save=False)

    assert not baseline_path.exists()


# ===== load_baseline =====


def test_load_baseline_raises_file_not_found_when_missing(tmp_path):
    monitor = PSIMonitor(baseline_path=tmp_path / "does-not-exist.pkl")

    with pytest.raises(FileNotFoundError, match="Run ingestion/quality/baseline_runner.py first"):
        monitor.load_baseline()


def test_load_baseline_round_trips_compute_baseline(tmp_path):
    baseline_path = tmp_path / "baseline.pkl"
    monitor = PSIMonitor(baseline_path=baseline_path)
    matrix = _feature_matrix()

    saved = monitor.compute_baseline(matrix, save=True)
    loaded = monitor.load_baseline()

    assert set(loaded.keys()) == set(saved.keys())
    for name in saved:
        assert np.array_equal(loaded[name]["bin_edges"], saved[name]["bin_edges"])
        assert np.array_equal(loaded[name]["baseline_pct"], saved[name]["baseline_pct"])


# ===== check_drift =====


def test_check_drift_uses_explicit_baseline_without_loading_from_disk(tmp_path):
    """Passing baseline= directly must skip self.load_baseline() entirely."""
    monitor = PSIMonitor(baseline_path=tmp_path / "never-created.pkl")
    train_matrix = _feature_matrix(seed=1)
    baseline = monitor.compute_baseline(train_matrix, save=False)

    today_matrix = _feature_matrix(seed=2)
    result = monitor.check_drift(today_matrix, baseline=baseline)

    assert set(result.keys()) == {"f1", "f2"}
    for entry in result.values():
        assert "psi" in entry and "status" in entry
        assert entry["status"] in ("ok", "warning", "halt")


def test_check_drift_loads_baseline_from_disk_when_not_supplied(tmp_path):
    baseline_path = tmp_path / "baseline.pkl"
    monitor = PSIMonitor(baseline_path=baseline_path)
    monitor.compute_baseline(_feature_matrix(seed=1), save=True)

    result = monitor.check_drift(_feature_matrix(seed=2))

    assert set(result.keys()) == {"f1", "f2"}


def test_check_drift_flags_shifted_feature_as_halt(tmp_path):
    monitor = PSIMonitor(baseline_path=tmp_path / "unused.pkl")  # not loaded -- baseline supplied directly
    baseline_matrix = _feature_matrix(seed=1)
    baseline = monitor.compute_baseline(baseline_matrix, save=False)

    shifted_matrix = _feature_matrix(seed=1)
    shifted_matrix["f1"] = shifted_matrix["f1"] + 10  # large mean shift

    result = monitor.check_drift(shifted_matrix, baseline=baseline)

    assert result["f1"]["status"] == "halt"
    assert result["f2"]["status"] == "ok"


def test_check_drift_respects_explicit_feature_names_subset(tmp_path):
    monitor = PSIMonitor(baseline_path=tmp_path / "unused.pkl")
    baseline = monitor.compute_baseline(_feature_matrix(seed=1), save=False)

    result = monitor.check_drift(_feature_matrix(seed=2), feature_names=["f1"], baseline=baseline)

    assert set(result.keys()) == {"f1"}


def test_check_drift_skips_feature_missing_from_feature_matrix(tmp_path):
    monitor = PSIMonitor(baseline_path=tmp_path / "unused.pkl")
    baseline = monitor.compute_baseline(_feature_matrix(seed=1), save=False)

    today = _feature_matrix(seed=2).drop(columns=["f2"])
    result = monitor.check_drift(today, baseline=baseline)

    assert set(result.keys()) == {"f1"}


def test_check_drift_skips_feature_entirely_nan_in_current(tmp_path):
    """A feature present in both baseline and feature_matrix, but all-NaN today, must be skipped, not crash."""
    monitor = PSIMonitor(baseline_path=tmp_path / "unused.pkl")
    baseline = monitor.compute_baseline(_feature_matrix(seed=1), save=False)

    today = _feature_matrix(seed=2)
    today["f1"] = np.nan
    result = monitor.check_drift(today, baseline=baseline)

    assert set(result.keys()) == {"f2"}


def test_check_drift_skips_feature_missing_from_baseline(tmp_path):
    monitor = PSIMonitor(baseline_path=tmp_path / "unused.pkl")
    baseline = monitor.compute_baseline(_feature_matrix(seed=1), save=False)
    del baseline["f2"]

    result = monitor.check_drift(_feature_matrix(seed=2), feature_names=["f1", "f2"], baseline=baseline)

    assert set(result.keys()) == {"f1"}


def test_check_drift_caps_default_feature_selection_at_top_n(monkeypatch, tmp_path):
    monkeypatch.setattr("ingestion.quality.drift_monitor.PSI_TOP_N_FEATURES", 1)
    monitor = PSIMonitor(baseline_path=tmp_path / "unused.pkl")
    baseline = monitor.compute_baseline(_feature_matrix(seed=1), save=False)

    result = monitor.check_drift(_feature_matrix(seed=2), baseline=baseline)

    assert len(result) == 1


# ===== [2026-08-13] coverage guard + degraded-monitoring floor =====


class TestBaselineRecordsCoverage:
    """compute_baseline persists each feature's non-null share so
    check_drift can tell a distribution shift apart from a coverage change."""

    def test_non_null_share_is_recorded(self, tmp_path):
        monitor = PSIMonitor(baseline_path=tmp_path / "b.pkl")
        matrix = _feature_matrix(n=100, seed=3)
        matrix.loc[:49, "f1"] = np.nan  # half the column missing

        baseline = monitor.compute_baseline(matrix, save=False)

        assert baseline["f1"]["non_null_share"] == pytest.approx(0.5)
        assert baseline["f2"]["non_null_share"] == pytest.approx(1.0)


class TestCoverageShiftIsNotMarketDrift:
    """[2026-08-12 incident] delivery_pct went from ~5% populated to ~90%
    populated when the delivery UPSERT fix landed. PSI against a baseline
    derived from the sparse era hit 0.272 and halted inference — on data
    that had just got BETTER. A halt must mean the market changed, never
    that we started collecting a field properly.
    """

    def _sparse_baseline_and_full_matrix(self, monitor):
        rng = np.random.default_rng(11)
        n = 500
        # Baseline era: only ~5% of rows carry the feature, and that sparse
        # subset sits at a different level from the full population.
        sparse = pd.DataFrame({"f1": np.full(n, np.nan), "f2": rng.normal(0, 1, n)})
        sparse.loc[:24, "f1"] = rng.normal(20, 1, 25)
        baseline = monitor.compute_baseline(sparse, save=False)

        # Today: fully populated, centred elsewhere — a big PSI purely
        # because the two populations are different, not because of drift.
        full = pd.DataFrame({"f1": rng.normal(55, 5, n), "f2": rng.normal(0, 1, n)})
        return baseline, full

    def test_large_coverage_gain_is_reported_as_stale_baseline_not_halt(self, tmp_path):
        monitor = PSIMonitor(baseline_path=tmp_path / "b.pkl")
        baseline, full = self._sparse_baseline_and_full_matrix(monitor)

        result = monitor.check_drift(full, baseline=baseline)

        assert result["f1"]["status"] == "stale_baseline", (
            "a coverage jump must not halt inference; it is a baseline-refresh signal"
        )
        assert result["f1"]["coverage_shift"] > 0.20

    def test_stable_coverage_still_halts_on_real_drift(self, tmp_path):
        """The guard must not swallow genuine drift: same coverage, shifted values."""
        monitor = PSIMonitor(baseline_path=tmp_path / "b.pkl")
        baseline = monitor.compute_baseline(_feature_matrix(seed=1), save=False)

        shifted = _feature_matrix(seed=1)
        shifted["f1"] = shifted["f1"] + 10

        result = monitor.check_drift(shifted, baseline=baseline)

        assert result["f1"]["status"] == "halt"
        assert result["f1"]["coverage_shift"] == pytest.approx(0.0)

    def test_legacy_baseline_without_coverage_still_works(self, tmp_path):
        """Baselines pickled before non_null_share existed must not crash."""
        monitor = PSIMonitor(baseline_path=tmp_path / "b.pkl")
        baseline = monitor.compute_baseline(_feature_matrix(seed=1), save=False)
        for entry in baseline.values():
            entry.pop("non_null_share")

        result = monitor.check_drift(_feature_matrix(seed=1), baseline=baseline)

        assert result["f1"]["coverage_shift"] is None
        assert result["f1"]["status"] == "ok"


class TestDegradedMonitoringIsLoud:
    """A monitor silently covering 1 of 50 features manufactures confidence.
    stats_baseline.pkl held 3 features, two absent from the feature panel,
    so 'top 50 features vs baseline' was in fact checking exactly one."""

    def test_thin_intersection_logs_an_error(self, tmp_path, caplog):
        monitor = PSIMonitor(baseline_path=tmp_path / "b.pkl")
        baseline = monitor.compute_baseline(_feature_matrix(seed=1), save=False)

        # Only f1 exists in today's matrix -> intersection of 1.
        today = pd.DataFrame({"f1": _feature_matrix(seed=1)["f1"]})
        with caplog.at_level("ERROR"):
            monitor.check_drift(today, baseline=baseline)

        assert "DEGRADED" in caplog.text
        assert "only 1 feature" in caplog.text

    def test_healthy_intersection_is_silent(self, tmp_path, caplog):
        monitor = PSIMonitor(baseline_path=tmp_path / "b.pkl")
        wide = pd.DataFrame({f"f{i}": np.random.default_rng(i).normal(0, 1, 200) for i in range(12)})
        baseline = monitor.compute_baseline(wide, save=False)

        with caplog.at_level("ERROR"):
            monitor.check_drift(wide, baseline=baseline)

        assert "DEGRADED" not in caplog.text
