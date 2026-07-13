"""
tests/unit/test_backtest_report_utils.py

ML17(b) — backtest/report_utils.py's write_per_horizon_reports(): pure
function over already-computed results dicts (no network/DB access, no
model training) so it's fully testable with injected data per this
repo's no-stub/synthetic-data policy (real BacktestResults.to_dict()
shape, not fabricated financial values).
"""

import json

from backtest.report_utils import write_per_horizon_reports


def _fake_results_dict(model_name: str, sharpe: float) -> dict:
    """Shaped like BacktestEngine's real BacktestResults.to_dict() output
    (fold_results list + aggregate dict) — values are simple round numbers
    chosen for the test, not fabricated as if from a real run."""
    return {
        "model_name": model_name,
        "fold_results": [
            {
                "fold": 0, "sharpe": sharpe, "cagr": 0.12,
                "benchmark_sharpe": 0.5, "benchmark_cagr": 0.08,
                "excess_return": 0.04,
            }
        ],
        "aggregate": {
            "sharpe_mean": sharpe, "cagr_mean": 0.12,
            "benchmark_cagr_mean": 0.08, "excess_return_mean": 0.04,
        },
        "integrity_passed": True,
    }


class TestWritePerHorizonReports:
    def test_writes_one_file_per_variant(self, tmp_path):
        variants = {
            "signal_5d": _fake_results_dict("signal_5d", 1.1),
            "signal_63d_watchlist": _fake_results_dict("signal_63d_watchlist", 1.4),
        }
        written = write_per_horizon_reports(variants, tmp_path, "20260713", "phase2")

        assert set(written.keys()) == {"signal_5d", "signal_63d_watchlist"}
        for name, path in written.items():
            assert path.name == f"phase2_{name}_20260713.json"
            assert path.exists()
            on_disk = json.loads(path.read_text())
            assert on_disk == variants[name]

    def test_each_variant_report_is_independent_of_the_others(self, tmp_path):
        """Each per-horizon file only contains its own variant's results —
        not the whole combined multi-variant report."""
        variants = {
            "signal_5d_p2baseline": _fake_results_dict("signal_5d_p2baseline", 0.9),
            "signal_21d_p3variant": _fake_results_dict("signal_21d_p3variant", 1.2),
        }
        written = write_per_horizon_reports(variants, tmp_path, "20260713", "phase3")

        baseline_on_disk = json.loads(written["signal_5d_p2baseline"].read_text())
        assert "signal_21d_p3variant" not in json.dumps(baseline_on_disk)
        assert baseline_on_disk["model_name"] == "signal_5d_p2baseline"

    def test_creates_reports_dir_if_missing(self, tmp_path):
        nested = tmp_path / "does" / "not" / "exist"
        written = write_per_horizon_reports(
            {"signal_5d": _fake_results_dict("signal_5d", 1.0)}, nested, "20260713", "phase2",
        )
        assert nested.exists()
        assert written["signal_5d"].exists()
