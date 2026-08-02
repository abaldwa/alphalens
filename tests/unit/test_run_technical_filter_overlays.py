"""tests/unit/test_run_technical_filter_overlays.py — pure job-list
construction and report-aggregation logic, no real backtest execution."""

import json
from datetime import date

from scripts import run_technical_experimentation as base_mod
from scripts import run_technical_filter_overlays as mod


class TestBuildJobs:
    def test_quick_grid_size(self):
        jobs = mod.build_jobs(date(2016, 1, 1), date(2026, 1, 1), quick=True)
        # 5 templates x 1 top_n x len(FILTERS)
        assert len(jobs) == 5 * 1 * len(mod.FILTERS)

    def test_every_filter_represented_per_template(self):
        jobs = mod.build_jobs(date(2016, 1, 1), date(2026, 1, 1), quick=True)
        filter_names = {j["_filter_name"] for j in jobs}
        assert filter_names == set(mod.FILTERS.keys())

    def test_baseline_row_has_no_extra_filter_fields(self):
        jobs = mod.build_jobs(date(2016, 1, 1), date(2026, 1, 1), quick=True)
        baseline_jobs = [j for j in jobs if j["_filter_name"] == "baseline"]
        assert baseline_jobs
        for j in baseline_jobs:
            assert "min_adtv_cr" not in j
            assert "quality_gate_min_f_score" not in j
            assert "circuit_band_pct" not in j

    def test_liquidity_floor_job_has_min_adtv_cr(self):
        jobs = mod.build_jobs(date(2016, 1, 1), date(2026, 1, 1), quick=True)
        liq_jobs = [j for j in jobs if j["_filter_name"] == "liquidity_floor"]
        assert all(j["min_adtv_cr"] == 0.1 for j in liq_jobs)

    def test_fixed_exit_variant_and_hold_days(self):
        jobs = mod.build_jobs(date(2016, 1, 1), date(2026, 1, 1), quick=True)
        assert all(j["exit_variant"] == mod.FIXED_EXIT_VARIANT for j in jobs)
        assert all(j["max_hold_days"] == mod.FIXED_MAX_HOLD_DAYS for j in jobs)

    def test_jobs_defer_db_writes_for_parallel_safety(self):
        jobs = mod.build_jobs(date(2016, 1, 1), date(2026, 1, 1), quick=True)
        assert all(j["defer_db_writes"] is True for j in jobs)

    def test_jobs_carry_precomputed_matches_dir(self):
        jobs = mod.build_jobs(date(2016, 1, 1), date(2026, 1, 1), quick=True)
        assert all(j["precomputed_matches_dir"] == str(mod.SCREENER_CACHE_DIR) for j in jobs)


class TestStripBookkeeping:
    def test_removes_underscore_prefixed_keys(self):
        job = {"template_name": "A1", "_filter_name": "baseline", "top_n": 10}
        stripped = mod._strip_bookkeeping(job)
        assert "_filter_name" not in stripped
        assert stripped == {"template_name": "A1", "top_n": 10}


class TestAggregateReport:
    def test_aggregates_and_labels_by_filter(self, tmp_path, monkeypatch):
        # _load_job_report is imported from run_technical_experimentation and
        # reads that module's own REPORTS_DIR global (a closure over the
        # original module, not this one's re-exported name) — patch there.
        monkeypatch.setattr(base_mod, "REPORTS_DIR", tmp_path)
        jobs = mod.build_jobs(date(2016, 1, 1), date(2026, 1, 1), quick=True)[:2]
        suffix = "testsuffix"
        report0 = {"run": {"run_id": "r0"}, "metrics": {"cagr": 0.12, "sharpe": 1.0, "total_trades": 5}}
        (tmp_path / "orchestrator_testsuffix_job0.json").write_text(json.dumps(report0))

        result = mod.aggregate_report(jobs, suffix)
        assert result["n_jobs_total"] == 2
        assert result["n_jobs_reported"] == 1
        assert result["variants"][0]["filter"] == jobs[0]["_filter_name"]
        assert result["variants"][0]["cagr"] == 0.12
