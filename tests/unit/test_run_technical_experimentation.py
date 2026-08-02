"""tests/unit/test_run_technical_experimentation.py — pure job-list
construction and report-aggregation logic, no real backtest execution
(per the 2026-08-01 "do not run the backtest yet" instruction)."""

import json
from datetime import date

from backtest.core.engine import EXIT_POLICY_VARIANTS
from scripts import run_technical_experimentation as mod


class TestBuildJobs:
    def test_full_grid_size(self):
        jobs = mod.build_jobs(date(2016, 1, 1), date(2026, 1, 1), quick=False)
        from systems.technical_analysis.screener.templates import TEMPLATES
        expected = 0
        for _ in TEMPLATES:
            for variant in EXIT_POLICY_VARIANTS:
                hold_days_n = 1 if variant in mod._VARIANTS_IGNORING_HOLD_DAYS else len(mod.MAX_HOLD_DAYS_OPTIONS)
                expected += hold_days_n * len(mod.TOP_N_OPTIONS)
        assert len(jobs) == expected

    def test_quick_grid_is_much_smaller(self):
        quick_jobs = mod.build_jobs(date(2016, 1, 1), date(2026, 1, 1), quick=True)
        full_jobs = mod.build_jobs(date(2016, 1, 1), date(2026, 1, 1), quick=False)
        assert len(quick_jobs) < len(full_jobs)
        assert len(quick_jobs) == 5 * len(EXIT_POLICY_VARIANTS)  # 1 hold-days x 1 top_n in quick mode

    def test_job_shape(self):
        jobs = mod.build_jobs(date(2016, 1, 1), date(2026, 1, 1), quick=True)
        job = jobs[0]
        assert job["kind"] == "orchestrator"
        assert job["channel"] == "technical"
        assert job["initial_capital"] == mod.TECHNICAL_INITIAL_CAPITAL
        assert job["start_date"] == "2016-01-01"
        assert "template_name" in job and "exit_variant" in job and "top_n" in job

    def test_condition_and_unconstrained_omit_max_hold_days(self):
        jobs = mod.build_jobs(date(2016, 1, 1), date(2026, 1, 1), quick=False)
        for job in jobs:
            if job["exit_variant"] in mod._VARIANTS_IGNORING_HOLD_DAYS:
                assert "max_hold_days" not in job
            else:
                assert "max_hold_days" in job

    def test_no_duplicate_condition_jobs_across_hold_days(self):
        jobs = mod.build_jobs(date(2016, 1, 1), date(2026, 1, 1), quick=False)
        condition_jobs = [j for j in jobs if j["exit_variant"] == "condition"]
        # One job per (template, top_n) combination, not x4 for max_hold_days
        from systems.technical_analysis.screener.templates import TEMPLATES
        assert len(condition_jobs) == len(TEMPLATES) * len(mod.TOP_N_OPTIONS)


class TestAggregateReport:
    def test_aggregates_available_job_reports(self, tmp_path, monkeypatch):
        monkeypatch.setattr(mod, "REPORTS_DIR", tmp_path)
        jobs = [
            {"template_name": "A1", "top_n": 10, "exit_variant": "baseline", "max_hold_days": 63},
            {"template_name": "D4", "top_n": 15, "exit_variant": "trailing", "max_hold_days": 21},
        ]
        suffix = "testsuffix"
        report0 = {
            "run": {"run_id": "orch_technical_1"},
            "metrics": {
                "cagr": 0.15, "sharpe": 1.2, "sortino": 1.5, "calmar": 0.8,
                "total_trades": 42, "avg_trade_duration_days": 30.5,
                "n_outlier_trades": 1, "max_abs_return_zscore": 3.5,
            },
        }
        (tmp_path / "orchestrator_testsuffix_job0.json").write_text(json.dumps(report0))
        # job1 has no report file — simulates a job that hasn't run/failed;
        # aggregate_report must skip it, not crash.

        result = mod.aggregate_report(jobs, suffix)
        assert result["n_jobs_total"] == 2
        assert result["n_jobs_reported"] == 1
        assert result["variants"][0]["template_name"] == "A1"
        assert result["variants"][0]["cagr"] == 0.15
        assert result["variants"][0]["total_trades"] == 42
        assert result["variants"][0]["max_abs_return_zscore"] == 3.5

    def test_no_report_files_yields_empty_variants(self, tmp_path, monkeypatch):
        monkeypatch.setattr(mod, "REPORTS_DIR", tmp_path)
        jobs = [{"template_name": "A1", "top_n": 10, "exit_variant": "baseline", "max_hold_days": 63}]
        result = mod.aggregate_report(jobs, "missing_suffix")
        assert result["n_jobs_reported"] == 0
        assert result["variants"] == []
