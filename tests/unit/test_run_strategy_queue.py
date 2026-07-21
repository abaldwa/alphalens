"""
tests/unit/test_run_strategy_queue.py

Unit tests for backtest/run_strategy_queue.py's _job_to_cmd — the pure
job-dict -> subprocess-argv mapping (no subprocess actually launched).
"""

import pytest

from backtest.run_strategy_queue import _job_to_cmd


class TestJobToCmd:
    def test_orchestrator_job_maps_flags(self):
        job = {
            "kind": "orchestrator", "channel": "technical", "template_name": "E2", "top_n": 10,
            "start_date": "2023-01-01", "end_date": "2026-07-01",
        }
        cmd = _job_to_cmd(job, job_index=0, report_suffix="q1")

        assert cmd[0:3] == [cmd[0], "-m", "backtest.run_orchestrator_backtest"]
        assert "--channel" in cmd and "technical" in cmd
        assert "--template-name" in cmd and "E2" in cmd
        assert "--top-n" in cmd and "10" in cmd
        assert "--report-suffix" in cmd and "q1_job0" in cmd

    def test_iterative_retrain_job_maps_flags(self):
        job = {"kind": "iterative_retrain", "horizon_days": 5, "folds": 4}
        cmd = _job_to_cmd(job, job_index=1, report_suffix="q1")

        assert cmd[0:3] == [cmd[0], "-m", "backtest.run_iterative_backtest"]
        assert "--horizon-days" in cmd and "5" in cmd
        assert "--report-suffix" in cmd and "q1_job1" in cmd

    def test_none_values_omitted(self):
        job = {"kind": "orchestrator", "channel": "technical", "template_name": "E2", "preset": None}
        cmd = _job_to_cmd(job, job_index=0, report_suffix="q1")
        assert "--preset" not in cmd

    def test_unknown_kind_raises(self):
        with pytest.raises(ValueError):
            _job_to_cmd({"kind": "bogus"}, job_index=0, report_suffix="q1")

    def test_unknown_field_for_kind_raises(self):
        with pytest.raises(ValueError):
            _job_to_cmd({"kind": "iterative_retrain", "template_name": "E2"}, job_index=0, report_suffix="q1")
