"""
tests/unit/test_run_strategy_queue.py

Unit tests for backtest/run_strategy_queue.py's _job_to_cmd — the pure
job-dict -> subprocess-argv mapping (no subprocess actually launched).
"""

import json

import pytest

from backtest.run_strategy_queue import _job_label, _job_to_cmd, _write_progress


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


class TestJobLabel:
    def test_iterative_retrain_label(self):
        assert _job_label({"kind": "iterative_retrain"}) == "Iterative Retrain (MetaLabeler)"

    def test_technical_uses_template_name(self):
        assert _job_label({"kind": "orchestrator", "channel": "technical", "template_name": "E2"}) == "technical · E2"

    def test_fundamental_uses_preset(self):
        assert _job_label(
            {"kind": "orchestrator", "channel": "fundamental", "preset": "quality_compounder"}
        ) == "fundamental · quality_compounder"

    def test_momentum_falls_back_to_topn_lookback(self):
        label = _job_label({"kind": "orchestrator", "channel": "momentum", "top_n": 10, "lookback_months": 6})
        assert label == "momentum · top10_6m"

    def test_no_descriptor_falls_back_to_channel(self):
        assert _job_label({"kind": "orchestrator", "channel": "technical"}) == "technical"


class TestWriteProgress:
    def test_writes_expected_job_statuses(self, tmp_path):
        jobs = [
            {"kind": "orchestrator", "channel": "technical", "template_name": "E2"},
            {"kind": "iterative_retrain"},
        ]
        path = tmp_path / "progress.json"
        _write_progress(path, jobs, ["running", "queued"])

        payload = json.loads(path.read_text())
        assert [j["status"] for j in payload["jobs"]] == ["running", "queued"]
        assert payload["jobs"][0]["label"] == "technical · E2"
        assert payload["jobs"][1]["label"] == "Iterative Retrain (MetaLabeler)"
        assert payload["jobs"][0]["job_index"] == 0
