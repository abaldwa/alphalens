"""
tests/unit/test_run_strategy_queue.py

Unit tests for backtest/run_strategy_queue.py's _job_to_cmd — the pure
job-dict -> subprocess-argv mapping (no subprocess actually launched).
"""

import json

import pytest

import backtest.run_strategy_queue as run_strategy_queue_mod
from backtest.run_strategy_queue import _job_label, _job_to_cmd, _write_progress, run_queue


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

    def test_min_dsr_threshold_accepted_but_not_passed_to_subprocess(self):
        """[BUG FIX, 2026-07-28 third model-review, item 2] min_dsr_threshold
        is now an accepted orchestrator-job field (queue-only gating
        bookkeeping) — but run_orchestrator_backtest.py has no matching CLI
        flag, so it must be accepted here without raising AND stripped
        before building the subprocess argv, not passed through as
        --min-dsr-threshold."""
        job = {
            "kind": "orchestrator", "channel": "fundamental", "preset": "quality_compounder",
            "min_dsr_threshold": 0.5,
        }
        cmd = _job_to_cmd(job, job_index=0, report_suffix="q1")
        assert "--min-dsr-threshold" not in cmd
        assert "0.5" not in cmd


class TestDsrGate:
    """[BUG FIX, 2026-07-28 third model-review, item 2] min_dsr_threshold is
    opt-in per job — unset (None, the default) must reproduce today's
    behavior exactly; set, it must fail the job (distinct 'dsr_gate_failed'
    status) when the computed DSR falls below it."""

    def _run(self, monkeypatch, tmp_path, job, dsr_value):
        monkeypatch.setattr(run_strategy_queue_mod, "REPORTS_DIR", tmp_path)
        monkeypatch.setattr(
            run_strategy_queue_mod, "_run_job",
            lambda job, i, suffix: {"job_index": i, "kind": job.get("kind"), "job": job, "returncode": 0, "elapsed_s": 1.0},
        )
        monkeypatch.setattr(
            run_strategy_queue_mod, "_compute_and_write_dsr",
            lambda job, i, suffix, n_trials: dsr_value,
        )
        monkeypatch.setattr(run_strategy_queue_mod, "wait_for_headroom", lambda *a, **k: None)
        return run_queue([job], report_suffix="qtest", resume=False)

    def test_no_threshold_set_is_unaffected_by_low_dsr(self, monkeypatch, tmp_path):
        job = {"kind": "orchestrator", "channel": "fundamental", "preset": "quality_compounder"}
        summary = self._run(monkeypatch, tmp_path, job, dsr_value=0.01)
        assert summary["all_passed"] is True
        progress = json.loads((tmp_path / "strategy_queue_progress_qtest.json").read_text())
        assert progress["jobs"][0]["status"] == "completed"

    def test_dsr_below_threshold_fails_the_job(self, monkeypatch, tmp_path):
        job = {
            "kind": "orchestrator", "channel": "fundamental", "preset": "quality_compounder",
            "min_dsr_threshold": 0.5,
        }
        summary = self._run(monkeypatch, tmp_path, job, dsr_value=0.1)
        assert summary["all_passed"] is False
        progress = json.loads((tmp_path / "strategy_queue_progress_qtest.json").read_text())
        assert progress["jobs"][0]["status"] == "dsr_gate_failed"

    def test_dsr_at_or_above_threshold_passes(self, monkeypatch, tmp_path):
        job = {
            "kind": "orchestrator", "channel": "fundamental", "preset": "quality_compounder",
            "min_dsr_threshold": 0.5,
        }
        summary = self._run(monkeypatch, tmp_path, job, dsr_value=0.5)
        assert summary["all_passed"] is True
        progress = json.loads((tmp_path / "strategy_queue_progress_qtest.json").read_text())
        assert progress["jobs"][0]["status"] == "completed"

    def test_unresolvable_dsr_with_threshold_set_fails_the_job(self, monkeypatch, tmp_path):
        """dsr=None (couldn't be computed) with a threshold set must be
        treated as a gate failure, not a silent pass."""
        job = {
            "kind": "orchestrator", "channel": "fundamental", "preset": "quality_compounder",
            "min_dsr_threshold": 0.5,
        }
        summary = self._run(monkeypatch, tmp_path, job, dsr_value=None)
        assert summary["all_passed"] is False
        progress = json.loads((tmp_path / "strategy_queue_progress_qtest.json").read_text())
        assert progress["jobs"][0]["status"] == "dsr_gate_failed"


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
