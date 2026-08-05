"""
tests/unit/test_run_strategy_queue.py

Unit tests for backtest/run_strategy_queue.py's _job_to_cmd — the pure
job-dict -> subprocess-argv mapping (no subprocess actually launched).
"""

import json
import threading
import time

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

    def test_momentum_rank_band_id_is_queueable(self):
        """2026-08-05 Momentum engine consolidation Phase 2 — --rank-band-id
        must be usable from a queue job, not CLI-only."""
        job = {
            "kind": "orchestrator", "channel": "momentum", "top_n": 10, "lookback_months": 6,
            "rank_band_id": 2, "start_date": "2016-01-01", "end_date": "2026-07-01",
        }
        cmd = _job_to_cmd(job, job_index=3, report_suffix="q1")
        assert "--rank-band-id" in cmd
        assert cmd[cmd.index("--rank-band-id") + 1] == "2"

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

    def test_bool_true_field_emits_bare_flag(self):
        """2026-08-02 (defer_db_writes): a bool-valued job field must map to
        a bare store_true flag, not "--flag True" (which argparse would
        reject as an unrecognized positional)."""
        job = {"kind": "orchestrator", "channel": "technical", "template_name": "E2", "defer_db_writes": True}
        cmd = _job_to_cmd(job, job_index=0, report_suffix="q1")
        assert "--defer-db-writes" in cmd
        assert "True" not in cmd

    def test_bool_false_field_omitted(self):
        job = {"kind": "orchestrator", "channel": "technical", "template_name": "E2", "defer_db_writes": False}
        cmd = _job_to_cmd(job, job_index=0, report_suffix="q1")
        assert "--defer-db-writes" not in cmd


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


class TestIntegrityGate:
    """[BUG FIX, 4th fundamental-strategies review, item 3] integrity_passed
    is NOT opt-in (unlike min_dsr_threshold) — a persisted run with
    integrity_passed=False must always surface as a distinct status
    ('integrity_check_failed'), never plain 'completed'/all_passed=True."""

    def _run(self, monkeypatch, tmp_path, job, integrity_passed, dsr_value=0.9):
        monkeypatch.setattr(run_strategy_queue_mod, "REPORTS_DIR", tmp_path)
        monkeypatch.setattr(
            run_strategy_queue_mod, "_run_job",
            lambda job, i, suffix: {"job_index": i, "kind": job.get("kind"), "job": job, "returncode": 0, "elapsed_s": 1.0},
        )
        monkeypatch.setattr(
            run_strategy_queue_mod, "_compute_and_write_dsr",
            lambda job, i, suffix, n_trials: dsr_value,
        )
        monkeypatch.setattr(
            run_strategy_queue_mod, "_check_integrity_passed",
            lambda job, i, suffix: integrity_passed,
        )
        monkeypatch.setattr(run_strategy_queue_mod, "wait_for_headroom", lambda *a, **k: None)
        return run_queue([job], report_suffix="qtest", resume=False)

    def test_integrity_passed_false_fails_the_job_even_without_dsr_threshold(self, monkeypatch, tmp_path):
        job = {"kind": "orchestrator", "channel": "fundamental", "preset": "quality_compounder"}
        summary = self._run(monkeypatch, tmp_path, job, integrity_passed=False)
        assert summary["all_passed"] is False
        progress = json.loads((tmp_path / "strategy_queue_progress_qtest.json").read_text())
        assert progress["jobs"][0]["status"] == "integrity_check_failed"

    def test_integrity_passed_true_leaves_job_completed(self, monkeypatch, tmp_path):
        job = {"kind": "orchestrator", "channel": "fundamental", "preset": "quality_compounder"}
        summary = self._run(monkeypatch, tmp_path, job, integrity_passed=True)
        assert summary["all_passed"] is True
        progress = json.loads((tmp_path / "strategy_queue_progress_qtest.json").read_text())
        assert progress["jobs"][0]["status"] == "completed"

    def test_integrity_unresolvable_none_does_not_fail_the_job(self, monkeypatch, tmp_path):
        """None means 'couldn't verify' — must not be conflated with a real
        failure (that would be a false positive, unlike the DSR gate's
        deliberate None-treated-as-failure choice, which only applies when
        the operator explicitly opted into a min_dsr_threshold)."""
        job = {"kind": "orchestrator", "channel": "fundamental", "preset": "quality_compounder"}
        summary = self._run(monkeypatch, tmp_path, job, integrity_passed=None)
        assert summary["all_passed"] is True
        progress = json.loads((tmp_path / "strategy_queue_progress_qtest.json").read_text())
        assert progress["jobs"][0]["status"] == "completed"


class TestDsrAnnualizationBug:
    """[BUG FIX, 4th fundamental-strategies review, item 1] The Sharpe stored
    in an orchestrator report's metrics.sharpe is ANNUALIZED (backtest/core/
    metrics.py::sharpe_ratio's docstring). deflated_sharpe_ratio's Bailey/
    Lopez de Prado formula expects a per-period (daily) Sharpe — feeding it
    the annualized value inflates the DSR statistic by ~sqrt(252). This test
    constructs a realistic daily-returns series, computes both scales, and
    confirms _compute_and_write_dsr wires the PER-PERIOD value into
    deflated_sharpe_ratio, not the annualized one."""

    def test_wires_per_period_not_annualized_sharpe(self, monkeypatch, tmp_path):
        import numpy as np

        rng = np.random.default_rng(0)
        daily_returns = rng.normal(loc=0.0006, scale=0.01, size=252)
        raw_sharpe = float(daily_returns.mean() / daily_returns.std())
        annualized_sharpe = raw_sharpe * (252 ** 0.5)
        assert abs(annualized_sharpe) > abs(raw_sharpe) * 10  # sanity: sqrt(252) ~ 15.9x

        monkeypatch.setattr(run_strategy_queue_mod, "REPORTS_DIR", tmp_path)
        report_path = tmp_path / "orchestrator_qtest_job0.json"
        report_path.write_text(json.dumps({
            "run": {"run_id": "run-1", "start_date": "2025-01-01", "end_date": "2026-01-01"},
            "metrics": {"sharpe": annualized_sharpe},
        }))

        captured = {}

        def _fake_dsr(sharpe, n_trials, n_obs, returns=None):
            captured["sharpe"] = sharpe
            return 0.42

        monkeypatch.setattr(run_strategy_queue_mod, "deflated_sharpe_ratio", _fake_dsr, raising=False)
        # deflated_sharpe_ratio is imported inside the function body (`from
        # backtest.overfit_checks import deflated_sharpe_ratio`) — patch it
        # at the source module so the local import picks up the fake.
        import backtest.overfit_checks as overfit_checks_mod
        monkeypatch.setattr(overfit_checks_mod, "deflated_sharpe_ratio", _fake_dsr)

        class _FakeConn:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        monkeypatch.setattr(run_strategy_queue_mod, "update_dsr", lambda *a, **k: None, raising=False)
        import backtest.core.run_store as run_store_mod
        monkeypatch.setattr(run_store_mod, "update_dsr", lambda *a, **k: None)
        monkeypatch.setattr(
            "datastore.api.db.get_duckdb_connection", lambda *a, **k: _FakeConn(),
        )

        job = {"kind": "orchestrator", "channel": "fundamental", "preset": "quality_compounder"}
        dsr = run_strategy_queue_mod._compute_and_write_dsr(job, 0, "qtest", n_trials_so_far=1)

        assert dsr == 0.42
        assert "sharpe" in captured
        # Wired value must match the per-period Sharpe (within float precision
        # of the exact de-annualization), not the annualized one.
        assert captured["sharpe"] == pytest.approx(raw_sharpe, rel=1e-9)
        assert captured["sharpe"] != pytest.approx(annualized_sharpe, rel=1e-3)


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


class TestConcurrentQueue:
    """max_workers>1 (2026-08-02, Technical sweep parallelization) —
    _run_job is monkeypatched to a fast fake (no real subprocess), same
    pattern TestDsrGate/TestIntegrityGate already use for the sequential
    path. These pin: (1) max_workers=1 is fully unaffected (dispatches to
    the untouched sequential branch), (2) all jobs still get processed and
    the summary shape matches the sequential path's, (3) the configured
    worker cap is actually respected (peak concurrency measured), (4)
    stop_on_failure stops NEW submissions after a failure but lets
    in-flight jobs finish rather than killing them."""

    def _patch_common(self, monkeypatch, tmp_path):
        monkeypatch.setattr(run_strategy_queue_mod, "REPORTS_DIR", tmp_path)
        monkeypatch.setattr(run_strategy_queue_mod, "wait_for_headroom", lambda *a, **k: None)
        monkeypatch.setattr(run_strategy_queue_mod, "_compute_and_write_dsr", lambda job, i, suffix, n_trials: None)
        monkeypatch.setattr(run_strategy_queue_mod, "_check_integrity_passed", lambda job, i, suffix: None)

    def test_max_workers_1_dispatches_to_sequential_path(self, monkeypatch, tmp_path):
        self._patch_common(monkeypatch, tmp_path)
        calls = []

        def fake_run_job(job, i, suffix):
            calls.append(i)
            return {"job_index": i, "kind": job.get("kind"), "job": job, "returncode": 0, "elapsed_s": 0.01}

        monkeypatch.setattr(run_strategy_queue_mod, "_run_job", fake_run_job)
        jobs = [{"kind": "orchestrator", "channel": "technical", "template_name": f"E{i}"} for i in range(3)]
        summary = run_queue(jobs, report_suffix="seqtest", resume=False, max_workers=1)

        assert calls == [0, 1, 2]  # strict launch order — the untouched sequential loop
        assert summary["all_passed"] is True
        assert summary["jobs_run"] == 3

    def test_concurrent_all_jobs_complete(self, monkeypatch, tmp_path):
        self._patch_common(monkeypatch, tmp_path)

        def fake_run_job(job, i, suffix):
            time.sleep(0.05)
            return {"job_index": i, "kind": job.get("kind"), "job": job, "returncode": 0, "elapsed_s": 0.05}

        monkeypatch.setattr(run_strategy_queue_mod, "_run_job", fake_run_job)
        jobs = [{"kind": "orchestrator", "channel": "technical", "template_name": f"E{i}"} for i in range(6)]
        summary = run_queue(jobs, report_suffix="concurrenttest", resume=False, max_workers=3)

        assert summary["all_passed"] is True
        assert summary["jobs_run"] == 6
        assert [r["job_index"] for r in summary["results"]] == list(range(6))  # sorted back to job order

    def test_concurrency_respects_max_workers_cap(self, monkeypatch, tmp_path):
        self._patch_common(monkeypatch, tmp_path)
        active = {"count": 0, "peak": 0}
        lock = threading.Lock()

        def fake_run_job(job, i, suffix):
            with lock:
                active["count"] += 1
                active["peak"] = max(active["peak"], active["count"])
            time.sleep(0.05)
            with lock:
                active["count"] -= 1
            return {"job_index": i, "kind": job.get("kind"), "job": job, "returncode": 0, "elapsed_s": 0.05}

        monkeypatch.setattr(run_strategy_queue_mod, "_run_job", fake_run_job)
        jobs = [{"kind": "orchestrator", "channel": "technical", "template_name": f"E{i}"} for i in range(8)]
        run_queue(jobs, report_suffix="captest", resume=False, max_workers=3)

        assert active["peak"] <= 3
        assert active["peak"] > 1  # actually ran concurrently, not accidentally serialized

    def test_stop_on_failure_skips_unsubmitted_jobs_but_finishes_in_flight(self, monkeypatch, tmp_path):
        self._patch_common(monkeypatch, tmp_path)

        def fake_run_job(job, i, suffix):
            time.sleep(0.03)
            returncode = 1 if i == 0 else 0
            return {"job_index": i, "kind": job.get("kind"), "job": job, "returncode": returncode, "elapsed_s": 0.03}

        monkeypatch.setattr(run_strategy_queue_mod, "_run_job", fake_run_job)
        jobs = [{"kind": "orchestrator", "channel": "technical", "template_name": f"E{i}"} for i in range(5)]
        summary = run_queue(jobs, report_suffix="failtest", resume=False, max_workers=1, stop_on_failure=True)

        # max_workers=1 here to make the "stop after first failure" ordering
        # deterministic to assert on — job 0 fails, jobs 1-4 must never run.
        assert summary["all_passed"] is False
        progress = json.loads((tmp_path / "strategy_queue_progress_failtest.json").read_text())
        statuses = [j["status"] for j in progress["jobs"]]
        assert statuses[0] == "failed"
        assert all(s == "skipped" for s in statuses[1:])

    def test_concurrent_continue_on_failure_runs_all_jobs(self, monkeypatch, tmp_path):
        self._patch_common(monkeypatch, tmp_path)

        def fake_run_job(job, i, suffix):
            time.sleep(0.02)
            returncode = 1 if i == 2 else 0
            return {"job_index": i, "kind": job.get("kind"), "job": job, "returncode": returncode, "elapsed_s": 0.02}

        monkeypatch.setattr(run_strategy_queue_mod, "_run_job", fake_run_job)
        jobs = [{"kind": "orchestrator", "channel": "technical", "template_name": f"E{i}"} for i in range(5)]
        summary = run_queue(jobs, report_suffix="contfailtest", resume=False, max_workers=3, stop_on_failure=False)

        assert summary["all_passed"] is False
        assert summary["jobs_run"] == 5  # every job still ran despite job 2 failing
