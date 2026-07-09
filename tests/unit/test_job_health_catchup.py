"""
tests/unit/test_job_health_catchup.py

Phase: A21 (Pipeline Health Checker)
Owner: Platform / QA

Tests datastore/health/catchup.py's executor registry. Never invokes a
real subprocess/force-run — monkeypatches each executor's own
dependencies.
"""

from datetime import date

import pytest

from datastore.health.catchup import run_catchup


def test_run_catchup_dispatches_force_run_daily_pipeline(monkeypatch):
    calls = []

    def fake_force_run_date_sync(step_name, run_dates, today, cascade=True):
        calls.append((step_name, run_dates, cascade))
        return []

    import ingestion.scheduler.force_run as force_run_mod

    monkeypatch.setattr(force_run_mod, "force_run_date_sync", fake_force_run_date_sync)

    run_catchup(
        "force_run_daily_pipeline",
        "daily_pipeline",
        date(2026, 6, 5),
        {"missed_dates": ["2026-06-04", "2026-06-05"]},
    )

    assert len(calls) == 1
    step_name, run_dates, cascade = calls[0]
    assert step_name == "download_bhavcopy"
    assert run_dates == [date(2026, 6, 4), date(2026, 6, 5)]
    assert cascade is True


def test_run_catchup_dispatches_rerun_script(monkeypatch):
    calls = []

    class _FakeResult:
        returncode = 0
        stderr = ""

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        return _FakeResult()

    import subprocess

    monkeypatch.setattr(subprocess, "run", fake_run)

    run_catchup(
        "rerun_script",
        "weekend_feature_backfill",
        date(2026, 6, 6),
        {"script": "scripts/feature_backfill_hybrid.py", "args": ["--stage2-chunk-size", "400"]},
    )

    assert len(calls) == 1
    assert calls[0][1] == "scripts/feature_backfill_hybrid.py"
    assert "--stage2-chunk-size" in calls[0]


def test_run_catchup_rerun_script_raises_on_nonzero_exit(monkeypatch):
    class _FakeResult:
        returncode = 1
        stderr = "boom"

    import subprocess

    monkeypatch.setattr(subprocess, "run", lambda *a, **k: _FakeResult())

    with pytest.raises(RuntimeError):
        run_catchup("rerun_script", "weekend_fundamentals", date(2026, 6, 6), {"script": "x.py", "args": []})


def test_run_catchup_dispatches_rerun_mf_holdings(monkeypatch):
    calls = []

    import ingestion.scheduler.pipeline_scheduler as ps

    monkeypatch.setattr(ps, "_execute_mf_holdings_job", lambda: calls.append(True))

    run_catchup("rerun_mf_holdings", "mf_holdings_ingestion", date(2026, 6, 6), {})
    assert calls == [True]


def test_run_catchup_unknown_action_raises_keyerror():
    with pytest.raises(KeyError):
        run_catchup("not_a_real_action", "some_job", date(2026, 6, 6), {})
