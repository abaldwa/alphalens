"""
tests/unit/test_model_training_overdue_union.py

Regression coverage for A33 (FeatureBacklog.md): _execute_model_training_job's
overdue-check loop iterates the union of registry.json's keys and
_MODEL_TRAINING_SCRIPT_MAP's non-None keys (ingestion/scheduler/
pipeline_scheduler.py, ~line 1378), not registry.json's keys alone — a model
that is mapped in _MODEL_TRAINING_SCRIPT_MAP but has never been trained (no
registry.json entry at all, e.g. multibagger before its first real run) must
still be caught as "never trained" rather than silently skipped. No unit test
existed for this before; this seeds a registry.json missing one mapped model
and asserts _execute_model_training_job queues a retrain for it.
"""
import json

import pytest

from ingestion.scheduler.pipeline_scheduler import _MODEL_TRAINING_SCRIPT_MAP, _execute_model_training_job


@pytest.fixture
def _isolated_model_training_env(tmp_path, monkeypatch):
    import config.settings as settings_mod

    models_dir = tmp_path / "models"
    models_dir.mkdir()
    monkeypatch.setattr(settings_mod, "MODELS_DIR", models_dir)
    monkeypatch.setattr(settings_mod, "PIPELINE_LOG_DB_PATH", tmp_path / "pipeline_log.db")
    # _record_heartbeat (called on every _execute_model_training_job
    # invocation) also writes a job_run_log row into config.settings.
    # DUCKDB_PATH — a separate DB from PIPELINE_LOG_DB_PATH that was NOT
    # isolated here before. Every run of this test therefore wrote a real
    # "skipped" row into the production alphalens.duckdb job_run_log
    # table, which is what made a healthy scheduler look like it had been
    # silently failing every week (see BuildLog 2026-07-29 audit).
    monkeypatch.setattr(settings_mod, "DUCKDB_PATH", tmp_path / "isolated_test.duckdb")

    return models_dir


def test_mapped_but_never_registered_model_is_flagged_overdue(_isolated_model_training_env, monkeypatch):
    mapped_models = {name for name, script in _MODEL_TRAINING_SCRIPT_MAP.items() if script is not None}
    assert mapped_models, "no scheduler-mapped models found — cannot pick one to omit from registry"
    missing_model = sorted(mapped_models)[0]

    registry = {
        name: {"last_trained_date": "2026-07-06", "training_interval_days": 28}
        for name in mapped_models
        if name != missing_model
    }
    registry_path = _isolated_model_training_env / "registry.json"
    registry_path.write_text(json.dumps(registry))

    triggered = []
    import ingestion.scheduler.scheduler_jobs as _sj
    # A46: _execute_model_training_job lives in scheduler_jobs and calls its
    # own local _trigger_model_retrain binding — patch that, not the facade.
    monkeypatch.setattr(
        _sj, "_trigger_model_retrain", lambda model_name: triggered.append(model_name)
    )

    _execute_model_training_job()

    assert missing_model in triggered, (
        f"'{missing_model}' is mapped in _MODEL_TRAINING_SCRIPT_MAP but absent from registry.json — "
        "it must still be queued as overdue ('never trained'), not silently skipped"
    )


def test_registry_only_model_without_script_mapping_is_not_dropped(_isolated_model_training_env, monkeypatch):
    """A registry.json entry for a model with no _MODEL_TRAINING_SCRIPT_MAP mapping
    is still part of the union (set(registry.keys()) | ...) and, if overdue, is
    passed to _trigger_model_retrain (which itself no-ops for unmapped models)."""
    registry = {"unmapped_legacy_model": {"last_trained_date": "2000-01-01", "training_interval_days": 28}}
    registry_path = _isolated_model_training_env / "registry.json"
    registry_path.write_text(json.dumps(registry))

    triggered = []
    import ingestion.scheduler.scheduler_jobs as _sj
    monkeypatch.setattr(
        _sj, "_trigger_model_retrain", lambda model_name: triggered.append(model_name)
    )

    _execute_model_training_job()

    assert "unmapped_legacy_model" in triggered
