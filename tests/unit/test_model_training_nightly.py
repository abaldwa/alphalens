"""
tests/unit/test_model_training_nightly.py

Phase: Pipeline & Monitoring Remediation, Phase 4 (A52)
Owner: Platform / Scheduler
Consumers: CI, pytest

Regression coverage for schedule_model_training_nightly / _MODEL_TRAINING_GROUPS /
_execute_model_training_job_for_group: spreading model-training checks
across Mon-Thu nights instead of one weekly Saturday job.
"""

import json

import pytest

from ingestion.scheduler.pipeline_scheduler import (
    _MODEL_TRAINING_GROUPS,
    _MODEL_TRAINING_SCRIPT_MAP,
    _execute_model_training_job_for_group,
    create_scheduler,
    schedule_model_training_nightly,
)


@pytest.fixture
def _isolated_model_training_env(tmp_path, monkeypatch):
    import config.settings as settings_mod

    models_dir = tmp_path / "models"
    models_dir.mkdir()
    monkeypatch.setattr(settings_mod, "MODELS_DIR", models_dir)
    monkeypatch.setattr(settings_mod, "PIPELINE_LOG_DB_PATH", tmp_path / "pipeline_log.db")
    return models_dir


class TestModelTrainingGroupsCoverGroupedModels:
    def test_every_group_model_is_a_known_mapped_model(self):
        """Every model listed in a group must actually be a real key in
        _MODEL_TRAINING_SCRIPT_MAP — a typo here would silently mean that
        model is never checked on any night."""
        all_mapped = set(_MODEL_TRAINING_SCRIPT_MAP.keys())
        for group_name, group in _MODEL_TRAINING_GROUPS.items():
            for model in group["models"]:
                assert model in all_mapped, (
                    f"group '{group_name}' lists '{model}', not a key in _MODEL_TRAINING_SCRIPT_MAP"
                )

    def test_every_mapped_model_belongs_to_exactly_one_group(self):
        """No model should be silently left out of every night's rotation,
        and no model should be double-scheduled across two different
        nights (which would retrain it twice in the same week)."""
        all_mapped = set(_MODEL_TRAINING_SCRIPT_MAP.keys())
        grouped_models = [m for group in _MODEL_TRAINING_GROUPS.values() for m in group["models"]]

        assert len(grouped_models) == len(set(grouped_models)), "a model appears in more than one group"
        assert set(grouped_models) == all_mapped, (
            "_MODEL_TRAINING_GROUPS does not exactly partition _MODEL_TRAINING_SCRIPT_MAP's models"
        )

    def test_groups_are_spread_across_distinct_weeknights(self):
        days = [group["day_of_week"] for group in _MODEL_TRAINING_GROUPS.values()]
        assert len(days) == len(set(days)), "two groups share the same day_of_week"
        weekend_days = {"fri", "sat", "sun"}
        assert not (set(days) & weekend_days), (
            "a training group is scheduled on a weekend day — those nights are reserved for "
            "weekend_feature_backfill/weekend_fundamentals/multibagger_scoring/forensic_scoring"
        )


class TestExecuteModelTrainingJobForGroup:
    def test_unknown_group_name_logs_and_returns_without_raising(self, _isolated_model_training_env):
        _execute_model_training_job_for_group("not_a_real_group")  # must not raise

    def test_only_triggers_retrains_for_its_own_group(self, _isolated_model_training_env, monkeypatch):
        """A 'phase2' night must never trigger a 'multibagger' or 'phase1'
        retrain, even if those models are also overdue — that's the whole
        point of spreading training across nights."""
        registry = {
            name: {"last_trained_date": "2020-01-01", "training_interval_days": 28}
            for name in _MODEL_TRAINING_SCRIPT_MAP
        }
        registry_path = _isolated_model_training_env / "registry.json"
        registry_path.write_text(json.dumps(registry))

        triggered = []
        monkeypatch.setattr(
            "ingestion.scheduler.pipeline_scheduler._trigger_model_retrain",
            lambda model_name: triggered.append(model_name),
        )

        _execute_model_training_job_for_group("phase2")

        assert set(triggered) == {"signal_63d"}

    def test_records_heartbeat_under_group_specific_job_id(self, _isolated_model_training_env, monkeypatch):
        from datastore.api.db import get_sqlite_connection
        from datastore.schema.create_signals import create_scheduler_heartbeats_schema
        import config.settings as settings_mod

        create_scheduler_heartbeats_schema(db_path=settings_mod.PIPELINE_LOG_DB_PATH)
        # No registry.json -> "registry.json not found" skip path, but
        # still must record its own heartbeat under the group's job_id.
        _execute_model_training_job_for_group("multibagger")

        with get_sqlite_connection(settings_mod.PIPELINE_LOG_DB_PATH) as conn:
            row = conn.execute(
                "SELECT job_id, last_status FROM scheduler_heartbeats WHERE job_id = ?",
                ("model_training_multibagger",),
            ).fetchone()
        assert row is not None
        assert row[1] == "skipped"


class TestScheduleModelTrainingNightlyRegistersOneJobPerGroup:
    def test_registers_four_distinct_jobs(self):
        scheduler = create_scheduler(db_path=None)
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as tmp:
            scheduler = create_scheduler(db_path=Path(tmp) / "jobstore.db")
            schedule_model_training_nightly(scheduler)
            job_ids = {job.id for job in scheduler.get_jobs()}
            assert job_ids == {f"model_training_{name}" for name in _MODEL_TRAINING_GROUPS}
