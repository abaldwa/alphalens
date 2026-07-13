"""
tests/unit/test_model_usage_audit.py

Phase: Pipeline & Monitoring Remediation, Phase 4 (A53)
Owner: Platform / ML Signal Engine
Consumers: CI, pytest

Unit tests for ingestion/scheduler/model_usage_audit.py's "trained but
unused" detector.
"""

import json

from ingestion.scheduler.model_usage_audit import (
    CONSUMERS,
    find_trained_but_unused_models,
)
from ingestion.scheduler.pipeline_scheduler import _MODEL_TRAINING_SCRIPT_MAP


class TestConsumersMapCompleteness:
    def test_every_scheduler_mapped_model_has_a_consumers_entry(self):
        """A model absent from CONSUMERS entirely would silently never be
        flagged even if trained-but-unused — every model the scheduler
        knows how to train must at least have a (possibly None) entry."""
        for model_name in _MODEL_TRAINING_SCRIPT_MAP:
            assert model_name in CONSUMERS, (
                f"'{model_name}' is trainable via _MODEL_TRAINING_SCRIPT_MAP but has no "
                "model_usage_audit.CONSUMERS entry"
            )


class TestFindTrainedButUnusedModels:
    def test_no_registry_file_returns_empty(self, tmp_path):
        assert find_trained_but_unused_models(tmp_path / "missing_registry.json") == []

    def test_never_trained_model_not_flagged(self, tmp_path):
        registry_path = tmp_path / "registry.json"
        registry_path.write_text(json.dumps({"tft": {"training_interval_days": 28}}))
        assert find_trained_but_unused_models(registry_path) == []

    def test_trained_model_with_no_consumer_is_flagged(self, tmp_path):
        registry_path = tmp_path / "registry.json"
        registry_path.write_text(json.dumps({
            "tft": {"last_trained_date": "2026-07-01", "training_interval_days": 28},
        }))
        findings = find_trained_but_unused_models(registry_path)
        assert len(findings) == 1
        assert findings[0].model_name == "tft"
        assert findings[0].last_trained_date == "2026-07-01"

    def test_trained_model_with_a_real_consumer_is_not_flagged(self, tmp_path):
        registry_path = tmp_path / "registry.json"
        registry_path.write_text(json.dumps({
            "signal_5d": {"last_trained_date": "2026-07-01", "training_interval_days": 28},
        }))
        assert find_trained_but_unused_models(registry_path) == []

    def test_unmapped_model_is_flagged_not_silently_ignored(self, tmp_path):
        registry_path = tmp_path / "registry.json"
        registry_path.write_text(json.dumps({
            "brand_new_model_nobody_added_to_consumers": {
                "last_trained_date": "2026-07-01", "training_interval_days": 28,
            },
        }))
        findings = find_trained_but_unused_models(registry_path)
        assert len(findings) == 1
        assert findings[0].model_name == "brand_new_model_nobody_added_to_consumers"

    def test_malformed_registry_returns_empty_not_raises(self, tmp_path):
        registry_path = tmp_path / "registry.json"
        registry_path.write_text("not valid json {{{")
        assert find_trained_but_unused_models(registry_path) == []
