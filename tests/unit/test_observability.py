"""
tests/unit/test_observability.py

Phase: 0.6 (Data Quality & Observability)
Specs: SPEC-OBS-001 through SPEC-OBS-005
Owner: Platform / Observability
Consumers: CI, pytest

Unit tests for config/observability.py.
"""

import json

import pytest

import config.observability as obs


# ===== is_enabled / is_production_mode =====


def test_is_enabled_reflects_master_switch(monkeypatch):
    monkeypatch.setattr(obs, "OBSERVABILITY_ENABLED", True)
    assert obs.is_enabled() is True
    monkeypatch.setattr(obs, "OBSERVABILITY_ENABLED", False)
    assert obs.is_enabled() is False


@pytest.mark.parametrize(
    "level,expected",
    [("error", True), ("warning", True), ("info", False), ("debug", False), ("off", False)],
)
def test_is_production_mode_per_spec_obs_005(monkeypatch, level, expected):
    """SPEC-OBS-005: production is defined as exactly {'error', 'warning'}."""
    monkeypatch.setattr(obs, "OBSERVABILITY_LEVEL", level)
    assert obs.is_production_mode() is expected


# ===== should_log =====


def test_should_log_raises_on_invalid_level():
    with pytest.raises(ValueError, match="Invalid event_level"):
        obs.should_log("critical")


def test_should_log_raises_on_off_as_event_level():
    """'off' is a configuration value, not a valid event severity."""
    with pytest.raises(ValueError, match="Invalid event_level"):
        obs.should_log("off")


def test_should_log_false_when_disabled(monkeypatch):
    monkeypatch.setattr(obs, "OBSERVABILITY_ENABLED", False)
    monkeypatch.setattr(obs, "OBSERVABILITY_LEVEL", "debug")
    assert obs.should_log("error") is False


def test_should_log_false_when_level_is_off(monkeypatch):
    monkeypatch.setattr(obs, "OBSERVABILITY_ENABLED", True)
    monkeypatch.setattr(obs, "OBSERVABILITY_LEVEL", "off")
    assert obs.should_log("error") is False


def test_should_log_error_always_passes_at_any_non_off_level(monkeypatch):
    monkeypatch.setattr(obs, "OBSERVABILITY_ENABLED", True)
    for level in ("error", "warning", "info", "debug"):
        monkeypatch.setattr(obs, "OBSERVABILITY_LEVEL", level)
        assert obs.should_log("error") is True


def test_should_log_respects_verbosity_ordering(monkeypatch):
    """SPEC-OBS-002: each level includes everything less verbose than it, not more."""
    monkeypatch.setattr(obs, "OBSERVABILITY_ENABLED", True)
    monkeypatch.setattr(obs, "OBSERVABILITY_LEVEL", "warning")

    assert obs.should_log("error") is True
    assert obs.should_log("warning") is True
    assert obs.should_log("info") is False
    assert obs.should_log("debug") is False


# ===== allow_intermediate_file_write =====


def test_allow_intermediate_file_write_false_in_production(monkeypatch):
    monkeypatch.setattr(obs, "OBSERVABILITY_ENABLED", True)
    monkeypatch.setattr(obs, "OBSERVABILITY_LEVEL", "warning")
    assert obs.allow_intermediate_file_write() is False


def test_allow_intermediate_file_write_true_in_development(monkeypatch):
    monkeypatch.setattr(obs, "OBSERVABILITY_ENABLED", True)
    monkeypatch.setattr(obs, "OBSERVABILITY_LEVEL", "info")
    assert obs.allow_intermediate_file_write() is True


def test_allow_intermediate_file_write_false_when_disabled(monkeypatch):
    monkeypatch.setattr(obs, "OBSERVABILITY_ENABLED", False)
    monkeypatch.setattr(obs, "OBSERVABILITY_LEVEL", "info")
    assert obs.allow_intermediate_file_write() is False


# ===== NoOpObservability =====


def test_noop_log_event_returns_none_and_does_nothing():
    noop = obs.NoOpObservability()
    assert noop.log_event("anything", level="debug", foo="bar") is None


# ===== JSONLObservability =====


def test_jsonl_log_event_writes_when_allowed(monkeypatch, tmp_path):
    log_path = tmp_path / "observability.jsonl"
    monkeypatch.setattr(obs, "OBSERVABILITY_LOG_PATH", log_path)
    monkeypatch.setattr(obs, "OBSERVABILITY_ENABLED", True)
    monkeypatch.setattr(obs, "OBSERVABILITY_LEVEL", "debug")

    emitter = obs.JSONLObservability()
    emitter.log_event("step_complete", level="info", step_id="download_bhavcopy", duration=1.2)

    lines = log_path.read_text().splitlines()
    assert len(lines) == 1
    event = json.loads(lines[0])
    assert event["event_type"] == "step_complete"
    assert event["level"] == "info"
    assert event["step_id"] == "download_bhavcopy"
    assert event["duration"] == 1.2
    assert "timestamp" in event


def test_jsonl_log_event_skips_write_when_not_allowed(monkeypatch, tmp_path):
    log_path = tmp_path / "observability.jsonl"
    monkeypatch.setattr(obs, "OBSERVABILITY_LOG_PATH", log_path)
    monkeypatch.setattr(obs, "OBSERVABILITY_ENABLED", True)
    monkeypatch.setattr(obs, "OBSERVABILITY_LEVEL", "error")

    emitter = obs.JSONLObservability()
    emitter.log_event("step_start", level="info")

    assert not log_path.exists()


def test_jsonl_log_event_creates_parent_dir(monkeypatch, tmp_path):
    log_path = tmp_path / "nested" / "observability.jsonl"
    monkeypatch.setattr(obs, "OBSERVABILITY_LOG_PATH", log_path)
    monkeypatch.setattr(obs, "OBSERVABILITY_ENABLED", True)
    monkeypatch.setattr(obs, "OBSERVABILITY_LEVEL", "debug")

    obs.JSONLObservability().log_event("step_start", level="info")

    assert log_path.exists()


def test_jsonl_log_event_appends_across_calls(monkeypatch, tmp_path):
    log_path = tmp_path / "observability.jsonl"
    monkeypatch.setattr(obs, "OBSERVABILITY_LOG_PATH", log_path)
    monkeypatch.setattr(obs, "OBSERVABILITY_ENABLED", True)
    monkeypatch.setattr(obs, "OBSERVABILITY_LEVEL", "debug")

    emitter = obs.JSONLObservability()
    emitter.log_event("step_start", level="info")
    emitter.log_event("step_complete", level="info")

    assert len(log_path.read_text().splitlines()) == 2


# ===== get_observability =====


def test_get_observability_returns_noop_when_disabled(monkeypatch):
    monkeypatch.setattr(obs, "OBSERVABILITY_ENABLED", False)
    assert isinstance(obs.get_observability(), obs.NoOpObservability)


def test_get_observability_returns_jsonl_when_enabled(monkeypatch):
    monkeypatch.setattr(obs, "OBSERVABILITY_ENABLED", True)
    assert isinstance(obs.get_observability(), obs.JSONLObservability)
