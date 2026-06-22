"""
tests/unit/test_structured_logger.py

Phase: 0.6 (Data Quality & Observability)
Specs: SPEC-OBS-003, SPEC-SEC-001
Owner: Platform / Observability
Consumers: CI, pytest

Unit tests for ingestion/quality/structured_logger.py.
"""

import json
from datetime import timedelta

import pytest

from config.timezone import now_ist
from ingestion.quality import structured_logger as sl


@pytest.fixture(autouse=True)
def _isolated_logs_dir(monkeypatch, tmp_path):
    """Every test writes to a throwaway directory, never the real datastore/logs."""
    monkeypatch.setattr(sl, "LOGS_DIR", tmp_path)
    monkeypatch.setattr(sl, "is_enabled", lambda: True)
    monkeypatch.setattr(sl, "should_log", lambda level: True)
    return tmp_path


def _read_events(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


# ===== log_pipeline_step: validation =====


def test_unknown_status_raises_value_error():
    """SPEC-OBS-003: status must be one of the documented vocabulary."""
    with pytest.raises(ValueError, match="Unknown status"):
        sl.log_pipeline_step("download_bhavcopy", "bogus", stocks=1, duration_s=1.0)


@pytest.mark.parametrize("bad_stocks", [[1, 2, 3], "460", None, object()])
def test_non_numeric_stocks_raises_type_error(bad_stocks):
    """SPEC-SEC-001: stocks must be a scalar count, never a list/array/DataFrame stand-in."""
    with pytest.raises(TypeError, match="stocks must be a number"):
        sl.log_pipeline_step("download_bhavcopy", "success", stocks=bad_stocks, duration_s=1.0)


def test_bool_stocks_raises_type_error():
    """bool is technically a Number subclass in Python — must still be rejected."""
    with pytest.raises(TypeError, match="stocks must be a number"):
        sl.log_pipeline_step("download_bhavcopy", "success", stocks=True, duration_s=1.0)


@pytest.mark.parametrize("bad_duration", [[1.0], "1.5", None])
def test_non_numeric_duration_raises_type_error(bad_duration):
    with pytest.raises(TypeError, match="duration_s must be a number"):
        sl.log_pipeline_step("download_bhavcopy", "success", stocks=1, duration_s=bad_duration)


def test_non_string_error_raises_type_error():
    """SPEC-SEC-001: error must be a string, never a raw exception object or data structure."""
    with pytest.raises(TypeError, match="error must be None or str"):
        sl.log_pipeline_step(
            "download_bhavcopy", "failed", stocks=0, duration_s=1.0, error=ValueError("boom")
        )


# ===== log_pipeline_step: gating (SPEC-OBS-001/002) =====


def test_disabled_master_switch_writes_nothing(monkeypatch, tmp_path):
    """SPEC-OBS-001: when observability is disabled, zero file writes — not even a directory stat."""
    monkeypatch.setattr(sl, "is_enabled", lambda: False)

    sl.log_pipeline_step("download_bhavcopy", "success", stocks=460, duration_s=12.3)

    assert list(tmp_path.glob("*.jsonl")) == []


def test_info_level_event_skipped_when_should_log_returns_false(monkeypatch, tmp_path):
    """SPEC-OBS-002: a routine (non-failure) event must be gated through should_log('info')."""
    calls = []
    monkeypatch.setattr(sl, "should_log", lambda level: calls.append(level) or False)

    sl.log_pipeline_step("download_bhavcopy", "success", stocks=460, duration_s=12.3)

    assert calls == ["info"]
    assert list(tmp_path.glob("*.jsonl")) == []


def test_failed_status_checked_at_error_level(monkeypatch, tmp_path):
    """SPEC-OBS-002: a failure event must be checked against 'error', not 'info'."""
    calls = []
    monkeypatch.setattr(sl, "should_log", lambda level: calls.append(level) or True)

    sl.log_pipeline_step("compute_features", "failed", stocks=0, duration_s=1.1, error="boom")

    assert calls == ["error"]


# ===== log_pipeline_step: successful writes =====


def test_writes_one_json_line_with_expected_fields(tmp_path):
    sl.log_pipeline_step("download_bhavcopy", "success", stocks=460, duration_s=12.3)

    today_file = tmp_path / f"pipeline_{now_ist().date().isoformat()}.jsonl"
    events = _read_events(today_file)

    assert len(events) == 1
    event = events[0]
    assert event["event_type"] == "pipeline_step"
    assert event["step"] == "download_bhavcopy"
    assert event["status"] == "success"
    assert event["stocks_processed"] == 460
    assert event["duration_seconds"] == 12.3
    assert event["error"] is None
    assert "timestamp" in event


def test_failed_status_includes_error_message(tmp_path):
    sl.log_pipeline_step("compute_features", "failed", stocks=0, duration_s=1.1, error="boom")

    today_file = tmp_path / f"pipeline_{now_ist().date().isoformat()}.jsonl"
    events = _read_events(today_file)

    assert events[0]["status"] == "failed"
    assert events[0]["error"] == "boom"


def test_multiple_calls_append_to_the_same_daily_file(tmp_path):
    sl.log_pipeline_step("download_bhavcopy", "success", stocks=460, duration_s=12.3)
    sl.log_pipeline_step("download_fno", "skipped", stocks=0, duration_s=0.1)

    today_file = tmp_path / f"pipeline_{now_ist().date().isoformat()}.jsonl"
    events = _read_events(today_file)

    assert len(events) == 2
    assert [e["step"] for e in events] == ["download_bhavcopy", "download_fno"]


def test_creates_logs_dir_if_missing(monkeypatch, tmp_path):
    nested = tmp_path / "nested" / "logs"
    assert not nested.exists()
    monkeypatch.setattr(sl, "LOGS_DIR", nested)

    sl.log_pipeline_step("download_bhavcopy", "success", stocks=1, duration_s=0.1)

    assert nested.exists()
    assert len(list(nested.glob("*.jsonl"))) == 1


# ===== prune_old_logs =====


def test_prune_deletes_files_older_than_retention(tmp_path):
    today = now_ist().date()
    old_date = today - timedelta(days=45)
    recent_date = today - timedelta(days=5)

    old_path = tmp_path / f"pipeline_{old_date.isoformat()}.jsonl"
    recent_path = tmp_path / f"pipeline_{recent_date.isoformat()}.jsonl"
    old_path.write_text("{}\n")
    recent_path.write_text("{}\n")

    deleted = sl.prune_old_logs(retention_days=30)

    assert deleted == 1
    assert not old_path.exists()
    assert recent_path.exists()


def test_prune_skips_files_with_malformed_dates(tmp_path):
    bad_path = tmp_path / "pipeline_not-a-date.jsonl"
    bad_path.write_text("{}\n")

    deleted = sl.prune_old_logs(retention_days=30)

    assert deleted == 0
    assert bad_path.exists()


def test_prune_returns_zero_when_logs_dir_missing(monkeypatch, tmp_path):
    missing = tmp_path / "does-not-exist"
    monkeypatch.setattr(sl, "LOGS_DIR", missing)

    assert sl.prune_old_logs(retention_days=30) == 0


def test_prune_keeps_file_exactly_at_retention_boundary(tmp_path):
    """A file exactly `retention_days` old must be kept (only strictly older files are deleted)."""
    today = now_ist().date()
    boundary_date = today - timedelta(days=30)
    boundary_path = tmp_path / f"pipeline_{boundary_date.isoformat()}.jsonl"
    boundary_path.write_text("{}\n")

    deleted = sl.prune_old_logs(retention_days=30)

    assert deleted == 0
    assert boundary_path.exists()
