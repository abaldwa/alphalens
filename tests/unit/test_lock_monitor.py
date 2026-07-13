"""
tests/unit/test_lock_monitor.py

Phase: Pipeline & Monitoring Remediation, Phase 2
Owner: Platform / Scheduler
Consumers: CI, pytest

Verifies ingestion/scheduler/lock_monitor.py correctly detects a
currently-held fcntl.flock lock without itself disturbing it, and that
checking status is safe to call repeatedly (never leaves an extra lock
behind).
"""

import fcntl

from ingestion.scheduler import lock_monitor


class TestProbeLock:
    def test_nonexistent_file_is_not_locked(self, tmp_path):
        missing = tmp_path / "does_not_exist.lock"
        locked, mtime = lock_monitor._probe_lock(missing)
        assert locked is False
        assert mtime is None

    def test_unlocked_existing_file_is_not_locked(self, tmp_path):
        lock_path = tmp_path / "test.lock"
        lock_path.write_text("")
        locked, mtime = lock_monitor._probe_lock(lock_path)
        assert locked is False
        assert mtime is not None

    def test_held_lock_is_detected(self, tmp_path):
        lock_path = tmp_path / "test.lock"
        lock_path.write_text("")

        with open(lock_path, "r+") as holder:
            fcntl.flock(holder.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            locked, _ = lock_monitor._probe_lock(lock_path)
            assert locked is True
            fcntl.flock(holder.fileno(), fcntl.LOCK_UN)

        # Released — must now read as unlocked.
        locked_after, _ = lock_monitor._probe_lock(lock_path)
        assert locked_after is False

    def test_probing_never_leaves_a_stray_lock_behind(self, tmp_path):
        """A monitor that itself contends for the lock would be worse
        than no monitor at all — checking status must be side-effect
        free."""
        lock_path = tmp_path / "test.lock"
        lock_path.write_text("")

        lock_monitor._probe_lock(lock_path)

        # A fresh holder must still be able to acquire it immediately.
        with open(lock_path, "r+") as holder:
            fcntl.flock(holder.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(holder.fileno(), fcntl.LOCK_UN)


class TestLockStatusHelpers:
    def test_pipeline_run_lock_status_reads_configured_path(self, tmp_path, monkeypatch):
        import config.settings as settings_mod

        lock_path = tmp_path / "pipeline_run.lock"
        monkeypatch.setattr(settings_mod, "PIPELINE_RUN_LOCK_PATH", lock_path)

        status = lock_monitor.pipeline_run_lock_status()
        assert status.name == "pipeline_run_lock"
        assert status.path == str(lock_path)
        assert status.exists is False
        assert status.locked is False

    def test_publish_run_lock_status_reads_configured_path(self, tmp_path, monkeypatch):
        import config.settings as settings_mod

        lock_path = tmp_path / "publish_run.lock"
        lock_path.write_text("")
        monkeypatch.setattr(settings_mod, "PUBLISH_RUN_LOCK_PATH", lock_path)

        status = lock_monitor.publish_run_lock_status()
        assert status.name == "publish_run_lock"
        assert status.exists is True
        assert status.locked is False

    def test_all_lock_statuses_returns_both(self, tmp_path, monkeypatch):
        import config.settings as settings_mod

        monkeypatch.setattr(settings_mod, "PIPELINE_RUN_LOCK_PATH", tmp_path / "a.lock")
        monkeypatch.setattr(settings_mod, "PUBLISH_RUN_LOCK_PATH", tmp_path / "b.lock")

        statuses = lock_monitor.all_lock_statuses()
        assert {s.name for s in statuses} == {"pipeline_run_lock", "publish_run_lock"}
