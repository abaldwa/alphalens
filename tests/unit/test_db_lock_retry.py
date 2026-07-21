"""
tests/unit/test_db_lock_retry.py

REV27 (2026-07-21 review): datastore/api/db.py's DuckDB lock-conflict retry
budget (SPEC-SCHED-013) was hardcoded; moved to config.settings
(DUCKDB_LOCK_RETRY_ATTEMPTS/DUCKDB_LOCK_RETRY_BASE_DELAY_S), env-overridable,
with the default attempt count raised 4 -> 6. Verifies db.py actually reads
these from config.settings (not a stale local copy) and that the retry loop
honors whatever count is configured — using a real duckdb.IOException with
the exact "Could not set lock" substring _connect_with_retry matches on, not
a generic mock.
"""

import duckdb
import pytest

from datastore.api import db as db_module


def test_module_level_constants_sourced_from_settings():
    from config.settings import DUCKDB_LOCK_RETRY_ATTEMPTS, DUCKDB_LOCK_RETRY_BASE_DELAY_S

    assert db_module.DUCKDB_LOCK_RETRY_ATTEMPTS == DUCKDB_LOCK_RETRY_ATTEMPTS
    assert db_module.DUCKDB_LOCK_RETRY_BASE_DELAY_S == DUCKDB_LOCK_RETRY_BASE_DELAY_S


def test_default_attempts_raised_to_six():
    assert db_module.DUCKDB_LOCK_RETRY_ATTEMPTS == 6


class TestConnectWithRetry:
    def test_retries_configured_attempt_count_then_succeeds(self, monkeypatch, tmp_path):
        monkeypatch.setattr(db_module, "DUCKDB_LOCK_RETRY_ATTEMPTS", 3)
        monkeypatch.setattr(db_module, "DUCKDB_LOCK_RETRY_BASE_DELAY_S", 0.0)  # don't actually sleep in tests
        monkeypatch.setattr(db_module.time, "sleep", lambda *_: None)

        real_path = str(tmp_path / "retry_test.duckdb")
        calls = {"n": 0}
        real_connect = duckdb.connect

        def flaky_connect(path, read_only=False):
            calls["n"] += 1
            if calls["n"] < 3:
                raise duckdb.IOException("Could not set lock on file")
            return real_connect(path, read_only=read_only)

        monkeypatch.setattr(db_module.duckdb, "connect", flaky_connect)
        conn = db_module._connect_with_retry(real_path, read_only=False)
        try:
            assert calls["n"] == 3
        finally:
            conn.close()

    def test_gives_up_after_configured_attempts_exhausted(self, monkeypatch, tmp_path):
        monkeypatch.setattr(db_module, "DUCKDB_LOCK_RETRY_ATTEMPTS", 2)
        monkeypatch.setattr(db_module, "DUCKDB_LOCK_RETRY_BASE_DELAY_S", 0.0)
        monkeypatch.setattr(db_module.time, "sleep", lambda *_: None)

        def always_locked(path, read_only=False):
            raise duckdb.IOException("Could not set lock on file")

        monkeypatch.setattr(db_module.duckdb, "connect", always_locked)
        with pytest.raises(duckdb.IOException):
            db_module._connect_with_retry(str(tmp_path / "always_locked.duckdb"), read_only=False)

    def test_non_lock_ioexception_raises_immediately_no_retry(self, monkeypatch, tmp_path):
        monkeypatch.setattr(db_module, "DUCKDB_LOCK_RETRY_ATTEMPTS", 5)
        calls = {"n": 0}

        def other_failure(path, read_only=False):
            calls["n"] += 1
            raise duckdb.IOException("disk full or some other unrelated I/O error")

        monkeypatch.setattr(db_module.duckdb, "connect", other_failure)
        with pytest.raises(duckdb.IOException):
            db_module._connect_with_retry(str(tmp_path / "other_failure.duckdb"), read_only=False)
        assert calls["n"] == 1  # no retry for a non-lock-conflict IOException
