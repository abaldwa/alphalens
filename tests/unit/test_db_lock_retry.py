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


class TestPerCallRetryOverride:
    """2026-07-26: backtest jobs' write connection to BACKTEST_DUCKDB_PATH
    needs a wider retry budget than the API's read-only endpoints, which
    share the same _connect_with_retry/get_duckdb_connection. Verifies the
    override params take precedence over the module-level settings, and
    that omitting them falls back to the module defaults unchanged (so the
    API's read-only callers, which never pass these, are unaffected)."""

    def test_override_attempts_takes_precedence_over_module_setting(self, monkeypatch, tmp_path):
        monkeypatch.setattr(db_module, "DUCKDB_LOCK_RETRY_ATTEMPTS", 2)  # module default: give up fast
        monkeypatch.setattr(db_module.time, "sleep", lambda *_: None)

        calls = {"n": 0}
        real_connect = duckdb.connect
        real_path = str(tmp_path / "override_test.duckdb")

        def flaky_connect(path, read_only=False):
            calls["n"] += 1
            if calls["n"] < 4:
                raise duckdb.IOException("Could not set lock on file")
            return real_connect(path, read_only=read_only)

        monkeypatch.setattr(db_module.duckdb, "connect", flaky_connect)
        # Module default (2 attempts) would give up before succeeding on the
        # 4th call — passing a wider override must let it ride out longer.
        conn = db_module._connect_with_retry(
            real_path, read_only=False, retry_attempts=5, retry_base_delay_s=0.0,
        )
        try:
            assert calls["n"] == 4
        finally:
            conn.close()

    def test_max_delay_caps_exponential_backoff(self, monkeypatch, tmp_path):
        monkeypatch.setattr(db_module, "DUCKDB_LOCK_RETRY_ATTEMPTS", 10)
        sleeps = []
        monkeypatch.setattr(db_module.time, "sleep", lambda d: sleeps.append(d))

        def always_locked(path, read_only=False):
            raise duckdb.IOException("Could not set lock on file")

        monkeypatch.setattr(db_module.duckdb, "connect", always_locked)
        with pytest.raises(duckdb.IOException):
            db_module._connect_with_retry(
                str(tmp_path / "capped.duckdb"), read_only=False,
                retry_attempts=6, retry_base_delay_s=1.0, retry_max_delay_s=3.0,
            )
        # Uncapped would be 1, 2, 4, 8, 16 — capped at 3.0 from the 3rd retry on.
        assert sleeps == [1.0, 2.0, 3.0, 3.0, 3.0]

    def test_no_override_falls_back_to_module_settings_unchanged(self, monkeypatch, tmp_path):
        monkeypatch.setattr(db_module, "DUCKDB_LOCK_RETRY_ATTEMPTS", 2)
        monkeypatch.setattr(db_module, "DUCKDB_LOCK_RETRY_BASE_DELAY_S", 0.0)
        monkeypatch.setattr(db_module.time, "sleep", lambda *_: None)

        def always_locked(path, read_only=False):
            raise duckdb.IOException("Could not set lock on file")

        monkeypatch.setattr(db_module.duckdb, "connect", always_locked)
        with pytest.raises(duckdb.IOException):
            db_module._connect_with_retry(str(tmp_path / "no_override.duckdb"), read_only=False)

    def test_get_duckdb_connection_threads_override_through_to_connect_with_retry(self, monkeypatch, tmp_path):
        captured = {}
        real_connect_with_retry = db_module._connect_with_retry

        def spy(path_key, read_only, retry_attempts=None, retry_base_delay_s=None, retry_max_delay_s=None):
            captured["retry_attempts"] = retry_attempts
            captured["retry_base_delay_s"] = retry_base_delay_s
            captured["retry_max_delay_s"] = retry_max_delay_s
            return real_connect_with_retry(path_key, read_only, retry_attempts, retry_base_delay_s, retry_max_delay_s)

        monkeypatch.setattr(db_module, "_connect_with_retry", spy)
        real_path = tmp_path / "threaded_override.duckdb"
        with db_module.get_duckdb_connection(
            real_path, read_only=False, persist=False,
            retry_attempts=9, retry_base_delay_s=1.5, retry_max_delay_s=6.0,
        ) as conn:
            conn.execute("SELECT 1").fetchone()
        assert captured == {"retry_attempts": 9, "retry_base_delay_s": 1.5, "retry_max_delay_s": 6.0}
