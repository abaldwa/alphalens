"""
tests/unit/test_file_lock.py

A65: real-flock tests for `datastore/api/utils/file_lock.py` (SPEC-PT-003),
previously untested (50% coverage, no test file). Uses real `fcntl.flock`
via `locked_file` against tmp_path files — no mocks.
"""

import fcntl

from datastore.api.utils.file_lock import locked_file


class TestLockedFile:
    def test_creates_lock_file_with_expected_suffix(self, tmp_path):
        target = tmp_path / "portfolio_state.json"
        with locked_file(target):
            assert (tmp_path / "portfolio_state.json.lock").exists()

    def test_creates_parent_directory_if_missing(self, tmp_path):
        target = tmp_path / "nested" / "dir" / "portfolio_state.json"
        with locked_file(target):
            assert (tmp_path / "nested" / "dir").is_dir()

    def test_lock_released_after_context_exits(self, tmp_path):
        target = tmp_path / "portfolio_state.json"
        with locked_file(target):
            pass
        # Confirm the lock is actually released: acquire a non-blocking
        # exclusive lock on the same lock file from a fresh file handle —
        # this raises BlockingIOError if the prior context didn't unlock.
        lock_path = tmp_path / "portfolio_state.json.lock"
        with open(lock_path, "w") as f:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(f, fcntl.LOCK_UN)

    def test_body_runs_and_can_write_target_file(self, tmp_path):
        target = tmp_path / "portfolio_state.json"
        with locked_file(target):
            target.write_text('{"cash": 100000}')
        assert target.read_text() == '{"cash": 100000}'

    def test_exception_inside_block_still_releases_lock(self, tmp_path):
        target = tmp_path / "portfolio_state.json"
        try:
            with locked_file(target):
                raise RuntimeError("boom")
        except RuntimeError:
            pass
        lock_path = tmp_path / "portfolio_state.json.lock"
        with open(lock_path, "w") as f:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(f, fcntl.LOCK_UN)
