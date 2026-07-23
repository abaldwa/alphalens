"""
tests/unit/test_batch_common.py

Unit tests for backtest/batch_common.py's exclusive_backtest_lock — the
system-wide mutex ensuring at most one backtest job runs at a time,
regardless of which queue/trigger launched it.
"""

import multiprocessing
import time

import pytest

from backtest.batch_common import exclusive_backtest_lock


def _hold_lock_in_subprocess(lock_path, hold_seconds, started_event, done_event):
    with exclusive_backtest_lock(lock_path=lock_path, label="child"):
        started_event.set()
        time.sleep(hold_seconds)
    done_event.set()


class TestExclusiveBacktestLock:
    def test_single_caller_acquires_and_releases(self, tmp_path):
        lock_path = tmp_path / "lock"
        with exclusive_backtest_lock(lock_path=lock_path, label="test"):
            pass  # no error = acquired and released cleanly

    def test_second_caller_waits_for_first_to_release(self, tmp_path):
        lock_path = tmp_path / "lock"
        ctx = multiprocessing.get_context("spawn")
        started = ctx.Event()
        done = ctx.Event()
        proc = ctx.Process(target=_hold_lock_in_subprocess, args=(lock_path, 1.5, started, done))
        proc.start()
        assert started.wait(timeout=5), "child never acquired the lock"

        # Parent should block here until the child releases (~1.5s later).
        acquire_started = time.monotonic()
        with exclusive_backtest_lock(lock_path=lock_path, wait_timeout_s=10, poll_interval_s=0.2, label="parent"):
            waited = time.monotonic() - acquire_started
        proc.join(timeout=5)

        assert done.is_set()
        assert waited >= 1.0, f"parent acquired the lock too early ({waited:.2f}s) — it should have waited for the child"

    def test_times_out_if_never_released(self, tmp_path):
        lock_path = tmp_path / "lock"
        ctx = multiprocessing.get_context("spawn")
        started = ctx.Event()
        done = ctx.Event()
        proc = ctx.Process(target=_hold_lock_in_subprocess, args=(lock_path, 5.0, started, done))
        proc.start()
        assert started.wait(timeout=5)
        try:
            with pytest.raises(RuntimeError, match="could not acquire"):
                with exclusive_backtest_lock(lock_path=lock_path, wait_timeout_s=0.5, poll_interval_s=0.1, label="parent"):
                    pass
        finally:
            proc.terminate()
            proc.join(timeout=5)

    def test_lock_released_after_exception_inside_block(self, tmp_path):
        lock_path = tmp_path / "lock"
        with pytest.raises(ValueError):
            with exclusive_backtest_lock(lock_path=lock_path, label="test"):
                raise ValueError("boom")
        # A subsequent acquire must succeed immediately — proves release-on-exception.
        with exclusive_backtest_lock(lock_path=lock_path, wait_timeout_s=1, label="test2"):
            pass
