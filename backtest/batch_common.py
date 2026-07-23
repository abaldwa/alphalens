"""
backtest/batch_common.py

Owner: Platform / Backtest
Consumers: backtest/run_batch_backtest.py, backtest/run_strategy_queue.py

Shared OOM guard for every "run several backtests sequentially, as
isolated subprocesses" script — extracted out of run_batch_backtest.py
(the first one built) so run_strategy_queue.py doesn't duplicate it.
See run_batch_backtest.py's module docstring for the full rationale
(subprocess isolation + sequential-not-parallel execution + a start-of
-job memory gate); this module is just the gate itself.
"""

import contextlib
import fcntl
import logging
import time
from pathlib import Path
from typing import Iterator, Optional

logger = logging.getLogger(__name__)

DEFAULT_LOCK_PATH = Path(__file__).resolve().parent / "reports" / ".backtest_exec.lock"


def available_mb() -> Optional[float]:
    try:
        import psutil

        return psutil.virtual_memory().available / (1024 * 1024)
    except ImportError:  # pragma: no cover - psutil is a hard dependency elsewhere in the repo
        return None


def wait_for_headroom(
    min_free_mb: float, wait_timeout_s: float, poll_interval_s: float = 10.0, label: str = "batch_common",
) -> None:
    """Blocks until system-available memory clears `min_free_mb`, or raises after `wait_timeout_s`.
    `label` is just a log-line prefix so callers' logs are distinguishable."""
    available = available_mb()
    if available is None:
        logger.warning(f"{label}: psutil unavailable — skipping the pre-flight memory check")
        return

    deadline = time.monotonic() + wait_timeout_s
    while available < min_free_mb:
        if time.monotonic() >= deadline:
            raise RuntimeError(
                f"{label}: only {available:.0f}MB free after waiting {wait_timeout_s:.0f}s "
                f"(need >= {min_free_mb:.0f}MB) — aborting the remainder of the run rather than risk an OOM kill"
            )
        logger.info(f"{label}: {available:.0f}MB free, below {min_free_mb:.0f}MB floor — waiting...")
        time.sleep(poll_interval_s)
        available = available_mb()


@contextlib.contextmanager
def exclusive_backtest_lock(
    lock_path: Path = DEFAULT_LOCK_PATH, wait_timeout_s: float = 3600.0, poll_interval_s: float = 5.0,
    label: str = "batch_common",
) -> Iterator[None]:
    """
    System-wide mutex: at most one backtest job (an orchestrator run or an
    iterative retrain) executes at a time, regardless of which queue or
    trigger launched it — user-confirmed requirement, backtests must run
    strictly sequentially, never concurrently, even across independently
    -triggered queues/single-run triggers (caught live: a 42-job Technical
    queue and a 6-job Fundamental/Momentum/ML queue running at the same
    time both starved on DB-lock contention and started failing).

    wait_for_headroom() alone doesn't cover this — it only checks free
    memory at THIS job's own launch, with no notion of "another job is
    already mid-flight." run_strategy_queue.py's own loop already
    serializes jobs WITHIN one queue, but does nothing to serialize
    ACROSS separately-triggered queues/direct single-run triggers — this
    lock is what does that, and every entry point that actually runs a
    backtest (run_orchestrator_backtest.py, run_iterative_backtest.py)
    acquires it around its real work, so it applies no matter how a job
    was launched.

    Uses a POSIX advisory flock (fcntl) rather than a lock-file+PID
    scheme: the kernel releases the lock automatically the instant the
    holding process's file descriptor closes — including on a hard crash
    — so a crashed job can never permanently wedge every future job
    behind a stale lock the way a PID-file convention could.
    """
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fd = open(lock_path, "w")
    deadline = time.monotonic() + wait_timeout_s
    acquired = False
    try:
        while not acquired:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                acquired = True
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise RuntimeError(
                        f"{label}: could not acquire the backtest execution lock after waiting "
                        f"{wait_timeout_s:.0f}s — another backtest job is still running"
                    )
                logger.info(f"{label}: another backtest job is currently running — waiting for it to finish...")
                time.sleep(poll_interval_s)
        logger.info(f"{label}: acquired the backtest execution lock")
        yield
    finally:
        if acquired:
            fcntl.flock(fd, fcntl.LOCK_UN)
            logger.info(f"{label}: released the backtest execution lock")
        fd.close()
