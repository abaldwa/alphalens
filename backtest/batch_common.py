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

import logging
import time
from typing import Optional

logger = logging.getLogger(__name__)


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
