"""
ingestion/scheduler/pipeline_run_lock.py

Cross-process advisory lock for the daily pipeline, extracted from
pipeline_scheduler.py (A46 — per-concern module split).

Consumers: pipeline_steps.py, scheduler_jobs.py, datastore/api/routers/ops.py
           (also re-exported via pipeline_scheduler.py for backward compat)
"""

import contextlib
import fcntl
import logging
from typing import Iterator

logger = logging.getLogger(__name__)


@contextlib.contextmanager
def pipeline_run_lock() -> Iterator[bool]:
    """
    Cross-process, non-blocking advisory lock (fcntl.flock) guarding every
    call to run_steps_for_date.

    See pipeline_scheduler.py docstring for the full incident history and
    architectural notes.

    Yields True if this call acquired the lock. False if another process
    already holds it.
    """
    from config.settings import PIPELINE_RUN_LOCK_PATH

    try:
        PIPELINE_RUN_LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
        lock_file = open(PIPELINE_RUN_LOCK_PATH, "w")
    except OSError as exc:
        logger.warning(f"pipeline_run_lock: could not open lock file ({exc}) — proceeding without it")
        yield True
        return

    acquired = False
    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        acquired = True
        yield True
    except BlockingIOError:
        yield False
    finally:
        if acquired:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        lock_file.close()