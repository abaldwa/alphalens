"""
ingestion/scheduler/lock_monitor.py

Phase: Pipeline & Monitoring Remediation, Phase 2
Owner: Platform / Scheduler
Consumers: datastore/api/routers/ops.py (lock-status panel)

Read-only status probe for the two cross-process fcntl.flock advisory
locks in this codebase — PIPELINE_RUN_LOCK_PATH
(ingestion/scheduler/pipeline_scheduler.py::pipeline_run_lock) and
PUBLISH_RUN_LOCK_PATH (datastore/staging/publish.py::publish_run_lock).
Neither lock previously had any external visibility: an operator had no
way to see "is a lock currently held, and for how long" short of reading
scheduler logs by hand. This module answers that without disturbing the
lock itself — a non-blocking flock probe that immediately releases if it
succeeds, so checking status never itself contends for the lock.

Caveat on `last_modified_at`: both lock-holder context managers open
their lock file in "w" mode (truncating) on EVERY call, whether or not
the lock is actually acquired — so the file's mtime reflects "last
attempt to acquire this lock", not "when the current holder acquired
it". Treat it as "activity around this lock as of this time", not a
precise hold-duration timer.
"""

from __future__ import annotations

import fcntl
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class LockStatus:
    name: str
    path: str
    exists: bool
    locked: bool
    last_modified_at: Optional[str]  # ISO 8601, local mtime — see module caveat


def _probe_lock(path: Path) -> tuple[bool, Optional[str]]:
    """
    Non-blocking check of whether `path` is currently flock'd by another
    process. Returns (is_locked, last_modified_at_iso_or_None).
    """
    if not path.exists():
        return False, None

    last_modified_at = datetime.fromtimestamp(path.stat().st_mtime).isoformat()

    try:
        with open(path, "r+") as fh:
            try:
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
                return False, last_modified_at
            except BlockingIOError:
                return True, last_modified_at
    except OSError:
        return False, last_modified_at


def pipeline_run_lock_status() -> LockStatus:
    from config.settings import PIPELINE_RUN_LOCK_PATH

    locked, mtime = _probe_lock(PIPELINE_RUN_LOCK_PATH)
    return LockStatus(
        name="pipeline_run_lock",
        path=str(PIPELINE_RUN_LOCK_PATH),
        exists=PIPELINE_RUN_LOCK_PATH.exists(),
        locked=locked,
        last_modified_at=mtime,
    )


def publish_run_lock_status() -> LockStatus:
    from config.settings import PUBLISH_RUN_LOCK_PATH

    locked, mtime = _probe_lock(PUBLISH_RUN_LOCK_PATH)
    return LockStatus(
        name="publish_run_lock",
        path=str(PUBLISH_RUN_LOCK_PATH),
        exists=PUBLISH_RUN_LOCK_PATH.exists(),
        locked=locked,
        last_modified_at=mtime,
    )


def all_lock_statuses() -> list[LockStatus]:
    return [pipeline_run_lock_status(), publish_run_lock_status()]
