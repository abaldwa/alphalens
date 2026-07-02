"""
datastore/api/utils/file_lock.py

Phase: 3.x (Paper Trading Pending Actions)
Specs: SPEC-PT-003
Owner: Platform / DataStore
Consumers: datastore/api/routers/paper_trading.py, scripts/run_daily_paper_trading.py

Before SPEC-PT-003, only one process ever wrote paper_trading/portfolio_state.json
(the daily bot) — single-writer by convention, no lock needed. The new
accept-proposal endpoint is a second writer that can run at any time, so any
read-modify-write of portfolio_state.json (or the pending-actions file) must
now be serialized against the bot's own writes. flock is sufficient here
(single-laptop deployment, Ubuntu — see CLAUDE.md's OS pin); no Redis/DB
lock is justified.
"""

import fcntl
import logging
from contextlib import contextmanager
from pathlib import Path

logger = logging.getLogger(__name__)


@contextmanager
def locked_file(target_path: Path):
    """
    Hold an exclusive flock on `target_path.with_suffix(target_path.suffix + '.lock')`
    for the duration of the `with` block. Blocks (does not raise) if another
    process holds the lock — callers doing a quick read-modify-write don't
    need a timeout.

    Usage
    -----
    with locked_file(PORTFOLIO_STATE_PATH):
        portfolio = load_portfolio_state(PORTFOLIO_STATE_PATH)
        ...
        save_portfolio_state(portfolio, PORTFOLIO_STATE_PATH, ...)
    """
    lock_path = target_path.parent / (target_path.name + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_path, "w") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_file, fcntl.LOCK_UN)
