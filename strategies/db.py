"""
strategies/db.py

Phase: Unified Backtest & Paper Trading Umbrella (A105)
Owner: Platform / Backtest
Consumers: strategies/registry.py, strategies/signals.py.

One place that decides HOW the registry and the signal ledger connect to
BACKTEST_DUCKDB_PATH, because getting that decision wrong is silent.

WHY THIS EXISTS
---------------
registry.py and signals.py each opened their connection with a bare
get_duckdb_connection(db_path) — which means the module defaults:
DUCKDB_LOCK_RETRY_ATTEMPTS=6 at a 0.5s base, a ~15.5s budget sized for the
API's read-only polling endpoints.

But these two modules are called from inside backtest jobs, whose OWN writes
to the same file use DUCKDB_WRITE_LOCK_RETRY_ATTEMPTS=16 / 1.0s base / 10s
cap — a ~125s budget, deliberately widened twice (2026-07-26) after jobs
kept losing the lock. So the ledger write sat immediately next to writes with
eight times its patience, and gave up first.

That is exactly what happened in the 2026-08-14 smoke queue at 3 workers:
four runs logged "strategy_signals write failed" and lost their signals
outright, and other runs fell back to strategy_version=0 (UNVERSIONED)
because resolve_strategy_version's registry READ lost the same race. Both
failures are logged, but neither fails the run — so the ledger quietly
becomes incomplete while every job still reports success. A ledger with
holes in it cannot answer the question it was built for ("is the signal I
backtested the signal I traded?"), and it does not announce that it can't.

persist=False for the same reason it is used everywhere else that shares a
file with another process: a cached connection holds the file lock for the
life of the process. A backtest job writes its ledger in the deferred tail
and then keeps running (finalize, report writing); with persist=True it
would hold BACKTEST_DUCKDB_PATH the whole time, blocking the other workers
it is racing. Here the lock is taken and given back.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterator, Optional

from config.settings import (
    DUCKDB_WRITE_LOCK_RETRY_ATTEMPTS,
    DUCKDB_WRITE_LOCK_RETRY_BASE_DELAY_S,
    DUCKDB_WRITE_LOCK_RETRY_MAX_DELAY_S,
)
from datastore.api.db import get_duckdb_connection


def resolve_db_path(db_path: Optional[Path]) -> Path:
    """BACKTEST_DUCKDB_PATH is the default, imported lazily so that a test
    monkeypatching config.settings.BACKTEST_DUCKDB_PATH is honoured — an
    import-time binding would capture the real path and write test rows into
    the production database."""
    if db_path is not None:
        return db_path
    from config.settings import BACKTEST_DUCKDB_PATH

    return BACKTEST_DUCKDB_PATH


def open_connection(db_path: Optional[Path] = None, *, read_only: bool = False) -> Iterator:
    """A connection to the registry/ledger database, with the backtest
    write-lock retry budget rather than the API's much shorter default.

    Returns the context manager itself (not an open connection) so callers
    keep the `with` semantics get_duckdb_connection already provides.
    """
    return get_duckdb_connection(
        resolve_db_path(db_path),
        read_only=read_only,
        persist=False,
        retry_attempts=DUCKDB_WRITE_LOCK_RETRY_ATTEMPTS,
        retry_base_delay_s=DUCKDB_WRITE_LOCK_RETRY_BASE_DELAY_S,
        retry_max_delay_s=DUCKDB_WRITE_LOCK_RETRY_MAX_DELAY_S,
    )
