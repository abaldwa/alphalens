"""
backtest/core/feature_log.py

Phase: Unified Backtest & Paper Trading Umbrella, Phase 1
Owner: Platform / Backtest
Consumers: backtest/core/engine.py (once refactored), every channel
adapter (via adapter.feature_vector()), Phase 6's fine-tuning loop

Per-decision feature-vector logger, writing to the backtest_feature_log
DuckDB table (datastore/schema/create_backtest.py). This is what makes
the feature-reengineering/model-finetuning feedback loop possible: every
candidate signal a run considers — not just the ones it acted on — gets
its full feature vector recorded, tagged with the run_id, so a researcher
can later query "what did the model/rule see for stocks it passed on."

record() is called once per (run_id, ticker, as_of_date) per the Standard
Backtesting Algorithm's step 3a. Batched (buffer + flush), not one INSERT
per call, since a single Walk-Forward run over 20 years can generate
millions of rows (thousands of tickers x hundreds of rebalance dates).
"""

import json
import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from backtest.core.horizon import HorizonBucket

logger = logging.getLogger(__name__)

DEFAULT_FLUSH_BATCH_SIZE = 5_000

_INSERT_SQL = """
    INSERT INTO backtest_feature_log
        (run_id, ticker, as_of_date, horizon_bucket, feature_vector_json, signal_output, decision_taken)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT (run_id, ticker, as_of_date) DO UPDATE SET
        horizon_bucket = excluded.horizon_bucket,
        feature_vector_json = excluded.feature_vector_json,
        signal_output = excluded.signal_output,
        decision_taken = excluded.decision_taken
"""


@dataclass(frozen=True)
class FeatureLogRow:
    run_id: str
    ticker: str
    as_of_date: date
    horizon_bucket: HorizonBucket
    feature_vector: Dict[str, Any]
    signal_output: Optional[str]
    decision_taken: str  # e.g. "bought", "held", "skipped_no_cash", "skipped_sector_cap", "sold"


class FeatureLogWriter:
    """
    Buffers FeatureLogRow records and flushes them to backtest_feature_log
    in batches. Constructed with a DuckDB connection (not a path) so it
    can share the same open write connection as the rest of a run — DuckDB
    allows only one read-write connection to a file at a time
    (datastore/api/db.py's get_duckdb_connection docstring), so the caller
    (core/engine.py) owns connection lifecycle, not this class.

    2026-08-02 (Technical sweep parallelization): pass `spill_path` instead
    of `conn` to run with NO live DuckDB connection at all — flush() then
    appends batches to a local JSONL file instead of executing an INSERT.
    This exists so a parallel-safe backtest run (run_orchestrator_backtest.py's
    defer_db_writes=True) never needs to hold BACKTEST_DUCKDB_PATH's single
    read-write connection open for its own multi-minute duration — the
    thing exclusive_backtest_lock was introduced to prevent concurrently.
    Memory stays bounded at flush_batch_size regardless of run length (a
    20-year walk-forward run's "millions of rows" spool to disk in batches,
    never held in RAM all at once) — load_spill_file() below does the
    actual DB insert later, in one short serialized step per job.
    Exactly one of conn/spill_path must be given.
    """

    def __init__(
        self, conn=None, flush_batch_size: int = DEFAULT_FLUSH_BATCH_SIZE,
        spill_path: Optional[Union[str, Path]] = None,
    ) -> None:
        if (conn is None) == (spill_path is None):
            raise ValueError("FeatureLogWriter needs exactly one of conn or spill_path")
        self._conn = conn
        self._spill_path = Path(spill_path) if spill_path is not None else None
        self._flush_batch_size = flush_batch_size
        self._buffer: List[FeatureLogRow] = []

    def record(
        self, run_id: str, ticker: str, as_of_date: date, horizon_bucket: HorizonBucket,
        feature_vector: Dict[str, Any], decision_taken: str, signal_output: Optional[str] = None,
    ) -> None:
        self._buffer.append(FeatureLogRow(
            run_id=run_id, ticker=ticker, as_of_date=as_of_date, horizon_bucket=horizon_bucket,
            feature_vector=feature_vector, signal_output=signal_output, decision_taken=decision_taken,
        ))
        if len(self._buffer) >= self._flush_batch_size:
            self.flush()

    def _serialize_row(self, r: FeatureLogRow) -> tuple:
        return (
            r.run_id, r.ticker, r.as_of_date, r.horizon_bucket.value,
            json.dumps(r.feature_vector, default=str), r.signal_output, r.decision_taken,
        )

    def flush(self) -> int:
        """Write buffered rows and clear the buffer — to backtest_feature_log
        directly (conn mode) or appended as JSONL to spill_path (spill
        mode). Returns the number of rows written. No-op (returns 0) if the
        buffer is empty — never issues an empty INSERT/write."""
        if not self._buffer:
            return 0
        rows = [self._serialize_row(r) for r in self._buffer]
        n = len(rows)
        if self._conn is not None:
            self._conn.executemany(_INSERT_SQL, rows)
            logger.debug(f"Flushed {n} backtest_feature_log rows")
        else:
            self._spill_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self._spill_path, "a") as fh:
                for row in rows:
                    fh.write(json.dumps(row, default=str) + "\n")
            logger.debug(f"Spilled {n} backtest_feature_log rows to {self._spill_path}")
        self._buffer.clear()
        return n

    def __len__(self) -> int:
        """Number of rows currently buffered (not yet flushed)."""
        return len(self._buffer)


def load_spill_file(conn, spill_path: Union[str, Path], delete_after: bool = True) -> int:
    """Reads a FeatureLogWriter spill file (JSONL, one serialized row tuple
    per line) and bulk-inserts it into backtest_feature_log via `conn` —
    the deferred write half of the spill-mode split above, run once per
    job inside the short serialized merge step (run_orchestrator_backtest.py's
    defer_db_writes=True path). Returns rows written; 0 (no-op) if the file
    doesn't exist or is empty. Deletes the spill file on success by default."""
    spill_path = Path(spill_path)
    if not spill_path.exists():
        return 0
    with open(spill_path) as fh:
        rows = [tuple(json.loads(line)) for line in fh if line.strip()]
    if not rows:
        if delete_after:
            spill_path.unlink(missing_ok=True)
        return 0
    conn.executemany(_INSERT_SQL, rows)
    if delete_after:
        spill_path.unlink(missing_ok=True)
    logger.debug(f"Loaded {len(rows)} backtest_feature_log rows from spill file {spill_path}")
    return len(rows)


def query_feature_log(conn, run_id: str) -> List[Dict[str, Any]]:
    """Read back every logged decision for a run, feature_vector_json parsed
    into a plain dict — the read side of the feedback loop (a researcher
    querying backtest_feature_log for a specific run's losing trades)."""
    rows = conn.execute(
        """
        SELECT ticker, as_of_date, horizon_bucket, feature_vector_json, signal_output, decision_taken
        FROM backtest_feature_log WHERE run_id = ? ORDER BY as_of_date, ticker
        """,
        [run_id],
    ).fetchall()
    return [
        {
            "ticker": ticker, "as_of_date": as_of_date, "horizon_bucket": horizon_bucket,
            "feature_vector": json.loads(feature_vector_json), "signal_output": signal_output,
            "decision_taken": decision_taken,
        }
        for ticker, as_of_date, horizon_bucket, feature_vector_json, signal_output, decision_taken in rows
    ]
