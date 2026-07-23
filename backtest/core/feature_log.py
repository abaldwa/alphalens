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
from typing import Any, Dict, List, Optional

from backtest.core.horizon import HorizonBucket

logger = logging.getLogger(__name__)

DEFAULT_FLUSH_BATCH_SIZE = 5_000


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
    """

    def __init__(self, conn, flush_batch_size: int = DEFAULT_FLUSH_BATCH_SIZE) -> None:
        self._conn = conn
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

    def flush(self) -> int:
        """Write buffered rows to backtest_feature_log and clear the buffer.
        Returns the number of rows written. No-op (returns 0) if the buffer
        is empty — never issues an empty INSERT."""
        if not self._buffer:
            return 0
        rows = [
            (
                r.run_id, r.ticker, r.as_of_date, r.horizon_bucket.value,
                json.dumps(r.feature_vector, default=str), r.signal_output, r.decision_taken,
            )
            for r in self._buffer
        ]
        self._conn.executemany(
            """
            INSERT INTO backtest_feature_log
                (run_id, ticker, as_of_date, horizon_bucket, feature_vector_json, signal_output, decision_taken)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (run_id, ticker, as_of_date) DO UPDATE SET
                horizon_bucket = excluded.horizon_bucket,
                feature_vector_json = excluded.feature_vector_json,
                signal_output = excluded.signal_output,
                decision_taken = excluded.decision_taken
            """,
            rows,
        )
        n = len(rows)
        self._buffer.clear()
        logger.debug(f"Flushed {n} backtest_feature_log rows")
        return n

    def __len__(self) -> int:
        """Number of rows currently buffered (not yet flushed)."""
        return len(self._buffer)


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
