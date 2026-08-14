"""
backtest/core/signal_ledger.py

Phase: Unified Backtest & Paper Trading Umbrella (A94)
Owner: Platform / Backtest
Consumers: backtest/core/engine.py (BacktestOrchestrator, source="backtest"),
backtest/paper_trading/live_runner.py (PaperTradingRunner, source="paper").

Buffers the Signal objects an adapter emits and persists them to the
strategy_signals ledger (strategies/signals.py). Exists so the two callers
share ONE encoding of "how a core.engine.Signal becomes a ledger row" — the
alternative was the same dict-building and the same write-lock discipline
copy-pasted into an orchestrator hot loop and a paper-trading runner, which
is exactly how the four incompatible signal shapes A94 replaces came about.

WHY THIS BUFFERS INSTEAD OF WRITING PER REBALANCE DATE
------------------------------------------------------
DuckDB here is single-writer, and write-lock contention is this project's
top operational failure mode: on 2026-08-04 the scheduler held the write
lock for ~20h and failed 450+ Technical sweep jobs, and the parallel backtest
queue only works at all because every job sets defer_db_writes=True
(backtest/run_orchestrator_backtest.py) so that its DB work happens once, at
the tail, rather than throughout the run.

A 2009-2026 backtest has ~4,300 sessions; at a 5-day cadence that is ~860
rebalance dates. Writing per rebalance date would mean ~860 lock
acquisitions per job, times however many jobs the queue is running
concurrently — reintroducing precisely the contention defer_db_writes was
built to remove, and turning a fast in-memory run into a lock-bound one.

So signals accumulate in memory and are written in ONE transaction at
flush(), which the orchestrator calls at the end of the run, alongside the
existing feature-log flush and inside the same deferred tail. A rebalance
date emits at most a few dozen signals, so a full run is order 10^4 rows —
a few MB, well inside a job's budget.

`max_buffer_rows` bounds that anyway: a pathological strategy that emits the
whole universe every day flushes in chunks instead of growing without limit
until the machine OOMs (backfill jobs have killed this desktop before). It
is a memory guard, not the normal path — a normal run flushes exactly once.

Failures here are logged and swallowed. A ledger row is an audit artifact;
losing one must never destroy a completed multi-hour backtest whose real
deliverable is its trade book.
"""

from __future__ import annotations

import logging
from datetime import date as date_type
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from strategies.signals import NO_RUN, write_signals

logger = logging.getLogger(__name__)

# One flush per run is the intent; this only caps a runaway emitter.
DEFAULT_MAX_BUFFER_ROWS = 50_000


def signal_to_row(
    signal: Any, as_of_date: date_type, *, channel: Optional[str] = None, rank: Optional[int] = None,
) -> Dict[str, Any]:
    """One core.engine.Signal -> one strategy_signals row dict.

    context carries only what the Signal OBJECT actually holds — sector,
    adtv_cr, the template/preset that produced it, and the channel. Nothing
    is invented: a context field that looks like a reason but was guessed is
    worse than an absent one, because a report would present it as the
    strategy's actual rationale.

    The adapter's richer feature_vector() (Technical's matched screener
    conditions) is deliberately NOT called here. It is a per-ticker lookup,
    this runs inside the orchestrator's hot loop for every emitted signal,
    and the engine already snapshots that same vector onto the Position it
    opens (entry_feature_vector), so the detail is not lost — it is simply
    not paid for twice.
    """
    context: Dict[str, Any] = {}
    for field in ("sector", "template", "adtv_cr"):
        value = getattr(signal, field, None)
        if value is not None and value != "Unknown":
            context[field] = value
    if channel:
        context["channel"] = channel

    return {
        "signal_date": as_of_date,
        "ticker": signal.ticker,
        "action": signal.action,
        "conviction": getattr(signal, "conviction", None),
        "rank": rank,
        "size_multiplier": getattr(signal, "size_multiplier", None),
        "context": context or None,
    }


class SignalLedgerRecorder:
    """Accumulate a run's emitted signals, then write them in one batch.

    strategy_version is resolved from the registry by the caller
    (backtest.core.engine.resolve_strategy_version) and must be passed. It is
    NOT defaulted to 1: a row stamped with a version the run did not execute
    is untraceable to the rules that produced it, and reads as authoritative
    while being wrong. A strategy absent from the registry yields None, which
    records the run as pre-registry rather than inventing a version for it.
    """

    def __init__(
        self,
        *,
        strategy_key: str,
        source: str,
        run_id: str = NO_RUN,
        strategy_version: Optional[int] = None,
        channel: Optional[str] = None,
        db_path: Optional[Path] = None,
        max_buffer_rows: int = DEFAULT_MAX_BUFFER_ROWS,
    ) -> None:
        self.strategy_key = strategy_key
        self.strategy_version = strategy_version
        self.source = source
        self.run_id = run_id
        self.channel = channel
        self.db_path = db_path
        self.max_buffer_rows = max_buffer_rows
        self.rows_written = 0
        self._buffer: List[Dict[str, Any]] = []
        self._schema_ensured = False

    def record(self, as_of_date: date_type, signals: Sequence[Any]) -> int:
        """Buffer one rebalance date's signals. Returns rows buffered.

        action="hold" is dropped here rather than passed down and rejected:
        write_signals() raises on holds by design (universe-wide holds are
        what turn this table from millions of rows into hundreds of
        millions), and a single hold anywhere in a run must not be able to
        blow up the whole batch. Adapters are not required to know that
        rule.
        """
        emitted = [s for s in signals if getattr(s, "action", None) != "hold"]
        if not emitted:
            return 0

        # rank is the order the engine ACTUALLY acts in — buys are executed
        # sorted by descending conviction, so 1 is the position that gets
        # first claim on capital. It is derived from real ordering, not from
        # any ranking field the Signal does not have.
        by_action: Dict[str, int] = {}
        for signal in sorted(emitted, key=lambda s: -(getattr(s, "conviction", 0.0) or 0.0)):
            action = getattr(signal, "action", None)
            by_action[action] = by_action.get(action, 0) + 1
            self._buffer.append(
                signal_to_row(signal, as_of_date, channel=self.channel, rank=by_action[action])
            )

        if len(self._buffer) >= self.max_buffer_rows:
            self.flush()
        return len(emitted)

    def flush(self) -> int:
        """Write everything buffered, in one transaction. Idempotent when
        empty, and safe to call more than once."""
        if not self._buffer:
            return 0
        batch, self._buffer = self._buffer, []
        try:
            self._ensure_schema()
            n = write_signals(
                batch,
                strategy_key=self.strategy_key,
                strategy_version=self.strategy_version,
                source=self.source,
                run_id=self.run_id,
                db_path=self.db_path,
            )
        except Exception:
            # See module docstring: a lost audit row must never destroy a
            # completed run. Logged at exception level so it is impossible
            # to mistake an empty ledger for "the strategy emitted nothing".
            logger.exception(
                "strategy_signals write failed for %s (%s, run_id=%r); %d signals not persisted",
                self.strategy_key, self.source, self.run_id, len(batch),
            )
            return 0
        self.rows_written += n
        return n

    def _ensure_schema(self) -> None:
        """CREATE TABLE IF NOT EXISTS, once per recorder — the same pattern
        _persist_run_result uses for strategy_catalog. A run must not fail
        just because this DB predates the A94 tables."""
        if self._schema_ensured:
            return
        from config.settings import (
            DUCKDB_WRITE_LOCK_RETRY_ATTEMPTS,
            DUCKDB_WRITE_LOCK_RETRY_BASE_DELAY_S,
            DUCKDB_WRITE_LOCK_RETRY_MAX_DELAY_S,
        )
        from datastore.schema.create_strategy_registry import create_strategy_registry_schema

        # A105: the backtest write-lock budget, matching strategies/db.py and
        # the job's own writes. On the default budget this DDL loses the lock
        # first, flush() swallows the failure, and the run's signals are lost
        # while the run still reports success.
        create_strategy_registry_schema(
            db_path=self.db_path,
            retry_attempts=DUCKDB_WRITE_LOCK_RETRY_ATTEMPTS,
            retry_base_delay_s=DUCKDB_WRITE_LOCK_RETRY_BASE_DELAY_S,
            retry_max_delay_s=DUCKDB_WRITE_LOCK_RETRY_MAX_DELAY_S,
        )
        self._schema_ensured = True
