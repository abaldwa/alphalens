"""
backtest/core/live_signal_runner.py

Phase: Unified Generator Refactor, Phase D (D2)
Owner: Platform / Backtest
Consumers: (D3) the technical holdings path, the daily scheduler, and any
caller that needs "what should this strategy hold today".

The third caller of the ONE generator per channel, alongside
BacktestOrchestrator (historical panel) and PaperTradingRunner
(.propose_today). Deliberately thin: it assembles today's universe and hands
it to `adapter.generate_signals` — it owns no selection logic of its own,
because every line of selection logic that lives outside an adapter is a
line that can drift away from what was backtested. That drift is exactly
what this refactor is removing (see UnifiedGeneratorRefactorPlan.md §3).

DIFFERENCE FROM PaperTradingRunner
----------------------------------
PaperTradingRunner queues proposals for human approval and maintains a
simulated portfolio. LiveSignalRunner does neither: it answers "what is the
target holding set today", records it in the A94 ledger with source="live",
and returns it. Nothing here executes, sizes, or persists a portfolio.

WHY NOT ta_signals / ml_signals
-------------------------------
Those feeds answer "what matched a template/model today" (an alert), which
is a different question from "what should the portfolio hold" (a holdings
decision). D1 re-scopes them to alerts-only; this class is where the
holdings answer comes from instead.
"""

from __future__ import annotations

import logging
from datetime import date as date_type
from pathlib import Path
from typing import Any, List, Optional

from backtest.core.engine import Signal, StrategyAdapter
from backtest.core.horizon import HorizonBucket
from backtest.core.readiness_gate import check_readiness
from backtest.core.signal_ledger import SignalLedgerRecorder
from strategies.signals import NO_RUN

logger = logging.getLogger(__name__)


class LiveSignalRunner:
    """One instance per (channel, strategy_id) live holdings track."""

    def __init__(
        self, channel: str, strategy_id: str,
        horizon_bucket: Optional[HorizonBucket] = None,
        persist_signals: bool = True, signal_ledger_db_path: Optional[Path] = None,
        enforce_readiness: bool = True, readiness_checker: Optional[Any] = None,
    ) -> None:
        """persist_signals (A94/B1): record today's target holdings in the
        strategy_signals ledger with source="live". Default ON — an
        unrecorded live decision cannot be audited after the fact, and the
        ledger is the only place backtest, paper and live answers can be
        compared on equal terms.

        enforce_readiness (A103): default ON. This path feeds real holdings,
        so generating on partial data is worse than generating a day late.
        """
        self.channel = channel
        self.strategy_id = strategy_id
        self.horizon_bucket = horizon_bucket
        self.persist_signals = persist_signals
        self.signal_ledger_db_path = signal_ledger_db_path
        self.enforce_readiness = enforce_readiness
        self._readiness_checker = readiness_checker

    def signals_for(
        self, adapter: StrategyAdapter, universe: List[str], as_of_date: date_type,
    ) -> List[Signal]:
        """Today's signals from the SAME adapter the backtest ran.

        Returns [] — not an exception — when the readiness gate refuses:
        "no holdings decision today" is a normal operational outcome the
        caller already handles, and raising here would fail every strategy
        queued behind this one. The refusal is logged and recorded, so it is
        never silent.
        """
        if self.horizon_bucket is None:
            raise ValueError("signals_for requires horizon_bucket to be set on the runner")
        if adapter.channel != self.channel:
            raise ValueError(
                f"adapter.channel={adapter.channel!r} does not match "
                f"runner.channel={self.channel!r}"
            )

        readiness = check_readiness(
            self.channel, self.strategy_id, universe, as_of_date,
            enforce=self.enforce_readiness, checker=self._readiness_checker,
            db_path=self.signal_ledger_db_path,
        )
        if readiness is not None and not readiness.ready:
            logger.warning(
                "%s/%s: NOT generating live signals for %s — %s. "
                "A delayed holdings decision is preferred to one computed on partial data.",
                self.channel, self.strategy_id, as_of_date,
                "; ".join(m.detail for m in readiness.missing),
            )
            return []

        signals = list(adapter.generate_signals(universe, as_of_date, self.horizon_bucket))
        self._record_signals(as_of_date, signals)
        return signals

    def target_holdings(
        self, adapter: StrategyAdapter, universe: List[str], as_of_date: date_type,
    ) -> List[str]:
        """The buy-side of signals_for(), as tickers, in emitted order.

        Order is the adapter's own ranking order — preserved rather than
        sorted, because for a top-N strategy the order IS the decision.
        """
        return [s.ticker for s in self.signals_for(adapter, universe, as_of_date) if s.action == "buy"]

    def _record_signals(self, as_of_date: date_type, signals: List[Any]) -> int:
        """Persist one day's holdings decision to the A94 ledger (source="live").

        Flushed immediately: this path produces ONE date per day, so a day is
        already the natural batch, and a deferred write would leave the ledger
        disagreeing with the holdings someone is acting on right now. run_id is
        NO_RUN ('') — the sentinel the schema requires, since run_id is in the
        primary key and DuckDB PK columns cannot be NULL.
        """
        if not self.persist_signals or not signals:
            return 0
        recorder = SignalLedgerRecorder(
            strategy_key=f"{self.channel}:{self.strategy_id}",
            source="live",
            run_id=NO_RUN,
            channel=self.channel,
            db_path=self.signal_ledger_db_path,
        )
        recorder.record(as_of_date, signals)
        return int(recorder.flush())
