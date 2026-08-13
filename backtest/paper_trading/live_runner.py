"""
backtest/paper_trading/live_runner.py

Phase: Unified Backtest & Paper Trading Umbrella, Phase 5
Owner: Platform / Backtest
Consumers: (future) unified paper-trading API endpoints, a daily
scheduled job per (channel, strategy_id)

Paper Trading as the confirmed downstream stage of the same pipeline
(backtest -> walk-forward -> paper trade -> live): reuses the exact same
core.engine.StrategyAdapter/Signal contract and core.portfolio.
StrategyPortfolio the Backtest and Walk-Forward modes use — the only
differences are (1) signals are generated for a single real "today", fed
from a live DataSource instead of a historical panel, and (2) nothing
executes automatically; every proposed action is queued via
approval_queue.py and requires an explicit human accept() before it
touches the persisted portfolio.

Per the confirmed 2026-07-20 decision (BacktestUmbrellaPlan.md, Phase 5):
Momentum's separate manual momentum_trades journal is intended to fold
into this same unified flow — that migration itself (moving existing
production rows) is a data-migration task for whoever owns that cutover,
not implemented here; this module is the destination schema/flow it
would migrate into.

Portfolio state persistence: a minimal JSON serialization of
StrategyPortfolio (cash, positions, trades, cash_flows,
total_contributed), one file per (channel, strategy_id) under
paper_trading/state/ — deliberately NOT reusing backtest/portfolio_state.py
(which serializes the OLDER, ML-only PortfolioSimulator shape); that
module is left untouched.
"""

import json
import logging
from dataclasses import asdict
from datetime import date as date_type
from pathlib import Path
from typing import Any, Dict, List, Optional

from backtest.core.engine import StrategyAdapter
from backtest.core.horizon import HorizonBucket
from backtest.core.portfolio import StrategyPortfolio
from backtest.core.signal_ledger import SignalLedgerRecorder
from strategies.signals import NO_RUN
from backtest.portfolio import Position, Trade
from backtest.paper_trading.approval_queue import (
    STATE_DIR, PendingAction, read_pending_actions, record_execution,
    update_action_status, write_pending_actions,
)

logger = logging.getLogger(__name__)


def _state_path(channel: str, strategy_id: str) -> Path:
    return STATE_DIR / channel / f"{strategy_id}.json"


def save_portfolio_state(channel: str, strategy_id: str, portfolio: StrategyPortfolio) -> None:
    path = _state_path(channel, strategy_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    state = {
        "initial_capital": portfolio.initial_capital,
        "cash": portfolio.cash,
        "horizon_bucket": portfolio.horizon_bucket.value,
        "total_contributed": portfolio.total_contributed,
        "cash_flows": portfolio.cash_flows,
        "positions": [asdict(p) for p in portfolio.positions.values()],
        "trades": [asdict(t) for t in portfolio.trades],
        "equity_curve": portfolio._equity_curve,
        "cash_position_series": portfolio._cash_position_series,
    }
    path.write_text(json.dumps(state, indent=2, default=str))


def load_portfolio_state(channel: str, strategy_id: str) -> Optional[StrategyPortfolio]:
    path = _state_path(channel, strategy_id)
    if not path.exists():
        return None
    state = json.loads(path.read_text())
    portfolio = StrategyPortfolio(
        initial_capital=state["initial_capital"], horizon_bucket=HorizonBucket(state["horizon_bucket"]),
    )
    portfolio.cash = state["cash"]
    portfolio.total_contributed = state["total_contributed"]
    portfolio.cash_flows = state["cash_flows"]
    portfolio.positions = {p["ticker"]: Position(**p) for p in state["positions"]}
    portfolio.trades = [Trade(**t) for t in state["trades"]]
    portfolio._equity_curve = state["equity_curve"]
    portfolio._cash_position_series = state["cash_position_series"]
    return portfolio


class PaperTradingRunner:
    """
    One instance per (channel, strategy_id) paper-trading track.

    horizon_bucket/initial_capital are only REQUIRED the first time a
    strategy's portfolio is created (i.e. on its very first accept() call,
    before any state file exists) — every call after that reads them back
    from persisted state, so callers that only need accept()/reject()/
    state_summary() on an already-initialized strategy can omit both.
    Never silently defaults them (e.g. to a dummy horizon bucket) when
    they're actually needed — see _portfolio()'s explicit error.
    """

    def __init__(
        self, channel: str, strategy_id: str,
        horizon_bucket: Optional[HorizonBucket] = None, initial_capital: Optional[float] = None,
        persist_signals: bool = True, signal_ledger_db_path: Optional[Path] = None,
    ) -> None:
        """persist_signals (A94): record every proposed signal in the
        strategy_signals ledger with source="paper". Default ON — this is
        the case the ledger exists for. A paper trade is a real decision a
        human will later be asked to explain, and until now the only trace
        of it was a JSON file under paper_trading/state/, so an accepted
        action could not be joined back to the signal that produced it.

        signal_ledger_db_path=None uses strategies.signals' default,
        BACKTEST_DUCKDB_PATH. Tests pass a tmp_path DB.
        """
        self.channel = channel
        self.strategy_id = strategy_id
        self.horizon_bucket = horizon_bucket
        self.initial_capital = initial_capital
        self.persist_signals = persist_signals
        self.signal_ledger_db_path = signal_ledger_db_path

    def _portfolio(self) -> StrategyPortfolio:
        existing = load_portfolio_state(self.channel, self.strategy_id)
        if existing is not None:
            return existing
        if self.horizon_bucket is None or self.initial_capital is None:
            raise ValueError(
                f"No existing paper-trading state for {self.channel}/{self.strategy_id}, and no "
                f"horizon_bucket/initial_capital supplied to initialize one — both are required "
                f"the first time this strategy trades."
            )
        return StrategyPortfolio(initial_capital=self.initial_capital, horizon_bucket=self.horizon_bucket)

    def propose_today(
        self, adapter: StrategyAdapter, universe: List[str], as_of_date: date_type,
    ) -> List[PendingAction]:
        """Generate today's signals via the adapter and queue them for human
        review — never auto-executed, per the project's Gate-7 risk policy
        (never risk capital, real or simulated, on an unreviewed action)."""
        if adapter.channel != self.channel:
            raise ValueError(f"adapter.channel={adapter.channel!r} does not match runner.channel={self.channel!r}")
        if self.horizon_bucket is None:
            raise ValueError("propose_today requires horizon_bucket to be set on the runner")
        signals = adapter.generate_signals(universe, as_of_date, self.horizon_bucket)
        self._record_signals(as_of_date, signals)
        return write_pending_actions(self.channel, self.strategy_id, as_of_date, signals)

    def _record_signals(self, as_of_date: date_type, signals: List[Any]) -> int:
        """Persist one day's proposals to the A94 ledger (source="paper").

        Flushed immediately, unlike the backtest path which buffers a whole
        run: paper trading generates ONE date per day, so a day's proposals
        are already the natural batch, and deferring them would mean the
        ledger disagreed with the approval queue a human is looking at right
        now. One small write a day is no threat to the DuckDB write lock;
        860 of them inside a tight backtest loop would be.

        run_id is NO_RUN ('') — the sentinel the schema requires, because
        run_id is in the primary key and DuckDB PK columns cannot be NULL.
        """
        if not self.persist_signals or not signals:
            return 0
        recorder = SignalLedgerRecorder(
            strategy_key=f"{self.channel}:{self.strategy_id}",
            source="paper",
            run_id=NO_RUN,
            channel=self.channel,
            db_path=self.signal_ledger_db_path,
        )
        recorder.record(as_of_date, signals)
        return recorder.flush()

    def accept(self, action_id: str, as_of_date: date_type, price: float, prices: Dict[str, float]) -> PendingAction:
        """Execute one accepted action against the persisted portfolio,
        save the updated state, and record the execution (advancing the
        Gate-7 day counter for this strategy). Rejecting an action instead
        of accepting it does NOT advance any portfolio state — see reject()."""
        pending = read_pending_actions(self.channel, self.strategy_id, as_of_date)
        action = next((a for a in pending if a.action_id == action_id), None)
        if action is None:
            raise ValueError(f"No pending action {action_id!r} for {self.channel}/{self.strategy_id}/{as_of_date}")

        portfolio = self._portfolio()
        quantity: Optional[int] = None
        if action.action == "buy":
            position = portfolio.buy(action.ticker, action.sector, price, as_of_date, prices, adtv_cr=action.adtv_cr)
            quantity = position.quantity if position else None
        elif action.action in ("sell", "forced_close"):
            trade = (
                portfolio.force_close(action.ticker, price, as_of_date)
                if action.action == "forced_close"
                else portfolio.sell(action.ticker, price, as_of_date, adtv_cr=action.adtv_cr)
            )
            quantity = trade.quantity if trade else None
        else:
            raise ValueError(f"Unknown action type {action.action!r}")

        save_portfolio_state(self.channel, self.strategy_id, portfolio)
        decided = update_action_status(
            self.channel, self.strategy_id, as_of_date, action_id, "accepted",
            executed_price=price, executed_quantity=quantity,
        )
        record_execution(self.channel, self.strategy_id, as_of_date, decided)
        return decided

    def reject(self, action_id: str, as_of_date: date_type) -> PendingAction:
        decided = update_action_status(self.channel, self.strategy_id, as_of_date, action_id, "rejected")
        record_execution(self.channel, self.strategy_id, as_of_date, decided)
        return decided

    def state_summary(self) -> Dict[str, Any]:
        portfolio = self._portfolio()
        return {
            "channel": self.channel, "strategy_id": self.strategy_id,
            "cash": portfolio.cash, "initial_capital": portfolio.initial_capital,
            "total_contributed": portfolio.total_contributed,
            "n_open_positions": len(portfolio.positions),
            "n_closed_trades": len(portfolio.trades),
        }
