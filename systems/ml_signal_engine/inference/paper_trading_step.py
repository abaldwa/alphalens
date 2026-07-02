"""
systems/ml_signal_engine/inference/paper_trading_step.py

Phase: 3.x (Automated Daily Paper Trading)
Specs: SPEC-BT-002, SPEC-MODEL-002
Owner: ml_signal_engine / exit
Consumers: scripts/run_paper_trading_sim.py (historical bootstrap),
    scripts/run_daily_paper_trading.py (forward-live bot)

Shared portfolio-mechanics step functions, factored out of
scripts/run_paper_trading_sim.py's private _apply_exits/_apply_entries
closures so the historical-bootstrap simulator and the new forward-live
daily bot don't duplicate the once-buggy partial-close-vs-full-close
logic (a `reduce_position()` call appends to portfolio.trades while the
position stays open — only a full close, or a reduce that exactly zeroes
quantity, is a genuinely closed trade fit for the closed-trade CSV log /
ExitSignalModel training data; see BuildLog.md "Paper Trading Logic Fix").

Model scoring (which features, which models, how candidates are ranked)
stays in each caller — these two functions only take an already-built
exit context / already-scored candidates DataFrame and execute the
portfolio actions, so the model-scoring pipeline can differ between the
historical multi-day replay (pre-loaded OHLCV/PnD panels) and the live
daily bot (today's DataStore API signals) without duplicating the
mechanics that matter for Gate 7 correctness.
"""

import logging
from datetime import date as date_type
from typing import Dict, List

import pandas as pd

from backtest.portfolio import PortfolioSimulator
from scripts.paper_trading_tracker import PaperTradingTracker

logger = logging.getLogger(__name__)

DEFAULT_ENTRY_TIME = "09:15:00"
DEFAULT_EXIT_TIME = "15:30:00"

# action_type -> what exit_action_for_urgency() bands map to; 'monitor' and
# 'hold' produce no proposal, same as apply_daily_exits() taking no action
# for those bands.
_URGENCY_ACTION_TYPE = {"immediate_exit": "sell", "reduce_position": "reduce"}


def apply_daily_exits(
    portfolio: PortfolioSimulator,
    exit_policy,
    held_context: pd.DataFrame,
    prices: Dict[str, float],
    date: date_type,
    tracker: PaperTradingTracker,
    entry_context: Dict[str, Dict],
) -> None:
    """
    Run `exit_policy.predict_full()` against an already-built exit context
    (one row per held ticker, EXIT_CONTEXT_COLUMNS shape, indexed by ticker)
    and execute the resulting portfolio actions.

    Parameters
    ----------
    held_context : pd.DataFrame
        Indexed by ticker, columns = backtest.engine.EXIT_CONTEXT_COLUMNS.
        Caller builds this (it varies: historical replay reads from
        pre-loaded OHLCV/momentum panels, the live bot reads today's
        already-computed feature parquet/PnD scores).
    entry_context : Dict[str, Dict]
        Per-ticker entry metadata (at minimum `entry_time`) captured at buy
        time by apply_daily_entries(), mutated in place — popped here once
        a position fully closes so the closed-trade log carries the real
        entry time instead of a fabricated default.
    """
    if held_context.empty:
        return

    exit_out = exit_policy.predict_full(held_context)

    for ticker in held_context.index:
        if ticker not in portfolio.positions or ticker not in prices:
            continue
        urgency = float(exit_out.loc[ticker, "exit_urgency"])
        exit_type = str(exit_out.loc[ticker, "exit_type"])
        trade = portfolio.apply_exit_signal(ticker, urgency, prices[ticker], date)
        # reduce_position() also appends to portfolio.trades for a partial
        # close while the position stays open — only log to the
        # closed-trade CSV once the ticker is actually no longer held (full
        # sell, or a reduce that exactly zeroed quantity). Training data for
        # ExitSignalModel must be genuinely closed positions, not partials.
        if trade is not None and ticker not in portfolio.positions:
            ctx = entry_context.pop(ticker, {})
            tracker.log_trade(
                date=str(trade.entry_date),
                ticker=ticker,
                signal_type="BUY",
                entry_price=trade.entry_price,
                quantity=trade.quantity,
                entry_time=ctx.get("entry_time", DEFAULT_ENTRY_TIME),
                exit_price=trade.exit_price,
                exit_time=DEFAULT_EXIT_TIME,
                exit_date=str(trade.exit_date),
                exit_type=exit_type,
                pnl=trade.pnl_inr,
                pnl_pct=trade.pnl_pct,
            )


def apply_daily_entries(
    portfolio: PortfolioSimulator,
    candidates: pd.DataFrame,
    sector_map: Dict[str, str],
    prices: Dict[str, float],
    date: date_type,
    tracker: PaperTradingTracker,
    entry_context: Dict[str, Dict],
    n_positions: int,
    buy_prob_col: str = "buy_prob",
) -> None:
    """
    Buy the top `n_positions` candidates (by `buy_prob_col` descending) that
    aren't already held, subject to PortfolioSimulator.can_buy()'s
    position/sector/cash gates.

    Parameters
    ----------
    candidates : pd.DataFrame
        Indexed by ticker, already filtered to buy-eligible rows (signal
        direction == BUY, meta-labeler approved, not PnD-blocked) and
        already sorted/unsorted — this function re-sorts by `buy_prob_col`
        and takes the top `n_positions` not already held. Caller did the
        model scoring; this function only executes the resulting trades.
    """
    if candidates.empty:
        return

    ranked = candidates[~candidates.index.isin(portfolio.positions.keys())].sort_values(
        buy_prob_col, ascending=False
    ).head(n_positions)

    for ticker, _row in ranked.iterrows():
        price = prices.get(ticker)
        if price is None or price <= 0:
            continue
        position = portfolio.buy(ticker, sector_map.get(ticker, "UNKNOWN"), price, date, prices)
        if position is not None:
            entry_context[ticker] = {"entry_time": DEFAULT_ENTRY_TIME}


def propose_daily_exits(
    portfolio: PortfolioSimulator,
    exit_policy,
    held_context: pd.DataFrame,
) -> List[Dict]:
    """
    Same candidate selection as apply_daily_exits() (exit_policy.predict_full()
    over an already-built exit context) but returns proposed actions instead
    of executing them — used by SPEC-PT-003's review/approve flow
    (PAPER_TRADING_REQUIRE_APPROVAL). Tickers whose urgency band is 'monitor'
    or 'hold' produce no proposal, matching apply_daily_exits()'s no-op for
    those bands.

    Returns
    -------
    List[Dict]
        One dict per proposed sell/reduce: {action_type, ticker, price,
        reason}. `price` here is the price the exit_policy was scored
        against (display only) — the accept endpoint re-fetches the live
        price at execution time rather than trusting a possibly-stale value.
    """
    if held_context.empty:
        return []

    exit_out = exit_policy.predict_full(held_context)
    proposals: List[Dict] = []
    for ticker in held_context.index:
        if ticker not in portfolio.positions:
            continue
        urgency = float(exit_out.loc[ticker, "exit_urgency"])
        exit_type = str(exit_out.loc[ticker, "exit_type"])
        action_type = _URGENCY_ACTION_TYPE.get(PortfolioSimulator.exit_action_for_urgency(urgency))
        if action_type is None:
            continue  # monitor / hold — no action proposed
        proposals.append({
            "action_type": action_type,
            "ticker": ticker,
            "price": held_context.loc[ticker].get("entry_price"),
            "reason": f"{exit_type} (urgency {urgency:.0f})",
        })
    return proposals


def propose_daily_entries(
    candidates: pd.DataFrame,
    sector_map: Dict[str, str],
    prices: Dict[str, float],
    n_positions: int,
    held_tickers: List[str],
    buy_prob_col: str = "buy_prob",
) -> List[Dict]:
    """
    Same candidate ranking as apply_daily_entries() but returns proposed buy
    actions instead of executing them — used by SPEC-PT-003's review/approve
    flow. Does not call PortfolioSimulator.can_buy() (cash/sector gates are
    re-checked at accept time, since other accepted proposals earlier the
    same day can change available cash/sector exposure).

    Returns
    -------
    List[Dict]
        One dict per proposed buy: {action_type='buy', ticker, sector,
        price, reason}.
    """
    if candidates.empty:
        return []

    ranked = candidates[~candidates.index.isin(held_tickers)].sort_values(
        buy_prob_col, ascending=False
    ).head(n_positions)

    proposals: List[Dict] = []
    for ticker, row in ranked.iterrows():
        price = prices.get(ticker)
        if price is None or price <= 0:
            continue
        proposals.append({
            "action_type": "buy",
            "ticker": ticker,
            "sector": sector_map.get(ticker, "UNKNOWN"),
            "price": price,
            "reason": f"buy_prob={row[buy_prob_col]:.2f}",
        })
    return proposals
