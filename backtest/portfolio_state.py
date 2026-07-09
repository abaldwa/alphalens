"""
backtest/portfolio_state.py

Phase: 3.x (Automated Daily Paper Trading)
Specs: SPEC-BT-002
Owner: Platform / Backtest
Consumers: scripts/run_daily_paper_trading.py, datastore/api/routers/paper_trading.py

PortfolioSimulator (backtest/portfolio.py) is in-memory only — a fresh
instance is created per process run, so open positions can't survive
across day-boundary process invocations. This module adds the minimal
persistence needed for a daily bot: serialize the whole portfolio
(cash, positions, equity curve) to a single JSON file after each run,
and reconstruct an equivalent PortfolioSimulator from it on the next
run. No new DuckDB table — single daily writer/reader, doesn't need
relational queryability.
"""

import json
import logging
from pathlib import Path
from typing import Optional

from backtest.portfolio import Position, PortfolioSimulator

logger = logging.getLogger(__name__)


def save_portfolio_state(
    portfolio: PortfolioSimulator, path: Path, as_of_date: str, position_meta: Optional[dict] = None
) -> None:
    """Write portfolio's cash/positions/equity_curve to `path` as JSON.

    position_meta : dict, optional
        {ticker: {buy_prob_entry, target_price, target_date, exit_criterion}}
        for tickers bought *this call* — merged into whatever meta the
        previous state file already had (so callers only need to pass the
        newly-opened tickers, not every held position's meta every run) and
        pruned down to currently-held tickers (a closed position's meta is
        dropped, same as its Position itself). This dict lives outside
        PortfolioSimulator/Position on purpose — it's paper-trading-UI
        display data (SPEC-PT-003 dashboard columns), not backtest mechanics
        the shared Position dataclass (also used by BacktestEngine) needs.
    """
    existing_meta = {}
    if path.exists():
        try:
            existing_meta = json.loads(path.read_text()).get("position_meta", {})
        except (json.JSONDecodeError, OSError):
            existing_meta = {}
    merged_meta = {**existing_meta, **(position_meta or {})}
    merged_meta = {t: m for t, m in merged_meta.items() if t in portfolio.positions}

    state = {
        "as_of_date": as_of_date,
        "cash": portfolio.cash,
        "initial_capital": portfolio.initial_capital,
        "sizing_mode": portfolio.sizing_mode,
        "n_target_positions": portfolio.n_target_positions,
        "positions": [
            {
                "ticker": pos.ticker,
                "sector": pos.sector,
                "entry_date": str(pos.entry_date),
                "entry_price": pos.entry_price,
                "quantity": pos.quantity,
                "peak_price": pos.peak_price,
                "entry_atr_pct": pos.entry_atr_pct,
            }
            for pos in portfolio.positions.values()
        ],
        "equity_curve": [
            {"date": str(point["date"]), "equity": point["equity"]} for point in portfolio._equity_curve
        ],
        "position_meta": merged_meta,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2))
    logger.info(f"portfolio_state.save: {as_of_date} -> {path} ({len(state['positions'])} open positions)")


def load_portfolio_state(path: Path) -> Optional[PortfolioSimulator]:
    """
    Reconstruct a PortfolioSimulator from a JSON file written by
    save_portfolio_state(). Returns None if `path` doesn't exist —
    callers should fall back to a fresh PortfolioSimulator(INITIAL_CAPITAL, ...)
    on first-ever run.
    """
    if not path.exists():
        return None

    state = json.loads(path.read_text())
    portfolio = PortfolioSimulator(
        initial_capital=state["initial_capital"],
        sizing_mode=state.get("sizing_mode", "equal_weight"),
        n_target_positions=state.get("n_target_positions", 10),
    )
    # No public mutators exist for cash/positions/equity_curve — set the
    # private attrs directly, same pattern PortfolioSimulator uses internally.
    portfolio.cash = state["cash"]
    portfolio.positions = {
        p["ticker"]: Position(
            ticker=p["ticker"],
            sector=p["sector"],
            entry_date=p["entry_date"],
            entry_price=p["entry_price"],
            quantity=p["quantity"],
            peak_price=p["peak_price"],
            entry_atr_pct=p.get("entry_atr_pct"),
        )
        for p in state["positions"]
    }
    portfolio._equity_curve = list(state.get("equity_curve", []))
    logger.info(f"portfolio_state.load: {path} -> {len(portfolio.positions)} open positions")
    return portfolio
