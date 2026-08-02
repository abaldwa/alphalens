"""
backtest/portfolio.py

Phase: 1.6 (Exit Signal + First Backtest)
Specs: SPEC-BT-002, SPEC-MODEL-002
Owner: Platform / Backtest
Consumers: backtest/engine.py, backtest/run_phase1_backtest.py

PortfolioSimulator: tracks open positions, applies exit signals (from
ExitSignalModel's urgency score via the documented action thresholds),
computes P&L per trade, and enforces SPEC-BT-002's position-sizing and
exposure limits. Every buy/sell/reduce routes through
backtest.costs.IndianTransactionCosts (built in P1.4) — costs are charged
on every trade, never assumed away.
"""

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import pandas as pd

from backtest.costs import IndianTransactionCosts
from config.settings import EXIT_REDUCE_THRESHOLD, EXIT_URGENT_THRESHOLD, MAX_POSITION_PCT, MAX_SECTOR_PCT

logger = logging.getLogger(__name__)

SIZING_MODES = ("equal_weight", "atr")
# 02_models.md M-07 action thresholds, read from config.settings (never hardcoded):
# urgency > EXIT_URGENT_THRESHOLD (80) -> exit today; EXIT_REDUCE_THRESHOLD (60)-80 -> reduce 50%;
# 40-60 -> monitor (no portfolio action, dashboard-only); else hold.
MONITOR_THRESHOLD = 40.0
REDUCE_FRACTION = 0.5


@dataclass
class Position:
    ticker: str
    sector: str
    entry_date: object
    entry_price: float
    quantity: int
    peak_price: float = field(default=None)
    # ATR/entry_price at entry time (e.g. atr_14_pct/100 from
    # features/technical.py), captured once at buy() and never recomputed —
    # feeds RuleBasedExitPolicy's ATR-scaled target/stop (FutureDevelopment.md
    # #28). None for positions opened before this field existed, or when the
    # caller doesn't have an ATR figure (flat-percentage fallback applies).
    entry_atr_pct: Optional[float] = None
    # Strategy identity for per-template/per-pillar exit routing
    # (PerTemplateExitPolicy, backtest/strategy_id.py). None for callers
    # that don't tag entries (paper trading sim scripts, older callers).
    template: Optional[str] = None
    pillar: Optional[str] = None
    # Market-cap rank (1=largest) within config.universe.get_market_cap_rank_map()
    # at buy time, so trades can be bucketed by market-cap tier after the fact
    # (trade_log CSV's stock_rank column). None when market cap data isn't
    # available for the ticker (config/universe.py's "not yet sourced" case).
    entry_market_cap_rank: Optional[int] = None
    # [BUG FIX, 5th fundamental-strategies review, item 4] the real ADTV
    # (INR crore) actually used to size/cap this position at buy time
    # (Signal.adtv_cr, threaded through StrategyPortfolio.buy's adtv_cr
    # param) — carried over to Trade at close so post_run_checks.py's
    # applied_min_adt_inr audit-trail figure can be derived from genuine
    # per-trade data instead of echoing back the MIN_ADT_INR config
    # constant. None when the signal that opened this position never
    # populated adtv_cr (same "uncapped" case check_06_liquidity's
    # no_adtv_data_position_sized_uncapped data_gap already flags).
    entry_adtv_cr: Optional[float] = None
    # 2026-08-01 (Technical signal-failure analysis): the adapter's own
    # feature_vector(ticker, as_of) snapshot at buy time — screener match
    # score/matched_conditions/indicator values for the Technical channel,
    # None for adapters whose feature_vector doesn't return anything
    # meaningful or wasn't wired at the call site. Carried over to Trade at
    # close so a losing trade can be inspected against what its entry
    # signal actually looked like ("did the buy signal fail, and why").
    entry_feature_vector: Optional[Dict[str, Any]] = None

    def __post_init__(self) -> None:
        if self.peak_price is None:
            self.peak_price = self.entry_price


@dataclass
class Trade:
    ticker: str
    entry_date: object
    exit_date: object
    entry_price: float
    exit_price: float
    quantity: int
    pnl_inr: float
    pnl_pct: float
    cost_inr: float
    exit_reason: str
    # Carried over from Position.entry_market_cap_rank at close time — see
    # that field's docstring.
    entry_market_cap_rank: Optional[int] = None
    # Carried over from Position.entry_adtv_cr at close time — see that
    # field's docstring ([BUG FIX, 5th fundamental-strategies review, item 4]).
    adtv_cr: Optional[float] = None
    # Carried over from Position.entry_feature_vector at close time — see
    # that field's docstring.
    entry_feature_vector: Optional[Dict[str, Any]] = None


class PortfolioSimulator:
    """
    Spec References
    ----------------
    SPEC-BT-002: full Indian transaction cost model applied per trade.
    config.settings.MAX_POSITION_PCT / MAX_SECTOR_PCT: position and
    sector exposure caps.
    """

    def __init__(
        self,
        initial_capital: float = 1_000_000.0,
        sizing_mode: str = "equal_weight",
        n_target_positions: int = 10,
        costs: Optional[IndianTransactionCosts] = None,
    ) -> None:
        if sizing_mode not in SIZING_MODES:
            raise ValueError(f"sizing_mode must be one of {SIZING_MODES}")
        if n_target_positions <= 0:
            raise ValueError("n_target_positions must be positive")
        self.initial_capital = initial_capital
        self.cash = initial_capital
        self.sizing_mode = sizing_mode
        self.n_target_positions = n_target_positions
        self.costs = costs or IndianTransactionCosts()
        self.positions: Dict[str, Position] = {}
        self.trades: List[Trade] = []
        self._equity_curve: List[Dict] = []

    # ===== Position sizing (SPEC-BT-002) =====
    def position_size(self, price: float, portfolio_value: float, atr: Optional[float] = None) -> int:
        """
        Parameters
        ----------
        price : float
            Current price per share.
        portfolio_value : float
            Total equity (cash + open positions at current prices) to size against.
        atr : float, optional
            Required when sizing_mode='atr' — Average True Range (INR) used to
            risk-budget the position (1% of portfolio risked per position,
            sized so a 1-ATR adverse move costs ~1% of portfolio). Falls back
            to equal-weight sizing if omitted.

        Returns
        -------
        int
            Share quantity, capped at MAX_POSITION_PCT of portfolio_value, >= 0.

        Raises
        ------
        ValueError
            If price <= 0 or portfolio_value < 0.
        """
        if price <= 0:
            raise ValueError("price must be positive")
        if portfolio_value < 0:
            raise ValueError("portfolio_value must be non-negative")

        max_position_inr = portfolio_value * MAX_POSITION_PCT
        equal_weight_inr = portfolio_value / self.n_target_positions

        if self.sizing_mode == "atr" and atr is not None and atr > 0:
            risk_budget_inr = portfolio_value * 0.01  # risk 1% of portfolio per position
            target_inr = risk_budget_inr / atr * price
        else:
            target_inr = equal_weight_inr

        target_inr = min(target_inr, max_position_inr, self.cash)
        return max(int(target_inr / price), 0)

    def sector_exposure_pct(self, sector: str, prices: Dict[str, float]) -> float:
        """Current sector exposure as a fraction of total equity."""
        equity = self.total_equity(prices)
        if equity <= 0:
            return 0.0
        sector_value = sum(
            pos.quantity * prices.get(t, pos.entry_price) for t, pos in self.positions.items() if pos.sector == sector
        )
        return sector_value / equity

    def can_buy(
        self, ticker: str, sector: str, price: float, prices: Dict[str, float], atr: Optional[float] = None
    ) -> bool:
        """SPEC-BT-002: position-size, sector-exposure, and available-cash gates, checked before any buy."""
        if ticker in self.positions:
            return False
        equity = self.total_equity(prices)
        qty = self.position_size(price, equity, atr)
        if qty <= 0:
            return False
        turnover = price * qty
        if turnover > self.cash:
            return False
        sector_value = sum(
            pos.quantity * prices.get(t, pos.entry_price) for t, pos in self.positions.items() if pos.sector == sector
        )
        if equity > 0 and (sector_value + turnover) / equity > MAX_SECTOR_PCT:
            return False
        return True

    def buy(
        self, ticker: str, sector: str, price: float, date, prices: Dict[str, float], atr: Optional[float] = None,
        entry_atr_pct: Optional[float] = None, template: Optional[str] = None, pillar: Optional[str] = None,
        adtv_cr: Optional[float] = None,
    ) -> Optional[Position]:
        """
        Open a new position if SPEC-BT-002's gates (can_buy) pass.
        No transaction cost is deducted here: IndianTransactionCosts models
        cost per round trip (both legs), so the full buy+sell cost is
        charged once, at sell()/reduce_position() (see _close), against the
        realized proceeds. cash is reduced by raw turnover (price * qty)
        only; total_equity()/the equity curve therefore doesn't reflect the
        pending exit cost until the position is actually closed.

        Parameters
        ----------
        entry_atr_pct : float, optional
            ATR/entry_price at entry time, stored on the Position for
            RuleBasedExitPolicy's ATR-scaled target/stop (see
            Position.entry_atr_pct). Independent of `atr` (INR, used only
            for atr-mode position sizing above).
        adtv_cr : float, optional
            [BUG FIX, 6th fundamental-strategies review, item 1] the real
            ADTV (INR crore) at entry time (e.g. the caller's own
            Signal.adtv_cr / this file's adtv-at-entry computation) —
            stored on Position.entry_adtv_cr and carried over to
            Trade.adtv_cr at close, so post_run_checks.py's
            applied_min_adt_inr audit-trail figure can be derived from
            genuine per-trade data for this (legacy) engine too, not just
            backtest/core/portfolio.py's StrategyPortfolio. None (the prior,
            silently-broken behavior) if the caller doesn't supply it.

        Returns
        -------
        Position or None
            None if can_buy() rejects the trade (already held, insufficient
            cash, position/sector cap, or non-positive size).
        """
        if not self.can_buy(ticker, sector, price, prices, atr):
            return None
        equity = self.total_equity(prices)
        qty = self.position_size(price, equity, atr)
        turnover = price * qty
        self.cash -= turnover
        position = Position(
            ticker=ticker, sector=sector, entry_date=date, entry_price=price, quantity=qty,
            entry_atr_pct=entry_atr_pct, template=template, pillar=pillar, entry_adtv_cr=adtv_cr,
        )
        self.positions[ticker] = position
        return position

    def update_peak(self, ticker: str, current_price: float) -> None:
        """Track the highest price seen since entry (drawdown_from_peak input for ExitSignalModel)."""
        position = self.positions.get(ticker)
        if position is not None:
            position.peak_price = max(position.peak_price, current_price)

    def sell(
        self, ticker: str, price: float, date, reason: str = "signal", adtv_cr: Optional[float] = None
    ) -> Optional[Trade]:
        """Fully close a position. Returns None if the ticker isn't held."""
        position = self.positions.pop(ticker, None)
        if position is None:
            return None
        return self._close(position, position.quantity, price, date, reason, adtv_cr)

    def reduce_position(
        self, ticker: str, price: float, date, fraction: float = REDUCE_FRACTION,
        reason: str = "exit_model_reduce", adtv_cr: Optional[float] = None,
    ) -> Optional[Trade]:
        """Partially close a position (urgency 60-80: "reduce 50%" per 02_models.md M-07)."""
        if not 0 < fraction <= 1:
            raise ValueError("fraction must be in (0, 1]")
        position = self.positions.get(ticker)
        if position is None:
            return None
        reduce_qty = int(position.quantity * fraction)
        if reduce_qty <= 0:
            return None
        trade = self._close(position, reduce_qty, price, date, reason, adtv_cr, partial=True)
        if position.quantity <= 0:
            self.positions.pop(ticker, None)
        return trade

    def _close(
        self, position: Position, qty: int, price: float, date, reason: str, adtv_cr, partial: bool = False
    ) -> Trade:
        cost = self.costs.compute_roundtrip_cost(position.entry_price, qty, adtv_cr)
        proceeds = price * qty - cost
        entry_basis = position.entry_price * qty
        pnl_inr = proceeds - entry_basis
        pnl_pct = pnl_inr / entry_basis if entry_basis else 0.0
        self.cash += proceeds
        if partial:
            position.quantity -= qty
        trade = Trade(
            ticker=position.ticker, entry_date=position.entry_date, exit_date=date,
            entry_price=position.entry_price, exit_price=price, quantity=qty,
            pnl_inr=pnl_inr, pnl_pct=pnl_pct, cost_inr=cost, exit_reason=reason,
            adtv_cr=position.entry_adtv_cr,
        )
        self.trades.append(trade)
        return trade

    @staticmethod
    def exit_action_for_urgency(urgency: float) -> str:
        """
        02_models.md M-07 action thresholds (config.settings.
        EXIT_URGENT_THRESHOLD=80 / EXIT_REDUCE_THRESHOLD=60, never
        hardcoded here): urgency > 80 -> 'immediate_exit' (exit today);
        60 < urgency <= 80 -> 'reduce_position' (reduce 50%);
        40 < urgency <= 60 -> 'monitor' (dashboard-only, no trade);
        else 'hold'.
        """
        if urgency > EXIT_URGENT_THRESHOLD:
            return "immediate_exit"
        if urgency > EXIT_REDUCE_THRESHOLD:
            return "reduce_position"
        if urgency > MONITOR_THRESHOLD:
            return "monitor"
        return "hold"

    def apply_exit_signal(
        self, ticker: str, urgency: float, price: float, date, adtv_cr: Optional[float] = None
    ) -> Optional[Trade]:
        """Maps an ExitSignalModel urgency score to a portfolio action and executes it."""
        action = self.exit_action_for_urgency(urgency)
        if action == "immediate_exit":
            return self.sell(ticker, price, date, reason="exit_model_urgent", adtv_cr=adtv_cr)
        if action == "reduce_position":
            return self.reduce_position(ticker, price, date, reason="exit_model_reduce", adtv_cr=adtv_cr)
        return None  # 'monitor' and 'hold' take no portfolio action

    def total_equity(self, prices: Dict[str, float]) -> float:
        """Cash + mark-to-market value of all open positions."""
        positions_value = sum(pos.quantity * prices.get(t, pos.entry_price) for t, pos in self.positions.items())
        return self.cash + positions_value

    def record_equity(self, date, prices: Dict[str, float]) -> None:
        self._equity_curve.append({"date": date, "equity": self.total_equity(prices)})

    @property
    def equity_curve(self) -> pd.DataFrame:
        return pd.DataFrame(self._equity_curve)

    @property
    def trades_df(self) -> pd.DataFrame:
        return pd.DataFrame([t.__dict__ for t in self.trades])
