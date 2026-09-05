"""
Portfolio — minimal position/cash tracker for native orchestrator
execution.

Deliberately simple relative to backtest/core/portfolio.py (no tax
lots, no FY-based settlement, no slippage model): this is the smallest
correct simulator that can (a) execute a strategy's target basket at a
day's close price and (b) mark the book to market on every trading day
so R08/R09's update_portfolio_equity() has real numbers to work with.
Sophistication (costs.py, tax.py) gets layered on once trade-by-trade
parity against the legacy engine is being checked — see
docs/MIGRATION.md's cutover criteria.

CORRECTNESS FIX 2026-09-04: the original version of this file executed
signals literally — a repeated "buy" for an already-held ticker was
treated as a fresh purchase funded from whatever cash happened to be
idle, and a ticker missing from a rebalance's signal list was never
sold (no strategy but R07 emits explicit sell signals). Concretely: R01
on Nifty 50, 2023, monthly rebalance, reported a 30.5% CAGR that turned
out to be "buy 5 stocks in January, do almost nothing for the rest of
the year" — because month 2 onward had near-zero idle cash to deploy,
so the portfolio was never actually rotating. Caught because the user
found the number implausible and asked; verified by inspecting real
rebalance() output (Feb signals correctly dropped 3 January names and
added 3 new ones — the STRATEGY was always correct, only the Portfolio's
execution semantics were wrong).

FIX: rebalance_to_target() replaces execute(). The set of tickers
carrying a "buy" action in one rebalance call IS the complete desired
portfolio composition for that period — this matches the legacy
adapter's own stated model ("this period's top_n is List 2, what we
hold is List 1 — anything held and no longer in List 2 is sold; anything
in List 2 and not held is bought"), applied generically instead of only
inside R07. Anything currently held but NOT in this period's target is
sold; anything in target but not held is bought at an equal-weight (x
size_multiplier) share of total portfolio value; anything in both is
left untouched (no wash trade) — matching R07's original comment that
"already-held survivors keep whatever size they entered at."
"""

from dataclasses import dataclass
from typing import Any, Dict, List
import logging

from momentum_framework.backtesting.adapter import Signal

logger = logging.getLogger(__name__)


@dataclass
class Position:
    ticker: str
    shares: float
    entry_price: float
    entry_date: str


class Portfolio:
    """Cash + positions, marked to market daily. No leverage: a
    size_multiplier > 1.0 is honored as intended relative exposure
    within the buy basket, but total spend is always capped at
    available cash — orders are scaled down proportionally, never
    over-filled on margin."""

    def __init__(self, initial_capital: float):
        self.cash = initial_capital
        self.initial_capital = initial_capital
        self.positions: Dict[str, Position] = {}
        self.trade_log: List[Dict[str, Any]] = []

    def market_value(self, prices: Dict[str, float]) -> float:
        """Total portfolio value (cash + mark-to-market positions) at
        today's close. A held ticker missing from `prices` (delisted,
        no data that day) keeps its LAST KNOWN value rather than being
        dropped to zero — a real data gap, not a real loss."""
        value = self.cash
        for ticker, pos in self.positions.items():
            price = prices.get(ticker, pos.entry_price)
            value += pos.shares * price
        return value

    def rebalance_to_target(self, signals: List[Signal], prices: Dict[str, float], as_of_date: str) -> None:
        """
        `signals`' "buy"-action tickers define the COMPLETE target
        portfolio for this period (see module docstring). Any explicit
        "sell"/"forced_close" signals are honored too (e.g. a strategy
        forcing an exit outside the normal target diff), but are
        redundant with — never required for — a ticker's absence from
        the buy set.
        """
        target: Dict[str, float] = {
            s.ticker: (s.size_multiplier or 1.0) for s in signals if s.action == "buy"
        }
        # PORTFOLIO-LEVEL scalar (R08/R09 — see Signal's docstring for why
        # this must be separate from size_multiplier, which gets normalized
        # across the buy set below and would silently cancel out a uniform
        # value). Every buy signal in one rebalance carries the SAME
        # exposure_multiplier by construction (it's a book-wide scalar, not
        # a per-ticker one) — any single buy signal's value represents it.
        exposure_multipliers = [s.exposure_multiplier for s in signals if s.action == "buy"]
        exposure = exposure_multipliers[0] if exposure_multipliers else 1.0
        explicit_sells = {s.ticker for s in signals if s.action in ("sell", "forced_close")}

        held = set(self.positions.keys())
        to_sell = (held - target.keys()) | (explicit_sells & held)
        to_buy = target.keys() - held
        # target ∩ held, minus anything explicitly force-sold: left untouched.

        for ticker in to_sell:
            self._sell(ticker, prices, as_of_date)

        if not to_buy:
            return

        total_value = self.market_value(prices)
        total_weight = sum(target[t] for t in to_buy)
        if total_weight <= 0:
            return

        requested = {t: total_value * (target[t] / total_weight) * exposure for t in to_buy}
        total_requested = sum(requested.values())
        scale = 1.0
        if total_requested > self.cash and total_requested > 0:
            scale = self.cash / total_requested

        for ticker in to_buy:
            price = prices.get(ticker)
            if price is None or price <= 0:
                continue  # no price data today — skip, reconsidered next rebalance
            alloc = requested[ticker] * scale
            shares = alloc / price
            if shares <= 0:
                continue
            self.cash -= shares * price
            self.positions[ticker] = Position(
                ticker=ticker, shares=shares, entry_price=price, entry_date=as_of_date,
            )
            self.trade_log.append({
                "date": as_of_date, "ticker": ticker, "action": "buy",
                "shares": shares, "price": price, "value": shares * price,
            })

    def _sell(self, ticker: str, prices: Dict[str, float], as_of_date: str) -> None:
        pos = self.positions.pop(ticker, None)
        if pos is None:
            return
        price = prices.get(ticker, pos.entry_price)
        proceeds = pos.shares * price
        self.cash += proceeds
        self.trade_log.append({
            "date": as_of_date, "ticker": ticker, "action": "sell",
            "shares": pos.shares, "price": price, "value": proceeds,
            "pnl": proceeds - (pos.shares * pos.entry_price),
        })
