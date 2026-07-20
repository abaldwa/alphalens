"""
backtest/core/tax.py

Phase: Unified Backtest & Paper Trading Umbrella, Phase 1
Owner: Platform / Backtest
Consumers: backtest/core/portfolio.py, every channel adapter (via the
shared orchestrator), backtest/core/metrics.py (XIRR cash-flow series)

Generalizes backtest/momentum_tax.py (which computes tax per-transaction,
gross, at time of sale) into the FY-netted engine the user specified
2026-07-20:
  - Fixed rates, no historical tax-rate table: LTCG 12.5%, STCG 20%
    (holding >= 365 days is LTCG, per the existing LTCG_HOLDING_DAYS
    convention in momentum_tax.py).
  - Tax is computed once per Indian Financial Year (April 1 - March 31)
    on that FY's NET realized profit — LTCG and STCG gains/losses are
    each netted separately within the FY (losses in one bucket offset
    gains in the same bucket; a net loss in a bucket pays zero tax for
    that bucket, per real Indian capital-gains set-off treatment), then
    charged as a single cash outflow on the FY's last day (March 31).
  - This intentionally differs from momentum_tax.py's per-transaction
    gross-tax-on-winners-only approach (documented there as a
    conservative simplification); this module is the more accurate
    successor and should be treated as canonical for the unified
    Backtest/Walk-Forward/Paper-Trade umbrella, with momentum_tax.py
    kept only for momentum_backtest.py's existing behavior until Phase 2
    migrates momentum onto this module (BacktestUmbrellaPlan.md Phase 2).
  - Only realized (closed) transactions produce a taxable event. An
    open position held past a FY boundary is not taxed until it is
    actually closed — no mark-to-market tax accrual.
"""

from dataclasses import dataclass
from datetime import date
from typing import Dict, List, Tuple

STCG_RATE = 0.20
LTCG_RATE = 0.125
LTCG_HOLDING_DAYS = 365


@dataclass(frozen=True)
class Transaction:
    ticker: str
    buy_date: date
    sell_date: date
    buy_price: float
    sell_price: float
    quantity: int

    @property
    def holding_days(self) -> int:
        return (self.sell_date - self.buy_date).days

    @property
    def gain(self) -> float:
        return (self.sell_price - self.buy_price) * self.quantity

    @property
    def is_long_term(self) -> bool:
        return self.holding_days >= LTCG_HOLDING_DAYS


def financial_year_end(as_of: date) -> date:
    """
    The March 31 that closes the Indian FY containing `as_of`.
    FY runs April 1 -> March 31; e.g. 2015-06-01 and 2016-03-31 are both
    in FY2015-16, closing 2016-03-31. 2015-03-15 is in FY2014-15, closing
    2015-03-15... no: 2015-03-15 is before April 1 2015, so it belongs to
    the FY that started 2014-04-01 and closes 2015-03-31.
    """
    if as_of.month >= 4:
        return date(as_of.year + 1, 3, 31)
    return date(as_of.year, 3, 31)


def group_by_financial_year(transactions: List[Transaction]) -> Dict[date, List[Transaction]]:
    """Group realized transactions by the FY-end date their sell_date falls into."""
    by_fy: Dict[date, List[Transaction]] = {}
    for txn in transactions:
        fy_end = financial_year_end(txn.sell_date)
        by_fy.setdefault(fy_end, []).append(txn)
    return by_fy


def fy_net_tax(transactions: List[Transaction]) -> float:
    """
    Tax owed (INR) for one Financial Year's realized transactions: LTCG
    and STCG gains are each netted separately (losses offset gains within
    the same bucket, per real Indian set-off rules), then taxed at their
    respective fixed rate. A net loss in either bucket contributes zero
    tax for that bucket (no refund modeled — this is a tax *paid* figure,
    not a tax *credit* figure).
    """
    net_ltcg = sum(t.gain for t in transactions if t.is_long_term)
    net_stcg = sum(t.gain for t in transactions if not t.is_long_term)
    tax = 0.0
    if net_ltcg > 0:
        tax += net_ltcg * LTCG_RATE
    if net_stcg > 0:
        tax += net_stcg * STCG_RATE
    return tax


def fy_tax_cash_flows(transactions: List[Transaction]) -> List[Tuple[date, float]]:
    """
    One (FY-end date, -tax_amount) cash-flow event per Financial Year that
    had any realized transactions and owed non-zero tax — feeds directly
    into core/metrics.py's XIRR calculation alongside SIP contributions,
    per the user's requirement that tax be "paid on the last day of the
    financial year for XIRR Calculations." FYs with zero net tax owed
    produce no cash-flow event (a zero-amount flow is a no-op for XIRR).
    """
    flows: List[Tuple[date, float]] = []
    for fy_end, fy_transactions in sorted(group_by_financial_year(transactions).items()):
        tax = fy_net_tax(fy_transactions)
        if tax > 0:
            flows.append((fy_end, -tax))
    return flows


def total_tax(transactions: List[Transaction]) -> float:
    return sum(fy_net_tax(fy_txns) for fy_txns in group_by_financial_year(transactions).values())


def post_tax_ending_value(ending_value: float, transactions: List[Transaction]) -> float:
    """Ending portfolio value net of FY-netted capital-gains tax across the whole run."""
    return ending_value - total_tax(transactions)
