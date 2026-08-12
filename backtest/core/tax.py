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
    on that FY's NET realized profit, then charged as a single cash
    outflow on the FY's last day (March 31). Netting follows the real
    (asymmetric) set-off rules: losses offset gains within their own
    bucket, AND a net short-term loss further offsets long-term gains,
    while a net long-term loss may NOT offset short-term gains. A bucket
    left in loss pays zero tax; losses are not carried into the next FY.
    See net_buckets_after_setoff — this file claimed to implement these
    rules but omitted the short-term-loss set-off until 2026-08-12.
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


def net_buckets_after_setoff(transactions: List[Transaction]) -> Tuple[float, float]:
    """
    One FY's realized gains netted into (net_stcg, net_ltcg) with the Indian
    inter-head set-off applied.

    THE SET-OFF RULE (Income-tax Act s.70/s.74) IS ASYMMETRIC:
      - A short-term capital LOSS may be set off against BOTH short-term and
        long-term capital gains.
      - A long-term capital LOSS may be set off ONLY against long-term gains.

    [BUG FIX 2026-08-12] Both fy_net_tax and fy_net_tax_with_regime used to net
    strictly within each bucket and never apply the first rule, while the
    docstrings claimed to follow "real Indian set-off rules". That overstated
    tax in every FY that combined a short-term loss with a long-term gain.
    Found by scripts/validate_fy_ledger.py on the 2009-2026 technical sweep:
    69 of 390 runs affected, tax overstated by ~Rs 31.25 lakh in aggregate.
    The worst single case (template C6, FY2022-23) booked a Rs 10.11 lakh
    short-term loss alongside a Rs 5.05 lakh long-term gain and was charged
    Rs 40,818 of tax on a year whose correct liability was zero.

    This matters beyond the reported tax number: under capital_mode="annual_reset"
    the tax figure sets the FY withdrawal, which sets the next year's opening
    capital, which changes which trades are affordable. An overstated tax
    silently propagates into a different trade history.

    Unused losses are NOT carried forward to later FYs. Real law allows an
    8-year carry-forward, but these are tax-PAID figures for a self-contained
    backtest, and a carry-forward would make each FY's number depend on the
    run's start date. Modelling it is a deliberate non-goal; see the module
    docstring's "no refund modeled" note, which follows the same reasoning.
    """
    net_ltcg = sum(t.gain for t in transactions if t.is_long_term)
    net_stcg = sum(t.gain for t in transactions if not t.is_long_term)
    return apply_stcg_loss_setoff(net_stcg, net_ltcg)


def apply_stcg_loss_setoff(net_stcg: float, net_ltcg: float) -> Tuple[float, float]:
    """
    The set-off itself, on already-netted bucket totals.

    Split out from `net_buckets_after_setoff` so the momentum channel can share
    it: features/momentum_strategy.py::compute_fy_net_tax works from plain dicts
    rather than Transaction objects and had independently reimplemented — and
    independently got wrong — the same rule. One rule, one implementation.
    """
    # Short-term loss shelters long-term gain. Deliberately NOT symmetric:
    # net_ltcg < 0 must leave net_stcg untouched.
    if net_stcg < 0 and net_ltcg > 0:
        absorbed = min(-net_stcg, net_ltcg)
        net_ltcg -= absorbed
        net_stcg += absorbed
    return net_stcg, net_ltcg


def fy_net_tax(transactions: List[Transaction]) -> float:
    """
    Tax owed (INR) for one Financial Year's realized transactions: gains are
    netted per `net_buckets_after_setoff` (including the short-term-loss
    set-off against long-term gains), then each remaining positive bucket is
    taxed at its fixed rate. A net loss in a bucket contributes zero tax (no
    refund modeled — this is a tax *paid* figure, not a tax *credit* figure).
    """
    net_stcg, net_ltcg = net_buckets_after_setoff(transactions)
    tax = 0.0
    if net_ltcg > 0:
        tax += net_ltcg * LTCG_RATE
    if net_stcg > 0:
        tax += net_stcg * STCG_RATE
    return tax


def fy_net_tax_with_regime(
    transactions: List[Transaction],
    ltcg_rate: float = LTCG_RATE,
    ltcg_exemption: float = 0.0,
    stcg_rate: float = STCG_RATE,
) -> float:
    """
    `fy_net_tax` generalised to a specific LTCG regime.

    [2026-08-12] Added for capital_mode="annual_reset", where the amount of
    cash withdrawn at each FY boundary is realised-profit-AFTER-TAX, so the
    tax figure must be the same one the comparison report shows for that
    regime — otherwise the withdrawal and the reported tax silently disagree.

    The two regimes the reports carry:
        ltcg_10pct_1L        -> ltcg_rate=0.10,  ltcg_exemption=100_000
        ltcg_12_5pct_1_25L   -> ltcg_rate=0.125, ltcg_exemption=125_000

    Netting is identical to `fy_net_tax` — both call `net_buckets_after_setoff`,
    so the asymmetric short-term-loss set-off cannot drift between them (they
    had already drifted from the law in exactly that way; see that function's
    2026-08-12 bug-fix note).

    The exemption applies to the NET long-term gain for the year AFTER set-off,
    which is how the Indian exemption actually works — it is neither per
    transaction nor applied to the pre-set-off figure.

    With the defaults this returns exactly `fy_net_tax`, so existing callers
    are unaffected.
    """
    net_stcg, net_ltcg = net_buckets_after_setoff(transactions)
    tax = 0.0
    if net_ltcg > 0:
        taxable_ltcg = max(0.0, net_ltcg - ltcg_exemption)
        tax += taxable_ltcg * ltcg_rate
    if net_stcg > 0:
        tax += net_stcg * stcg_rate
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
