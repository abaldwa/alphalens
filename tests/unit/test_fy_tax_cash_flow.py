"""
tests/unit/test_fy_tax_cash_flow.py

STEP 5: capital-gains tax is a real per-FY cash outflow.

It used to be computed in _finalize and subtracted ONCE from the closing
equity value. Two consequences, the second much larger than the first:

  1. It contradicted the standing requirement that tax is paid every year
     rather than at the end of the period.
  2. Every rupee of tax stayed in the portfolio and COMPOUNDED. A 17-year
     backtest traded sixteen extra years on money it owed the government and
     then wrote the bill off the closing balance.

The compounding is the part no metric revealed: CAGR, Sharpe and trade count
all looked entirely plausible while the book carried an interest-free
government loan for the length of the run.

Synthetic portfolios per SPEC-SYS-006's fixture exemption — the point is to
control realised gains exactly, which real trades cannot.
"""

from datetime import date

import pandas as pd
import pytest

from backtest.core.horizon import HorizonBucket
from backtest.core.portfolio import AnnualResetConfig, StrategyPortfolio


TRADING_DAYS = pd.bdate_range("2020-01-01", "2024-06-30")


def _portfolio(**kwargs) -> StrategyPortfolio:
    p = StrategyPortfolio(
        initial_capital=1_000_000.0, horizon_bucket=HorizonBucket.D21, **kwargs
    )
    p.prime_tax_schedule(TRADING_DAYS)
    return p


def _book_gain(p: StrategyPortfolio, amount: float, entry: date, exit_: date):
    """Realise a short-term gain by round-tripping a position."""
    p.buy("AAA", "IT", 100.0, entry, {"AAA": 100.0}, adtv_cr=500.0)
    qty = p.positions["AAA"].quantity
    p.sell("AAA", 100.0 + amount / max(qty, 1), exit_, reason="exit_model_urgent")


def test_tax_leaves_the_portfolio_at_the_fy_boundary():
    p = _portfolio()
    _book_gain(p, 200_000.0, date(2020, 6, 1), date(2020, 9, 1))
    cash_before = p.cash

    p.apply_due_fy_tax(pd.Timestamp("2021-04-01"))

    assert p.total_tax_paid > 0, "tax must actually be paid, not merely computed"
    assert p.cash < cash_before, "cash must fall by the tax paid"
    assert p.cash == pytest.approx(cash_before - p.total_tax_paid)


def test_the_tax_ledger_records_every_year_it_assessed():
    p = _portfolio()
    _book_gain(p, 150_000.0, date(2020, 6, 1), date(2020, 9, 1))
    p.apply_due_fy_tax(pd.Timestamp("2021-04-01"))

    assert len(p.tax_ledger) >= 1
    row = p.tax_ledger[0]
    assert set(row) == {"fy_end", "assessed", "paid", "deferred", "cash_after"}
    assert row["assessed"] == pytest.approx(row["paid"] + row["deferred"])


def test_tax_appears_as_a_cash_flow_for_xirr():
    p = _portfolio()
    _book_gain(p, 200_000.0, date(2020, 6, 1), date(2020, 9, 1))
    p.apply_due_fy_tax(pd.Timestamp("2021-04-01"))

    tax_flows = [cf for cf in p.cash_flows if cf["date"] == "2021-04-01"]
    assert tax_flows, "the payment must be visible in the cash-flow series"
    assert tax_flows[0]["amount"] > 0, "money leaving the strategy is a positive flow"


def test_a_loss_year_pays_nothing():
    p = _portfolio()
    p.apply_due_fy_tax(pd.Timestamp("2021-04-01"))
    assert p.total_tax_paid == 0.0
    assert p.deferred_tax_liability == 0.0


def test_a_book_short_of_cash_defers_rather_than_going_negative():
    """A near-fully-invested portfolio can genuinely owe more at 31 March than
    it holds in cash. The alternatives are to sell positions the strategy
    never signalled, or to let cash go negative and size every later position
    against imaginary capital. Deferring invents neither a trade nor money."""
    p = _portfolio()
    _book_gain(p, 400_000.0, date(2020, 6, 1), date(2020, 9, 1))
    p.cash = 1_000.0  # spent almost everything on positions

    p.apply_due_fy_tax(pd.Timestamp("2021-04-01"))

    assert p.cash >= 0.0
    assert p.deferred_tax_liability > 0.0
    assert p.tax_ledger[-1]["deferred"] > 0.0


def test_a_deferred_balance_is_settled_at_the_next_boundary_with_cash():
    p = _portfolio()
    _book_gain(p, 400_000.0, date(2020, 6, 1), date(2020, 9, 1))
    p.cash = 1_000.0
    p.apply_due_fy_tax(pd.Timestamp("2021-04-01"))
    carried = p.deferred_tax_liability
    assert carried > 0

    p.cash = 500_000.0  # positions liquidated during the following year
    p.apply_due_fy_tax(pd.Timestamp("2022-04-01"))

    assert p.deferred_tax_liability == pytest.approx(0.0)
    assert p.total_tax_paid >= carried


def test_annual_reset_does_not_pay_twice():
    """capital_mode='annual_reset' already computes the FY's tax and nets it
    out of the withdrawal. Charging it again here would take the same money
    twice, so the two mechanisms are mutually exclusive by construction rather
    than by a caller remembering to disable one."""
    p = StrategyPortfolio(
        initial_capital=1_000_000.0,
        horizon_bucket=HorizonBucket.D21,
        annual_reset=AnnualResetConfig(base_capital=1_000_000.0),
    )
    p.prime_tax_schedule(TRADING_DAYS)
    _book_gain(p, 300_000.0, date(2020, 6, 1), date(2020, 9, 1))
    cash_before = p.cash

    p.apply_due_fy_tax(pd.Timestamp("2021-04-01"))

    assert p.cash == cash_before
    assert p.total_tax_paid == 0.0
    assert p.tax_ledger == []


def test_disabling_annual_deduction_preserves_the_previous_behaviour():
    p = _portfolio(deduct_tax_annually=False)
    _book_gain(p, 200_000.0, date(2020, 6, 1), date(2020, 9, 1))
    cash_before = p.cash

    p.apply_due_fy_tax(pd.Timestamp("2021-04-01"))

    assert p.cash == cash_before
    assert p.tax_ledger == []


def test_unpaid_tax_no_longer_compounds_across_years():
    """The substantive property. Two portfolios with identical gains, one
    paying annually and one not, must diverge — and the payer must hold less,
    by the tax plus whatever that tax would have earned."""
    paying = _portfolio()
    hoarding = _portfolio(deduct_tax_annually=False)
    for p in (paying, hoarding):
        _book_gain(p, 300_000.0, date(2020, 6, 1), date(2020, 9, 1))

    for p in (paying, hoarding):
        p.apply_due_fy_tax(pd.Timestamp("2021-04-01"))

    assert paying.cash < hoarding.cash
    assert hoarding.cash - paying.cash == pytest.approx(paying.total_tax_paid)
