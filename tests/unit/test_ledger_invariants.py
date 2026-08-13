"""
tests/unit/test_ledger_invariants.py

The gate that would have caught this refactor's capital defects.

Every one of them was invisible in the metrics — CAGR, Sharpe and trade count
all looked plausible while money quietly failed to move:

  - tax assessed per FY, reported in a ledger, never debited;
  - a whole urgency band's exits resolving to no portfolio action;
  - positions carried at a stale price across data blackouts.

None of those shift a number in a way a reader can spot. All of them break an
accounting identity. That is the point of these checks: an identity cannot be
satisfied by a plausible-looking wrong answer, which is exactly what the
metrics could not distinguish.

So these tests assert two things per check — that it passes on a correct
ledger, AND that it actually fires on the specific broken one it exists for. A
check that has never been seen to fail is not known to work.
"""

import pytest

from backtest.ledger_invariants import (
    MONEY_TOLERANCE_INR,
    check_all,
    check_cash_flow_signs,
    check_fy_ledger_continuity,
    check_no_negative_cash,
    check_tax_was_actually_paid,
)


# ---------------------------------------------------------------------------
# Tax assessed == paid + deferred
# ---------------------------------------------------------------------------

def test_tax_identity_holds_when_everything_was_paid():
    ledger = [{"assessed": 50_000.0}, {"assessed": 30_000.0}]
    assert check_tax_was_actually_paid(ledger, total_tax_paid=80_000.0, deferred=0.0) is None


def test_tax_identity_holds_when_some_was_deferred():
    ledger = [{"assessed": 50_000.0}]
    assert check_tax_was_actually_paid(ledger, total_tax_paid=20_000.0, deferred=30_000.0) is None


def test_tax_identity_fires_on_the_exact_defect_it_exists_for():
    """Assessed and reported per year, never actually debited. This is what
    the codebase did: the ledger had an entry for every FY, which is precisely
    why the missing cash movement went unnoticed."""
    ledger = [{"assessed": 1_23_00_000.0}]
    violation = check_tax_was_actually_paid(ledger, total_tax_paid=0.0, deferred=0.0)
    assert violation is not None
    assert violation.magnitude == pytest.approx(1_23_00_000.0)
    assert "assessed" in str(violation)


def test_tax_identity_tolerates_rounding_but_not_real_money():
    ledger = [{"assessed": 100_000.0}]
    assert check_tax_was_actually_paid(ledger, 100_000.0 - MONEY_TOLERANCE_INR / 2, 0.0) is None
    assert check_tax_was_actually_paid(ledger, 90_000.0, 0.0) is not None


# ---------------------------------------------------------------------------
# No negative cash
# ---------------------------------------------------------------------------

def test_negative_cash_is_a_violation():
    """Negative cash means a debit skipped its affordability check, and every
    position sized afterwards was sized against imaginary capital."""
    assert check_no_negative_cash(-50_000.0) is not None
    assert check_no_negative_cash(0.0) is None
    assert check_no_negative_cash(1_000_000.0) is None


# ---------------------------------------------------------------------------
# FY ledger continuity
# ---------------------------------------------------------------------------

def test_continuous_ledger_passes():
    ledger = [
        {"fy_end": "2021-03-31", "opening_capital": 1_000_000.0, "opening_capital_next": 1_200_000.0},
        {"fy_end": "2022-03-31", "opening_capital": 1_200_000.0, "opening_capital_next": 1_400_000.0},
    ]
    assert check_fy_ledger_continuity(ledger) is None


def test_a_gap_between_years_is_caught():
    """Money appearing or vanishing between 31 March and 1 April is not a
    performance result; it is an accounting error."""
    ledger = [
        {"fy_end": "2021-03-31", "opening_capital": 1_000_000.0, "opening_capital_next": 1_200_000.0},
        {"fy_end": "2022-03-31", "opening_capital": 1_900_000.0, "opening_capital_next": 2_000_000.0},
    ]
    violation = check_fy_ledger_continuity(ledger)
    assert violation is not None
    assert violation.magnitude == pytest.approx(700_000.0)


def test_duplicated_financial_years_are_caught():
    """A real bug this pins: an off-by-one in the FY-end derivation produced a
    17-year ledger with three years duplicated and three missing. Because the
    label drives which realised-P&L bucket is pulled, the mislabelled years
    withdrew the wrong amounts — and the ledger still had seventeen
    plausible-looking rows."""
    ledger = [
        {"fy_end": "2013-03-31", "opening_capital": 1_000_000.0, "opening_capital_next": 1_100_000.0},
        {"fy_end": "2013-03-31", "opening_capital": 1_100_000.0, "opening_capital_next": 1_200_000.0},
    ]
    violation = check_fy_ledger_continuity(ledger)
    assert violation is not None
    assert "duplicate" in str(violation)


def test_an_empty_or_single_row_ledger_is_not_a_violation():
    assert check_fy_ledger_continuity([]) is None
    assert check_fy_ledger_continuity([{"fy_end": "2021-03-31", "opening_capital": 1.0}]) is None


# ---------------------------------------------------------------------------
# Cash-flow sign convention
# ---------------------------------------------------------------------------

def test_initial_contribution_must_be_negative():
    """A sign error here does not crash and does not look wrong in a trade
    log — it silently inverts the reported return."""
    assert check_cash_flow_signs([{"date": "2020-01-01", "amount": -1_000_000.0}]) is None
    assert check_cash_flow_signs([{"date": "2020-01-01", "amount": 1_000_000.0}]) is not None
    assert check_cash_flow_signs([]) is None


# ---------------------------------------------------------------------------
# End to end against a real portfolio
# ---------------------------------------------------------------------------

def test_a_clean_run_reports_no_violations():
    from datetime import date

    import pandas as pd

    from backtest.core.horizon import HorizonBucket
    from backtest.core.portfolio import StrategyPortfolio

    p = StrategyPortfolio(initial_capital=1_000_000.0, horizon_bucket=HorizonBucket.D21)
    p.prime_tax_schedule(pd.bdate_range("2020-01-01", "2022-06-30"))
    p.buy("AAA", "IT", 100.0, date(2020, 6, 1), {"AAA": 100.0}, adtv_cr=500.0)
    p.sell("AAA", 130.0, date(2020, 9, 1), reason="exit_model_urgent")
    p.apply_due_fy_tax(pd.Timestamp("2021-04-01"))

    assert check_all(p) == []


def test_check_all_reports_every_broken_identity_not_just_the_first():
    """One run must surface everything wrong with it. Stopping at the first
    violation turns fixing a run into a sequence of re-runs."""

    class _Broken:
        cash = -10_000.0
        tax_ledger = [{"assessed": 500_000.0}]
        total_tax_paid = 0.0
        deferred_tax_liability = 0.0
        fy_ledger = []
        cash_flows = [{"date": "2020-01-01", "amount": 1_000_000.0}]

    violations = check_all(_Broken())
    checks = {v.check for v in violations}
    assert checks == {
        "tax_assessed_equals_paid_plus_deferred",
        "no_negative_cash",
        "cash_flow_signs",
    }
