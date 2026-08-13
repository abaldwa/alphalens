"""
backtest/ledger_invariants.py

Phase: 3.x (Technical backtest refactor — STEP 5)
Owner: backtest
Consumers: backtest/core/engine.py (post-run), tests/unit/test_ledger_invariants.py

Accounting identities every run must satisfy. These are not metrics and not
quality heuristics — each is a statement that must be true of any correct
simulation, so a violation is a bug rather than a bad result.

WHY THIS MODULE EXISTS

Every capital defect this refactor found was invisible in the metrics. The run
reported a plausible CAGR, a plausible Sharpe and a plausible trade count while
money quietly failed to move:

  - tax was computed, reported per FY, and never debited, so seventeen years of
    unpaid tax compounded inside the portfolio and was written off the closing
    balance at the end;
  - a whole urgency band's exits resolved to no portfolio action at all;
  - positions were carried at a stale price across data blackouts.

None of those change a number in a way a reader can spot. All of them break an
identity. An invariant that says "cash in minus cash out must equal what the
book holds" cannot be satisfied by a plausible-looking wrong answer, which is
exactly the property the metrics lack.

DESIGN

Each check returns a Violation rather than raising, so one run reports every
identity it breaks instead of only the first. The caller decides whether a
violation fails the run or is recorded — but it is always RECORDED, because a
check whose result can be discarded silently is not a gate.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

# Rupee tolerance for float comparison. Generous by design: these identities
# accumulate over ~4,300 trading days and thousands of trades, and a
# false alarm at the fifth decimal place teaches people to ignore the gate.
# A real accounting bug is off by lakhs, not by paise.
MONEY_TOLERANCE_INR = 1.0


@dataclass(frozen=True)
class Violation:
    check: str
    detail: str
    magnitude: float

    def __str__(self) -> str:
        return f"{self.check}: {self.detail}"


def check_tax_was_actually_paid(
    tax_ledger: List[Dict[str, Any]], total_tax_paid: float, deferred: float,
) -> Optional[Violation]:
    """Every rupee assessed must be either paid or explicitly deferred.

    The defect this pins: tax was assessed per FY, reported in the ledger, and
    never left the portfolio. The ledger looked complete — it had an assessed
    figure for every year — which is precisely why nobody noticed that the
    cash side of it did not exist.
    """
    assessed = sum(row["assessed"] for row in tax_ledger)
    accounted = total_tax_paid + deferred
    gap = assessed - accounted
    if abs(gap) > MONEY_TOLERANCE_INR:
        return Violation(
            "tax_assessed_equals_paid_plus_deferred",
            f"assessed Rs {assessed:,.2f} but paid Rs {total_tax_paid:,.2f} "
            f"+ deferred Rs {deferred:,.2f} = Rs {accounted:,.2f} (gap Rs {gap:,.2f})",
            abs(gap),
        )
    return None


def check_no_negative_cash(cash: float) -> Optional[Violation]:
    """A backtest cannot spend money it does not have. Negative cash means a
    debit somewhere skipped its affordability check, and every position sized
    after that point was sized against imaginary capital."""
    if cash < -MONEY_TOLERANCE_INR:
        return Violation("no_negative_cash", f"cash is Rs {cash:,.2f}", abs(cash))
    return None


def check_fy_ledger_continuity(fy_ledger: List[Dict[str, Any]]) -> Optional[Violation]:
    """Each FY must open where the previous one closed.

    A real bug this catches: an off-by-one in the FY-end derivation once
    produced a 17-year ledger with three years duplicated and three missing.
    Because the label drives which realised-P&L bucket is pulled, the
    mislabelled years withdrew the wrong amounts — and the ledger still had
    seventeen plausible-looking rows.
    """
    for prev, cur in zip(fy_ledger, fy_ledger[1:]):
        expected = prev.get("opening_capital_next")
        actual = cur.get("opening_capital")
        if expected is None or actual is None:
            continue
        if abs(expected - actual) > MONEY_TOLERANCE_INR:
            return Violation(
                "fy_ledger_continuity",
                f"FY ending {cur.get('fy_end')} opened at Rs {actual:,.2f} but the "
                f"previous year closed at Rs {expected:,.2f}",
                abs(expected - actual),
            )

    fy_ends = [row.get("fy_end") for row in fy_ledger]
    duplicates = {fy for fy in fy_ends if fy_ends.count(fy) > 1}
    if duplicates:
        return Violation(
            "fy_ledger_continuity",
            f"duplicate financial years in the ledger: {sorted(duplicates)}",
            float(len(duplicates)),
        )
    return None


def check_cash_flow_signs(cash_flows: List[Dict[str, Any]]) -> Optional[Violation]:
    """Contributions are negative and withdrawals positive, from the
    INVESTOR's perspective — the convention XIRR needs.

    Worth checking rather than assuming: a sign error here does not crash and
    does not look wrong in a trade log; it silently inverts the return.
    """
    if not cash_flows:
        return None
    initial = cash_flows[0].get("amount")
    if initial is not None and initial >= 0:
        return Violation(
            "cash_flow_signs",
            f"the initial contribution is Rs {initial:,.2f}; a contribution must be "
            "negative from the investor's perspective or XIRR inverts",
            abs(initial),
        )
    return None


def check_all(portfolio) -> List[Violation]:
    """Runs every identity against a finished StrategyPortfolio."""
    violations = [
        check_tax_was_actually_paid(
            getattr(portfolio, "tax_ledger", []),
            getattr(portfolio, "total_tax_paid", 0.0),
            getattr(portfolio, "deferred_tax_liability", 0.0),
        ),
        check_no_negative_cash(portfolio.cash),
        check_fy_ledger_continuity(getattr(portfolio, "fy_ledger", [])),
        check_cash_flow_signs(getattr(portfolio, "cash_flows", [])),
    ]
    return [v for v in violations if v is not None]
