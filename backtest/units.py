"""
backtest/units.py

Phase: 3.x (Technical backtest refactor — STEP 8)
Owner: backtest
Consumers: backtest artifact writers, tests/unit/test_units.py

Declares the unit of every numeric field a backtest emits, and range-checks a
frame against those declarations.

WHY THIS EXISTS

The field names do not say what they mean, and two of them actively mislead:

    backtest_trades.pnl_pct         is a FRACTION   (-0.05 means -5%)
    technical_screener_cache.score  is a FRACTION   (0.0 - 1.0)
    exit_urgency                    is 0-100        (thresholds at 40/60/80)
    exit_survival_*                 is a PROBABILITY

So a field named `_pct` holds a fraction while a field named `score` holds a
fraction and another score-like field holds 0-100. Anything reading these has
to already know; nothing in the artifact says so.

That is not hypothetical. Reading pnl_pct as a percentage understates every
return by 100x, and reading a 0-100 urgency as a probability puts every value
above 1.0 — which silently trips every "is this urgent" comparison to true.
Both produce plausible output: a plausible CAGR, a plausible exit rate.

WHAT THE RANGE CHECK ACTUALLY CATCHES

A fraction/percent mixup is a 100x scaling error, so it shows up as values
sitting far outside the declared range. That is a cheap and reliable signal
precisely because the two ranges barely overlap: a column of returns declared
FRACTION whose median is 4.2 has been multiplied by 100 somewhere upstream.

It deliberately does NOT reject individual outliers. A single trade at
pnl_pct = 186.1 (an 18,610% return) is real data — an unadjusted corporate
action, not a unit error — and a checker that failed the run on it would be
switched off within a week. Unit errors move the whole distribution; data
defects move one row. Only the first is a unit question, so the check reads
the median and not the extremes.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional

import pandas as pd


class Unit(Enum):
    """What a number means, and the range a correctly-scaled column sits in.

    `typical_max` bounds where the MEDIAN may sit, not where any single value
    may sit. Individual rows legitimately exceed it.
    """

    FRACTION = ("fraction", -1.0, 1.0)          # -0.05 == -5%
    PERCENT = ("percent", -100.0, 100.0)        # -5.0 == -5%
    PROBABILITY = ("probability", 0.0, 1.0)
    SCORE_0_100 = ("score 0-100", 0.0, 100.0)
    INR = ("INR", None, None)                   # unbounded by nature
    DAYS = ("days", 0.0, None)
    COUNT = ("count", 0.0, None)
    MULTIPLE = ("multiple", 0.0, None)          # 1.5x, unbounded above

    def __init__(self, label: str, typical_min: Optional[float],
                 typical_max: Optional[float]):
        self.label = label
        self.typical_min = typical_min
        self.typical_max = typical_max


# Verified against the live store on 2026-08-13 rather than assumed from the
# names — pnl_pct's median of -0.0034 matches sale_price/buy_price - 1, which
# is what makes it a fraction despite the _pct suffix.
FIELD_UNITS: Dict[str, Unit] = {
    "pnl_pct": Unit.FRACTION,
    "pnl_inr": Unit.INR,
    "buy_price": Unit.INR,
    "sale_price": Unit.INR,
    "buy_value": Unit.INR,
    "sale_value": Unit.INR,
    "initial_capital": Unit.INR,
    "sip_amount": Unit.INR,
    "qty": Unit.COUNT,
    "holding_days": Unit.DAYS,
    "stock_rank": Unit.COUNT,
    "rank": Unit.COUNT,
    "score": Unit.FRACTION,
    "matched_conditions": Unit.COUNT,
    "total_conditions": Unit.COUNT,
    "conviction": Unit.FRACTION,
    "size_multiplier": Unit.MULTIPLE,
    # 0-100, with thresholds at 40 (monitor), 60 (reduce), 80 (urgent) in
    # backtest/portfolio.py. Reading this as a probability puts every value
    # above 1.0 and makes every urgency comparison true.
    "exit_urgency": Unit.SCORE_0_100,
    "exit_survival_5d": Unit.PROBABILITY,
    "exit_survival_21d": Unit.PROBABILITY,
    "exit_survival_63d": Unit.PROBABILITY,
    "cagr": Unit.FRACTION,
    "max_drawdown": Unit.FRACTION,
    "win_rate": Unit.FRACTION,
    "sharpe": Unit.MULTIPLE,
    "sortino": Unit.MULTIPLE,
    "calmar": Unit.MULTIPLE,
}


@dataclass(frozen=True)
class UnitViolation:
    field: str
    unit: Unit
    median: float
    detail: str

    def __str__(self) -> str:
        return (
            f"{self.field}: declared {self.unit.label}, but the median is "
            f"{self.median:,.4g} — {self.detail}"
        )


def unit_of(field: str) -> Optional[Unit]:
    return FIELD_UNITS.get(field)


def describe(field: str) -> str:
    """Header text for a report column, so a reader never has to guess."""
    unit = unit_of(field)
    return f"{field} ({unit.label})" if unit else field


def check_frame(frame: pd.DataFrame) -> List[UnitViolation]:
    """Range-check every declared column, reading the median.

    Undeclared columns are skipped rather than flagged: this must not fail
    a run because somebody added a column, or nobody will add columns.
    """
    violations: List[UnitViolation] = []
    for column in frame.columns:
        unit = unit_of(column)
        if unit is None or unit.typical_max is None and unit.typical_min is None:
            continue
        values = pd.to_numeric(frame[column], errors="coerce").dropna()
        if values.empty:
            continue
        median = float(values.median())

        if unit.typical_max is not None and abs(median) > unit.typical_max:
            violations.append(UnitViolation(
                column, unit, median,
                f"a median beyond {unit.typical_max:g} suggests a scaling error "
                f"(a {unit.label} multiplied by 100?)",
            ))
        elif unit.typical_min is not None and median < unit.typical_min:
            violations.append(UnitViolation(
                column, unit, median,
                f"a {unit.label} cannot sit below {unit.typical_min:g}",
            ))
    return violations
