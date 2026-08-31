"""
ingestion/adjust/continuity_gate.py

Phase: 3.x (legacy corporate-action repair)
Owner: ingestion.adjust
Consumers: scripts/repair_corporate_action_ratios.py, data_integrity_check,
           tests/unit/test_continuity_gate.py

Flags price discontinuities that no real market move can produce, so an
unapplied corporate action is caught by the pipeline rather than by somebody
investigating a strange backtest result months later.

WHY A GATE AND NOT JUST THE REPAIR

The repair fixes the 156 actions currently detectable. It does not stop the
next one. Every defect in this area shared one property: the corrupted data
looked entirely plausible. A 10:1 split that was never applied reads as a
-90% day, which is indistinguishable from a genuine collapse unless you know
NSE price bands make it impossible.

That impossibility is the whole basis of the check. Under NSE's circuit
framework a stock cannot move beyond its band in a session, so a close-to-
close move past MAX_LEGACY_DAILY_MOVE is not a market event — it is a data
defect, by construction rather than by judgement.

WHAT IS DELIBERATELY EXCLUDED

  - source='fyers' bars, which are pre-adjusted at source and carry
    adj_factor=1.0 by standing rule.
  - circuit-locked bars (high == low with volume), where the band was hit and
    the move is real.
  - the ex-date of a KNOWN action, where a gap is the expected state before
    adjustment runs rather than a defect.

The threshold sits at 35% rather than at a band edge because bands widen for
some securities and a genuine gap-open can clear 20%. 35% is comfortably
above any real session and comfortably below the smallest corporate action
that matters (a 1:2 bonus, at 33%) — which is why the gate reports rather
than blocks: the two ranges very nearly touch.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Optional

import pandas as pd

# A close-to-close move beyond this is not achievable under NSE price bands
# in a non-circuit session, so it indicates a data defect rather than a
# market event.
MAX_LEGACY_DAILY_MOVE = 0.35


@dataclass(frozen=True)
class Discontinuity:
    ticker: str
    date: pd.Timestamp
    prev_close: float
    close: float

    @property
    def move(self) -> float:
        return self.close / self.prev_close - 1.0

    @property
    def implied_ratio(self) -> float:
        """The split/bonus factor that would explain the jump, which is the
        first thing a reader wants: 4.98 says 'unapplied 5:1' immediately."""
        return self.prev_close / self.close

    def __str__(self) -> str:
        return (
            f"{self.ticker} {self.date.date()}: {self.move:+.1%} "
            f"({self.prev_close:,.2f} -> {self.close:,.2f}, "
            f"implies {self.implied_ratio:.2f}x)"
        )


def find_discontinuities(
    prices: pd.DataFrame,
    known_action_dates: Optional[set[Any]] = None,
    threshold: float = MAX_LEGACY_DAILY_MOVE,
) -> List[Discontinuity]:
    """Impossible close-to-close moves in a single ticker's bars.

    `prices` needs date, close, high, low, volume, sorted by date, with Fyers
    rows already excluded. `known_action_dates` suppresses ex-dates of actions
    that are known and simply not yet adjusted — a gap there is the expected
    state, not a discovery.
    """
    if len(prices) < 2:
        return []

    known = known_action_dates or set()
    frame = prices.copy()
    frame["date"] = pd.to_datetime(frame["date"])
    frame = frame.sort_values("date")
    frame["prev_close"] = frame["close"].shift(1)

    found: List[Discontinuity] = []
    for row in frame.itertuples():
        prev_close_val: float = float(row.prev_close) if not pd.isna(row.prev_close) else float('nan')
        close_val: float = float(row.close)

        if pd.isna(prev_close_val) or prev_close_val <= 0 or close_val <= 0:
            continue
        if row.date.date() in known or row.date in known:
            continue
        # A circuit-locked bar opened and closed at the band: the move is
        # real and the exchange enforced it.
        if float(row.high) == float(row.low) and float(row.volume) > 0:
            continue
        if abs(close_val / prev_close_val - 1.0) > threshold:
            found.append(
                Discontinuity(
                    ticker=getattr(row, "ticker", ""),
                    date=row.date,
                    prev_close=prev_close_val,
                    close=close_val,
                )
            )
    return found
