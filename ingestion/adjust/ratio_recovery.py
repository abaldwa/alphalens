"""
ingestion/adjust/ratio_recovery.py

Phase: 3.x (legacy corporate-action repair)
Owner: ingestion.adjust
Consumers: scripts/repair_corporate_action_ratios.py, tests/unit/test_ratio_recovery.py

Recovers SPLIT ratios that were ingested as 0.0, and decides — per action —
whether the recovered ratio may actually be written.

WHY THE RATIO IS MISSING

Pre-2017 NSE corporate-action records carry the split in the `details` prose
("Fv Split Rs.10/- To Rs.2/") rather than a numeric field, and the ingester of
that era did not parse it. 86 of 396 pre-2017 SPLITs carry ratio=0.0; 0 of 412
post-2017 do. The parser was evidently fixed and never backfilled.

A zero ratio is not inert. price_adjuster._action_factors computes
SPLIT price_factor = 1/ratio, so ratio=0 cannot be applied and the action is
skipped — leaving a 5:1 split as a genuine-looking -80% single-day return in
the price history.

WHY PARSING THE RATIO IS NOT ENOUGH

The obvious repair — parse the ratio, write it, re-run the adjuster — silently
corrupts data. Of the 86 recoverable ratios, 18 sit on price histories that
show NO gap at the ex-date: the split is already reflected in the stored
prices even though adj_factor records 1.0. Applying the recovered ratio to
those double-adjusts them, turning correct history into a 5x error, and
nothing downstream would flag it because the result is still a plausible
price series.

So `details` is treated as a HYPOTHESIS, and the price history is the
evidence. Two competing explanations are scored against the observed
close-to-close gap at the ex-date:

    adjustment owed  -> gap should be `expected_gap(action, ratio)`
    already adjusted -> gap should be 1.0

Whichever explains the observation better wins, and a ratio is written only
when "adjustment owed" wins by a clear margin. This matters most for small
bonus ratios (1:10 -> expected gap 1.1), where the two hypotheses are only 10%
apart and a coin-flip would corrupt half of them.

WHAT THIS MODULE DOES NOT DO

It never writes. Classification and repair are separated so the verdicts can
be reviewed before any change reaches the database.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import Optional

import pandas as pd

# "Fv Split Rs.10/- To Rs.2/" -> (10, 2) -> ratio 5.0.
# Both "Rs." and "Re." appear (Re. for one-rupee face values), the trailing
# "/-" is inconsistent, and at least one record reads "Split-Rs.10tors.2/"
# with the separators eaten — hence the tolerant whitespace/punctuation.
SPLIT_FV_PATTERN = re.compile(
    r"(?:rs|re)\.?\s*([0-9]+(?:\.[0-9]+)?)\s*/?\s*-?\s*to\s*"
    r"(?:rs|re)\.?\s*([0-9]+(?:\.[0-9]+)?)",
    re.IGNORECASE,
)

# Sessions either side of the ex-date to search for a usable close. A gap is
# measured across the ex-date boundary itself; the window only needs to be
# wide enough to survive a suspension or a holiday run.
GAP_WINDOW_SESSIONS = 12

# How much better the "adjustment owed" hypothesis must fit than "already
# adjusted" before a ratio is written. 1.5 means the rival explanation's error
# must be at least half again as large. A margin of 1.0 (merely "better") would
# decide near-ties by float noise, and near-ties are exactly the small-ratio
# bonuses where being wrong is silent.
DECISION_MARGIN = 1.5

# Absolute ceiling on the winning hypothesis's relative error. Real ex-date
# gaps carry a day of genuine market movement, so an exact match is not
# expected; but a 25%-off "match" is not evidence of anything.
MAX_CONFIRMING_ERROR = 0.20


class Verdict(str, Enum):
    """Why an action may or may not be repaired."""

    CONFIRMED = "confirmed"                 # gap matches the ratio; repair it
    ALREADY_ADJUSTED = "already_adjusted"   # no gap; prices are already correct
    CONTRADICTED = "contradicted"           # gap matches neither hypothesis
    NO_PRICE_DATA = "no_price_data"         # no legacy bars either side
    UNPARSEABLE = "unparseable"             # details carry no ratio


@dataclass(frozen=True)
class Classification:
    ticker: str
    ex_date: pd.Timestamp
    action_type: str
    verdict: Verdict
    ratio: Optional[float] = None
    expected_gap: Optional[float] = None
    observed_gap: Optional[float] = None
    error_if_owed: Optional[float] = None
    error_if_adjusted: Optional[float] = None

    @property
    def repairable(self) -> bool:
        return self.verdict is Verdict.CONFIRMED and self.ratio is not None


def parse_split_ratio(details: str) -> Optional[float]:
    """Recover a split ratio from an NSE face-value-change description.

    A face-value split from Rs.10 to Rs.2 multiplies the share count fivefold,
    so the ratio is old_fv / new_fv. Returns None when `details` describes
    something else — which is the correct answer for the bonus DEBENTURE and
    bonus DVR records that also carry ratio=0, because those do not dilute the
    equity share count and must never receive a price adjustment.
    """
    match = SPLIT_FV_PATTERN.search(details or "")
    if not match:
        return None
    old_fv, new_fv = float(match.group(1)), float(match.group(2))
    if new_fv <= 0 or old_fv <= new_fv:
        # A split cannot raise the face value or leave it unchanged; anything
        # that reads that way is a consolidation or a misparse, not a split.
        return None
    return old_fv / new_fv


def expected_gap(action_type: str, ratio: float) -> Optional[float]:
    """The prior-close / ex-date-close ratio an unadjusted action would show.

    Mirrors price_adjuster._action_factors, inverted: that module divides
    prior prices by this quantity, so an action still awaiting adjustment
    displays it as an apparent one-day crash.
    """
    if ratio is None or ratio <= 0:
        return None
    if action_type == "SPLIT":
        return float(ratio)
    if action_type == "BONUS":
        return 1.0 + float(ratio)
    return None


def observed_gap(prices: pd.DataFrame, ex_date) -> Optional[float]:
    """Measured close-to-close gap across the ex-date.

    `prices` must carry `date` and `close` and must already exclude
    source='fyers' rows: Fyers delivers pre-adjusted prices and by standing
    rule carries adj_factor=1.0, so a Fyers bar shows no gap regardless of
    whether this project's own adjustment was ever applied. Including one
    would make every legacy action look already-adjusted.
    """
    if prices.empty:
        return None
    ex = pd.Timestamp(ex_date)
    dates = pd.to_datetime(prices["date"])
    before = prices[dates < ex]
    on_or_after = prices[dates >= ex]
    if before.empty or on_or_after.empty:
        return None
    prior_close = float(before["close"].iloc[-1])
    ex_close = float(on_or_after["close"].iloc[0])
    if ex_close <= 0 or prior_close <= 0:
        return None
    return prior_close / ex_close


def classify_gap(observed: Optional[float], expected: Optional[float]) -> Verdict:
    """Score "adjustment owed" against "already adjusted" on the same evidence.

    Deliberately symmetric: the null hypothesis (prices are already fine) gets
    tested just as explicitly as the one being proposed. Assuming the
    adjustment is owed unless proven otherwise is what would double-adjust the
    18 histories that are already correct.
    """
    if expected is None:
        return Verdict.UNPARSEABLE
    if observed is None:
        return Verdict.NO_PRICE_DATA

    error_if_owed = abs(observed - expected) / expected
    error_if_adjusted = abs(observed - 1.0)

    if error_if_owed > MAX_CONFIRMING_ERROR:
        # Nothing about this gap looks like the action we think happened.
        return (
            Verdict.ALREADY_ADJUSTED
            if error_if_adjusted <= MAX_CONFIRMING_ERROR
            else Verdict.CONTRADICTED
        )
    if error_if_adjusted < error_if_owed * DECISION_MARGIN:
        # "Already adjusted" explains it as well or nearly as well. Refusing to
        # act on an ambiguous gap keeps a correct history correct; the cost of
        # a false negative is one unrepaired action, the cost of a false
        # positive is a silently corrupted price series.
        return Verdict.ALREADY_ADJUSTED
    return Verdict.CONFIRMED


def resolve_ratio(action_type: str, ratio: Optional[float], details: str) -> Optional[float]:
    """The usable ratio for an action: the stored one when valid, else
    recovered from `details`. Bonuses are never recovered from prose — the
    zero-ratio bonus records are debentures and DVR shares, which must not be
    adjusted at all."""
    if ratio is not None and ratio > 0:
        return float(ratio)
    if action_type == "SPLIT":
        return parse_split_ratio(details)
    return None


def combined_expected_gap(actions: "list[tuple[str, float]]") -> Optional[float]:
    """Expected gap when several actions share one ex-date.

    A 10:1 split alongside a bonus on the same day produces ONE price gap, and
    it is the product of both factors. Scoring each action separately measures
    the combined gap against a single action's expectation, so both come back
    CONTRADICTED and a real repair is refused — which is what happened to
    JAYBARMARU, JYOTISTRUC, ONMOBILE and SHRENIK.
    """
    factors = [
        gap for gap in (expected_gap(kind, ratio) for kind, ratio in actions)
        if gap is not None
    ]
    if not factors:
        return None
    product = 1.0
    for factor in factors:
        product *= factor
    return product


def classify_action(
    ticker: str,
    ex_date,
    action_type: str,
    ratio: Optional[float],
    details: str,
    prices: pd.DataFrame,
    siblings: "Optional[list]" = None,
) -> Classification:
    """Full verdict for one corporate action.

    A stored ratio is trusted when present; `details` is consulted only to
    recover a missing one. The gap test then runs identically either way —
    an action whose ratio was always fine but never applied is the same repair
    as one whose ratio had to be recovered first.

    `siblings` carries every action sharing this ticker and ex-date (including
    this one). When several land on the same day they produce one combined
    gap, and scoring this action against its own factor alone would reject a
    real repair as CONTRADICTED.
    """
    effective_ratio = resolve_ratio(action_type, ratio, details)

    if siblings and len(siblings) > 1:
        expected = combined_expected_gap(
            [(kind, resolve_ratio(kind, r, d)) for kind, r, d in siblings]
        )
    else:
        expected = expected_gap(action_type, effective_ratio) if effective_ratio else None
    observed = observed_gap(prices, ex_date)
    verdict = classify_gap(observed, expected)

    return Classification(
        ticker=ticker,
        ex_date=pd.Timestamp(ex_date),
        action_type=action_type,
        verdict=verdict,
        ratio=effective_ratio,
        expected_gap=expected,
        observed_gap=observed,
        error_if_owed=(
            abs(observed - expected) / expected
            if observed is not None and expected is not None
            else None
        ),
        error_if_adjusted=abs(observed - 1.0) if observed is not None else None,
    )
