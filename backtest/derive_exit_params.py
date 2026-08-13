"""
backtest/derive_exit_params.py

Phase: 3.x (Technical backtest refactor — exit-regime redesign)
Owner: backtest
Consumers: scripts/derive_exit_params_from_unconstrained.py,
           systems/ml_signal_engine/models/exit/per_template_exit_policy.py,
           tests/unit/test_derive_exit_params.py

Derives per-template stop / target / max-hold from the "unconstrained" control
runs — the only variant that imposes no engine barrier at all, so its trades
show what each screen does when nothing cuts it short. Every other variant's
trades are truncated by its own barriers and therefore cannot be used to choose
barriers without circularity.

WHY THIS MODULE EXISTS RATHER THAN A TABLE OF CONSTANTS
The previous per-template parameters were hand-chosen, and three of the four
"baseline" exit triggers turned out to be unreachable — across 65 runs and
108,762 model-driven exits, 0.00% were time exits. Numbers nobody can re-derive
are numbers nobody can check. Everything here is computed from trade data, and
the provenance (run count, trade count, date window) is emitted alongside the
parameters.

THE MEASUREMENT TRAP THIS MODULE AVOIDS
A stop fires on the PATH a trade takes, not on the outcome it eventually
reached. Choosing a stop from realised final P&L therefore answers the wrong
question: it tells you how big the losses were, not how many eventual WINNERS
dipped through the level first and would have been cut. Those are very
different numbers. Measured over a 15,832-trade sample with real OHLCV paths:

    stop    kills this % of        touches this % of
            eventual WINNERS       all trades
    -3%           45.7                  66.9
    -5%           27.4                  50.7
   -10%            8.6                  25.8
   -15%            2.7                  12.3
   -20%            0.9                   6.0

A -5% stop — which looks eminently reasonable next to a median losing trade of
about -5% — destroys more than a quarter of the trades that would have ended
green. So stops here are derived from MAE (maximum adverse excursion, the
worst drawdown actually traversed while the position was open), never from
pnl_pct. Targets are likewise derived from MFE (maximum favourable excursion):
a target only pays if the price actually reaches it intraday, and MFE is the
only field that says whether it did.

WHY NOT "THE AVERAGE OF MEDIAN AND MAX"
That rule was considered for target% and max-hold and is NOT used, because on
this data it reconstructs the exact defect the redesign exists to remove. The
maxima are single extreme trades — one +1493.95% trade recurs across five
templates, and the longest hold is 1,447 days — so averaging median with max
yields targets around +750% and max-holds around 740 days. Barriers that far
out never fire, which is precisely how "baseline" ended up with three dead
triggers. Percentiles are used instead: they are the same idea (a level above
the median) computed in a way that one outlier cannot move.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

import numpy as np
import pandas as pd

# Horizon clusters. Median holding period under no barrier at all is not
# spread smoothly across templates — it lands in three tight groups (7 days,
# ~31 days, ~93 days), reflecting the screens' own natural turnover. A single
# global parameter set would be wrong for all three: at a -5% stop, 15.1% of
# eventual winners are killed in the short bucket but 60.1% in the long one,
# because a 93-day trade simply has more chances to touch any given level.
# Parameters are therefore derived per template but sanity-bounded per bucket.
SHORT_MAX_HOLD_DAYS = 14
MID_MAX_HOLD_DAYS = 45

# Fraction of eventual winners we are willing to stop out. This is the real
# cost of a stop, and it is a choice rather than a computed quantity — so it
# is named, documented and testable instead of buried in a percentile. 10%
# keeps the large majority of the right tail intact while still cutting the
# genuinely broken trades.
MAX_WINNERS_STOPPED_FRACTION = 0.10

# Target percentile of MFE. A target set at the median winner's gain caps every
# winner at the median and throws away the entire right tail — the tail is where
# a momentum strategy's return lives. p75 of MFE sets the target above what the
# typical trade reaches, so it binds on strong trades without truncating them.
TARGET_MFE_PERCENTILE = 0.75

# Max-hold is the median holding period under no constraint, per the agreed
# rule. Unlike target and stop this one is safe as a median: it is a duration,
# bounded below by zero and not driven by outliers.
MAX_HOLD_PERCENTILE = 0.50

# A template with too few trades cannot support its own parameters; it falls
# back to its horizon bucket's aggregate. Chosen so every percentile below is
# computed from at least a few dozen observations per tail.
MIN_TRADES_FOR_OWN_PARAMS = 200


@dataclass
class TemplateExitParams:
    """Derived barriers for one template. stop_pct is negative, target_pct
    positive, both as FRACTIONS (-0.05 == -5%) to match RuleBasedExitPolicy's
    validation — the trade log's pnl_pct is also a fraction, and mixing the two
    conventions has already caused one 100x reporting error in this project."""

    template: str
    stop_pct: float
    target_pct: float
    max_hold_days: int
    horizon_bucket: str
    n_trades: int
    winners_stopped_pct: float
    target_reached_pct: float
    derived_from_own_trades: bool

    def as_policy_kwargs(self) -> Dict[str, float]:
        return {
            "stop_pct": self.stop_pct,
            "target_pct": self.target_pct,
            "max_hold_days": self.max_hold_days,
        }


def horizon_bucket(median_hold_days: float) -> str:
    if median_hold_days <= SHORT_MAX_HOLD_DAYS:
        return "short"
    if median_hold_days <= MID_MAX_HOLD_DAYS:
        return "mid"
    return "long"


def _stop_from_mae(mae: pd.Series, is_winner: pd.Series) -> float:
    """Loosest (smallest-magnitude) stop that still cuts no more than
    MAX_WINNERS_STOPPED_FRACTION of eventual winners.

    Expressed as a quantile of the winners' own MAE rather than a search over
    candidate levels: the level below which only 10% of winners ever traded is
    exactly the 10th percentile of winner MAE. Winners are the constraint —
    losers' MAE is irrelevant to the cost of the stop, since stopping a loser
    early is the point.
    """
    winner_mae = mae[is_winner]
    if winner_mae.empty:
        return float("nan")
    return float(np.quantile(winner_mae, MAX_WINNERS_STOPPED_FRACTION))


def derive_params(trades: pd.DataFrame) -> List[TemplateExitParams]:
    """
    trades : one row per closed trade from the unconstrained runs, with columns
        template : str
        pnl_pct  : float  FRACTION, not percent
        holding_days : int
        mae : float  FRACTION, <= 0, worst drawdown traversed while open
        mfe : float  FRACTION, >= 0, best gain traversed while open

    Raises rather than guessing if mae/mfe are absent. Deriving barriers from
    pnl_pct alone is the trap this module exists to prevent, so a caller that
    cannot supply path data must fail loudly instead of silently receiving
    plausible-looking numbers computed the wrong way.
    """
    required = {"template", "pnl_pct", "holding_days", "mae", "mfe"}
    missing = required - set(trades.columns)
    if missing:
        raise ValueError(
            f"derive_params requires path data; missing columns {sorted(missing)}. "
            "Stops and targets fire on the path a trade takes, not on its final "
            "P&L — see this module's docstring. Compute mae/mfe by replaying "
            "OHLCV between buy_date and sale_date rather than dropping this check."
        )
    if trades.empty:
        return []

    trades = trades.copy()
    trades["is_winner"] = trades["pnl_pct"] > 0

    median_hold = trades.groupby("template")["holding_days"].median()
    trades["bucket"] = trades["template"].map(median_hold).map(horizon_bucket)

    bucket_fallback = {
        bucket: (
            _stop_from_mae(g["mae"], g["is_winner"]),
            float(np.quantile(g["mfe"], TARGET_MFE_PERCENTILE)),
            int(np.quantile(g["holding_days"], MAX_HOLD_PERCENTILE)),
        )
        for bucket, g in trades.groupby("bucket")
    }

    out: List[TemplateExitParams] = []
    for template, g in trades.groupby("template"):
        bucket = g["bucket"].iloc[0]
        own = len(g) >= MIN_TRADES_FOR_OWN_PARAMS
        if own:
            stop = _stop_from_mae(g["mae"], g["is_winner"])
            target = float(np.quantile(g["mfe"], TARGET_MFE_PERCENTILE))
            hold = int(np.quantile(g["holding_days"], MAX_HOLD_PERCENTILE))
        else:
            stop, target, hold = bucket_fallback[bucket]

        # Guard the degenerate cases rather than emitting an invalid policy:
        # RuleBasedExitPolicy requires stop_pct < 0 and target_pct > 0, and a
        # zero-day max hold would exit every position on entry day.
        stop = -abs(stop) if np.isfinite(stop) and stop != 0 else -0.10
        target = abs(target) if np.isfinite(target) and target != 0 else 0.10
        hold = max(int(hold), 1)

        winners = g[g["is_winner"]]
        out.append(
            TemplateExitParams(
                template=template,
                stop_pct=round(stop, 4),
                target_pct=round(target, 4),
                max_hold_days=hold,
                horizon_bucket=bucket,
                n_trades=len(g),
                winners_stopped_pct=(
                    round(float((winners["mae"] <= stop).mean() * 100), 2)
                    if len(winners) else float("nan")
                ),
                target_reached_pct=round(float((g["mfe"] >= target).mean() * 100), 2),
                derived_from_own_trades=own,
            )
        )
    return out


def params_to_frame(params: List[TemplateExitParams]) -> pd.DataFrame:
    return pd.DataFrame([p.__dict__ for p in params]).sort_values("template")
