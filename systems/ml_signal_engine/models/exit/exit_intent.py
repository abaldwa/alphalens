"""
systems/ml_signal_engine/models/exit/exit_intent.py

Phase: 3.x (Technical backtest refactor — STEP 4)
Owner: ml_signal_engine / exit
Consumers: every module under systems/ml_signal_engine/models/exit/,
           backtest/core/engine.py, backtest/portfolio.py,
           tests/unit/test_exit_intent.py

The exit interface carries INTENT. Previously it carried only a score, which
the consumer re-thresholded to recover an action the policy had already
decided — a lossy round trip through a number, with two failure modes that
both actually happened.

HOW THE INFORMATION WAS LOST

RuleBasedExitPolicy computes four booleans (target_hit, stop_hit,
max_hold_hit, momentum_exhausted). It knows exactly what it wants. It then
encodes that decision into an urgency band:

    stop_hit             80-100
    target_hit           70-90
    momentum_exhausted   60-79
    max_hold_hit         50-65
    nothing triggered    45

and PortfolioSimulator.exit_action_for_urgency decodes it back:

    > 80  immediate_exit
    > 60  reduce_position
    > 40  monitor
    else  hold

The bands do not survive the round trip. max_hold_hit tops out at 65 and
momentum_exhausted at 79, so NEITHER can ever reach the >80 needed to sell:
across 65 baseline runs and 108,762 model-driven exits, 0.00% were time exits.
A policy that decided "exit, the holding period is up" had that decision
silently downgraded to "reduce" by a threshold table it never saw. And
target_hit's band starts at 70, so a position exactly at target does not sell
either — it must overshoot by roughly 20 percentage points to clear 80.

WHY THE DOWNGRADE WAS INVISIBLE

BacktestOrchestrator._apply_exit_policy calls PortfolioSimulator's STATIC
threshold map while driving a StrategyPortfolio — a different class, which has
no reduce operation at all. So every 'reduce_position' fell through to no
action whatsoever, with no counter, no log line and no data_gap. The whole
60-80 band evaporated. The two defects compound: baseline's dead triggers
emitted 50-79, squarely inside the band that had no implementation behind it,
so even raising their urgency would not have made them fire.

THE CONTRACT NOW

Policies emit an `exit_action` column stating intent directly. `exit_urgency`
remains — it is genuinely useful for ranking which of several exits is most
pressing, and for reporting — but it is no longer how the decision is
transmitted. An action a consumer cannot perform raises rather than silently
resolving to nothing.
"""

from __future__ import annotations

from typing import Iterable

import numpy as np
import pandas as pd

# The complete set of intents a policy may express.
#
# "reduce" is retained rather than dropped even though partial exits are rare,
# because dropping it would push policies into expressing "partly wrong" as
# either a full exit or nothing — and the second is what silently happened for
# months.
EXIT_ACTION_HOLD = "hold"
EXIT_ACTION_MONITOR = "monitor"
EXIT_ACTION_REDUCE = "reduce"
EXIT_ACTION_EXIT = "exit"

EXIT_ACTIONS = (EXIT_ACTION_HOLD, EXIT_ACTION_MONITOR, EXIT_ACTION_REDUCE, EXIT_ACTION_EXIT)

# Actions that move stock. Used by consumers to decide whether they need a
# price and a fill at all.
ACTIONABLE = (EXIT_ACTION_REDUCE, EXIT_ACTION_EXIT)


class UnsupportedExitAction(NotImplementedError):
    """A policy asked for an action this consumer cannot perform.

    Raised, never swallowed. The condition this replaces — StrategyPortfolio
    receiving 'reduce_position' and doing nothing — is exactly the kind of
    silent no-op that leaves a backtest reporting a strategy nobody ran. A
    crash during development is cheap; a sweep whose exits quietly did not
    happen costs a re-run and, worse, may not be noticed at all.
    """


def validate_actions(actions: Iterable[str]) -> None:
    """Raise on any action outside EXIT_ACTIONS.

    Deliberately strict. A typo'd action string would otherwise flow through
    to a consumer's if/elif chain and fall out of the bottom as a no-op — the
    same failure this module exists to remove, reintroduced one layer down.
    """
    unknown = {a for a in actions if a not in EXIT_ACTIONS}
    if unknown:
        raise ValueError(
            f"unknown exit action(s) {sorted(unknown)}; must be one of {list(EXIT_ACTIONS)}"
        )


def action_from_urgency(
    urgency: pd.Series, urgent_threshold: float, reduce_threshold: float,
    monitor_threshold: float,
) -> pd.Series:
    """Legacy urgency -> action mapping, for policies not yet emitting intent.

    Provided so a policy without an explicit exit_action still produces a
    defined result rather than an empty column, NOT as the preferred path. Any
    policy relying on this inherits the band problem described in the module
    docstring: a decision expressed as a number in a band that the thresholds
    may not agree with.

    Consumers should treat a policy that needs this as unfinished, and the
    caller records it (see engine.py's fallback counter) so "still on the
    legacy path" is a visible fact rather than an assumption.
    """
    return pd.Series(
        np.select(
            [urgency > urgent_threshold, urgency > reduce_threshold, urgency > monitor_threshold],
            [EXIT_ACTION_EXIT, EXIT_ACTION_REDUCE, EXIT_ACTION_MONITOR],
            default=EXIT_ACTION_HOLD,
        ),
        index=urgency.index,
        dtype=object,
    )
