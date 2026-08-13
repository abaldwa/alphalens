"""
tests/unit/test_exit_intent.py

STEP 4: the exit interface carries INTENT, not a score the consumer
re-thresholds.

The defect being fixed had two halves that compounded:

  1. RuleBasedExitPolicy encoded its decision into an urgency band, and the
     bands did not survive the round trip through
     exit_action_for_urgency's >80 sale threshold. max_hold topped out at 65
     and momentum_exhaustion at 79, so neither could EVER sell — across 65
     baseline runs and 108,762 model-driven exits, 0.00% were time exits.

  2. BacktestOrchestrator called PortfolioSimulator's STATIC threshold map
     while driving a StrategyPortfolio, which had no reduce operation at all,
     so every 'reduce_position' fell out of the if/elif chain and did nothing
     — no trade, no counter, no log line. The whole 60-80 band evaporated, and
     that is exactly the band the dead triggers above emitted into.

These tests pin reachability per trigger rather than in aggregate. An
aggregate "some exits happen" assertion passes cleanly while three of four
triggers are dead, which is how this survived for months.
"""

from datetime import date

import pandas as pd
import pytest

from backtest.core.horizon import HorizonBucket
from backtest.core.portfolio import StrategyPortfolio
from systems.ml_signal_engine.models.exit.exit_intent import (
    EXIT_ACTION_EXIT,
    EXIT_ACTION_HOLD,
    EXIT_ACTIONS,
    action_from_urgency,
    validate_actions,
)
from systems.ml_signal_engine.models.exit.rule_based_exit_policy import RuleBasedExitPolicy


POLICY = RuleBasedExitPolicy(target_pct=0.10, stop_pct=-0.05, max_hold_days=21)


def _ctx(**overrides):
    row = {"unrealised_pnl_pct": 0.0, "days_held": 1, "drawdown_from_peak": 0.0}
    row.update(overrides)
    return pd.DataFrame([row], index=["AAA"])


def _action(**overrides):
    return POLICY.predict_full(_ctx(**overrides))["exit_action"].iloc[0]


# ---------------------------------------------------------------------------
# Every trigger must be individually reachable
# ---------------------------------------------------------------------------

def test_stop_hit_exits():
    assert _action(unrealised_pnl_pct=-0.06) == EXIT_ACTION_EXIT


def test_target_hit_exits_without_needing_an_overshoot():
    """Under the urgency round trip, target_hit's band STARTED at 70 and a
    position exactly at target scored below the >80 sale threshold — it had to
    overshoot by roughly 20 percentage points to sell. Hitting the target is
    the whole point of having one."""
    assert _action(unrealised_pnl_pct=0.10) == EXIT_ACTION_EXIT


def test_max_hold_exits():
    """Previously unreachable: the band topped out at 65 against a >80
    threshold, producing 0.00% time exits across 108,762 exits."""
    assert _action(days_held=21) == EXIT_ACTION_EXIT


def test_momentum_exhaustion_exits():
    """Previously unreachable: band capped at 79 against a >80 threshold."""
    assert _action(unrealised_pnl_pct=0.05, drawdown_from_peak=-0.15) == EXIT_ACTION_EXIT


def test_nothing_triggered_holds():
    assert _action(unrealised_pnl_pct=0.02, days_held=3) == EXIT_ACTION_HOLD


def test_pnd_score_forces_an_exit():
    ctx = _ctx(unrealised_pnl_pct=0.02, days_held=3)
    # PND_EXIT_SCORE_THRESHOLD is 50.0 on a 0-100 scale, not 0-1.
    ctx["pnd_score"] = 99.0
    assert POLICY.predict_full(ctx)["exit_action"].iloc[0] == EXIT_ACTION_EXIT


def test_urgency_is_still_emitted_for_ranking():
    """Urgency is retained deliberately — it usefully ranks which of several
    simultaneous exits is most pressing. It simply is no longer how the
    decision travels."""
    out = POLICY.predict_full(_ctx(unrealised_pnl_pct=-0.20))
    assert out["exit_urgency"].iloc[0] > 80
    assert out["exit_action"].iloc[0] == EXIT_ACTION_EXIT


def test_the_two_previously_dead_triggers_sit_below_the_old_sale_threshold():
    """Proves the old contract really could not fire them, so the tests above
    are guarding a real regression rather than a hypothetical one."""
    from backtest.portfolio import EXIT_URGENT_THRESHOLD

    max_hold = POLICY.predict_full(_ctx(days_held=21))
    momentum = POLICY.predict_full(_ctx(unrealised_pnl_pct=0.05, drawdown_from_peak=-0.15))

    assert max_hold["exit_urgency"].iloc[0] <= EXIT_URGENT_THRESHOLD
    assert momentum["exit_urgency"].iloc[0] <= EXIT_URGENT_THRESHOLD
    # ...and yet both now exit, because intent no longer travels as a score.
    assert max_hold["exit_action"].iloc[0] == EXIT_ACTION_EXIT
    assert momentum["exit_action"].iloc[0] == EXIT_ACTION_EXIT


# ---------------------------------------------------------------------------
# Contract validation
# ---------------------------------------------------------------------------

def test_unknown_action_raises_rather_than_falling_through():
    """A typo'd action would otherwise reach a consumer's if/elif chain and
    drop out of the bottom as a no-op — the same silent failure one layer
    down."""
    with pytest.raises(ValueError, match="unknown exit action"):
        validate_actions(["exit", "sell_everything"])


def test_all_declared_actions_validate():
    validate_actions(EXIT_ACTIONS)


def test_legacy_urgency_mapping_still_produces_defined_actions():
    urgency = pd.Series([95.0, 70.0, 50.0, 10.0], index=list("abcd"))
    actions = action_from_urgency(
        urgency, urgent_threshold=80.0, reduce_threshold=60.0, monitor_threshold=40.0,
    )
    assert list(actions) == ["exit", "reduce", "monitor", "hold"]
    validate_actions(actions)


# ---------------------------------------------------------------------------
# StrategyPortfolio.reduce_position — the method whose absence was the defect
# ---------------------------------------------------------------------------

def _portfolio_holding(qty: int = 100) -> StrategyPortfolio:
    p = StrategyPortfolio(initial_capital=1_000_000.0, horizon_bucket=HorizonBucket.D21)
    p.buy("AAA", "IT", 100.0, date(2024, 1, 1), {"AAA": 100.0}, adtv_cr=500.0)
    p.positions["AAA"].quantity = qty
    return p


def test_reduce_position_exists_and_halves_the_holding():
    """The orchestrator's StrategyPortfolio had no reduce method at all, which
    is why the 60-80 band did nothing. PortfolioSimulator has had a working
    one the whole time; only the class actually used lacked it."""
    p = _portfolio_holding(100)
    trade = p.reduce_position("AAA", 110.0, date(2024, 2, 1))
    assert trade is not None
    assert trade.quantity == 50
    assert p.positions["AAA"].quantity == 50


def test_reducing_the_whole_position_removes_it_from_the_book():
    p = _portfolio_holding(100)
    p.reduce_position("AAA", 110.0, date(2024, 2, 1), fraction=1.0)
    assert "AAA" not in p.positions


def test_a_sub_one_share_reduction_is_a_genuine_no_op():
    """Not a silent failure: a reduction that rounds to zero shares has no
    executable form, so returning None leaves nothing intended undone."""
    p = _portfolio_holding(1)
    assert p.reduce_position("AAA", 110.0, date(2024, 2, 1), fraction=0.4) is None
    assert p.positions["AAA"].quantity == 1


def test_reducing_a_position_that_is_not_held_returns_none():
    p = StrategyPortfolio(initial_capital=1_000_000.0, horizon_bucket=HorizonBucket.D21)
    assert p.reduce_position("NOPE", 100.0, date(2024, 2, 1)) is None


def test_invalid_fraction_raises():
    p = _portfolio_holding(100)
    for bad in (0.0, -0.5, 1.5):
        with pytest.raises(ValueError, match="fraction must be in"):
            p.reduce_position("AAA", 110.0, date(2024, 2, 1), fraction=bad)


def test_reduce_releases_cash_and_books_a_trade():
    p = _portfolio_holding(100)
    cash_before = p.cash
    trade = p.reduce_position("AAA", 110.0, date(2024, 2, 1))
    assert p.cash > cash_before
    assert trade in p.trades
    assert trade.exit_reason == "exit_model_reduce"


# ---------------------------------------------------------------------------
# Contract coverage across every registered policy
# ---------------------------------------------------------------------------

def test_every_registered_variant_emits_intent():
    """The gate that makes the legacy fallback dead code rather than a quiet
    default.

    Two policies were caught by exactly this check during the migration and
    would otherwise have shipped on the legacy path — including
    `risk_managed`, which is the DEFAULT variant. Both failed for the same
    unobvious reason: their predict_full ends with an explicit column
    selection, so adding a column upstream is not enough; it has to be added
    to the projection too. A per-policy assertion catches that where testing
    one representative policy would not.
    """
    from backtest.core.engine import ALL_EXIT_POLICY_VARIANTS, build_exit_model_for_variant

    X = pd.DataFrame(
        {
            "unrealised_pnl_pct": [0.12, -0.06, 0.02],
            "days_held": [3, 3, 30],
            "drawdown_from_peak": [0.0, 0.0, 0.0],
            "template": ["C1"] * 3,
            "atr_pct": [0.02] * 3,
            "sma_200_ratio": [1.1, 0.9, 1.05],
            "roc_10": [1.0, -1.0, 0.5],
        },
        index=list("abc"),
    )

    missing = []
    for variant in ALL_EXIT_POLICY_VARIANTS:
        out = build_exit_model_for_variant(variant).predict_full(X)
        if "exit_action" not in out.columns:
            missing.append(variant)
        else:
            validate_actions(out["exit_action"].unique())
    assert not missing, f"policies still on the legacy urgency contract: {missing}"


def test_composite_takes_the_severity_union_not_the_loudest_child():
    """`combined` means "barriers OR thesis break, whichever fires first", so
    one child saying exit must exit even when the other says hold and scores
    higher urgency. Selecting the max-urgency child's ACTION would make the
    result depend on the children's urgency scales agreeing — they do not: a
    max-hold exit scores 59 against 45 for nothing-happening, only 14 points
    clear.
    """
    from systems.ml_signal_engine.models.exit.composite_exit_policy import CompositeExitPolicy

    class _Quiet:
        def predict_full(self, X):
            return pd.DataFrame(
                {
                    "exit_action": ["exit"] * len(X),
                    "exit_urgency": [50.0] * len(X),
                    "exit_type": ["opportunity_cost"] * len(X),
                    "exit_survival_5d": [float("nan")] * len(X),
                    "exit_survival_21d": [float("nan")] * len(X),
                    "exit_survival_63d": [float("nan")] * len(X),
                },
                index=X.index,
            )

    class _LoudButHolding:
        def predict_full(self, X):
            return pd.DataFrame(
                {
                    "exit_action": ["hold"] * len(X),
                    "exit_urgency": [79.0] * len(X),
                    "exit_type": ["momentum_exhaustion"] * len(X),
                    "exit_survival_5d": [float("nan")] * len(X),
                    "exit_survival_21d": [float("nan")] * len(X),
                    "exit_survival_63d": [float("nan")] * len(X),
                },
                index=X.index,
            )

    out = CompositeExitPolicy([_Quiet(), _LoudButHolding()]).predict_full(_ctx())
    assert out["exit_action"].iloc[0] == EXIT_ACTION_EXIT
    # The louder child still sets the reported urgency — that half is unchanged.
    assert out["exit_urgency"].iloc[0] == 79.0
