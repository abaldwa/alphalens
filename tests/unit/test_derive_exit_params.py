"""
tests/unit/test_derive_exit_params.py

backtest/derive_exit_params.py now sets every stop, target and max-hold the
Technical backtests run with, so its failure modes are expensive and quiet: a
wrong number here does not crash anything, it just produces a plausible-looking
sweep whose barriers are wrong. These tests pin the properties that make the
derivation trustworthy rather than merely runnable.

Synthetic frames per SPEC-SYS-006's test-fixture exemption — the point is to
control MAE/MFE exactly, which real trades cannot do.
"""

import numpy as np
import pandas as pd
import pytest

from backtest.derive_exit_params import (
    MAX_WINNERS_STOPPED_FRACTION,
    TARGET_MFE_PERCENTILE,
    derive_params,
    horizon_bucket,
    params_to_frame,
)


def _trades(n=500, template="X1", hold=30, seed=0):
    """Winners and losers with controlled paths. Winners are given shallow
    drawdowns and losers deep ones, so a correct stop separates them."""
    rng = np.random.default_rng(seed)
    half = n // 2
    return pd.DataFrame(
        {
            "template": [template] * n,
            "holding_days": [hold] * n,
            "pnl_pct": np.r_[rng.uniform(0.01, 0.40, half), rng.uniform(-0.40, -0.01, n - half)],
            "mae": np.r_[-rng.uniform(0.0, 0.08, half), -rng.uniform(0.05, 0.45, n - half)],
            "mfe": np.r_[rng.uniform(0.05, 0.60, half), rng.uniform(0.0, 0.10, n - half)],
        }
    )


def test_requires_path_data_and_says_why():
    """Deriving a stop from pnl_pct is the specific mistake this module exists
    to prevent — a stop fires on the path, not the outcome. Missing mae/mfe
    must raise, not fall back to final P&L."""
    df = _trades().drop(columns=["mae", "mfe"])
    with pytest.raises(ValueError, match="path data"):
        derive_params(df)


def test_stop_respects_the_winner_kill_budget():
    """The stop's real cost is eventual winners cut short. Whatever level the
    derivation picks, no more than MAX_WINNERS_STOPPED_FRACTION of winners may
    have traded through it."""
    df = _trades(n=2000)
    (p,) = derive_params(df)
    assert p.stop_pct < 0
    assert p.winners_stopped_pct <= MAX_WINNERS_STOPPED_FRACTION * 100 + 1.0


def test_stop_is_not_merely_the_median_loss():
    """Regression guard for the tempting-but-wrong derivation. With winners
    drawing down at most 8% and losers far more, a path-aware stop must sit
    outside the winners' range — a stop derived from the losers' typical P&L
    (around -20% here) is a different number, and one derived from the median
    loss magnitude would sit inside the winner cloud and shred it."""
    df = _trades(n=2000)
    (p,) = derive_params(df)
    winners = df[df.pnl_pct > 0]
    assert p.stop_pct <= winners.mae.quantile(MAX_WINNERS_STOPPED_FRACTION) + 1e-9


def test_target_sits_above_the_median_reachable_move():
    """A target at the median winner's gain caps every winner at the median and
    throws away the right tail. The derived target must be strictly above the
    median MFE."""
    df = _trades(n=2000)
    (p,) = derive_params(df)
    assert p.target_pct > df.mfe.median()
    # abs=5e-5 because the module rounds emitted params to 4 decimals — the
    # parameters land in a JSON file humans read and diff, and 0.3348 is a
    # reviewable number where 0.33475307053025116 is not.
    assert p.target_pct == pytest.approx(df.mfe.quantile(TARGET_MFE_PERCENTILE), abs=5e-5)


def test_single_outlier_cannot_move_the_parameters():
    """The reason 'average of median and max' was rejected: one +1494% trade
    and one 1447-day hold exist in the real data. Percentile-based derivation
    must be materially unmoved by adding such a trade — an averaging rule
    would swing the target by hundreds of percent and reproduce the very
    unreachable-barrier defect this redesign removes."""
    df = _trades(n=2000)
    (before,) = derive_params(df)

    outlier = df.iloc[[0]].copy()
    outlier["pnl_pct"], outlier["mfe"], outlier["holding_days"] = 14.9, 14.9, 1447
    (after,) = derive_params(pd.concat([df, outlier], ignore_index=True))

    assert after.target_pct == pytest.approx(before.target_pct, rel=0.02)
    assert after.max_hold_days == before.max_hold_days
    # And the naive rule really would have exploded, which is what makes the
    # guarantee above meaningful rather than vacuous.
    naive = (df.mfe.median() + 14.9) / 2
    assert naive > before.target_pct * 5


def test_emitted_params_are_valid_for_the_exit_policy():
    """RuleBasedExitPolicy rejects stop_pct >= 0, target_pct <= 0 and
    max_hold_days <= 0. Emitting an invalid set would fail deep inside a sweep,
    hours in."""
    from systems.ml_signal_engine.models.exit.rule_based_exit_policy import RuleBasedExitPolicy

    for p in derive_params(_trades(n=600)):
        RuleBasedExitPolicy(**p.as_policy_kwargs())


def test_thin_templates_fall_back_to_their_horizon_bucket():
    """A template with a handful of trades cannot support its own percentiles.
    It must inherit the bucket aggregate and SAY it did, so a reader can tell
    a derived number from a borrowed one."""
    big = _trades(n=800, template="BIG", hold=30)
    thin = _trades(n=20, template="THIN", hold=30, seed=7)
    out = {p.template: p for p in derive_params(pd.concat([big, thin], ignore_index=True))}
    assert out["BIG"].derived_from_own_trades is True
    assert out["THIN"].derived_from_own_trades is False
    assert out["THIN"].horizon_bucket == out["BIG"].horizon_bucket


def test_horizon_buckets_match_the_observed_clusters():
    """Median hold under no constraint clusters at 7 / ~31 / ~93 days, and the
    same stop costs 15% of winners in the short cluster but 60% in the long
    one — so the boundaries are load-bearing, not cosmetic."""
    assert horizon_bucket(7) == "short"
    assert horizon_bucket(31) == "mid"
    assert horizon_bucket(93) == "long"


def test_per_template_parameters_actually_differ_by_horizon():
    """If short and long templates came back with the same stop, the whole
    per-template derivation would be theatre."""
    short = _trades(n=800, template="S", hold=7)
    short["mae"] = short["mae"] * 0.3  # short trades traverse less
    long_ = _trades(n=800, template="L", hold=93, seed=3)
    out = {p.template: p for p in derive_params(pd.concat([short, long_], ignore_index=True))}
    assert out["S"].max_hold_days < out["L"].max_hold_days
    assert out["S"].stop_pct > out["L"].stop_pct  # tighter (less negative)


def test_empty_input_returns_nothing_rather_than_defaults():
    assert derive_params(_trades().iloc[0:0]) == []


def test_frame_round_trip_has_one_row_per_template():
    df = pd.concat([_trades(template="A"), _trades(template="B", seed=1)], ignore_index=True)
    frame = params_to_frame(derive_params(df))
    assert list(frame.template) == ["A", "B"]
