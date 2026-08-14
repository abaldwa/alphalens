"""tests/unit/test_momentum_descriptor_identity.py

Momentum's strategy_key was built from top_n and lookback_months only, so a
control run and a filtered run of the same lookback collided: both wrote to
strategy_signals under one strategy_key, and the ledger could no longer say
which variant emitted a given buy. Measured 2026-08-14 -- 8 momentum jobs
produced 4 distinct keys.

Technical and Fundamental do not have this problem because they name
themselves with a real template/preset. Momentum has no such name.
"""

from __future__ import annotations

import pytest

from backtest.run_orchestrator_backtest import _momentum_descriptor as descriptor

CONTROL = dict(top_n=10, lookback_months=3, min_adtv_cr=None, downtrend_filter_pct=None,
               circuit_band_pct=None, grace_cycles=2, exit_policy_variant="risk_managed")


def _d(**over):
    return descriptor(**{**CONTROL, **over})


def test_control_and_filtered_arms_do_not_collide():
    """The defect itself."""
    assert _d() != _d(min_adtv_cr=1.0, downtrend_filter_pct=0.15, circuit_band_pct=0.05)


def test_an_unfiltered_run_keeps_its_historical_key():
    """Existing ledger rows and reports must stay addressable -- a key that
    changes for runs whose behaviour did not change orphans them."""
    assert _d() == "top10_3m"


@pytest.mark.parametrize("field,value", [
    ("min_adtv_cr", 1.0),
    ("downtrend_filter_pct", 0.15),
    ("circuit_band_pct", 0.05),
    ("grace_cycles", 0),
    ("exit_policy_variant", "unconstrained"),
])
def test_every_signal_changing_parameter_is_in_the_key(field, value):
    """Each of these changes which signals are emitted, so each must change
    the identity. A parameter missing here is a silent collision."""
    assert _d(**{field: value}) != _d()


def test_zero_grace_is_distinguished_although_falsy():
    """0 is falsy; a truthiness guard would drop it and re-collide the
    rank-drop-only runs with the default-grace ones."""
    assert _d(grace_cycles=0) != _d()
    assert "g0" in _d(grace_cycles=0)


def test_descriptor_is_deterministic():
    assert _d(min_adtv_cr=1.0) == _d(min_adtv_cr=1.0)
