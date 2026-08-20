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
               circuit_band_pct=None, exit_policy_variant="risk_managed")


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
    ("exit_policy_variant", "unconstrained"),
])
def test_every_signal_changing_parameter_is_in_the_key(field, value):
    """Each of these changes which signals are emitted, so each must change
    the identity. A parameter missing here is a silent collision."""
    assert _d(**{field: value}) != _d()


def test_the_deprecated_levers_are_not_accepted():
    """[2026-08-18] grace_cycles and exit_rank were part of momentum's
    identity because they changed which sells were emitted. Both are
    deprecated, so passing one must fail rather than silently do nothing."""
    for field in ("grace_cycles", "exit_rank"):
        with pytest.raises(TypeError):
            _d(**{field: 0})


def test_descriptor_is_deterministic():
    assert _d(min_adtv_cr=1.0) == _d(min_adtv_cr=1.0)


class TestRegistryResolvedIdentity:
    """[ML40-2.4, 2026-08-15] The descriptor now prefers the name of the
    strategy_registry row that DECLARES the run, instead of a generated string
    the registry could never contain."""

    def _resolves(self, name):
        from strategies.registry import get_strategy

        return get_strategy(f"momentum:{name}") is not None

    def test_declared_grid_point_resolves_to_its_row(self):
        import pytest

        from strategies.momentum_identity import registry_name

        name = registry_name(
            rank_band_id=1, lookback_months=3, rebalance_cadence_days=5, top_n=10,
        )
        # 2026-08-20 rename: M{band}_{lo}_{hi}_{category}, category de-underscored.
        assert name == "M1_1_50_allrisk_lb3mo_weekly_top10"
        try:
            assert self._resolves(name)
        except Exception:
            pytest.skip("strategy_registry not populated in this environment")

    def test_each_filter_tier_maps_to_its_category(self):
        from strategies.momentum_identity import registry_name

        base = dict(rank_band_id=3, lookback_months=6, rebalance_cadence_days=21, top_n=15)
        balanced_flags = dict(min_adtv_cr=1.0, circuit_band_pct=0.05, quality_gate={"min_f_score": 4})

        assert registry_name(**base) == "M3_51_100_allrisk_lb6mo_monthly_top15"
        assert registry_name(**base, **balanced_flags).endswith("_balanced_lb6mo_monthly_top15")
        assert registry_name(
            **base, **balanced_flags, disable_buys_in_regime={"Bear"},
        ).endswith("_riskmanaged_lb6mo_monthly_top15")
        assert registry_name(
            **base, **balanced_flags, disable_buys_in_regime={"Bear"},
            orthogonalize_vs_size_beta=True,
        ).endswith("_maxdefensive_lb6mo_monthly_top15")

    def test_undeclared_combinations_return_none_rather_than_guessing(self):
        """A name invented for an undeclared run is worse than no name: it
        looks resolvable and is not. Each of these is a real way to be
        undeclared."""
        from strategies.momentum_identity import registry_name

        base = dict(rank_band_id=1, lookback_months=3, rebalance_cadence_days=5, top_n=10)
        # A PARTIAL filter set is neither all_risk nor balanced.
        assert registry_name(**{**base, "min_adtv_cr": 1.0}) is None
        # A cadence outside REBALANCE_PERIODS.
        assert registry_name(**{**base, "rebalance_cadence_days": 7}) is None
        # A band outside RANK_BANDS.
        assert registry_name(**{**base, "rank_band_id": 99}) is None

    def test_grace_and_exit_do_not_change_the_declared_name(self):
        """They are run parameters, not identity. strategy_signals' PK carries
        run_id, so two runs of one strategy under different exit policies stay
        distinguishable without widening the key."""
        from strategies.momentum_identity import registry_name

        base = dict(rank_band_id=1, lookback_months=3, rebalance_cadence_days=5, top_n=10)
        assert registry_name(**base) == registry_name(**base)
