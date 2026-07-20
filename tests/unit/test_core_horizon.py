"""tests/unit/test_core_horizon.py — backtest/core/horizon.py."""

import pytest

from backtest.core.horizon import HORIZON_SIZING, HorizonBucket, sizing_for


class TestSizingFor:
    def test_returns_default_policy_for_each_defined_bucket(self):
        for bucket in HORIZON_SIZING:
            policy = sizing_for(bucket)
            assert policy.horizon_bucket == bucket
            assert 0 < policy.max_position_pct <= 1
            assert 0 < policy.max_sector_pct <= 1

    def test_override_replaces_only_specified_fields(self):
        base = sizing_for(HorizonBucket.D21)
        overridden = sizing_for(HorizonBucket.D21, overrides={"max_position_pct": 0.01})
        assert overridden.max_position_pct == 0.01
        assert overridden.max_sector_pct == base.max_sector_pct
        assert overridden.default_rebalance_cadence_days == base.default_rebalance_cadence_days

    def test_custom_bucket_requires_overrides(self):
        with pytest.raises(ValueError):
            sizing_for(HorizonBucket.CUSTOM)

    def test_custom_bucket_with_overrides_builds_policy(self):
        policy = sizing_for(
            HorizonBucket.CUSTOM,
            overrides={
                "max_position_pct": 0.10, "max_sector_pct": 0.30,
                "default_rebalance_cadence_days": 10, "min_holding_days": 2,
            },
        )
        assert policy.horizon_bucket == HorizonBucket.CUSTOM
        assert policy.max_position_pct == 0.10

    def test_multibagger_has_longest_min_holding(self):
        multibagger = sizing_for(HorizonBucket.MULTIBAGGER)
        for bucket in (HorizonBucket.D5, HorizonBucket.D21, HorizonBucket.D63, HorizonBucket.Y1):
            assert multibagger.min_holding_days >= sizing_for(bucket).min_holding_days
