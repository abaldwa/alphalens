"""
tests/unit/test_strategy_id.py

Unit tests for backtest/strategy_id.py's codified strategy_id format —
build_strategy_id() / parse_strategy_id() round-trip, and the
default_horizon_for_* helpers.
"""

from datetime import date

import pytest

from backtest.core.horizon import HorizonBucket
from backtest.strategy_id import (
    build_strategy_id,
    default_horizon_for_fundamental,
    default_horizon_for_momentum,
    default_horizon_for_technical,
    parse_strategy_id,
)


class TestBuildAndParseRoundTrip:
    """2026-08-20: the id is `{descriptor}_{horizon_code}`. The channel prefix
    and the trigger date were removed — both duplicated columns that
    backtest_runs already carries (`channel`, `created_at`), and together they
    made every id long enough to be unreadable at a glance."""

    def test_technical_round_trip(self):
        sid = build_strategy_id("technical", "E2_unconstrained", HorizonBucket.D21, as_of=date(2026, 7, 22))
        assert sid == "E2_unconstrained_21d"

        parsed = parse_strategy_id(sid)
        assert parsed.descriptor == "E2_unconstrained"
        assert parsed.horizon_bucket == HorizonBucket.D21

    def test_case_is_preserved(self):
        """Template codes ("E6") and momentum band ids ("M10") are uppercase;
        lowercasing them made ids look like a different scheme than the one
        the registry and the reports use."""
        assert build_strategy_id("technical", "E6", HorizonBucket.D63) == "E6_63d"

    def test_fundamental_round_trip(self):
        sid = build_strategy_id("fundamental", "garp", HorizonBucket.D63, as_of=date(2026, 1, 1))
        assert sid == "garp_63d"
        parsed = parse_strategy_id(sid)
        assert parsed.descriptor == "garp"
        assert parsed.horizon_bucket == HorizonBucket.D63

    def test_momentum_descriptor_with_underscores(self):
        sid = build_strategy_id(
            "momentum", "M10_301_500_allrisk_lb3mo_bimonthly_top10", HorizonBucket.D21,
        )
        assert sid == "M10_301_500_allrisk_lb3mo_bimonthly_top10_21d"
        parsed = parse_strategy_id(sid)
        assert parsed.descriptor == "M10_301_500_allrisk_lb3mo_bimonthly_top10"
        assert parsed.horizon_bucket == HorizonBucket.D21

    def test_descriptor_is_sanitized(self):
        """Non-alphanumerics still collapse to underscores — only the
        lowercasing was dropped, not the sanitisation."""
        sid = build_strategy_id("technical", "Minervini SEPA!", HorizonBucket.D63, as_of=date(2026, 1, 1))
        assert sid == "Minervini_SEPA_63d"

    def test_unknown_channel_raises(self):
        """channel no longer appears in the id but is still validated — a
        caller passing a bogus channel has a real bug."""
        with pytest.raises(ValueError):
            build_strategy_id("ml", "x", HorizonBucket.D5)

    def test_non_canonical_id_raises_on_parse(self):
        with pytest.raises(ValueError):
            parse_strategy_id("E2_unconstrained")  # no horizon code

    def test_pre_rename_id_is_not_canonical(self):
        """An id built before 2026-08-20 still carries a channel prefix and a
        trailing date. Those rows are real data, so parse must reject them
        loudly rather than mis-read the date as part of the descriptor."""
        with pytest.raises(ValueError):
            parse_strategy_id("ta_e2_21d_20260722")


class TestDefaultHorizons:
    def test_technical_style_defaults_match_explainer(self):
        assert default_horizon_for_technical("Mean Reversion") == HorizonBucket.D5
        assert default_horizon_for_technical("Momentum") == HorizonBucket.D21
        assert default_horizon_for_technical("Volatility") == HorizonBucket.D21
        assert default_horizon_for_technical("Trend Following") == HorizonBucket.D63

    def test_technical_unknown_style_raises(self):
        with pytest.raises(ValueError):
            default_horizon_for_technical("Unknown Style")

    def test_fundamental_preset_defaults(self):
        assert default_horizon_for_fundamental("quality_compounder") == HorizonBucket.Y1
        assert default_horizon_for_fundamental("garp") == HorizonBucket.D63
        assert default_horizon_for_fundamental("turnaround") == HorizonBucket.D63

    def test_momentum_lookback_scaling(self):
        assert default_horizon_for_momentum(1) == HorizonBucket.D21
        assert default_horizon_for_momentum(3) == HorizonBucket.D21
        assert default_horizon_for_momentum(6) == HorizonBucket.D63
        assert default_horizon_for_momentum(9) == HorizonBucket.D63
        assert default_horizon_for_momentum(12) == HorizonBucket.Y1
