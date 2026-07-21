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
    def test_technical_round_trip(self):
        sid = build_strategy_id("technical", "E2", HorizonBucket.D21, as_of=date(2026, 7, 22))
        assert sid == "ta_e2_21d_20260722"

        parsed = parse_strategy_id(sid)
        assert parsed.channel == "technical"
        assert parsed.descriptor == "e2"
        assert parsed.horizon_bucket == HorizonBucket.D21
        assert parsed.run_date == date(2026, 7, 22)

    def test_fundamental_round_trip(self):
        sid = build_strategy_id("fundamental", "garp", HorizonBucket.D63, as_of=date(2026, 1, 1))
        parsed = parse_strategy_id(sid)
        assert parsed.channel == "fundamental"
        assert parsed.descriptor == "garp"
        assert parsed.horizon_bucket == HorizonBucket.D63
        assert parsed.run_date == date(2026, 1, 1)

    def test_momentum_descriptor_with_underscores(self):
        sid = build_strategy_id("momentum", "top10_6m", HorizonBucket.D21, as_of=date(2026, 3, 15))
        parsed = parse_strategy_id(sid)
        assert parsed.descriptor == "top10_6m"
        assert parsed.run_date == date(2026, 3, 15)

    def test_descriptor_is_sanitized(self):
        sid = build_strategy_id("technical", "Minervini SEPA!", HorizonBucket.D63, as_of=date(2026, 1, 1))
        assert sid == "ta_minervini_sepa_63d_20260101"

    def test_unknown_channel_raises(self):
        with pytest.raises(ValueError):
            build_strategy_id("ml", "x", HorizonBucket.D5)

    def test_non_canonical_id_raises_on_parse(self):
        with pytest.raises(ValueError):
            parse_strategy_id("signal_5d")


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
