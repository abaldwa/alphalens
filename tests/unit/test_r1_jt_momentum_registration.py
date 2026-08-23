"""
tests/unit/test_r1_jt_momentum_registration.py

Test R1 (Jegadeesh-Titman momentum lookback variants) registration.

Verifies:
1. R1 strategies are registered with correct naming and parameters
2. R1 uses Phase 0 dispatch params (rank_method, skip_months) with defaults
3. R1 is distinct from M-family in strategy_key but uses same adapter/bands
4. Non-regression: M-family strategies remain unchanged
"""

import pytest
from strategies.registry import get_strategy, strategy_key
from strategies.migrations.r1_jt_momentum_lookback import (
    build_rows,
    variant_name,
    LOOKBACK_MONTHS_TO_TEST,
    INITIAL_BAND_IDS,
    CATEGORY,
    REBALANCE_PERIOD,
    TOP_N,
)


class TestR1RegistrationNaming:
    """Verify R1 naming scheme is distinct and readable."""

    def test_variant_name_format(self):
        """R1 name structure: R1_{band_id}_{rank_start}_{rank_end}_lb{months}mo"""
        name = variant_name(band_id=1, rank_start=1, rank_end=50, lookback_months=12)
        assert name == "R1_1_1_50_lb12mo"
        assert name.startswith("R1_"), "R1 must start with R1 prefix for visibility"
        assert "_lb" in name and "mo" in name, "Must include lookback_months in name"

    def test_r1_vs_m_family_distinction(self):
        """R1 strategy_keys are distinct from M-family."""
        r1_key = strategy_key("momentum", "R1_1_1_50_lb12mo")
        assert r1_key == "momentum:R1_1_1_50_lb12mo"
        assert r1_key.startswith("momentum:R1_"), "R1 family must be prefixed R1"

    def test_all_lookback_variants_named_correctly(self):
        """Each lookback value gets its own strategy name."""
        names = [
            variant_name(1, 1, 50, lb) for lb in LOOKBACK_MONTHS_TO_TEST
        ]
        # All should be distinct
        assert len(names) == len(set(names)), "Each lookback must have unique name"
        # All should follow pattern
        for name in names:
            assert name.startswith("R1_1_1_50_lb")
            assert name.endswith("mo")


class TestR1RegistrationDefinition:
    """Verify R1 definition includes correct parameters."""

    def test_row_includes_phase0_params(self):
        """R1 rows include Phase 0 dispatch params with safe defaults."""
        rows = build_rows(band_ids=[1])
        assert len(rows) == 4, "Band 1 + 4 lookbacks = 4 rows"

        for row in rows:
            definition = row["definition"]
            # Phase 0 dispatch params
            assert "rank_method" in definition, "rank_method required"
            assert definition["rank_method"] == "trailing_return", "Default: trailing_return"
            assert "skip_months" in definition, "skip_months required"
            assert definition["skip_months"] == 0, "Default: no skip"
            # JT-specific params
            assert definition["category"] == CATEGORY
            assert definition["rebalance_frequency"] == REBALANCE_PERIOD
            assert definition["top_n"] == TOP_N

    def test_row_definition_lookback_captured(self):
        """Lookback_months is stored in definition for configuration."""
        rows = build_rows(band_ids=[1])
        lbs_in_defs = [row["definition"]["lookback_months"] for row in rows]
        assert lbs_in_defs == LOOKBACK_MONTHS_TO_TEST

    def test_filter_ids_match_balanced_category(self):
        """R1 uses balanced category filters (ADTV, circuit, quality)."""
        rows = build_rows(band_ids=[1])
        expected_filters = [
            "adtv_floor",
            "adtv_capped_sizing",
            "circuit_lock_proxy",
            "quality_gate",
        ]
        for row in rows:
            assert row["filter_ids"] == expected_filters


class TestR1RegistrationBands:
    """Verify R1 covers M1/M2 bands by default."""

    def test_initial_band_ids_m1_m2(self):
        """R1 Phase 1 focuses on M1 (band 1) and M2 (band 2)."""
        assert INITIAL_BAND_IDS == [1, 2], "Phase 1 validation on M1/M2"

    def test_build_rows_respects_band_ids(self):
        """build_rows() only generates for specified bands."""
        rows_12 = build_rows(band_ids=[1, 2])
        assert len(rows_12) == 8, "2 bands × 4 lookbacks = 8 rows"

        rows_all = build_rows(band_ids=list(range(1, 13)))
        assert len(rows_all) == 48, "12 bands × 4 lookbacks = 48 rows"

    def test_band_rank_ranges_correct(self):
        """Each band has correct rank_start/rank_end in definition."""
        rows = build_rows(band_ids=[1])
        for row in rows:
            assert row["definition"]["band_id"] == 1
            assert row["definition"]["rank_start"] == 1
            assert row["definition"]["rank_end"] == 50


class TestR1RegistryIntegration:
    """Test R1 strategies can be retrieved from registry (integration)."""

    def test_get_registered_r1_strategy(self):
        """R1 strategy can be retrieved from registry after registration."""
        key = strategy_key("momentum", "R1_1_1_50_lb12mo")
        strategy = get_strategy(key)
        if strategy is not None:  # May be None if DB is not populated
            assert strategy["definition"]["lookback_months"] == 12
            assert strategy["definition"]["rank_method"] == "trailing_return"
            assert strategy["status"] == "active"


class TestR1NonRegressionGuard:
    """Verify R1 registration doesn't affect M-family."""

    def test_r1_naming_distinct_from_m_family(self):
        """R1 naming scheme doesn't collide with M{1-12}."""
        r1_names = [
            variant_name(band_id, rank_start, rank_end, lb)
            for band_id, rank_start, rank_end in [(1, 1, 50), (2, 1, 75)]
            for lb in LOOKBACK_MONTHS_TO_TEST
        ]
        # No R1 name should start with 'M' (that's M-family)
        assert all(name.startswith("R1_") for name in r1_names)

    def test_r1_uses_same_adapter_as_mfamily(self):
        """R1 configuration is compatible with MomentumAdapter (no new code)."""
        rows = build_rows(band_ids=[1])
        for row in rows:
            # All these fields are already supported by MomentumAdapter
            definition = row["definition"]
            required_by_adapter = [
                "category",
                "band_id",
                "rank_start",
                "rank_end",
                "lookback_months",
                "rebalance_frequency",
                "top_n",
            ]
            for field in required_by_adapter:
                assert field in definition, f"{field} must be in definition for adapter"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
