"""
tests/unit/test_r10_nigam_pandey_registration.py

Test R10 (Nigam-Pandey Indian long-only momentum) registration.

Verifies:
1. R10 strategies are registered with correct naming and parameters
2. R10 uses 6-month lookback, 1-month skip, quarterly (63-day) rebalance
3. R10 is distinct from M-family and R1/R3 in strategy_key but uses same adapter/bands
4. Non-regression: M-family strategies remain unchanged
"""

import pytest
from strategies.registry import get_strategy, strategy_key
from strategies.migrations.r10_nigam_pandey_momentum import (
    build_rows,
    variant_name,
    LOOKBACK_MONTHS,
    SKIP_MONTHS,
    REBALANCE_CADENCE_DAYS,
    INITIAL_BAND_IDS,
    CATEGORY,
    TOP_N,
)


class TestR10RegistrationNaming:
    """Verify R10 naming scheme is distinct and readable."""

    def test_variant_name_format(self):
        """R10 name structure: R10_{band_id}_{rank_start}_{rank_end}"""
        name = variant_name(band_id=1, rank_start=1, rank_end=50)
        assert name == "R10_1_1_50"
        assert name.startswith("R10_"), "R10 must start with R10 prefix for visibility"

    def test_r10_vs_mfamily_distinction(self):
        """R10 strategy_keys are distinct from M-family."""
        r10_key = strategy_key("momentum", "R10_1_1_50")
        assert r10_key == "momentum:R10_1_1_50"
        assert r10_key.startswith("momentum:R10_"), "R10 family must be prefixed R10"

    def test_r10_vs_r1_distinction(self):
        """R10 naming is distinct from R1 (lookback is fixed, not varied)."""
        r1_name = "R1_1_1_50_lb6mo"  # R1's variant for 6-month lookback
        r10_name = variant_name(band_id=1, rank_start=1, rank_end=50)
        assert r10_name != r1_name, "R10 and R1 must have distinct names"
        assert r10_name.startswith("R10_")
        assert "_lb" not in r10_name, "R10 doesn't vary lookback, so omit it from name"


class TestR10RegistrationDefinition:
    """Verify R10 definition includes correct parameters."""

    def test_row_includes_r10_params(self):
        """R10 rows include Nigam-Pandey config: 6mo lookback, 1mo skip, quarterly rebalance."""
        rows = build_rows(band_ids=[1])
        assert len(rows) == 1, "Band 1 = 1 row (single config)"

        row = rows[0]
        definition = row["definition"]

        # R10-specific config
        assert definition["lookback_months"] == LOOKBACK_MONTHS == 6
        assert definition["skip_months"] == SKIP_MONTHS == 1
        assert definition["rebalance_cadence_days"] == REBALANCE_CADENCE_DAYS == 63

        # Common params
        assert definition["category"] == CATEGORY
        assert definition["top_n"] == TOP_N
        assert definition["rank_method"] == "trailing_return"

    def test_row_definition_band_captured(self):
        """Band_id and rank range are stored in definition."""
        rows = build_rows(band_ids=[1, 2])
        assert len(rows) == 2, "Two bands = 2 rows"

        assert rows[0]["definition"]["band_id"] == 1
        assert rows[0]["definition"]["rank_start"] == 1
        assert rows[0]["definition"]["rank_end"] == 50

        assert rows[1]["definition"]["band_id"] == 2

    def test_filter_ids_match_balanced_category(self):
        """R10 uses balanced category filters (ADTV, circuit, quality)."""
        rows = build_rows(band_ids=[1])
        expected_filters = [
            "adtv_floor",
            "adtv_capped_sizing",
            "circuit_lock_proxy",
            "quality_gate",
        ]
        assert rows[0]["filter_ids"] == expected_filters


class TestR10RegistrationBands:
    """Verify R10 covers M1/M2 bands by default."""

    def test_initial_band_ids_m1_m2(self):
        """R10 Phase 10 validation focuses on M1 (band 1) and M2 (band 2)."""
        assert INITIAL_BAND_IDS == [1, 2], "Phase 10 validation on M1/M2"

    def test_build_rows_respects_band_ids(self):
        """build_rows() only generates for specified bands."""
        rows_12 = build_rows(band_ids=[1, 2])
        assert len(rows_12) == 2, "2 bands × 1 config = 2 rows"

        rows_all = build_rows(band_ids=list(range(1, 13)))
        assert len(rows_all) == 12, "12 bands × 1 config = 12 rows"

    def test_band_rank_ranges_correct(self):
        """Each band has correct rank_start/rank_end in definition."""
        rows = build_rows(band_ids=[1, 2])

        assert rows[0]["definition"]["band_id"] == 1
        assert rows[0]["definition"]["rank_start"] == 1
        assert rows[0]["definition"]["rank_end"] == 50

        assert rows[1]["definition"]["band_id"] == 2
        assert rows[1]["definition"]["rank_start"] == 1
        assert rows[1]["definition"]["rank_end"] == 75


class TestR10RegistryIntegration:
    """Test R10 strategies can be retrieved from registry (integration)."""

    def test_get_registered_r10_strategy(self):
        """R10 strategy can be retrieved from registry after registration."""
        key = strategy_key("momentum", "R10_1_1_50")
        strategy = get_strategy(key)
        if strategy is not None:  # May be None if DB is not populated
            assert strategy["definition"]["lookback_months"] == 6
            assert strategy["definition"]["skip_months"] == 1
            assert strategy["definition"]["rebalance_cadence_days"] == 63
            assert strategy["definition"]["rank_method"] == "trailing_return"
            assert strategy["status"] == "active"


class TestR10NonRegressionGuard:
    """Verify R10 registration doesn't affect M-family or R1."""

    def test_r10_naming_distinct_from_m_family(self):
        """R10 naming scheme doesn't collide with M{1-12}."""
        r10_names = [
            variant_name(band_id, rank_start, rank_end)
            for band_id, rank_start, rank_end in [(1, 1, 50), (2, 1, 75)]
        ]
        # No R10 name should start with 'M' (that's M-family)
        assert all(name.startswith("R10_") for name in r10_names)

    def test_r10_naming_distinct_from_r1(self):
        """R10 names don't overlap with R1 (which includes lookback in name)."""
        r10_name = variant_name(1, 1, 50)
        # R1 names include "_lb6mo" etc., R10 doesn't
        assert "_lb" not in r10_name
        assert r10_name == "R10_1_1_50"

    def test_r10_uses_same_adapter_as_mfamily(self):
        """R10 configuration is compatible with MomentumAdapter (no new code)."""
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
                "skip_months",
                "rebalance_cadence_days",
                "top_n",
            ]
            for field in required_by_adapter:
                assert field in definition, f"{field} must be in definition for adapter"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
