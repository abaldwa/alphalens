"""
tests/unit/test_r12_reversal_registration.py

Test R12 strategy registration from strategies/migrations/r12_momentum_reversal_liquidity.py.

Verifies:
1. R12 naming is distinct from M-family and other R variants
2. All required adapter params are present in the definition
3. Variant names are constructed correctly for each band
4. Registration would succeed with proper row structure
"""

import pytest
from strategies.migrations.r12_momentum_reversal_liquidity import (
    build_rows,
    variant_name,
)


class TestR12RegistrationNaming:
    """Test R12 strategy naming and variant generation."""

    def test_variant_name_format(self):
        """Variant name should follow R12_reversal_1mo_{band}_{start}_{end} pattern."""
        name = variant_name(band_id=1, rank_start=1, rank_end=50)
        assert name.startswith("R12_reversal_1mo_")
        assert "1_1_50" in name
        assert name == "R12_reversal_1mo_1_1_50"

    def test_variant_names_are_unique_per_band(self):
        """Each band should produce a unique variant name."""
        name_band_1 = variant_name(band_id=1, rank_start=1, rank_end=50)
        name_band_9 = variant_name(band_id=9, rank_start=201, rank_end=450)
        assert name_band_1 != name_band_9
        assert "R12_reversal_1mo_1" in name_band_1
        assert "R12_reversal_1mo_9" in name_band_9

    def test_variant_name_distinguishes_from_m_family(self):
        """R12 variant names should be clearly distinct from M-family."""
        r12_name = variant_name(band_id=1, rank_start=1, rank_end=50)
        # M-family would be M1_1_1_50 or similar
        # R12 is R12_reversal_1mo_1_1_50
        assert r12_name.startswith("R12_reversal_1mo_")


class TestR12RegistrationRowStructure:
    """Test R12 row structure and completeness."""

    def test_build_rows_creates_non_empty_list(self):
        """build_rows() should return a list of row dicts."""
        rows = build_rows()
        assert isinstance(rows, list)
        assert len(rows) > 0

    def test_build_rows_includes_both_initial_bands(self):
        """Default build_rows() should include bands 1 and 9."""
        rows = build_rows()
        bands = [row["definition"]["band_id"] for row in rows]
        assert 1 in bands
        assert 9 in bands

    def test_row_has_required_fields(self):
        """Each row should have channel, name, definition, filters, etc."""
        rows = build_rows()
        for row in rows:
            assert "channel" in row
            assert row["channel"] == "momentum"
            assert "name" in row
            assert "definition" in row
            assert "entry_criterion" in row
            assert "exit_criterion" in row
            assert "filter_ids" in row
            assert "status" in row

    def test_definition_includes_rank_method_reversal(self):
        """Definition should specify trailing_reversal_1mo as rank_method."""
        rows = build_rows()
        for row in rows:
            assert row["definition"]["rank_method"] == "trailing_reversal_1mo"

    def test_definition_includes_required_params(self):
        """Definition should have all required momentum adapter params."""
        rows = build_rows()
        for row in rows:
            defn = row["definition"]
            assert "category" in defn
            assert "band_id" in defn
            assert "rank_start" in defn
            assert "rank_end" in defn
            assert "lookback_days" in defn
            assert defn["lookback_days"] == 21  # 1 month
            assert "rebalance_cadence_days" in defn
            assert defn["rebalance_cadence_days"] == 63  # quarterly
            assert "top_n" in defn
            assert defn["top_n"] == 15
            assert "rank_method" in defn

    def test_filter_ids_are_balanced_category(self):
        """R12 should use balanced filters like M-family."""
        rows = build_rows()
        for row in rows:
            expected_filters = [
                "adtv_floor",
                "adtv_capped_sizing",
                "circuit_lock_proxy",
                "quality_gate",
            ]
            assert row["filter_ids"] == expected_filters

    def test_all_bands_registration(self):
        """build_rows(band_ids=[1..12]) should register all bands."""
        all_bands = list(range(1, 13))
        rows = build_rows(band_ids=all_bands)
        assert len(rows) == 12
        bands_in_rows = [row["definition"]["band_id"] for row in rows]
        assert set(bands_in_rows) == set(all_bands)

    def test_row_names_match_variant_naming_convention(self):
        """Each row's name should match the variant_name() output."""
        rows = build_rows()
        for row in rows:
            band_id = row["definition"]["band_id"]
            rank_start = row["definition"]["rank_start"]
            rank_end = row["definition"]["rank_end"]
            expected_name = variant_name(band_id, rank_start, rank_end)
            assert row["name"] == expected_name

    def test_entry_criterion_is_empty(self):
        """R12 reversal has no entry predicates."""
        rows = build_rows()
        for row in rows:
            assert row["entry_criterion"] == []

    def test_exit_criterion_is_rank_exit(self):
        """R12 reversal exits via plain list swap on rank."""
        rows = build_rows()
        for row in rows:
            assert row["exit_criterion"]["variant"] == "rank_exit"
            assert row["exit_criterion"]["exit_rank"] == 15


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
