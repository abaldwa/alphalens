"""
tests/unit/test_registry.py

Tests features/registry.py — the feature discovery and documentation registry.
All tests are offline (no DB, no HTTP).
"""

import json
import tempfile
from pathlib import Path


from features.registry import (
    FEATURE_REGISTRY,
    DataSource,
    FeatureCategory,
    FeatureDefinition,
    PITRule,
    UpdateFrequency,
    export_feature_catalog,
    validate_feature_registry,
)


class TestFeatureRegistry:
    def test_registry_is_non_empty(self):
        assert len(FEATURE_REGISTRY) > 0

    def test_all_entries_are_feature_definitions(self):
        for name, defn in FEATURE_REGISTRY.items():
            assert isinstance(defn, FeatureDefinition), name

    def test_registry_key_matches_name(self):
        for key, defn in FEATURE_REGISTRY.items():
            assert defn.name == key

    def test_all_categories_are_valid(self):
        valid = set(FeatureCategory)
        for name, defn in FEATURE_REGISTRY.items():
            assert defn.category in valid, name

    def test_phases_are_in_valid_range(self):
        for name, defn in FEATURE_REGISTRY.items():
            assert 0 <= defn.phase <= 5, name

    def test_update_frequencies_are_valid(self):
        valid = set(UpdateFrequency)
        for name, defn in FEATURE_REGISTRY.items():
            assert defn.update_frequency in valid, name

    def test_data_sources_are_valid(self):
        valid = set(DataSource)
        for name, defn in FEATURE_REGISTRY.items():
            assert defn.source_store in valid, name

    def test_pit_rules_are_valid(self):
        valid = set(PITRule)
        for name, defn in FEATURE_REGISTRY.items():
            assert defn.pit_rule in valid, name

    def test_consumers_are_lists(self):
        for name, defn in FEATURE_REGISTRY.items():
            assert isinstance(defn.consumers, list), name


class TestValidateRegistry:
    def test_validate_returns_empty_list_for_clean_registry(self):
        errors = validate_feature_registry()
        assert errors == [], f"Validation errors: {errors}"

    def test_validate_return_type(self):
        result = validate_feature_registry()
        assert isinstance(result, list)


class TestExportCatalog:
    def test_export_returns_dict(self):
        catalog = export_feature_catalog()
        assert isinstance(catalog, dict)

    def test_export_has_version(self):
        catalog = export_feature_catalog()
        assert "version" in catalog

    def test_export_features_count_matches_registry(self):
        catalog = export_feature_catalog()
        assert catalog["total_features"] == len(FEATURE_REGISTRY)

    def test_export_writes_json_when_path_given(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir) / "catalog.json"
            catalog = export_feature_catalog(output_path=out)
            assert out.exists()
            loaded = json.loads(out.read_text())
            assert loaded["total_features"] == catalog["total_features"]

    def test_export_features_key_exists(self):
        catalog = export_feature_catalog()
        assert "features" in catalog
        assert isinstance(catalog["features"], dict)


class TestEnums:
    def test_feature_category_values(self):
        assert FeatureCategory.MOMENTUM.value == "momentum"
        assert FeatureCategory.FUNDAMENTAL.value == "fundamental"

    def test_update_frequency_values(self):
        assert UpdateFrequency.DAILY.value == "daily"
        assert UpdateFrequency.QUARTERLY.value == "quarterly"

    def test_data_source_has_members(self):
        assert len(list(DataSource)) > 0

    def test_pit_rule_has_members(self):
        assert len(list(PITRule)) > 0
