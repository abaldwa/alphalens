"""
Unit tests for the Fundamental -> strategy_registry migration (F7).

tmp_path DuckDB only.

The sign-convention tests are the important ones. SCREENER_PRESETS encodes
"lower is better" as a NEGATIVE threshold evaluated as `-v >= abs(t)`.
Translating that to `gte` instead of `lte` would invert every such screen --
selecting the most indebted and most expensive names while the label still
read quality or value, and backtesting as a plausible, wrong result.
"""

import pytest

from datastore.schema.create_strategy_registry import create_strategy_registry_schema
from strategies.migrations.fundamental import build_rows, migrate, preset_predicates
from strategies.predicates import validate_predicates
from strategies.registry import get_strategy, list_strategies


@pytest.fixture
def db(tmp_path):
    path = tmp_path / "registry.duckdb"
    create_strategy_registry_schema(db_path=path)
    return path


class TestSignConvention:
    def test_negative_threshold_becomes_lte(self):
        """quality_compounder's debt_to_equity: -0.5 means '-v >= 0.5', i.e.
        v <= -0.5: at least half a sector-sigma LESS levered than peers."""
        preds = {p["feature"]: p for p in preset_predicates("quality_compounder")}
        assert preds["debt_to_equity"]["op"] == "lte"
        assert preds["debt_to_equity"]["value"] == -0.5

    def test_positive_threshold_becomes_gte(self):
        preds = {p["feature"]: p for p in preset_predicates("quality_compounder")}
        assert preds["roe"]["op"] == "gte"
        assert preds["roe"]["value"] == 1.0

    def test_translation_agrees_with_matches_screener_preset(self):
        """The predicates must accept and reject exactly what the live
        function does, or the registry describes a different screen."""
        from features.fundamental_composites import SCREENER_PRESETS, matches_screener_preset

        for preset in SCREENER_PRESETS:
            preds = [
                p for p in preset_predicates(preset) if p["feature"] != "sector"
            ]
            for probe in (-3.0, -0.5, 0.0, 0.5, 3.0):
                ratios = {p["feature"]: probe for p in preds}
                expected = matches_screener_preset(ratios, preset)
                actual = all(
                    probe >= p["value"] if p["op"] == "gte" else probe <= p["value"]
                    for p in preds
                )
                assert actual == expected, (preset, probe)

    def test_missing_ratio_semantics_documented_not_encoded(self):
        """matches_screener_preset fails a ticker on a missing/NaN input. That
        is evaluator behaviour, not a predicate, so it must NOT appear as a
        condition -- it belongs to whatever evaluates the criterion."""
        assert all(
            p["op"] in ("gte", "lte", "not_in") for p in preset_predicates("garp")
        )


class TestSectorExclusions:
    def test_excluded_sectors_become_a_not_in_predicate(self):
        """PRESET_EXCLUDED_SECTORS lives outside the condition system today as
        a Python set. not_in was added to the grammar for exactly this."""
        preds = {p["feature"]: p for p in preset_predicates("magic_formula")}
        assert preds["sector"]["op"] == "not_in"
        assert "Financial Services" in preds["sector"]["value"]

    def test_exclusion_list_is_sorted_for_stability(self):
        """A set's iteration order would make the migration look like it
        drifted on every run and inflate the version history."""
        value = next(
            p["value"] for p in preset_predicates("magic_formula") if p["feature"] == "sector"
        )
        assert value == sorted(value)

    def test_preset_without_exclusions_has_no_sector_predicate(self):
        from features.fundamental_composites import PRESET_EXCLUDED_SECTORS, SCREENER_PRESETS

        unexcluded = [p for p in SCREENER_PRESETS if not PRESET_EXCLUDED_SECTORS.get(p)]
        for preset in unexcluded:
            assert all(p["feature"] != "sector" for p in preset_predicates(preset))


class TestKinds:
    def test_covers_the_whole_catalog(self):
        from features.fundamental_composites import STRATEGY_CATALOG

        assert len(build_rows()) == len(STRATEGY_CATALOG)

    def test_every_criterion_validates(self):
        for row in build_rows():
            validate_predicates(row["entry_criterion"], where=row["name"])

    def test_presets_carry_predicates(self):
        rows = [r for r in build_rows() if r["definition"]["kind"] == "preset"]
        assert rows
        assert all(r["entry_criterion"] for r in rows)

    def test_composite_scores_carry_a_score_function_not_predicates(self):
        """A ranking has no threshold to express. Empty is truthful here."""
        rows = [r for r in build_rows() if r["definition"]["kind"] == "composite_score"]
        assert rows
        for r in rows:
            assert r["entry_criterion"] == []
            assert r["definition"]["score_function"] == r["name"]

    def test_bespoke_flagged_as_not_yet_declarative(self):
        """So A95's guard can tell 'no declarative form yet' apart from
        'genuinely has no conditions'."""
        rows = [r for r in build_rows() if r["definition"]["kind"] == "bespoke"]
        assert {r["name"] for r in rows} == {
            "piotroski_on_value",
            "margin_of_safety",
            "net_net",
        }
        for r in rows:
            assert r["definition"]["not_yet_declarative"] is True
            assert r["definition"]["bespoke_ref"]

    def test_composite_scores_are_not_flagged_not_yet_declarative(self):
        rows = [r for r in build_rows() if r["definition"]["kind"] == "composite_score"]
        assert all("not_yet_declarative" not in r["definition"] for r in rows)


class TestMigrate:
    def test_registers_the_catalog(self, db):
        stats = migrate(db_path=db)
        assert stats["registered"] == len(build_rows())
        assert len(list_strategies(channel="fundamental", db_path=db)) == len(build_rows())

    def test_rerun_is_a_no_op(self, db):
        migrate(db_path=db)
        stats = migrate(db_path=db)
        assert stats["registered"] == 0
        assert stats["unchanged"] == len(build_rows())

    def test_dry_run_writes_nothing(self, db):
        migrate(db_path=db, dry_run=True)
        assert list_strategies(channel="fundamental", db_path=db) == []

    def test_preset_row_reads_back_with_its_predicates(self, db):
        migrate(db_path=db)
        row = get_strategy("fundamental:magic_formula", db_path=db)
        ops = {p["feature"]: p["op"] for p in row["entry_criterion"]}
        assert ops["sector"] == "not_in"
        assert row["definition"]["preset"] == "magic_formula"

    def test_backtested_flag_preserved(self, db):
        """F4 records that none of the 26 clear the benchmark-beat bar and
        11 produce zero trades. Which have actually been run must survive."""
        from features.fundamental_composites import STRATEGY_CATALOG

        migrate(db_path=db)
        for name, meta in STRATEGY_CATALOG.items():
            row = get_strategy(f"fundamental:{name}", db_path=db)
            assert row["definition"]["backtested"] == bool(meta.get("backtested"))
