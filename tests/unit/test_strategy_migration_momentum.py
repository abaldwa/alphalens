"""
Unit tests for the Momentum -> strategy_registry migration (ML41).

tmp_path DuckDB only.

The tests that matter here are the ones asserting the migration reproduces
what the sweep ACTUALLY runs: the grid dimensions come from the same
constants the dynamic report iterates, and the generated name is byte-identical
to the report's variant_id. If either drifts, registry rows and report rows
stop matching and every frontend deep link breaks silently.
"""

import pytest

from datastore.schema.create_strategy_registry import create_strategy_registry_schema
from strategies.migrations.filters import migrate as migrate_filters
from strategies.migrations.momentum import (
    CATEGORY_FILTERS,
    build_rows,
    migrate,
    variant_name,
)
from strategies.registry import get_strategy, list_strategies, resolve_filters


@pytest.fixture
def db(tmp_path):
    path = tmp_path / "registry.duckdb"
    create_strategy_registry_schema(db_path=path)
    return path


class TestGrid:
    def test_grid_size_matches_the_sweep_dimensions(self):
        from features.momentum_signal import LOOKBACK_MONTHS
        from features.momentum_universe import RANK_BANDS
        from scripts.run_momentum_dynamic_report import (
            REBALANCE_PERIODS,
            TOP_N_OPTIONS,
        )

        expected = (
            len(RANK_BANDS)
            * len(CATEGORY_FILTERS)
            * len(LOOKBACK_MONTHS)
            * len(REBALANCE_PERIODS)
            * len(TOP_N_OPTIONS)
        )
        assert len(build_rows()) == expected

    def test_presets_only_returns_the_four_categories(self):
        rows = build_rows(include_grid=False)
        assert {r["category"] for r in rows} == set(CATEGORY_FILTERS)
        assert len(rows) == 4

    def test_names_are_unique(self):
        names = [r["name"] for r in build_rows()]
        assert len(names) == len(set(names))

    def test_name_matches_the_reports_variant_id_format(self):
        """The report builds this string inline; a mismatch means registry
        rows and report rows cannot be joined."""
        assert (
            variant_name("balanced", 1, 1, 50, 6, "monthly", 15)
            == "balanced_b1_1-50_lb6mo_monthly_top15"
        )

    def test_generated_names_use_that_format(self):
        row = next(
            r
            for r in build_rows()
            if r["definition"]["category"] == "balanced"
            and r["definition"]["band_id"] == 1
            and r["definition"]["lookback_months"] == 6
            and r["definition"]["rebalance_frequency"] == "monthly"
            and r["definition"]["top_n"] == 15
        )
        assert row["name"] == "balanced_b1_1-50_lb6mo_monthly_top15"


class TestPresetLayering:
    """build_category_presets layers each level on the previous one. The
    registry rows must preserve that, or 'risk_managed' stops meaning
    'balanced plus regime disabling'."""

    def test_all_risk_has_no_filters(self):
        assert CATEGORY_FILTERS["all_risk"] == []

    def test_each_level_is_a_superset_of_the_previous(self):
        order = ["all_risk", "balanced", "risk_managed", "max_defensive"]
        for prev, nxt in zip(order, order[1:]):
            assert set(CATEGORY_FILTERS[prev]) < set(CATEGORY_FILTERS[nxt]), (
                f"{nxt} must add to {prev}"
            )

    def test_risk_managed_adds_exactly_the_regime_filter(self):
        added = set(CATEGORY_FILTERS["risk_managed"]) - set(CATEGORY_FILTERS["balanced"])
        assert added == {"hmm_regime"}

    def test_max_defensive_adds_exactly_orthogonalization(self):
        added = set(CATEGORY_FILTERS["max_defensive"]) - set(
            CATEGORY_FILTERS["risk_managed"]
        )
        assert added == {"size_beta_orthogonalized"}


class TestRowShape:
    def test_entry_criterion_is_empty_by_design(self):
        """Momentum ranks a band and buys the top N. The selection rule IS the
        definition, not a per-ticker predicate -- an empty entry criterion is
        truthful here rather than a gap."""
        assert all(r["entry_criterion"] == [] for r in build_rows(include_grid=False))

    def test_exit_is_rank_plus_grace(self):
        row = build_rows()[0]
        assert row["exit_criterion"]["variant"] == "rank_grace"
        assert row["exit_criterion"]["exit_rank"] == row["definition"]["top_n"]

    def test_filters_are_part_of_the_definition_unlike_technical(self):
        """Momentum's category IS its filter set, so these rows carry
        filter_ids where the Technical rows deliberately do not."""
        rows = {r["category"]: r for r in build_rows(include_grid=False)}
        assert rows["max_defensive"]["filter_ids"]
        assert rows["all_risk"]["filter_ids"] == []

    def test_universe_spec_recorded(self):
        """Momentum ranks by market cap into bands, not by ADTV like the other
        channels -- a report must be able to say which."""
        assert all(r["universe_spec"] == "momentum_rank_band" for r in build_rows())


class TestMigrate:
    def test_registers_the_presets(self, db):
        stats = migrate(db_path=db, include_grid=False)
        assert stats["registered"] == 4
        assert len(list_strategies(channel="momentum", db_path=db)) == 4

    def test_rerun_is_a_no_op(self, db):
        migrate(db_path=db, include_grid=False)
        stats = migrate(db_path=db, include_grid=False)
        assert stats["registered"] == 0
        assert stats["unchanged"] == 4

    def test_dry_run_writes_nothing(self, db):
        migrate(db_path=db, include_grid=False, dry_run=True)
        assert list_strategies(channel="momentum", db_path=db) == []

    def test_filter_ids_resolve_against_the_filter_registry(self, db):
        """A registered strategy whose filter_ids do not resolve is a broken
        row that would only fail at run time."""
        migrate_filters(db_path=db)
        migrate(db_path=db, include_grid=False)

        row = get_strategy("momentum:preset_max_defensive", db_path=db)
        resolved = resolve_filters(row["filter_ids"], db_path=db)
        assert len(resolved) == len(CATEGORY_FILTERS["max_defensive"])
        assert {r["filter_id"] for r in resolved} == set(
            CATEGORY_FILTERS["max_defensive"]
        )

    def test_full_grid_registers(self, db):
        stats = migrate(db_path=db)
        assert stats["registered"] == len(build_rows())
        assert len(list_strategies(channel="momentum", db_path=db)) == len(build_rows())
