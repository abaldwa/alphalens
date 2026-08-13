"""
Unit tests for the Technical -> strategy_registry migration (T15).

tmp_path DuckDB only.

The point of these tests is not that 63 rows appear. It is that the migration
is re-runnable: it must register only what is new, revise only what actually
drifted, and leave the version history alone otherwise -- because a run
records the version it executed, and a migration that bumps every version on
every run makes that recorded number meaningless.
"""

import pytest

from datastore.schema.create_strategy_registry import create_strategy_registry_schema
from strategies.migrations.technical import PER_TEMPLATE_EXIT, build_rows, migrate
from strategies.predicates import validate_predicates
from strategies.registry import get_strategy, list_strategies


@pytest.fixture
def db(tmp_path):
    path = tmp_path / "registry.duckdb"
    create_strategy_registry_schema(db_path=path)
    return path


class TestBuildRows:
    def test_covers_every_template(self):
        from systems.technical_analysis.screener.templates import TEMPLATES

        assert len(build_rows()) == len(TEMPLATES)

    def test_every_condition_is_expressible_in_the_shared_grammar(self):
        """This is the real question T15 answers for A92: can one predicate
        grammar carry definitions that were written before it existed?"""
        for row in build_rows():
            validate_predicates(row["entry_criterion"], where=row["name"])

    def test_conditions_are_carried_verbatim(self):
        from systems.technical_analysis.screener.templates import TEMPLATE_MAP

        by_name = {r["name"]: r for r in build_rows()}
        for name, template in TEMPLATE_MAP.items():
            assert by_name[name]["entry_criterion"] == [dict(c) for c in template.conditions]

    def test_exit_params_come_from_the_style_table(self):
        """Templates get their barriers from STYLE_EXIT_PARAMS via
        TEMPLATE_STYLE at import time; the migration must pick those up rather
        than write nulls."""
        from systems.technical_analysis.screener.templates import TEMPLATE_MAP

        for row in build_rows():
            t = TEMPLATE_MAP[row["name"]]
            assert row["exit_criterion"]["stop_pct"] == t.exit_stop_pct
            assert row["exit_criterion"]["target_pct"] == t.exit_target_pct
            assert row["exit_criterion"]["max_hold_days"] == t.exit_max_hold_days
            assert row["exit_criterion"]["stop_pct"] is not None

    def test_exit_variant_defers_to_the_template_barriers(self):
        assert all(r["exit_criterion"]["variant"] == PER_TEMPLATE_EXIT for r in build_rows())

    def test_style_recorded_in_definition(self):
        from systems.technical_analysis.screener.templates import TEMPLATE_STYLE

        for row in build_rows():
            assert row["definition"]["style"] == TEMPLATE_STYLE[row["name"]]

    def test_no_filter_ids_asserted(self):
        """Technical filters are per-run job fields, not per-template
        properties. Attaching them here would assert something untrue."""
        assert all("filter_ids" not in r for r in build_rows())


class TestMigrate:
    def test_registers_every_template(self, db):
        stats = migrate(db_path=db)
        assert stats["registered"] == len(build_rows())
        assert stats["revised"] == 0
        assert len(list_strategies(channel="technical", db_path=db)) == len(build_rows())

    def test_rows_are_readable_and_keyed_by_channel(self, db):
        migrate(db_path=db)
        a1 = get_strategy("technical:A1", db_path=db)
        assert a1 is not None
        assert a1["channel"] == "technical"
        assert a1["version"] == 1
        assert a1["status"] == "active"
        assert a1["entry_criterion"][0]["feature"] == "bb_width_pct"

    def test_rerun_is_a_no_op(self, db):
        """Idempotence is what makes this safe to run after a template is
        added, which is the only way it stays in sync."""
        migrate(db_path=db)
        stats = migrate(db_path=db)
        assert stats["registered"] == 0
        assert stats["revised"] == 0
        assert stats["unchanged"] == len(build_rows())

    def test_rerun_does_not_bump_versions(self, db):
        migrate(db_path=db)
        migrate(db_path=db)
        assert all(s["version"] == 1 for s in list_strategies(channel="technical", db_path=db))

    def test_drift_produces_a_new_version_not_a_mutation(self, db):
        """A changed template must write v2 and leave v1 intact, or every run
        that recorded v1 becomes unreproducible."""
        migrate(db_path=db)
        from strategies.registry import revise_strategy

        revise_strategy("technical:A1", display_label="hand-edited", db_path=db)
        stats = migrate(db_path=db)

        assert stats["revised"] == 1
        assert get_strategy("technical:A1", db_path=db)["version"] == 3
        assert get_strategy("technical:A1", version=1, db_path=db)["display_label"] != "hand-edited"

    def test_dry_run_writes_nothing(self, db):
        stats = migrate(db_path=db, dry_run=True)
        assert stats["registered"] == len(build_rows())
        assert list_strategies(channel="technical", db_path=db) == []
