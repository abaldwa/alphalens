"""
Unit tests for the filter_registry seed (A93).

tmp_path DuckDB only.

Beyond "the rows load", these pin the three divergences the migration found.
Those assertions exist so that if someone later reconciles momentum's and
technical's differing calibrations, the test fails and forces the divergence
note to be updated rather than left describing a state that no longer exists.
"""

import pytest

from datastore.schema.create_strategy_registry import create_strategy_registry_schema
from strategies.migrations.filters import build_filters, migrate
from strategies.registry import get_filter, list_filters, resolve_filters


@pytest.fixture
def db(tmp_path):
    path = tmp_path / "registry.duckdb"
    create_strategy_registry_schema(db_path=path)
    return path


def _by_id():
    return {f["filter_id"]: f for f in build_filters()}


class TestSeedShape:
    def test_every_filter_names_one_implementation(self):
        """Invariant 2: exactly one implementation per filter."""
        for f in build_filters():
            assert "." in f["implementation_ref"], f["filter_id"]

    def test_defaults_are_declared_in_the_schema(self):
        for f in build_filters():
            undeclared = set(f["default_params"]) - set(f["params_schema"])
            assert not undeclared, f"{f['filter_id']}: {undeclared}"

    def test_filter_types_are_valid(self):
        from strategies.registry import FILTER_TYPES

        assert all(f["filter_type"] in FILTER_TYPES for f in build_filters())

    def test_ids_are_unique(self):
        ids = [f["filter_id"] for f in build_filters()]
        assert len(ids) == len(set(ids))


class TestDivergencesFound:
    """The substantive output of A93: three places where one NAME means two
    different RULES. Recorded, not silently resolved -- picking a winner is a
    strategy decision."""

    def test_circuit_proxy_calibration_differs_across_channels(self):
        """momentum 0.20 vs technical 0.19 -- a cross-channel 'circuit filter
        on' comparison is not comparing the same filter."""
        f = _by_id()["circuit_lock_proxy"]
        assert f["default_params"]["circuit_band_pct"] == 0.20
        assert "0.19" in f["divergence"]

    def test_regime_filter_disables_on_different_regimes(self):
        """momentum sits out high_vol, technical sits out bear. Different
        market states, not different spellings."""
        f = _by_id()["hmm_regime"]
        assert f["default_params"]["disable_in_regimes"] == ["high_vol"]
        assert "bear" in f["divergence"]

    def test_quality_gate_is_stricter_on_momentum(self):
        """The Beneish M-score half is absent from the technical path."""
        f = _by_id()["quality_gate"]
        assert f["default_params"]["max_m_score"] == -1.78
        assert "M-score" in f["divergence"]

    def test_circuit_lock_default_states_the_a85_target_not_current_behaviour(self):
        """The registry says True (what A85 wants); the orchestrator still
        defaults False. The note must keep that gap visible."""
        f = _by_id()["circuit_lock"]
        assert f["default_params"]["block_circuit_fills"] is True
        assert "False" in f["divergence"]

    def test_adtv_sizing_cap_is_momentum_only(self):
        """The other three channels apply no ADTV ceiling to position size, so
        their fills can exceed what the market would absorb."""
        assert "Momentum-only" in _by_id()["adtv_capped_sizing"]["divergence"]


class TestMigrate:
    def test_registers_all_filters(self, db):
        stats = migrate(db_path=db)
        assert stats["registered"] == len(build_filters())
        assert len(list_filters(db_path=db)) == len(build_filters())

    def test_rerun_is_a_no_op(self, db):
        migrate(db_path=db)
        stats = migrate(db_path=db)
        assert stats["registered"] == 0
        assert stats["existing"] == len(build_filters())

    def test_dry_run_writes_nothing(self, db):
        migrate(db_path=db, dry_run=True)
        assert list_filters(db_path=db) == []

    def test_divergence_note_lands_in_the_stored_description(self, db):
        """The note has to survive into the row, or the finding is lost the
        moment someone reads the registry instead of this file."""
        migrate(db_path=db)
        desc = get_filter("hmm_regime", db_path=db)["description"]
        assert "DIVERGENCE" in desc and "bear" in desc

    def test_momentum_only_filter_scoped_to_momentum(self, db):
        migrate(db_path=db)
        ids = {f["filter_id"] for f in list_filters(channel="technical", db_path=db)}
        assert "size_beta_orthogonalized" not in ids
        assert "size_beta_orthogonalized" in {
            f["filter_id"] for f in list_filters(channel="momentum", db_path=db)
        }

    def test_resolve_a_realistic_strategy_filter_set(self, db):
        migrate(db_path=db)
        resolved = resolve_filters(
            ["adtv_floor", "circuit_lock", "downtrend_filter"],
            {"adtv_floor": {"min_adtv_cr": 5.0}},
            db_path=db,
        )
        params = {r["filter_id"]: r["params"] for r in resolved}
        assert params["adtv_floor"]["min_adtv_cr"] == 5.0
        assert params["circuit_lock"]["block_circuit_fills"] is True
        assert params["downtrend_filter"]["downtrend_lookback_days"] == 20
