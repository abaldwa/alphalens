"""
Unit tests for the ML -> strategy_registry migration (ML42).

tmp_path DuckDB only; the model registry is read from a tmp JSON fixture, so
these never depend on which artifacts happen to be trained on this machine.

The retrain-is-a-revision test is the important one. An ML strategy is
(architecture + trained weights); if a retrain silently overwrote the row,
"what did the model that produced this backtest look like" would have no
answer.
"""

import json

import pytest

from datastore.schema.create_strategy_registry import create_strategy_registry_schema
from strategies.migrations.ml import (
    NON_STRATEGY_ARTIFACTS,
    SIGNAL_MODELS,
    build_rows,
    load_artifacts,
    migrate,
)
from strategies.registry import get_strategy, list_strategies


@pytest.fixture
def db(tmp_path):
    path = tmp_path / "registry.duckdb"
    create_strategy_registry_schema(db_path=path)
    return path


@pytest.fixture
def artifacts(tmp_path):
    path = tmp_path / "model_registry.json"
    path.write_text(
        json.dumps(
            {
                "signal_21d": {
                    "saved_path": "/models/signal_21d_v1.pkl",
                    "last_trained_date": "2026-07-13",
                    "training_interval_days": 28,
                },
                "hmm_market": {"saved_path": "/models/hmm.pkl"},
            }
        )
    )
    return path


class TestSelection:
    def test_only_serving_signal_models_registered(self, artifacts):
        assert {r["name"] for r in build_rows(artifacts)} == set(SIGNAL_MODELS)

    def test_regime_and_wrapper_artifacts_excluded(self, artifacts):
        """Registering these as strategies would claim they can be deployed
        and backtested standalone, which is true of none of them."""
        names = {r["name"] for r in build_rows(artifacts)}
        assert names.isdisjoint(NON_STRATEGY_ARTIFACTS)

    def test_every_exclusion_carries_a_stated_reason(self):
        assert all(why for why in NON_STRATEGY_ARTIFACTS.values())

    def test_hmm_is_excluded_because_it_is_a_filter(self):
        assert "filter" in NON_STRATEGY_ARTIFACTS["hmm_market"]


class TestRowShape:
    def test_entry_criterion_is_empty(self, artifacts):
        """The rule is 'the model said so' -- learned, in a pickle, not a
        threshold over named columns."""
        assert all(r["entry_criterion"] == [] for r in build_rows(artifacts))

    def test_signal_seam_exemption_is_recorded(self, artifacts):
        """A87 and A94 both assume generate_signals() exists. ml_adapter
        deliberately does not implement it; that has to be visible."""
        for row in build_rows(artifacts):
            assert row["definition"]["emits_signals"] is False
            assert "generate_signals" in row["definition"]["signal_seam_exemption"]

    def test_artifact_provenance_captured_when_present(self, artifacts):
        row = next(r for r in build_rows(artifacts) if r["name"] == "signal_21d")
        assert row["definition"]["artifact_path"] == "/models/signal_21d_v1.pkl"
        assert row["definition"]["last_trained_date"] == "2026-07-13"

    def test_missing_artifact_is_not_fatal(self, artifacts):
        """A checkout without trained models must still register definitions."""
        row = next(r for r in build_rows(artifacts) if r["name"] == "signal_5d")
        assert row["definition"]["artifact_path"] is None

    def test_horizons_recorded(self, artifacts):
        by_name = {r["name"]: r for r in build_rows(artifacts)}
        assert by_name["signal_5d"]["definition"]["horizon_days"] == 5
        assert by_name["signal_63d"]["definition"]["horizon_days"] == 63

    def test_missing_registry_file_returns_empty(self, tmp_path):
        assert load_artifacts(tmp_path / "nope.json") == {}


class TestMigrate:
    def test_registers_the_models(self, db, artifacts):
        stats = migrate(db_path=db, registry_path=artifacts)
        assert stats["registered"] == len(SIGNAL_MODELS)
        assert len(list_strategies(channel="ml", db_path=db)) == len(SIGNAL_MODELS)

    def test_rerun_is_a_no_op(self, db, artifacts):
        migrate(db_path=db, registry_path=artifacts)
        stats = migrate(db_path=db, registry_path=artifacts)
        assert stats["registered"] == 0
        assert stats["unchanged"] == len(SIGNAL_MODELS)

    def test_dry_run_writes_nothing(self, db, artifacts):
        migrate(db_path=db, registry_path=artifacts, dry_run=True)
        assert list_strategies(channel="ml", db_path=db) == []

    def test_retrain_produces_a_new_version_and_preserves_the_old(self, db, artifacts, tmp_path):
        """The strategy genuinely changed -- new weights are a new strategy --
        so a retrain must revise, not overwrite."""
        migrate(db_path=db, registry_path=artifacts)

        retrained = tmp_path / "retrained.json"
        retrained.write_text(
            json.dumps(
                {
                    "signal_21d": {
                        "saved_path": "/models/signal_21d_v2.pkl",
                        "last_trained_date": "2026-08-10",
                        "training_interval_days": 28,
                    }
                }
            )
        )
        stats = migrate(db_path=db, registry_path=retrained)
        assert stats["revised"] == 1

        current = get_strategy("ml:signal_21d", db_path=db)
        v1 = get_strategy("ml:signal_21d", version=1, db_path=db)
        assert current["version"] == 2
        assert current["definition"]["artifact_path"] == "/models/signal_21d_v2.pkl"
        assert v1["definition"]["artifact_path"] == "/models/signal_21d_v1.pkl"
