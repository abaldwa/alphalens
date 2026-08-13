"""
Unit tests for the strategy/filter registry (A92, A93).

Every test writes to a tmp_path DuckDB file, never the real database -- see
the project's no-synthetic-DB-writes rule.

The tests that matter most here are the point-in-time ones. An append-only
registry whose get(as_of=...) is wrong is worse than no registry at all: it
would return today's definition for a historical run and make an invalidated
backtest look reproducible.
"""

from datetime import date

import pytest

from datastore.schema.create_strategy_registry import create_strategy_registry_schema
from strategies.predicates import (
    PredicateError,
    features_used,
    screener_compatible,
    validate_predicate,
    validate_predicates,
)
from strategies.registry import (
    RegistryError,
    get_filter,
    get_strategy,
    list_filters,
    list_strategies,
    parse_strategy_key,
    register_filter,
    register_strategy,
    resolve_filters,
    retire_strategy,
    revise_strategy,
    strategy_key,
)


@pytest.fixture
def db(tmp_path):
    path = tmp_path / "registry.duckdb"
    create_strategy_registry_schema(db_path=path)
    return path


def _register(db, **overrides):
    kwargs = dict(
        channel="technical",
        name="A1_pullback",
        display_label="A1 Pullback in Uptrend",
        definition={"template_name": "A1_pullback", "holding_horizon": "21d"},
        entry_criterion=[
            {"feature": "rsi_14", "op": "lt", "value": 40},
            {"feature": "sma_200_ratio", "op": "gt", "value": 1.0},
        ],
        exit_criterion={"variant": "risk_managed", "stop_pct": 8.0, "max_hold_days": 21},
        status="active",
        db_path=db,
    )
    kwargs.update(overrides)
    return register_strategy(**kwargs)


# ===== predicate grammar =====


class TestPredicates:
    def test_valid_forms_accepted(self):
        for pred in [
            {"feature": "rsi_14", "op": "lt", "value": 30},
            {"feature": "close", "op": "between", "value": [10, 20]},
            {"feature": "roc_10", "op": "top_pct", "value": 0.2},
            {"feature": "close", "op": "gt_col", "feature2": "sma_50"},
            {"feature": "sector", "op": "not_in", "value": ["Financials"]},
        ]:
            validate_predicate(pred)

    def test_unknown_op_rejected(self):
        """The screener treats an unknown op as an unmet condition and logs a
        warning. That is right for a live screen and wrong for a stored
        definition -- it would backtest as flat and look like a real result."""
        with pytest.raises(PredicateError, match="unknown op"):
            validate_predicate({"feature": "rsi_14", "op": "les_than", "value": 30})

    def test_percentile_must_be_a_fraction(self):
        """20 instead of 0.20 would select the entire universe silently."""
        with pytest.raises(PredicateError, match="fraction"):
            validate_predicate({"feature": "roc_10", "op": "top_pct", "value": 20})

    def test_between_bounds_ordered(self):
        with pytest.raises(PredicateError, match="lo > hi"):
            validate_predicate({"feature": "close", "op": "between", "value": [20, 10]})

    def test_col_vs_col_self_comparison_rejected(self):
        with pytest.raises(PredicateError, match="itself"):
            validate_predicate({"feature": "close", "op": "gt_col", "feature2": "close"})

    def test_col_vs_col_with_value_instead_of_feature2_rejected(self):
        """The screener engine reads feature2 for these ops. A predicate
        written with `value` would be treated as unmet at screen time without
        raising, so it has to fail here instead."""
        with pytest.raises(PredicateError, match="feature2"):
            validate_predicate({"feature": "close", "op": "gt_col", "value": "sma_50"})

    def test_missing_value_rejected(self):
        with pytest.raises(PredicateError, match="missing 'value'"):
            validate_predicate({"feature": "rsi_14", "op": "lt"})

    def test_empty_criterion_allowed(self):
        """A momentum strategy that buys the whole ranked universe has no
        entry predicates. That is a real case, not an error."""
        validate_predicates([])

    def test_features_used_includes_rhs_column(self):
        preds = [
            {"feature": "close", "op": "gt_col", "feature2": "sma_50"},
            {"feature": "rsi_14", "op": "lt", "value": 30},
        ]
        assert features_used(preds) == ["close", "sma_50", "rsi_14"]

    def test_set_ops_flagged_as_screener_incompatible(self):
        assert screener_compatible([{"feature": "rsi_14", "op": "lt", "value": 30}])
        assert not screener_compatible(
            [{"feature": "sector", "op": "in", "value": ["IT"]}]
        )


# ===== strategy_key =====


class TestStrategyKey:
    def test_round_trip(self):
        assert parse_strategy_key(strategy_key("momentum", "b1_top15")) == (
            "momentum",
            "b1_top15",
        )

    def test_unknown_channel_rejected(self):
        with pytest.raises(RegistryError, match="unknown channel"):
            strategy_key("astrology", "x")

    def test_colon_in_name_rejected(self):
        """A ':' in the name would make the key ambiguous to parse back."""
        with pytest.raises(RegistryError):
            strategy_key("technical", "a:b")


# ===== registration =====


class TestRegisterStrategy:
    def test_register_and_read_back(self, db):
        key = _register(db)
        got = get_strategy(key, db_path=db)
        assert got["version"] == 1
        assert got["display_label"] == "A1 Pullback in Uptrend"
        assert got["entry_criterion"][0]["feature"] == "rsi_14"
        assert got["exit_criterion"]["variant"] == "risk_managed"

    def test_duplicate_registration_rejected(self, db):
        _register(db)
        with pytest.raises(RegistryError, match="already exists"):
            _register(db)

    def test_bad_predicate_rejected_at_write(self, db):
        with pytest.raises(RegistryError, match="unknown op"):
            _register(db, entry_criterion=[{"feature": "rsi_14", "op": "nope", "value": 1}])

    def test_exit_criterion_needs_a_variant(self, db):
        with pytest.raises(RegistryError, match="variant"):
            _register(db, exit_criterion={"stop_pct": 5})

    def test_list_filters_by_channel_and_status(self, db):
        _register(db)
        _register(db, name="draft_one", status="draft")
        assert [s["name"] for s in list_strategies(channel="technical", db_path=db)] == [
            "A1_pullback"
        ]
        assert len(list_strategies(channel="technical", status=None, db_path=db)) == 2
        assert list_strategies(channel="momentum", db_path=db) == []


# ===== versioning: the point-in-time guarantee =====


class TestVersioning:
    def test_revise_creates_new_version_and_carries_fields_forward(self, db):
        key = _register(db)
        v2 = revise_strategy(
            key, display_label="A1 Pullback (tightened)", db_path=db
        )
        assert v2 == 2

        current = get_strategy(key, db_path=db)
        assert current["version"] == 2
        assert current["display_label"] == "A1 Pullback (tightened)"
        # Untouched fields carried forward, not blanked.
        assert current["exit_criterion"]["stop_pct"] == 8.0

    def test_old_version_still_readable_by_number(self, db):
        """A run records the version it executed; that version must resolve
        forever, or the run's result becomes unreproducible."""
        key = _register(db)
        revise_strategy(key, entry_criterion=[], db_path=db)

        v1 = get_strategy(key, version=1, db_path=db)
        assert len(v1["entry_criterion"]) == 2
        assert get_strategy(key, version=2, db_path=db)["entry_criterion"] == []

    def test_as_of_returns_the_version_in_force_then(self, db):
        key = _register(db, valid_from=date(2026, 1, 1))
        revise_strategy(key, display_label="revised", valid_from=date(2026, 6, 1), db_path=db)

        before = get_strategy(key, as_of=date(2026, 3, 1), db_path=db)
        after = get_strategy(key, as_of=date(2026, 7, 1), db_path=db)
        assert before["version"] == 1
        assert before["display_label"] == "A1 Pullback in Uptrend"
        assert after["version"] == 2

    def test_as_of_on_the_changeover_date_returns_the_new_version(self, db):
        key = _register(db, valid_from=date(2026, 1, 1))
        revise_strategy(key, display_label="revised", valid_from=date(2026, 6, 1), db_path=db)
        on_the_day = get_strategy(key, as_of=date(2026, 6, 1), db_path=db)
        assert on_the_day["version"] == 2

    def test_revise_rejects_unknown_field(self, db):
        """Silently ignoring a typo'd field would mean the caller thinks it
        changed something it did not."""
        key = _register(db)
        with pytest.raises(RegistryError, match="unknown field"):
            revise_strategy(key, lookback_months=6, db_path=db)

    def test_revise_unregistered_rejected(self, db):
        with pytest.raises(RegistryError, match="not registered"):
            revise_strategy("technical:ghost", display_label="x", db_path=db)

    def test_retire_preserves_history(self, db):
        key = _register(db)
        retire_strategy(key, db_path=db)
        assert get_strategy(key, db_path=db)["status"] == "retired"
        assert get_strategy(key, version=1, db_path=db)["status"] == "active"
        assert list_strategies(channel="technical", db_path=db) == []


# ===== filters =====


def _register_adtv(db, **overrides):
    kwargs = dict(
        filter_id="adtv_floor",
        name="ADTV floor",
        filter_type="universe",
        params_schema={"min_adtv_cr": {"type": "float", "default": 1.0, "min": 0.0}},
        default_params={"min_adtv_cr": 1.0},
        applies_to_channels=["momentum", "technical", "fundamental"],
        implementation_ref="backtest.core.adtv.adtv_cr_for_ticker",
        db_path=db,
    )
    kwargs.update(overrides)
    return register_filter(**kwargs)


class TestFilterRegistry:
    def test_register_and_read_back(self, db):
        _register_adtv(db)
        f = get_filter("adtv_floor", db_path=db)
        assert f["filter_type"] == "universe"
        assert f["default_params"]["min_adtv_cr"] == 1.0
        assert "momentum" in f["applies_to_channels"]

    def test_defaults_must_be_declared_in_the_schema(self, db):
        """Schema and defaults drifting apart is exactly how one filter
        concept ended up with three incompatible declarations."""
        with pytest.raises(RegistryError, match="undeclared"):
            _register_adtv(db, default_params={"min_adt_inr": 10_000_000})

    def test_implementation_ref_must_be_a_dotted_path(self, db):
        with pytest.raises(RegistryError, match="dotted path"):
            _register_adtv(db, implementation_ref="adtv")

    def test_unknown_channel_rejected(self, db):
        with pytest.raises(RegistryError, match="unknown channel"):
            _register_adtv(db, applies_to_channels=["momentum", "crypto"])

    def test_list_filters_by_channel(self, db):
        _register_adtv(db)
        _register_adtv(
            db,
            filter_id="hmm_regime",
            name="HMM regime",
            filter_type="entry",
            params_schema={"disable_in": {"type": "list", "default": []}},
            default_params={"disable_in": []},
            applies_to_channels=["momentum"],
            implementation_ref="features.regime_signal.regime_series",
        )
        assert len(list_filters(db_path=db)) == 2
        assert [f["filter_id"] for f in list_filters(channel="technical", db_path=db)] == [
            "adtv_floor"
        ]

    def test_resolve_applies_overrides_over_defaults(self, db):
        _register_adtv(db)
        resolved = resolve_filters(
            ["adtv_floor"], {"adtv_floor": {"min_adtv_cr": 5.0}}, db_path=db
        )
        assert resolved[0]["params"]["min_adtv_cr"] == 5.0
        assert resolved[0]["implementation_ref"].startswith("backtest.core.adtv")

    def test_resolve_uses_defaults_when_not_overridden(self, db):
        _register_adtv(db)
        assert resolve_filters(["adtv_floor"], db_path=db)[0]["params"] == {
            "min_adtv_cr": 1.0
        }

    def test_override_of_undeclared_param_rejected(self, db):
        """circuit_band_pct was None in all 195 unconstrained Technical runs.
        A param that silently does nothing must fail loudly instead."""
        _register_adtv(db)
        with pytest.raises(RegistryError, match="no parameter"):
            resolve_filters(["adtv_floor"], {"adtv_floor": {"min_adtv": 5.0}}, db_path=db)

    def test_override_for_unused_filter_rejected(self, db):
        _register_adtv(db)
        with pytest.raises(RegistryError, match="not used by this strategy"):
            resolve_filters(["adtv_floor"], {"downtrend": {"pct": 5}}, db_path=db)

    def test_unknown_filter_rejected(self, db):
        with pytest.raises(RegistryError, match="unknown filter_id"):
            resolve_filters(["does_not_exist"], db_path=db)


class TestSchemaCreation:
    def test_idempotent(self, tmp_path):
        path = tmp_path / "r.duckdb"
        create_strategy_registry_schema(db_path=path)
        create_strategy_registry_schema(db_path=path)

        from datastore.api.db import get_duckdb_connection

        with get_duckdb_connection(path) as conn:
            tables = {
                r[0]
                for r in conn.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = 'main'"
                ).fetchall()
            }
        assert {"strategy_registry", "filter_registry", "strategy_signals"} <= tables
