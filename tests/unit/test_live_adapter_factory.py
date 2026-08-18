"""tests/unit/test_live_adapter_factory.py — backtest/core/live_adapter_factory.py (F1).

The factory answers "what were this strategy's parameters" for the live path.
These tests pin the two things that make it worth having: it reads the answer
from the registry rather than inventing it, and it REFUSES when it cannot
honour what the registry declared.
"""

from datetime import date

import pytest

from backtest.core import live_adapter_factory as laf
from features.momentum_live import StrategyNotRunnableLive

AS_OF = date(2026, 8, 14)


class TestFilterTranslation:
    def test_declared_filters_become_adapter_kwargs(self, monkeypatch):
        monkeypatch.setattr(
            "strategies.registry.resolve_filters",
            lambda ids: [
                {"filter_id": "adtv_floor", "params": {"min_adtv_cr": 0.5}},
                {"filter_id": "circuit_lock_proxy", "params": {"circuit_band_pct": 0.19}},
            ],
        )
        kwargs = laf._filter_kwargs("technical:A1", ["adtv_floor", "circuit_lock_proxy"])
        assert kwargs == {"min_adtv_cr": 0.5, "circuit_band_pct": 0.19}

    def test_no_declared_filters_means_no_kwargs(self):
        assert laf._filter_kwargs("technical:A1", []) == {}

    def test_a_sizing_only_filter_is_not_a_selection_filter(self):
        """adtv_capped_sizing changes position SIZE, not which names are held,
        so it must not be mistaken for an unsupported selection filter and
        block the run."""
        assert laf._filter_kwargs("momentum:x", ["adtv_capped_sizing"]) == {}

    def test_an_unsupported_filter_refuses_rather_than_being_dropped(self):
        with pytest.raises(StrategyNotRunnableLive, match="quality_gate"):
            laf._filter_kwargs("technical:A1", ["quality_gate"])


class TestRefusals:
    def test_technical_without_top_n_refuses(self):
        with pytest.raises(ValueError, match="requires an explicit top_n"):
            laf.build_live_adapter("technical", "A1", AS_OF)

    def test_fundamental_without_top_n_refuses(self):
        with pytest.raises(ValueError, match="requires an explicit top_n"):
            laf.build_live_adapter("fundamental", "quality_compounder", AS_OF)

    def test_momentum_without_a_connection_refuses(self):
        with pytest.raises(ValueError, match="normalised DuckDB connection"):
            laf.build_live_adapter("momentum", "all_risk_b3_101-150_lb6mo_monthly_top15", AS_OF)

    def test_ml_has_no_live_adapter_yet(self):
        with pytest.raises(ValueError, match="PHASE-H5"):
            laf.build_live_adapter("ml", "whatever", AS_OF, top_n=10)


class TestBuildsFromTheRegistry:
    """Real registry rows — the point of the factory is that these values are
    the registry's, not the factory's."""

    def test_technical_adapter_carries_its_declared_template(self):
        adapter, universe = laf.build_live_adapter("technical", "A1", AS_OF, top_n=7)
        assert adapter.channel == "technical"
        assert adapter.template_name == "A1"
        assert adapter.top_n == 7
        assert universe, "the real feature day should carry a universe"

    def test_fundamental_adapter_carries_its_declared_preset(self):
        adapter, _ = laf.build_live_adapter("fundamental", "quality_compounder", AS_OF, top_n=7)
        assert adapter.channel == "fundamental"
        assert adapter.preset == "quality_compounder"
        assert adapter.top_n == 7

    def test_a_top_n_argument_never_overrides_momentums_declared_one(self):
        """Momentum DOES declare top_n. A caller passing a different one must
        not be able to quietly resize a backtested portfolio."""
        import duckdb

        from strategies.definitions import get_definition

        declared = get_definition("momentum", "all_risk_b3_101-150_lb6mo_monthly_top15")["definition"]

        captured = {}

        class _FakeAdapter:
            channel = "momentum"

            def __init__(self, **kw):
                captured.update(kw)

        monkey = pytest.MonkeyPatch()
        monkey.setattr("backtest.adapters.momentum_adapter.MomentumAdapter", _FakeAdapter)
        monkey.setattr(
            "features.momentum_universe.current_momentum_band_universe", lambda *a, **k: ["AAA"]
        )
        monkey.setattr(laf, "_panels", lambda conn, u, d: (None, None))
        try:
            conn = duckdb.connect(":memory:")
            laf.build_live_adapter(
                "momentum", "all_risk_b3_101-150_lb6mo_monthly_top15", AS_OF,
                conn=conn, top_n=999,
            )
        finally:
            monkey.undo()
        assert captured["top_n"] == int(declared["top_n"]) != 999
        assert captured["lookback_months"] == int(declared["lookback_months"])
