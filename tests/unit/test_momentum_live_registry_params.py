"""
tests/unit/test_momentum_live_registry_params.py

Phase: Signal-generator consolidation (UnifiedGeneratorRefactorPlan.md, C1)
Owner: Platform / Features
Consumers: CI / `pytest tests/unit/`

Covers the C1 contract: features/momentum_live.py reads top_n,
lookback_months and grace_cycles from strategy_registry rather than
declaring them, so two registry strategies differing only in top_n can no
longer collapse into the same live strategy.

The static gate (tests/quality/test_no_hardcoded_strategy_params.py) proves
the constants are GONE. These tests prove the replacement actually works,
and — more importantly — that it refuses to guess when the registry cannot
answer.

PIT Assumptions
---------------
Registry reads are as-of the current version, matching what the live path
does. Tests that need the real registry skip when it is unavailable rather
than fabricating rows.
"""

from __future__ import annotations

import pytest

from features import momentum_live


def _registry_available() -> bool:
    try:
        momentum_live.strategy_params(momentum_live.DEFAULT_STRATEGY_ID)
    except Exception:
        return False
    return True


requires_registry = pytest.mark.skipif(
    not _registry_available(),
    reason="strategy_registry unavailable (unpopulated or DB locked); C1 reads "
           "real declarations and must not be tested against invented rows",
)


def test_the_removed_constants_are_really_gone():
    """The three module constants must not come back under any name.

    Asserted on the module rather than only in the static gate because a
    re-added constant with a live default is the exact regression C1
    exists to prevent, and it would otherwise only be caught by a gate
    someone could edit in the same commit."""
    for name in ("TOP_N", "LOOKBACK_MONTHS", "GRACE_CYCLES"):
        assert not hasattr(momentum_live, name), (
            f"features/momentum_live.{name} is back. Per-strategy parameters "
            "belong in strategy_registry.definition_json (PHASE-C1); a module "
            "constant applies one value to every rank band."
        )


@requires_registry
def test_every_live_strategy_resolves_to_a_registry_row():
    """A live strategy whose registry key does not resolve cannot be run at
    all under C1 -- it raises instead of guessing. If this fails, the live
    dashboard is down, so it is worth catching here rather than at 09:00."""
    for strategy in momentum_live.STRATEGIES:
        params = momentum_live.strategy_params(strategy["strategy_id"])
        assert params["top_n"] is not None
        assert params["lookback_months"] is not None
        assert params["grace_cycles"] is not None


@requires_registry
def test_registry_band_matches_the_rank_band_it_ranks():
    """The registry key embeds the rank band, and momentum_universe.RANK_BANDS
    decides which tickers are actually ranked. If those two drift, the live
    path would rank one universe while reporting itself as a strategy defined
    over another -- the A112 boundary defect, one layer up."""
    for strategy in momentum_live.STRATEGIES:
        params = momentum_live.strategy_params(strategy["strategy_id"])
        assert (params["rank_start"], params["rank_end"], params["band_id"]) == (
            strategy["rank_start"], strategy["rank_end"], strategy["band_id"]
        ), (
            f"{strategy['strategy_id']} ranks "
            f"{strategy['rank_start']}-{strategy['rank_end']} but its registry "
            f"row {strategy['registry_key']} declares "
            f"{params['rank_start']}-{params['rank_end']}."
        )


@requires_registry
def test_live_path_declares_the_category_it_actually_runs():
    """The live path applies no filters, which is precisely the registry's
    `all_risk` category. Saying so explicitly is what makes the C2 gap
    visible; leaving it implicit is how a filtered strategy could quietly be
    served by an unfiltered code path."""
    for strategy in momentum_live.STRATEGIES:
        assert strategy["category"] == "all_risk"
        assert momentum_live.strategy_params(strategy["strategy_id"])["category"] == "all_risk"


def test_unknown_strategy_id_is_rejected():
    with pytest.raises(ValueError, match="Unknown momentum strategy_id"):
        momentum_live.strategy_params("band99_does_not_exist")


def test_missing_registry_row_raises_instead_of_defaulting(monkeypatch):
    """The single most important behaviour in C1.

    A fallback default is what made the old constants dangerous: the run
    would proceed, look healthy, and trade parameters nobody approved. When
    the registry cannot answer, the live path must stop."""
    momentum_live._declared_params.cache_clear()
    monkeypatch.setattr(
        "strategies.registry.get_strategy", lambda *a, **k: None,
    )
    with pytest.raises(momentum_live.StrategyParamsUnavailable, match="no active row"):
        momentum_live.strategy_params(momentum_live.DEFAULT_STRATEGY_ID)
    momentum_live._declared_params.cache_clear()


def test_partially_declared_row_raises(monkeypatch):
    """A row that declares top_n but not lookback_months is worse than no
    row: it would silently supply one real value and one invented one. The
    registry is authoritative for both or for neither."""
    momentum_live._declared_params.cache_clear()
    monkeypatch.setattr(
        "strategies.registry.get_strategy",
        lambda *a, **k: {"definition": {"top_n": 15}},
    )
    with pytest.raises(momentum_live.StrategyParamsUnavailable, match="lookback_months"):
        momentum_live.strategy_params(momentum_live.DEFAULT_STRATEGY_ID)
    momentum_live._declared_params.cache_clear()


# ---------------------------------------------------------------------------
# C2 -- declared filters are applied, or the strategy is refused
# ---------------------------------------------------------------------------


def test_unsupported_declared_filter_is_refused_not_skipped():
    """The single most important behaviour C2 adds.

    A `balanced` strategy declares a quality gate; the live path has no
    quality-score source. Before C2 there was no filter chain at all, so
    such a strategy would have run COMPLETELY UNFILTERED while its backtest
    applied the whole chain -- silently, looking perfectly healthy, and
    deploying capital against a rule nobody measured.

    Refusing is the safe failure. Skipping the filter is the dangerous one,
    so it must be impossible rather than merely discouraged."""
    with pytest.raises(momentum_live.StrategyNotRunnableLive, match="quality_gate"):
        momentum_live._buy_pool_kwargs(
            "momentum:balanced_b3_101-150_lb6mo_monthly_top15", volume_panel=None,
        )


def test_sizing_only_filters_do_not_block_selection():
    """`adtv_capped_sizing` is typed `sizing` in filter_registry: it changes
    how much of a name is bought, not whether it is selected. Treating it as
    a selection filter would refuse strategies that are perfectly runnable,
    which is the opposite failure but a failure all the same."""
    assert "adtv_capped_sizing" in momentum_live._SIZING_ONLY_FILTER_IDS
    assert "adtv_capped_sizing" not in momentum_live._SUPPORTED_FILTER_IDS


@requires_registry
def test_all_risk_declares_no_selection_filters():
    """all_risk is the unfiltered baseline, so every live strategy today
    resolves to an empty kwargs dict -- which is why C2 changed no filter
    behaviour for them, only the ranking window."""
    for strategy in momentum_live.STRATEGIES:
        kwargs = momentum_live._buy_pool_kwargs(strategy["registry_key"], volume_panel=None)
        assert kwargs == {}, f"{strategy['strategy_id']} unexpectedly declares {kwargs}"


def test_adtv_floor_without_volume_history_is_refused(monkeypatch):
    """A declared liquidity floor with no volume data must refuse, not
    select without it. Same principle as the unsupported-filter case: the
    filter was declared, so proceeding without it runs a different strategy
    from the declared one."""
    monkeypatch.setattr(
        "strategies.registry.get_strategy",
        lambda *a, **k: {"filter_ids": ["adtv_floor"]},
    )
    momentum_live._registry_filter_ids.cache_clear()
    with pytest.raises(momentum_live.StrategyNotRunnableLive, match="volume"):
        momentum_live._buy_pool_kwargs("momentum:fake_for_test", volume_panel=None)
    momentum_live._registry_filter_ids.cache_clear()
