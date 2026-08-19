"""
tests/quality/test_one_momentum_universe.py

Phase: Unified Generator Refactor, Phase C (user decision 2026-08-18)
Owner: Platform / Backtest
Consumers: CI

The user's stated requirement, made checkable: "We need to ensure that there
is only 1 Logic in Generator for Backtest, Paper Trade and Live Trading."

For momentum's UNIVERSE that means every path resolves through
features.momentum_universe.momentum_band_universe. This is a STATIC gate --
it proves the callers are wired to one function, which is the property that
cannot be verified by running one of them.
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Set

REPO_ROOT = Path(__file__).resolve().parents[2]

#: The one definition, and the two helpers that resolve a date onto it.
OWNER_MODULE = "features/momentum_universe.py"
SHARED_ENTRY_POINTS = frozenset({
    "momentum_band_universe",            # the definition
    "current_momentum_band_universe",    # live/paper: resolve today's snapshot
    "build_momentum_universe_provider",  # backtest: cache one per grid point
})

#: Every path that answers "which stocks is this momentum strategy choosing
#: from", and must therefore reach one of the entry points above.
MOMENTUM_UNIVERSE_CALLERS = {
    "backtest/run_orchestrator_backtest.py": "backtest",
    "backtest/core/live_adapter_factory.py": "paper trading / live holdings",
    "features/momentum_live.py": "the live dashboard + ledger",
}

#: The market-cap-only lookup the three paths used before. It ranks by market
#: cap with NO liquidity gate, so a band built from it can contain names an
#: order could not fill. Still legitimate for callers asking a market-cap
#: question; a momentum universe caller reaching for it has left the one rule.
SUPERSEDED_LOOKUP = "rank_band_tickers"


def _imported_names(tree: ast.Module) -> Set[str]:
    names: Set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module and "momentum_universe" in node.module:
            names.update(alias.name for alias in node.names)
    return names


def _called_names(tree: ast.Module) -> Set[str]:
    called: Set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Name):
                called.add(func.id)
            elif isinstance(func, ast.Attribute):
                called.add(func.attr)
    return called


def _parse(rel_path: str) -> ast.Module:
    return ast.parse((REPO_ROOT / rel_path).read_text())


def test_every_momentum_path_resolves_through_the_one_definition():
    """Backtest, paper/live holdings and the live dashboard must each call a
    shared entry point. A path that builds its universe another way is a
    second definition of what a band contains."""
    missing = []
    for rel_path, role in sorted(MOMENTUM_UNIVERSE_CALLERS.items()):
        tree = _parse(rel_path)
        reached = (_imported_names(tree) | _called_names(tree)) & SHARED_ENTRY_POINTS
        if not reached:
            missing.append(f"  {rel_path} ({role}) reaches none of {sorted(SHARED_ENTRY_POINTS)}")
    assert not missing, (
        "These momentum paths no longer resolve their universe through "
        f"{OWNER_MODULE}:\n" + "\n".join(missing) + "\n\nOne generator logic for "
        "backtest, paper trading and live means one universe definition too."
    )


def test_no_momentum_path_falls_back_to_the_market_cap_only_lookup():
    """rank_band_tickers ranks by market cap with no liquidity gate. It is
    what the three paths used before 2026-08-18, and a band built from it can
    hold names an order could not fill."""
    offenders = []
    for rel_path in sorted(MOMENTUM_UNIVERSE_CALLERS):
        tree = _parse(rel_path)
        if SUPERSEDED_LOOKUP in (_imported_names(tree) | _called_names(tree)):
            offenders.append(rel_path)
    assert not offenders, (
        f"{offenders} call {SUPERSEDED_LOOKUP}, which applies no liquidity "
        "filter. Momentum's universe is top-800-by-ADTV first, then market-cap "
        "rank within it — call momentum_band_universe (or one of its two "
        "resolvers) instead."
    )


def test_the_definition_composes_liquidity_before_market_cap():
    """The order is the decision, so it is asserted structurally as well as
    behaviourally (tests/unit/test_momentum_universe_definition.py): the
    liquid set must be computed BEFORE the market-cap snapshot, and the
    snapshot must be taken over it.

    The composition moved one level down on 2026-08-19, when the band-
    independent half was extracted into ranked_liquid_universe so a sweep
    could memoise it. momentum_band_universe now delegates rather than
    composing directly -- so this asserts BOTH halves of that arrangement:
    the order inside the extracted function, and that the entry point still
    reaches it. Asserting only the order would let a caller quietly stop
    using it; asserting only the delegation would let the order invert.
    """
    tree = _parse(OWNER_MODULE)

    def _calls_in(name: str) -> list:
        func = next(
            node for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef) and node.name == name
        )
        return [
            node.func.id for node in ast.walk(func)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        ]

    composer = _calls_in("ranked_liquid_universe")
    assert "liquid_universe" in composer, "the liquidity step is gone"
    assert "market_cap_snapshot" in composer, "the market-cap step is gone"
    assert composer.index("liquid_universe") < composer.index("market_cap_snapshot"), (
        "market cap is being ranked before liquidity is filtered — that leaves a "
        "band short whenever an illiquid large-cap holds a slot"
    )

    assert "ranked_liquid_universe" in _calls_in("momentum_band_universe"), (
        "momentum_band_universe no longer goes through ranked_liquid_universe, "
        "so the composition asserted above is not the one the band actually uses."
    )
