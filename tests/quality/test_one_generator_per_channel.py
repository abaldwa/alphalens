"""
tests/quality/test_one_generator_per_channel.py

Phase: Signal-generator consolidation
Owner: project-wide quality gate
Consumers: CI / `pytest tests/quality/`

WHY THIS GATE EXISTS
--------------------
A "channel" (momentum, technical, ml, fundamental) is supposed to have
exactly ONE piece of code that turns (universe, as_of_date) into a list of
Signals. Historically it has had several: an adapter under
backtest/adapters/, an inline ranking loop inside a bespoke backtester, and
a third variant in the live/paper path. They start identical and drift.

The concrete failure this prevents: a backtest reports one set of holdings
and the live/paper runner buys a DIFFERENT set on the same date, because
each walked its own copy of the selection logic. Nothing crashes, no test
goes red, and the divergence is only discoverable by hand-diffing a
backtest's holdings against a live run's. Every "the backtest said X but we
own Y" incident in this codebase traces back to a duplicated generator.

Static analysis only: everything here is `ast`-based. These modules are slow
to import and importing them has side effects (DuckDB connections, config
resolution), so this gate never imports production code. That also means the
gate runs in CI with no database present.

HOW KNOWN_VIOLATIONS WORKS -- READ BEFORE EDITING
-------------------------------------------------
KNOWN_VIOLATIONS is NOT an allowlist that lets debt sit forever. It is
asserted to be EXACTLY EQUAL to the set of violations found. That means:

  * a NEW violation fails the gate (the usual regression guard), AND
  * FIXING a violation without deleting its entry ALSO fails the gate.

The second half is the point. An allowlist that only ever gets appended to
grows quietly; a set that must match exactly forces every removal to be a
deliberate, reviewed edit to this file. Each entry carries the phase /
backlog ID that deletes it, so the set has a scheduled path to empty.

WHEN KNOWN_VIOLATIONS IS EMPTY: delete the `_expected` indirection in each
test and assert the found set is empty outright. The rules below then become
absolute, and this docstring section can go.

PIT Assumptions
---------------
None -- pure static analysis, no data access.
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Set, Tuple

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]

CHANNELS = frozenset({"momentum", "technical", "ml", "fundamental"})

EXCLUDE_DIR_PARTS = {".venv", "__pycache__", ".git", "node_modules", "catboost_info"}

# The ONE module::class that is allowed to generate signals for each channel.
# Anything else that generates signals for the same channel is a violation.
DESIGNATED_GENERATORS: Dict[str, str] = {
    "momentum": "backtest/adapters/momentum_adapter.py::MomentumAdapter",
    "technical": "backtest/adapters/technical_adapter.py::TechnicalAdapter",
    "fundamental": "backtest/adapters/fundamental_adapter.py::FundamentalAdapter",
    "ml": "backtest/adapters/ml_adapter.py::MLAdapter",
}

# Composites that DELEGATE to a channel's designated generator rather than
# re-implementing its selection logic. These are not violations, and folding
# them into the designated adapter would delete a real capability.
#
# TechnicalComboAdapter pools candidates from 2+ screener templates into one
# ranked selection (the 2026-08-01 "combination of strategies" request). Its
# own docstring is explicit that it WRAPS N TechnicalAdapter instances and
# reuses each one's _filtered_candidates() "rather than duplicating that
# fetch/filter logic" -- so there is exactly one implementation of technical
# candidate selection, and this composes it. It is used in production by
# backtest/run_orchestrator_backtest.py:735 and :1008.
#
# The rule this encodes: one generator per channel means one *implementation*
# of the selection, not one class that may call it. A composite qualifies only
# if it delegates; the moment one re-implements selection it becomes an
# extra_generator like any other.
DELEGATING_COMPOSITES: Dict[str, str] = {
    "backtest/adapters/technical_combo_adapter.py::TechnicalComboAdapter": "technical",
}

# backtest/core/engine.py defines the StrategyAdapter *Protocol* -- a typing
# declaration of the contract, not an implementation. It names
# generate_signals by necessity and must never count as a generator.
PROTOCOL_DEFINITIONS = frozenset({
    "backtest/core/engine.py::StrategyAdapter",
})

# Directories searched for Signal() construction and generator declarations.
SCAN_DIRS = ["backtest", "systems", "scripts", "features"]

# The frozen legacy ML engine. It is scheduled for deletion; until then no
# new code may take a dependency on it, because anything that imports it
# inherits a second, divergent backtest implementation.
LEGACY_ENGINE_MODULE = "backtest.engine"
LEGACY_ENGINE_IMPORT_BANNED_DIRS = ["scripts", "backtest/paper_trading"]

# Calling these turns a universe into a ranked momentum selection. Only a
# designated generator may do it; a second call site is a second ranking.
MOMENTUM_SELECTION_PRIMITIVES = frozenset({
    "trailing_momentum_from_panel",
    "orthogonalize_momentum_vs_factors",
})
# Scoped to backtest/: features/ is the shared primitive layer those
# functions legitimately live in and compose within.
MOMENTUM_PRIMITIVE_SCAN_DIRS = ["backtest"]


# ---------------------------------------------------------------------------
# KNOWN VIOLATIONS -- must shrink to empty. Each entry: (kind, where, ticket)
# ---------------------------------------------------------------------------

KNOWN_VIOLATIONS: frozenset[Tuple[str, str, str]] = frozenset({
    # ml_adapter.py is a result-schema translator over the frozen
    # backtest/engine.py, not a generator: it has channel() but no
    # generate_signals. Until it has one, the ML channel cannot run through
    # the unified engine at all, so ML backtests and ML paper trading share
    # no code path whatsoever.
    (
        "missing_generator",
        "ml",
        "PHASE-4: give ml_adapter a real generate_signals",
    ),
    # MomentumBacktester.run() re-ranks momentum inline instead of calling
    # MomentumAdapter. This is the literal backtest-vs-live divergence this
    # gate is named for: the adapter and this loop apply the same filters in
    # separately maintained code.
    (
        "duplicate_momentum_ranking",
        "backtest/momentum_backtest.py",
        "PHASE-3: delete MomentumBacktester, route through MomentumAdapter",
    ),
    # Legacy-engine importers. Each one pins backtest/engine.py in place.
    (
        "legacy_engine_import",
        "scripts/run_paper_trading_sim.py",
        "PHASE-5: delete backtest/engine.py",
    ),
    (
        "legacy_engine_import",
        "scripts/run_daily_paper_trading.py",
        "PHASE-5: delete backtest/engine.py",
    ),
    (
        "legacy_engine_import",
        "scripts/train_stacking.py",
        "PHASE-5: delete backtest/engine.py",
    ),
})


def _expected(kind: str) -> Set[Tuple[str, str, str]]:
    return {v for v in KNOWN_VIOLATIONS if v[0] == kind}


def _ticket_for(kind: str, where: str) -> str:
    for k, w, ticket in KNOWN_VIOLATIONS:
        if k == kind and w == where:
            return ticket
    return "UNTRACKED -- new violation, no backlog entry"


# ---------------------------------------------------------------------------
# AST helpers
# ---------------------------------------------------------------------------


def _iter_py_files(dirs: List[str]) -> Iterator[Path]:
    for d in dirs:
        base = REPO_ROOT / d
        if not base.exists():
            continue
        for path in sorted(base.rglob("*.py")):
            if EXCLUDE_DIR_PARTS & set(path.parts):
                continue
            yield path


def _rel(path: Path) -> str:
    return str(path.relative_to(REPO_ROOT))


def _parse(path: Path) -> Optional[ast.Module]:
    try:
        return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except (SyntaxError, UnicodeDecodeError):  # pragma: no cover - defensive
        return None


def _class_channel(node: ast.ClassDef) -> Optional[str]:
    """Read a `channel = "..."` class attribute, the way every adapter
    declares which channel it belongs to."""
    for stmt in node.body:
        targets = []
        if isinstance(stmt, ast.Assign):
            targets = stmt.targets
            value = stmt.value
        elif isinstance(stmt, ast.AnnAssign) and stmt.target is not None:
            targets = [stmt.target]
            value = stmt.value
        else:
            continue
        for t in targets:
            if isinstance(t, ast.Name) and t.id == "channel":
                if isinstance(value, ast.Constant) and isinstance(value.value, str):
                    return value.value
    return None


def _module_channel(tree: ast.Module) -> Optional[str]:
    """Read a module-level `def channel() -> str: return "..."` (ml_adapter's
    form) or a module-level `channel = "..."`."""
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == "channel":
            for sub in ast.walk(node):
                if isinstance(sub, ast.Return) and isinstance(sub.value, ast.Constant):
                    if isinstance(sub.value.value, str):
                        return sub.value.value
        if isinstance(node, ast.Assign):
            for t in node.targets:
                if isinstance(t, ast.Name) and t.id == "channel":
                    if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
                        return node.value.value
    return None


def _declares_generate_signals(node: ast.ClassDef) -> bool:
    return any(
        isinstance(s, (ast.FunctionDef, ast.AsyncFunctionDef)) and s.name == "generate_signals"
        for s in node.body
    )


def _find_generators() -> Dict[str, List[str]]:
    """channel -> sorted list of "path::Class" that declare generate_signals.

    Protocol declarations are excluded; a class with no channel attribute
    falls back to its module's channel() so a generator can never hide by
    omitting the attribute.
    """
    found: Dict[str, List[str]] = {c: [] for c in CHANNELS}
    for path in _iter_py_files(SCAN_DIRS):
        tree = _parse(path)
        if tree is None:
            continue
        mod_channel = _module_channel(tree)
        for node in ast.walk(tree):
            if not isinstance(node, ast.ClassDef):
                continue
            if not _declares_generate_signals(node):
                continue
            ident = f"{_rel(path)}::{node.name}"
            if ident in PROTOCOL_DEFINITIONS:
                continue
            channel = _class_channel(node) or mod_channel
            if channel in CHANNELS:
                found[channel].append(ident)
    return {c: sorted(v) for c, v in found.items()}


@pytest.fixture(scope="module")
def generators() -> Dict[str, List[str]]:
    return _find_generators()


# ---------------------------------------------------------------------------
# 1. Exactly one generator class per channel
# ---------------------------------------------------------------------------


def test_designated_generator_exists_and_is_a_generator(generators):
    """The designated generator must actually declare generate_signals.

    If a rename or refactor moves it, every other check below silently
    re-baselines around the wrong module -- so this is asserted first and
    absolutely, with no KNOWN_VIOLATIONS escape for the three channels that
    do have one today."""
    for channel, designated in DESIGNATED_GENERATORS.items():
        if ("missing_generator", channel, _ticket_for("missing_generator", channel)) in KNOWN_VIOLATIONS:
            continue
        assert designated in generators[channel], (
            f"Designated generator {designated} for channel '{channel}' no longer "
            f"declares generate_signals. Found instead: {generators[channel]}. "
            "Update DESIGNATED_GENERATORS if this was an intentional move."
        )


def test_no_channel_is_missing_its_generator(generators):
    """A channel with zero generators cannot run through the unified engine
    at all, which means its backtest and its live path necessarily use
    different code -- the exact divergence this file guards."""
    missing = {
        ("missing_generator", c, _ticket_for("missing_generator", c))
        for c in sorted(CHANNELS)
        if not generators[c]
    }
    assert missing == _expected("missing_generator"), (
        "Channels with no signal generator changed.\n"
        f"  found:    {sorted(missing)}\n"
        f"  expected: {sorted(_expected('missing_generator'))}\n"
        "If you FIXED one, delete its KNOWN_VIOLATIONS entry in this file."
    )


def test_exactly_one_generator_per_channel(generators):
    """Two classes generating signals for one channel is two answers to
    'what does this strategy hold today'. They are maintained separately,
    drift within weeks, and the divergence surfaces only as a backtest that
    disagrees with the live book."""
    extras: Set[Tuple[str, str, str]] = set()
    for channel in sorted(CHANNELS):
        designated = DESIGNATED_GENERATORS[channel]
        for ident in generators[channel]:
            if ident == designated:
                continue
            # A composite that delegates to the designated generator is one
            # implementation used twice, not two implementations.
            if DELEGATING_COMPOSITES.get(ident) == channel:
                continue
            extras.add(("extra_generator", ident, _ticket_for("extra_generator", ident)))

    assert extras == _expected("extra_generator"), (
        "Set of EXTRA signal generators changed.\n"
        f"  found:    {sorted(extras)}\n"
        f"  expected: {sorted(_expected('extra_generator'))}\n"
        "A new entry means a second generator was added for a channel -- fold it "
        "into the designated adapter. A missing entry means one was removed: "
        "delete it from KNOWN_VIOLATIONS."
    )


# ---------------------------------------------------------------------------
# 2. Only generators may construct a Signal
# ---------------------------------------------------------------------------


def _signal_construction_modules() -> Set[str]:
    modules: Set[str] = set()
    for path in _iter_py_files(SCAN_DIRS):
        tree = _parse(path)
        if tree is None:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                func = node.func
                name = (
                    func.id if isinstance(func, ast.Name)
                    else func.attr if isinstance(func, ast.Attribute)
                    else None
                )
                if name == "Signal":
                    modules.add(_rel(path))
    return modules


def test_only_generators_construct_signals():
    """A Signal built outside a generator is a strategy decision made
    somewhere nobody looks for one -- a script or a live runner minting its
    own buy list that no backtest ever evaluated.

    Allowed modules are derived: the designated generators, plus any module
    already recorded as an extra_generator. That derivation is deliberate --
    when an extra generator is deleted, its right to build Signals evaporates
    in the same edit, with no second list to remember to update."""
    allowed = {d.split("::", 1)[0] for d in DESIGNATED_GENERATORS.values()}
    allowed |= {w.split("::", 1)[0] for k, w, _ in KNOWN_VIOLATIONS if k == "extra_generator"}
    # A delegating composite emits the Signals it pooled from its sub-adapters,
    # so it necessarily constructs them.
    allowed |= {c.split("::", 1)[0] for c in DELEGATING_COMPOSITES}

    offenders = sorted(_signal_construction_modules() - allowed)
    assert not offenders, (
        "These modules construct Signal() but are not signal generators:\n  "
        + "\n  ".join(offenders)
        + "\nMove the decision into the channel's designated adapter and have "
        "this module consume its output."
    )


# ---------------------------------------------------------------------------
# 3. Nothing new may import the frozen legacy engine
# ---------------------------------------------------------------------------


def _imports_legacy_engine(tree: ast.Module) -> bool:
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.module and (
                node.module == LEGACY_ENGINE_MODULE
                or node.module.startswith(LEGACY_ENGINE_MODULE + ".")
            ):
                return True
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == LEGACY_ENGINE_MODULE or alias.name.startswith(
                    LEGACY_ENGINE_MODULE + "."
                ):
                    return True
    return False


def test_no_legacy_engine_imports_in_scripts_or_paper_trading():
    """backtest/engine.py is a frozen second backtest implementation. Every
    importer pins it alive, and a paper-trading path that imports it is
    executing entirely different selection and exit logic from the unified
    engine that produced the numbers the strategy was approved on."""
    offenders: Set[Tuple[str, str, str]] = set()
    for path in _iter_py_files(LEGACY_ENGINE_IMPORT_BANNED_DIRS):
        tree = _parse(path)
        if tree is None:
            continue
        if _imports_legacy_engine(tree):
            rel = _rel(path)
            offenders.add(("legacy_engine_import", rel, _ticket_for("legacy_engine_import", rel)))

    assert offenders == _expected("legacy_engine_import"), (
        "Set of legacy-engine importers changed.\n"
        f"  found:    {sorted(offenders)}\n"
        f"  expected: {sorted(_expected('legacy_engine_import'))}\n"
        "New entries are forbidden outright -- do not build on backtest/engine.py. "
        "If you removed a dependency, delete its KNOWN_VIOLATIONS entry."
    )


# ---------------------------------------------------------------------------
# 4. No duplicated momentum ranking outside the designated generator
# ---------------------------------------------------------------------------


def test_momentum_ranking_happens_in_exactly_one_place():
    """A class-level `generate_signals` check cannot see a generator that was
    never given that name. MomentumBacktester ranks momentum inline in its
    simulation loop -- structurally invisible to check 1, behaviourally a
    second momentum strategy.

    So this looks for the primitives that ARE the ranking
    (trailing_momentum_from_panel / orthogonalize_momentum_vs_factors) and
    requires every call site under backtest/ to live in a designated
    generator module."""
    allowed = {d.split("::", 1)[0] for d in DESIGNATED_GENERATORS.values()}

    offenders: Set[Tuple[str, str, str]] = set()
    for path in _iter_py_files(MOMENTUM_PRIMITIVE_SCAN_DIRS):
        rel = _rel(path)
        if rel in allowed:
            continue
        tree = _parse(path)
        if tree is None:
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            name = (
                func.id if isinstance(func, ast.Name)
                else func.attr if isinstance(func, ast.Attribute)
                else None
            )
            if name in MOMENTUM_SELECTION_PRIMITIVES:
                offenders.add(
                    ("duplicate_momentum_ranking", rel, _ticket_for("duplicate_momentum_ranking", rel))
                )
                break

    assert offenders == _expected("duplicate_momentum_ranking"), (
        "Set of modules re-implementing momentum ranking changed.\n"
        f"  found:    {sorted(offenders)}\n"
        f"  expected: {sorted(_expected('duplicate_momentum_ranking'))}\n"
        "Call MomentumAdapter.generate_signals instead of re-ranking inline."
    )


# ---------------------------------------------------------------------------
# 5. Determinism -- runtime, and therefore optional
# ---------------------------------------------------------------------------


def _fixture_db_available() -> bool:
    try:
        from config.settings import BACKTEST_DUCKDB_PATH
    except Exception:
        return False
    return Path(BACKTEST_DUCKDB_PATH).exists()


@pytest.mark.skipif(
    not _fixture_db_available(),
    reason="determinism check needs the backtest DuckDB; the rest of this gate is "
           "static and must keep passing in CI without one",
)
def test_generator_is_deterministic_for_a_fixed_date():
    """One generator per channel is only worth having if that generator
    returns the same thing twice. A generator seeded by wall-clock time or
    by unordered set iteration produces a different book on every call, so
    a backtest can never be reproduced -- and 'the backtest disagrees with
    live' becomes unfalsifiable.

    This is the one rule that cannot be checked statically. It is skipped,
    never failed, when the database is absent: the gate above must remain
    runnable with no data access at all."""
    pytest.importorskip("pandas")
    from datetime import date

    from backtest.adapters.momentum_adapter import MomentumAdapter  # noqa: F401
    from backtest.core.horizon import HorizonBucket  # noqa: F401

    # Requires a prepared price/momentum panel fixture. Until that fixture
    # exists as real (non-synthetic) stored data, skip rather than fabricate
    # inputs -- CLAUDE.md Absolute Rule 6.
    pytest.skip(
        "No real stored panel fixture wired for a two-call determinism "
        "comparison yet; see PHASE-3 for the fixture that enables it. "
        f"(would compare two generate_signals calls for {date.today()})"
    )
