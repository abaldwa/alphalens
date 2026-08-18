"""
tests/quality/test_no_hardcoded_strategy_params.py

Phase: Signal-generator consolidation (UnifiedGeneratorRefactorPlan.md, A2)
Owner: project-wide quality gate
Consumers: CI / `pytest tests/quality/`

WHY THIS GATE EXISTS
--------------------
`strategy_registry` is the declared source of truth for what a strategy IS:
`top_n`, `rank_start`, `rank_end`, `lookback_months`, `grace_cycles` and
`rebalance_frequency` all live in each row's `definition_json`. A live path
that pins any of them as a module-level constant silently overrides the
registry for EVERY strategy it runs.

The concrete failure, from §1.3 of the plan: `features/momentum_live.py`
sets `TOP_N = 15` once and applies it to every rank band, while the registry
declares per-band, per-strategy values -- including pairs of strategies that
differ ONLY in `top_n`:

    momentum:all_risk_b1_1-50_lb3mo_weekly_top10   {"top_n": 10, ...}
    momentum:all_risk_b1_1-50_lb3mo_weekly_top15   {"top_n": 15, ...}

Those two are the same strategy live. One of them is unrunnable as declared,
and nothing reports that -- the backtest measured a top-10 rule and the live
path holds 15 names.

This is the same declaration/implementation split that
tests/quality/test_registry_is_load_bearing.py enforces for strategy
IDENTITY: the registry declares WHICH strategies exist and with what
parameters; Python supplies only the logic. A hardcoded parameter is the
implementation quietly re-declaring itself.

SCOPE -- WHY ONLY LIVE PATHS
----------------------------
Only modules that run in the daily scheduler or serve the API are policed.
`scripts/run_momentum_*.py` also set these constants (8 sites), and that is
legitimate: a research runner pins the parameters of the one experiment it
exists to run, and it deploys no capital. Policing them would add entries
that can never be removed, turning a shrink-only list into permanent noise
-- the failure mode the sibling gate's docstring warns about.

PIT Assumptions
---------------
None -- pure static analysis, no data access.
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Iterator, List, Optional, Set, Tuple

REPO_ROOT = Path(__file__).resolve().parents[2]

EXCLUDE_DIR_PARTS = {".venv", "__pycache__", ".git", "node_modules", "catboost_info"}

# Parameters the registry declares per strategy. Pinning one of these in a
# live module overrides the registry for every strategy that module runs.
# [2026-08-18] GRACE_CYCLES and MIN_MOMENTUM removed: they are no longer
# registry-declared because they are no longer parameters at all. Momentum is
# a plain list swap, so there is no grace period and no momentum floor. A
# module pinning them now pins nothing -- the deprecation gate that matters
# for them is tests/unit/test_strategy_migration_momentum.py's
# test_no_row_declares_a_deprecated_knob.
REGISTRY_DECLARED_PARAMS = frozenset({
    "TOP_N",
    "LOOKBACK_MONTHS",
    "REBALANCE_FREQUENCY",
    "RANK_START",
    "RANK_END",
})

# Paths that make or serve real daily decisions: the scheduler's live steps
# and the API. See SCOPE above for why scripts/ is deliberately absent.
LIVE_PATH_ROOTS: List[str] = [
    "features/momentum_live.py",
    "systems/technical_analysis/alerts",
    "systems/ml_signal_engine/inference",
    "datastore/api",
]

# Known offenders -- asserted EXACTLY, so fixing one without deleting its
# entry fails too. Same contract as test_one_generator_per_channel.py.
#
# EMPTY as of C1 (2026-08-18): features/momentum_live.py now reads top_n,
# lookback_months and grace_cycles from strategy_registry.definition_json.
# The rule below is therefore absolute -- there is no longer any tolerated
# hardcoded strategy parameter in a live path, and the first one to appear
# fails this gate outright.
KNOWN_VIOLATIONS: frozenset[Tuple[str, str, str]] = frozenset()


def _ticket_for(module: str, param: str) -> str:
    for m, p, ticket in KNOWN_VIOLATIONS:
        if m == module and p == param:
            return ticket
    return "UNTRACKED -- new violation, no backlog entry"


def _iter_live_files() -> Iterator[Path]:
    for root in LIVE_PATH_ROOTS:
        base = REPO_ROOT / root
        if not base.exists():
            continue
        paths = [base] if base.is_file() else sorted(base.rglob("*.py"))
        for path in paths:
            if EXCLUDE_DIR_PARTS & set(path.parts):
                continue
            yield path


def _chosen_value(node: ast.AST) -> Optional[object]:
    """The value of a module-level assignment, if it is a single CHOSEN
    scalar rather than a menu of options.

    `features/momentum_signal.py` declares `LOOKBACK_MONTHS = [3, 6, 9, 12]`
    -- an enumeration of the lookbacks the system SUPPORTS, which every
    strategy then picks from. That is the opposite of overriding the
    registry, so container values are not decisions and are never flagged.
    A bare int/float/str is a strategy parameter someone picked."""
    if not isinstance(node, ast.Constant):
        return None
    if isinstance(node.value, bool) or node.value is None:
        return None
    if isinstance(node.value, (int, float, str)):
        return node.value
    return None


def _find_hardcoded_params() -> Set[Tuple[str, str, str]]:
    offenders: Set[Tuple[str, str, str]] = set()
    for path in _iter_live_files():
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (SyntaxError, UnicodeDecodeError):  # pragma: no cover - defensive
            continue
        rel = str(path.relative_to(REPO_ROOT))
        # Module level only: a local variable inside a function is scoped to
        # one call and is typically the registry value already unpacked.
        for node in tree.body:
            if isinstance(node, ast.Assign):
                targets, value = node.targets, node.value
            elif isinstance(node, ast.AnnAssign) and node.target is not None:
                targets, value = [node.target], node.value
            else:
                continue
            if value is None or _chosen_value(value) is None:
                continue
            for target in targets:
                if isinstance(target, ast.Name) and target.id in REGISTRY_DECLARED_PARAMS:
                    offenders.add((rel, target.id, _ticket_for(rel, target.id)))
    return offenders


def test_live_paths_do_not_hardcode_registry_declared_parameters():
    """A live module that pins `top_n` applies one value to every strategy
    it runs, so two registry rows differing only in `top_n` become the same
    strategy in production -- and the one whose backtest was approved may be
    the one that never runs.

    The registry already has the answer for every strategy. Read it."""
    offenders = _find_hardcoded_params()
    assert offenders == KNOWN_VIOLATIONS, (
        "Set of hardcoded registry-declared parameters in live paths changed.\n"
        f"  found:    {sorted(offenders)}\n"
        f"  expected: {sorted(KNOWN_VIOLATIONS)}\n"
        "A new entry means a live path pinned a parameter the registry declares "
        "-- read it from definition_json instead. A missing entry means one was "
        "fixed: delete it from KNOWN_VIOLATIONS in this file."
    )


def test_the_registry_actually_declares_these_parameters():
    """Guards the gate's own premise. If `definition_json` stopped carrying
    these keys, the rule above would be demanding that live paths read a
    source that no longer has the answer -- so the constants would be
    correct and this file would be the bug.

    Static: reads the momentum migration that WRITES definition_json rather
    than querying the database, so the gate keeps running in CI with no
    DuckDB present."""
    migration = REPO_ROOT / "strategies/migrations/momentum.py"
    assert migration.exists(), f"{migration} moved; update this gate's premise check."
    source = migration.read_text(encoding="utf-8")
    for key in ("top_n", "lookback_months", "rank_start", "rank_end"):
        assert f'"{key}"' in source, (
            f"strategies/migrations/momentum.py no longer declares '{key}' in "
            "definition_json. Either the registry stopped being the source of "
            "truth for it, or the key was renamed -- reconcile "
            "REGISTRY_DECLARED_PARAMS in this file before trusting the gate."
        )
