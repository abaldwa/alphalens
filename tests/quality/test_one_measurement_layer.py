"""
tests/quality/test_one_measurement_layer.py

Phase: Signal-generator consolidation (UnifiedGeneratorRefactorPlan.md, H3)
Owner: project-wide quality gate
Consumers: CI / `pytest tests/quality/`

WHY THIS GATE EXISTS
--------------------
A strategy's reported Sharpe, Sortino, Calmar and CAGR must come from ONE
implementation, for the same reason its holdings must come from one
generator: two implementations of "the number" drift, and the drift is
invisible because both sides look like plausible finance.

This is not hypothetical here. `backtest/core/metrics.sharpe_ratio`
hardcoded 252 periods per year, correct for the orchestrator's daily equity
curves and wrong for a per-rebalance momentum curve -- the same weekly
returns read **2.46 vs 1.12**, a sqrt(252/52) overstatement. That single
mismatch is the entire reason a second Sharpe was ever written.
`infer_periods_per_year()` now lets one implementation serve every cadence.

WHAT IS ASSERTED
----------------
Every metric primitive is DEFINED in exactly one module. Callers may import
it from anywhere; what may not happen is a second `def sharpe_...` growing
somewhere else.

`backtest/momentum_metrics.py` WAS a tracked exception (it backed published
external results). H4 (2026-08-18) deleted it along with MomentumBacktester
and moved its cagr/sharpe_sortino_calmar/win_rate/rolling_window_summary
implementations directly into the OWNER module below -- so KNOWN_DUPLICATES
is now empty, per this file's own shrink-only contract.

PIT Assumptions
---------------
None -- pure static analysis, no data access.
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Iterator, List, Set, Tuple

REPO_ROOT = Path(__file__).resolve().parents[2]

EXCLUDE_DIR_PARTS = {".venv", "__pycache__", ".git", "node_modules", "catboost_info"}

SCAN_DIRS: List[str] = ["backtest", "systems", "features", "datastore", "strategies"]

# The module that owns the measurement layer.
OWNER = "backtest/core/metrics.py"

# Function names that ARE the measurement layer. A second definition of any
# of these is a second answer to "how did this strategy do".
METRIC_PRIMITIVES = frozenset({
    "sharpe_ratio",
    "sortino_ratio",
    "calmar_ratio",
    "sharpe_sortino_calmar",
    "calendar_cagr",
    "trading_day_cagr",
    "cagr",
    "max_drawdown",
    "xirr",
    "churn_factor",
    "win_rate",
    "win_rate_and_profit_factor",
    "turnover_ratio",
    "rolling_window_summary",
})

# Tracked duplicate definitions. MUST SHRINK TO EMPTY -- asserted exactly, so
# deleting a duplicate without deleting its entry fails too.
#
# [H4, 2026-08-18] Was {cagr, sharpe_sortino_calmar, win_rate,
# rolling_window_summary} in backtest/momentum_metrics.py, now empty: H4
# deleted that module and moved those implementations into OWNER itself
# (backtest/core/metrics.py), where _find_duplicate_definitions() skips them
# by definition (`if rel == OWNER: continue`).
KNOWN_DUPLICATES: frozenset[Tuple[str, str, str]] = frozenset()


def _ticket_for(name: str, module: str) -> str:
    for n, m, ticket in KNOWN_DUPLICATES:
        if n == name and m == module:
            return ticket
    return "UNTRACKED -- new duplicate metric implementation, no backlog entry"


def _iter_py_files() -> Iterator[Path]:
    for d in SCAN_DIRS:
        base = REPO_ROOT / d
        if not base.exists():
            continue
        for path in sorted(base.rglob("*.py")):
            if EXCLUDE_DIR_PARTS & set(path.parts):
                continue
            yield path


def _module_level_defs(tree: ast.Module) -> Set[str]:
    """Top-level function definitions only.

    A metric computed inline inside a method is a different problem (and one
    the orchestrator's single compute_metrics call site already prevents);
    what this gate is looking for is a second REUSABLE implementation, which
    is what a module-level def is."""
    return {
        node.name
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }


def _is_reexport(tree: ast.Module, name: str) -> bool:
    """True if `name` is imported rather than defined here.

    momentum_metrics re-exports xirr/churn_factor from core (H1), and a
    re-export is the OPPOSITE of a duplicate -- it is the same object. The
    gate must not punish the very pattern that fixed the duplication."""
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            for alias in node.names:
                if (alias.asname or alias.name) == name:
                    return True
    return False


def _find_duplicate_definitions() -> Set[Tuple[str, str, str]]:
    duplicates: Set[Tuple[str, str, str]] = set()
    for path in _iter_py_files():
        rel = str(path.relative_to(REPO_ROOT))
        if rel == OWNER:
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (SyntaxError, UnicodeDecodeError):  # pragma: no cover - defensive
            continue
        for name in _module_level_defs(tree) & METRIC_PRIMITIVES:
            if _is_reexport(tree, name):
                continue
            duplicates.add((name, rel, _ticket_for(name, rel)))
    return duplicates


def test_metric_primitives_have_one_implementation():
    """A second Sharpe is a second answer to how a strategy performed.

    The two implementations that existed disagreed by a factor of
    sqrt(252/52) on weekly returns and neither looked wrong in isolation."""
    duplicates = _find_duplicate_definitions()
    assert duplicates == KNOWN_DUPLICATES, (
        "Set of duplicate metric implementations changed.\n"
        f"  found:    {sorted(duplicates)}\n"
        f"  expected: {sorted(KNOWN_DUPLICATES)}\n"
        f"New entries are forbidden -- import from {OWNER} instead of writing a "
        "second implementation. If you DELETED one, remove its KNOWN_DUPLICATES "
        "entry in this file."
    )


def test_the_owner_actually_defines_the_primitives():
    """Guards the gate's premise. If the owning module were split or renamed,
    every rule above would still pass while measuring nothing."""
    tree = ast.parse((REPO_ROOT / OWNER).read_text(encoding="utf-8"))
    defined = _module_level_defs(tree)
    for required in ("sharpe_ratio", "sortino_ratio", "calmar_ratio", "max_drawdown", "xirr"):
        assert required in defined, (
            f"{OWNER} no longer defines {required!r}. The measurement layer moved; "
            "update OWNER before trusting this gate."
        )


def test_cadence_is_configurable_not_hardcoded():
    """The specific defect that caused the duplication.

    A Sharpe that assumes 252 periods per year is wrong for every non-daily
    curve, and 'wrong' here means a weekly-rebalance strategy reporting 2.46
    where the truth is 1.12. One implementation can only serve every channel
    if the cadence is an input."""
    from backtest.core.metrics import infer_periods_per_year, sharpe_ratio

    import inspect

    params = inspect.signature(sharpe_ratio).parameters
    assert "periods_per_year" in params, (
        "core.metrics.sharpe_ratio no longer accepts periods_per_year. Without "
        "it, non-daily curves are mismeasured and a second implementation "
        "becomes necessary again -- which is how the duplication started."
    )
    assert params["periods_per_year"].default is None, (
        "periods_per_year must default to None (meaning 'daily, 252') so that "
        "no existing published number moves."
    )
    assert callable(infer_periods_per_year)
