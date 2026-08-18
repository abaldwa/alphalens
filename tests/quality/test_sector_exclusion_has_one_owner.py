"""
tests/quality/test_sector_exclusion_has_one_owner.py

Phase: Signal-generator consolidation (UnifiedGeneratorRefactorPlan.md, E1)
Owner: project-wide quality gate
Consumers: CI / `pytest tests/quality/`

WHY THIS GATE EXISTS
--------------------
"Does this preset exclude this sector?" must be answered in exactly one
place. It was answered in four -- `matches_screener_preset`, two branches in
the fundamentals router, and an inline comprehension in `/scores` -- and
they drifted exactly as duplicated decisions do: `/scores` skipped the
exclusion entirely and shipped methodologically-invalid numbers to four
frontend pages (a regulated lender's reported ROE is not comparable to an
industrial's, which is the entire reason the exclusion exists). That was
found in the 2026-07-28 model review, not by a test.

E1 collapsed them onto `features.fundamental_composites.is_sector_excluded`.
This gate keeps them collapsed.

PIT Assumptions
---------------
None -- pure static analysis, no data access.
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Iterator, List, Set

REPO_ROOT = Path(__file__).resolve().parents[2]

EXCLUDE_DIR_PARTS = {".venv", "__pycache__", ".git", "node_modules", "catboost_info"}

SCAN_DIRS: List[str] = ["backtest", "systems", "scripts", "features", "datastore", "strategies"]

# The dict itself, and the one function allowed to read it.
EXCLUSION_TABLE = "PRESET_EXCLUDED_SECTORS"

# Modules permitted to READ the table. The canonical module owns the rule;
# the migration DECLARES the strategies and must read it to record what each
# one excludes -- that is declaration, not a second decision.
OWNERS = frozenset({
    "features/fundamental_composites.py",
    "strategies/migrations/fundamental.py",
})


def _iter_py_files() -> Iterator[Path]:
    for d in SCAN_DIRS:
        base = REPO_ROOT / d
        if not base.exists():
            continue
        for path in sorted(base.rglob("*.py")):
            if EXCLUDE_DIR_PARTS & set(path.parts):
                continue
            yield path


def _reads_the_table(tree: ast.Module) -> bool:
    """A real read of PRESET_EXCLUDED_SECTORS -- subscript, .get(), or
    membership test. A bare mention inside a string or comment is not a
    read and must not be flagged, or the gate would punish documentation."""
    for node in ast.walk(tree):
        if isinstance(node, ast.Subscript) and _is_table(node.value):
            return True
        if isinstance(node, ast.Attribute) and _is_table(node.value):
            return True
        if isinstance(node, ast.Compare):
            for comparator in node.comparators:
                if _is_table(comparator):
                    return True
    return False


def _is_table(node: ast.AST) -> bool:
    return isinstance(node, ast.Name) and node.id == EXCLUSION_TABLE


def test_only_the_owning_modules_read_the_exclusion_table():
    """Every other module must ask `is_sector_excluded(preset, sector)`.

    Reading the table directly is how a caller ends up re-deciding the rule
    -- including deciding, as `/scores` did, not to apply it at all."""
    offenders: Set[str] = set()
    for path in _iter_py_files():
        rel = str(path.relative_to(REPO_ROOT))
        if rel in OWNERS:
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (SyntaxError, UnicodeDecodeError):  # pragma: no cover - defensive
            continue
        if _reads_the_table(tree):
            offenders.add(rel)

    assert not offenders, (
        "These modules read PRESET_EXCLUDED_SECTORS directly instead of "
        "calling features.fundamental_composites.is_sector_excluded:\n  "
        + "\n  ".join(sorted(offenders))
        + "\nOne module deciding the rule differently from the others is how "
        "/scores shipped numbers that excluded nothing."
    )


def test_the_helper_exists_and_is_exported():
    """Guards the gate's premise: if the helper were renamed, the rule above
    would be demanding callers use something that no longer exists."""
    from features import fundamental_composites

    assert hasattr(fundamental_composites, "is_sector_excluded")
    assert "is_sector_excluded" in fundamental_composites.__all__
