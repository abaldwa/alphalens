"""
tests/quality/test_live_eligible_invariant.py

Phase: UnifiedGeneratorRefactorPlan.md, Phase G (G2)
Owner: project-wide quality gate
Consumers: CI / `pytest tests/quality/`

WHY THIS GATE EXISTS
--------------------
`backtest_runs.live_eligible` (datastore/schema/create_backtest.py) is the
single flag that would let a backtest's numbers be treated as a green light
for real capital. backtest/core/run_store.py's own docstring states the
intended invariant: "live_eligible is never set by save_run_result() or any
function in this module -- it's DEFAULT FALSE at the schema level and only a
separate, explicitly human-invoked function (not built yet -- Phase 5/6) may
ever flip it."

That was a stated intention, not an enforced one -- nothing checked it. A
future write path (a new persistence helper, a bulk-update script, a
well-meaning "mark this run live" endpoint) could flip the flag without
going through deliberate human review, and nothing would fail.

WHAT IS ASSERTED
----------------
No source file outside GATE_7_MODULES sets live_eligible to a truthy
literal, in either Python (`.live_eligible = True` / `live_eligible=True`)
or embedded SQL (`SET live_eligible = TRUE` / an INSERT's VALUES list
supplying TRUE/1 for that column). Verified today: backtest/iterative_
retrain.py's INSERT hardcodes `FALSE` for live_eligible, and backtest/core/
run_store.py's _COLUMNS only ever reads/lists the column -- never assigns
into it. GATE_7_MODULES is empty because the human-gated Gate-7 flow this
plan alludes to has not been built yet; the day it is, its module goes in
that allowlist and nowhere else may set the flag.

Static analysis only: regex-based scanning of file text, not AST -- the
flag is set through raw SQL strings as often as through Python attribute
assignment, and a single regex catches both without needing two different
detectors.

PIT Assumptions
---------------
None -- pure static analysis, no data access.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Iterator, List, Tuple

REPO_ROOT = Path(__file__).resolve().parents[2]

EXCLUDE_DIR_PARTS = {".venv", "__pycache__", ".git", "node_modules", "catboost_info"}

SCAN_DIRS = ["backtest", "systems", "scripts", "features", "datastore", "ingestion", "strategies"]

# Modules allowed to flip live_eligible to true -- the human-gated Gate-7
# flow. Empty until that flow is built (UnifiedGeneratorRefactorPlan.md G2);
# a PR that adds it must add its module path here, deliberately, in the same
# change -- not as a side effect of writing generic persistence code.
GATE_7_MODULES: frozenset[str] = frozenset()

# Matches a truthy assignment to live_eligible in Python attribute/keyword
# form, or in embedded SQL (SET/column list assigning TRUE or 1). Deliberately
# does NOT match `live_eligible = False`/`FALSE`/`0`, `live_eligible: bool`
# (type annotations), `live_eligible BOOLEAN ... DEFAULT FALSE` (schema DDL),
# or a bare column-name mention in a SELECT/tuple list.
_TRUTHY_ASSIGNMENT = re.compile(
    r"live_eligible\s*[:=]\s*(True|TRUE|1)\b(?!\s*[,)]?\s*#.*DEFAULT)"
)
_SQL_SET_TRUE = re.compile(r"SET\s+live_eligible\s*=\s*(True|TRUE|1)\b", re.IGNORECASE)


def _iter_py_files() -> Iterator[Path]:
    for d in SCAN_DIRS:
        base = REPO_ROOT / d
        if not base.exists():
            continue
        for path in sorted(base.rglob("*.py")):
            if EXCLUDE_DIR_PARTS & set(path.parts):
                continue
            yield path


def _find_offenders() -> List[Tuple[str, int, str]]:
    offenders: List[Tuple[str, int, str]] = []
    for path in _iter_py_files():
        rel = str(path.relative_to(REPO_ROOT))
        if rel in GATE_7_MODULES:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):  # pragma: no cover - defensive
            continue
        for lineno, line in enumerate(text.splitlines(), start=1):
            stripped = line.strip()
            if stripped.startswith("#"):
                continue
            # Schema DDL declaring the column (DEFAULT FALSE) is not a write.
            if "DEFAULT FALSE" in line or "BOOLEAN" in line:
                continue
            # A bare mention in a column-name tuple/list (no `=`/`:` binding
            # a truthy value right after "live_eligible") is not a write.
            if _TRUTHY_ASSIGNMENT.search(line) or _SQL_SET_TRUE.search(line):
                offenders.append((rel, lineno, stripped))
    return offenders


def test_only_gate_7_may_set_live_eligible_true():
    """No code path outside the (currently nonexistent) human-gated Gate-7
    flow may set live_eligible to true -- see module docstring."""
    offenders = _find_offenders()
    assert offenders == [], (
        "Found code outside GATE_7_MODULES setting live_eligible to a truthy "
        "value -- this must go through the human-gated Gate-7 flow, not a "
        "generic persistence/write path:\n"
        + "\n".join(f"  {rel}:{lineno}: {line}" for rel, lineno, line in offenders)
    )


def test_schema_default_is_false():
    """Guards the gate's premise: the column must default to FALSE at the
    schema level, so an unset/uninitialized row is never live-eligible by
    accident."""
    schema_path = REPO_ROOT / "datastore" / "schema" / "create_backtest.py"
    text = schema_path.read_text(encoding="utf-8")
    assert "live_eligible BOOLEAN NOT NULL DEFAULT FALSE" in text, (
        "datastore/schema/create_backtest.py no longer declares live_eligible "
        "as NOT NULL DEFAULT FALSE -- update this gate's premise before trusting it."
    )
