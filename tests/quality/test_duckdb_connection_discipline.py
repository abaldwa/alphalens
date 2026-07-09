"""
tests/quality/test_duckdb_connection_discipline.py

Phase: cross-cutting (all phases)
Specs: SPEC-SCHED-013 (DuckDB connection-lifecycle discipline)
Owner: Platform / DataStore
Consumers: CI / `pytest tests/quality/`

Static "fitness function" (not a behavioural test): AST-walks
`datastore/api/routers/*.py` for `get_duckdb_connection(...)` call sites
and fails if any lacks explicit `persist=` AND `read_only=` keyword
arguments.

Why: `get_duckdb_connection` in datastore/api/db.py defaults to
`persist=True, read_only=False` — a connection cached and held open for
the life of the API process, permanently locking out any other process
(the ingestion scheduler's write steps, a manual backfill run) from ever
opening a read-write connection to the same DuckDB file while the API is
up. This has already caused two documented production incidents (see
db.py's module docstring and BuildLog.md's "Fix check_ta_alerts
cross-process DuckDB lock race" entry, commit 8147579). Router call
sites must always pass both kwargs explicitly — `persist=False` so the
lock releases after each request, and an explicit `read_only=` so the
mode is a deliberate choice, not whatever the default happens to be.

PIT Assumptions
----------------
None — pure static analysis, no data access.
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Iterator

REPO_ROOT = Path(__file__).resolve().parents[2]

# Only router call sites are in scope for this check — see AF-1 in
# FutureDevelopment.md. Other callers (ingestion/scheduler, systems/,
# scripts/) are long-lived batch/scheduler processes with different
# lifecycle needs and are not covered here.
SCAN_DIRS = ["datastore/api/routers"]

EXCLUDE_DIR_PARTS = {".venv", "__pycache__", ".git", "node_modules"}


def _iter_py_files(dirs: list[str]) -> Iterator[Path]:
    for d in dirs:
        base = REPO_ROOT / d
        if not base.exists():
            continue
        for path in base.rglob("*.py"):
            if EXCLUDE_DIR_PARTS & set(path.parts):
                continue
            yield path


def _rel(path: Path) -> str:
    return str(path.relative_to(REPO_ROOT))


def _is_get_duckdb_connection_call(node: ast.AST) -> bool:
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    if isinstance(func, ast.Name):
        return func.id == "get_duckdb_connection"
    if isinstance(func, ast.Attribute):
        return func.attr == "get_duckdb_connection"
    return False


def test_router_duckdb_calls_pass_explicit_persist_and_read_only():
    violations = []
    for path in _iter_py_files(SCAN_DIRS):
        rel = _rel(path)
        try:
            tree = ast.parse(path.read_text())
        except (SyntaxError, UnicodeDecodeError):
            continue
        for node in ast.walk(tree):
            if not _is_get_duckdb_connection_call(node):
                continue
            kwarg_names = {kw.arg for kw in node.keywords if kw.arg is not None}
            missing = {"persist", "read_only"} - kwarg_names
            if missing:
                violations.append(
                    f"{rel}:{node.lineno}: get_duckdb_connection(...) missing "
                    f"explicit kwarg(s): {sorted(missing)}"
                )
    assert not violations, (
        "Found get_duckdb_connection(...) call site(s) in datastore/api/routers "
        "relying on the default persist=True, read_only=False — this caches a "
        "connection and holds the DuckDB file locked open for the life of the "
        "API process, which has already caused two production incidents (see "
        "db.py's module docstring and BuildLog.md's \"Fix check_ta_alerts "
        "cross-process DuckDB lock race\" entry). Pass persist=False explicitly "
        "everywhere, and read_only=True for pure-read endpoints / read_only=False "
        "only where the endpoint genuinely writes:\n" + "\n".join(violations)
    )
