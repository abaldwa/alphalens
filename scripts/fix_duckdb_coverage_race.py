"""
scripts/fix_duckdb_coverage_race.py

Owner: Platform / QA
Run this once after creating/recreating .venv (it's gitignored, so this
fix does not survive a fresh `python -m venv .venv && pip install -r
requirements/...`) — `python scripts/fix_duckdb_coverage_race.py`.

[2026-07-28 third model-review, item 4] Works around a real duckdb 1.2.0 +
coverage.py race: `pytest --cov=<anything that imports duckdb>` can
intermittently fail collection with
`ModuleNotFoundError: No module named 'duckdb.duckdb.functional'; 'duckdb.duckdb'
is not a package` (or hang on a bad enough loss of the same race), because
duckdb's own `duckdb/__init__.py` does `import duckdb.functional as
functional` BEFORE `from .duckdb import (...)` (the compiled C extension
submodule) — safe on a fast, untraced cold import, but coverage.py's
per-call tracing changes the interleaving enough to sporadically lose
that race on duckdb's first import of a test session.

Reproduced independently of pytest with:
    import coverage; c = coverage.Coverage(); c.start(); import duckdb
— and confirmed that importing duckdb BEFORE coverage.start() avoids it
deterministically (sys.modules caches the completed import, so every
later `import duckdb` anywhere in the process becomes a no-op cache hit
instead of a second, traced, racy cold import).

pytest-cov starts coverage.py measurement inside its own
`pytest_load_initial_conftests` hookimpl specifically so conftest.py files
themselves get measured too — which means by the time ANY conftest.py
(including tests/conftest.py, which also does a defensive `import duckdb`
as its very first import — see that file's comment) is imported, coverage
is already running. A `tryfirst=True` conftest-level
`pytest_load_initial_conftests` hookimpl was tried and did NOT win this
race in practice (pytest-cov begins even earlier than that hookspec).
The only reliable fix found is a `sitecustomize.py` in the venv's
site-packages — Python auto-imports `sitecustomize` at interpreter
startup, before pytest (or coverage) has loaded at all.

This script just (re)writes that file. Idempotent — safe to re-run.
"""

import site
from pathlib import Path


def main() -> None:
    site_packages = Path(site.getsitepackages()[0])
    target = site_packages / "sitecustomize.py"
    target.write_text(
        "# Written by scripts/fix_duckdb_coverage_race.py — see that file's\n"
        "# docstring for why this needs to exist. Pre-imports duckdb before\n"
        "# pytest-cov/coverage.py starts tracing, avoiding a real duckdb 1.2.0\n"
        "# import-ordering race that coverage's tracing overhead exposes.\n"
        "import duckdb  # noqa: F401\n"
    )
    print(f"wrote {target}")


if __name__ == "__main__":
    main()
