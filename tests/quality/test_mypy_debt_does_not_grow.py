"""
tests/quality/test_mypy_debt_does_not_grow.py

A109 — make mypy enforcement match its own declaration.

THE PROBLEM THIS EXISTS FOR
---------------------------
`pyproject.toml` sets `strict = true` and `disallow_any_generics = true`, so
the project DECLARES full strictness. But the only thing that ever ran mypy
was the pre-commit hook, and pre-commit passes it the STAGED files. So
strictness was declared globally and enforced incidentally: a legacy module
accumulated debt silently for as long as nobody touched it, and then dumped
all of it on whoever next happened to stage it -- for errors they did not
write. That is what happened on 2026-08-18, when a two-line change to
`backtest/core/metrics.py` surfaced 69 pre-existing errors and blocked the
commit.

Relaxing the config would have been the wrong answer. Clearing the first 12
files found TWO real bugs the checker had been pointing at the whole time:
the annual-reset call receiving an empty price map (A110), and
`calendar_cagr` returning a COMPLEX number from a function declared
`Optional[float]`. The type checker was right; the code has debt.

WHY A BASELINE RATHER THAN A CLEAN GATE
---------------------------------------
There are 1,514 errors across 236 files. A gate demanding zero would be
switched off within a day. This gate instead says: the debt may not GROW.
Per-file counts are pinned in mypy_baseline.json; a file may improve freely,
but a regression or a brand-new file with errors fails here, immediately,
for the person who introduced it rather than for whoever stages that file
next year.

Same shape as `_UNCONVERTED` in test_registry_is_load_bearing.py: an
allowlist that can only shrink is the honest way to hold a line you cannot
reach in one step.

WHEN THIS FAILS ON YOU
----------------------
Fix the errors you introduced. If you legitimately IMPROVED a file, run
`python -m tests.quality.test_mypy_debt_does_not_grow` to rewrite the
baseline downward and commit it with your change.
"""

from __future__ import annotations

import collections
import json
import subprocess
import sys
from pathlib import Path
from typing import Dict

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
BASELINE_PATH = Path(__file__).parent / "mypy_baseline.json"

# The application packages. tests/ is excluded by pyproject's own override.
PACKAGES = [
    "backtest",
    "strategies",
    "features",
    "config",
    "datastore",
    "ingestion",
    "systems",
]


def _run_mypy() -> Dict[str, int]:
    """Per-file error counts. Runs the same config the pre-commit hook uses."""
    proc = subprocess.run(
        [sys.executable, "-m", "mypy", "--config-file=pyproject.toml", *PACKAGES],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
    )
    counts: collections.Counter = collections.Counter()
    for line in proc.stdout.splitlines():
        if ": error:" in line:
            counts[line.split(":", 1)[0]] += 1
    if not counts and "Success" not in proc.stdout:
        pytest.fail(f"mypy produced no parseable output:\n{proc.stdout[-2000:]}\n{proc.stderr[-2000:]}")
    return dict(counts)


@pytest.mark.slow
def test_mypy_debt_does_not_grow():
    baseline: Dict[str, int] = json.loads(BASELINE_PATH.read_text())
    current = _run_mypy()

    regressions = []
    for path, n in sorted(current.items()):
        was = baseline.get(path)
        if was is None:
            regressions.append(f"  {path}: NEW file with {n} error(s) — it must be clean")
        elif n > was:
            regressions.append(f"  {path}: {was} -> {n} (+{n - was})")

    assert not regressions, (
        "mypy debt grew. Fix the errors you introduced rather than adding them "
        "to the baseline:\n" + "\n".join(regressions) + "\n\n"
        "If a file got WORSE for a reason you cannot avoid, say so explicitly in "
        "the commit message and update the baseline deliberately — but the "
        "baseline is meant to shrink, not to absorb."
    )


@pytest.mark.slow
def test_baseline_has_no_stale_entries():
    """A file that no longer has errors, or no longer exists, must leave the
    baseline. Stale entries are how a shrink-only list stops shrinking."""
    baseline: Dict[str, int] = json.loads(BASELINE_PATH.read_text())
    current = _run_mypy()

    stale = [
        path
        for path, n in sorted(baseline.items())
        if current.get(path, 0) < n
    ]
    assert not stale, (
        "These files are now BETTER than the baseline records. That is good news "
        "— lock it in by regenerating the baseline so the improvement cannot be "
        "silently undone:\n"
        + "\n".join(f"  {p}: {baseline[p]} -> {current.get(p, 0)}" for p in stale)
        + "\n\nRun: python -m tests.quality.test_mypy_debt_does_not_grow"
    )


if __name__ == "__main__":
    # Regenerate the baseline. Deliberately the only way to change it, so that
    # lowering the numbers is a conscious act with a diff attached.
    counts = _run_mypy()
    BASELINE_PATH.write_text(json.dumps(dict(sorted(counts.items())), indent=1, sort_keys=True) + "\n")
    print(f"baseline rewritten: {len(counts)} files, {sum(counts.values())} errors")
