"""
tests/unit/test_exception_catalog.py

Phase: Pipeline & Monitoring Remediation, Phase 0.3
Owner: Platform / Scheduler
Consumers: CI, pytest

Verifies ingestion/scheduler/exception_catalog.py's entries stay pinned
to real `except` statements — a catalog entry that silently drifts out
of sync with the code it describes (e.g. after a refactor shifts line
numbers) is worse than no catalog at all, since it would give operators
false confidence about impact/remediation for the wrong code path.
"""

from pathlib import Path

import pytest

from ingestion.scheduler.exception_catalog import all_entries

REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize("entry", all_entries(), ids=lambda e: e.location)
def test_location_points_at_except_line(entry):
    rel_path, line_no_str = entry.location.rsplit(":", 1)
    line_no = int(line_no_str)
    source_path = REPO_ROOT / rel_path
    assert source_path.exists(), f"{entry.location}: file does not exist"

    lines = source_path.read_text().splitlines()
    assert 0 < line_no <= len(lines), (
        f"{entry.location}: line {line_no} out of range for {rel_path} "
        f"({len(lines)} lines)"
    )
    target_line = lines[line_no - 1]
    # [2026-08-18] Was `"except" in target_line`, a substring test that any
    # line CONTAINING the word satisfied. The
    # daily_pipeline.py:2781 entry had already drifted onto the comment
    # "# the stale job, caught by the bare except below)." and passed for
    # months on the strength of that comment's own wording -- a drift
    # detector defeated by prose describing the thing it was meant to find.
    #
    # An `except` STATEMENT starts the line (modulo indentation) and ends in
    # a colon, so anchoring to that is what actually pins the entry.
    stripped = target_line.strip()
    assert stripped.startswith("except") and stripped.endswith(":"), (
        f"{entry.location}: expected an 'except' statement, found "
        f"{target_line!r} — catalog entry has drifted, update its "
        "location or the underlying code reference"
    )


def test_all_entries_have_required_fields():
    for entry in all_entries():
        assert entry.step_name
        assert entry.caught
        assert entry.impact
        assert entry.remediation
        assert entry.severity in {"info", "warning", "critical"}


def test_no_duplicate_locations():
    locations = [entry.location for entry in all_entries()]
    assert len(locations) == len(set(locations))
