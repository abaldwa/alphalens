"""
Stale-results cutoff — 2026-09-06 remediation session.

WHY THIS EXISTS: every framework_backtest_runs row written before this
session's fixes landed (B1 portfolio-weighting no-op, B3 R07
crash_reduce_sizing, B6 R11/R12/R13 crash guard, the shared Nifty-500/
-12.5% crash detector, E1/E2 top_n grid shrink, E5 costs+tax) is not
comparable to anything produced after them — see
docs/MASTER_ISSUES_CATALOG_2026_09_06.md. There is no existing
`archived`/`is_stale` column on framework_backtest_runs (see
results/db_schema.py), and the user explicitly does NOT want old rows
deleted — they remain useful for a documented before/after comparison,
just excluded from the default "current results" view.

WHY A TIMESTAMP CUTOFF, NOT A SCHEMA MIGRATION OR A source_commit
ALLOWLIST: the fix commits for this session had not landed yet when this
cutoff was chosen (working tree was dirty — see `git status` at the time
of writing), so there is no fixed commit SHA to filter on that would
already exist. `run_executed_at` (already a required, indexed column on
every row — see db_schema.py) needs no migration and is exactly what the
report layer already sorts by. Every run before STALE_RESULTS_CUTOFF_AT
predates ALL of this session's fixes; every run at or after it was
produced by the campaign queues built in
scripts/build_pass_queues_2026_09_06.py, which only exist because the
fixes already landed in the working tree by that point. This mirrors
config/backtest_exclusions.py's philosophy for ticker exclusion —
explicit, documented, reversible (flip a query param / bump the
constant), never destructive.

HOW IT'S APPLIED: datastore/api/routers/framework_backtest_runs.py's
`GET /runs` list endpoint filters `run_executed_at >= STALE_RESULTS_CUTOFF_AT`
by default; pass `include_stale=true` to see everything (e.g. for a
deliberate before/after comparison). Per-run detail endpoints
(yearly-returns, rolling-return, trade-quality, benchmark) are NOT
filtered here — once a reader has a specific run_id (necessarily surfaced
via the list endpoint, or already known), inspecting it in detail is
never "silent contamination."

IF THIS NEEDS REVISITING: if a run is later found to have executed under
the OLD code despite a `run_executed_at` timestamp after the cutoff (e.g.
a long-running process that started before the fix but flushed rows
after it), source_commit / source_commit_dirty on that row is the
authoritative fallback — see common/git_provenance.py's docstring for
why config+commit identity are tracked separately.
"""

from datetime import datetime, timezone

# Chosen as the moment the 2026-09-06 remediation queue-build script
# (scripts/build_pass_queues_2026_09_06.py) was first run — every row
# strictly before this reflects at least one of the stale conditions
# above. Update this constant (with a comment explaining why) if a later
# remediation session invalidates results again; never delete or rewrite
# old rows to "fix" their timestamps.
STALE_RESULTS_CUTOFF_AT = datetime(2026, 9, 6, 0, 0, 0, tzinfo=timezone.utc)
