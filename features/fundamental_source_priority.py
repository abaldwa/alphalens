"""
features/fundamental_source_priority.py

Phase: Data Layer / Ingestion (A36)
Owner: Platform / Features
Consumers: scripts/backfill_fundamentals_nse_xbrl.py,
           scripts/backfill_fundamentals_trendlyne.py,
           datastore/api/routers/fundamentals.py

Why this exists
----------------
A36: `fundamentals` has 4 independent writers (kaggle, trendlyne, nse_xbrl,
screener) that each hand-wrote their own `ON CONFLICT ... DO UPDATE`
COALESCE clause, and those clauses disagreed with each other on which
value should win a real conflict (existing-DB-value-wins for
kaggle/trendlyne, new-value-wins for nse_xbrl/screener) — an accident of
which developer wrote which script, not a documented policy. Per explicit
operator decision (2026-07-09): the real source-priority order is

    NSE XBRL (4) > Trendlyne (3) > Screener (2)

NSE XBRL is the regulatory filing itself (see
`datastore/schema/create_normalised.py`'s `_CREATE_FUNDAMENTALS` comment
above `goodwill`/`inventories`/etc., which already established this
precedent for the fields NSE XBRL uniquely covers); Trendlyne and
Screener are both third-party renderings of the same filings (Trendlyne
ranked above Screener per the same operator decision — Trendlyne's
existing-wins COALESCE was the project's original/most-audited
precedence rule). Kaggle (a one-time historical seed load, never actually
invoked by any scheduler/job registry) was removed entirely (A53,
2026-07-10) rather than kept as priority 1 — dead code, not a live source.

This module is the SINGLE source of truth for that ordering and for the
SQL clause every writer uses to enforce it — a plain per-writer COALESCE
direction is exactly the bug A36 found (4 independent hand-written
clauses drifting out of sync), so this is deliberately the *only* place
the ordering or the merge SQL shape is spelled out.
"""

from __future__ import annotations

from typing import Iterable, List

# Higher number wins a real (both-sides-non-NULL) conflict.
# F5 (2026-07-10): "external_csv" is scripts/ingest_external_fundamentals.py's
# generic CSV-source-fusion path — lowest priority, since it's an unaudited
# external file, not a live-verified scraper against a real regulatory/
# vendor source like the other three.
SOURCE_PRIORITY = {
    "nse_xbrl": 4,
    "trendlyne": 3,
    "screener": 2,
    "external_csv": 1,
}


def build_priority_update_clause(columns: Iterable[str]) -> str:
    """
    Build the `col = ..., col2 = ...` fragment for an `ON CONFLICT ... DO
    UPDATE SET` clause that:

    1. Never blanks an existing value with an incoming NULL (a writer that
       doesn't cover a given field must not erase another writer's data
       for it) — same "additive write" contract every writer already had.
    2. On a REAL conflict (both existing and incoming values are non-NULL,
       i.e. two sources disagree), the higher-priority source wins,
       using the incoming `excluded.fundamentals_source_priority` against
       the stored `fundamentals.fundamentals_source_priority` — NOT a
       hardcoded per-writer COALESCE direction, so all 4 writers now
       share one policy instead of 4 independently-drifting ones.
    3. A row with no recorded `fundamentals_source_priority` (written
       before this fix, or by a writer that predates provenance
       tracking) is treated as priority 0 — any covered writer can win
       against it, since "unknown provenance" should never outrank a
       known, ranked source.

    Callers pass their own `excluded.fundamentals_source` /
    `excluded.fundamentals_source_priority` values via the INSERT's own
    VALUES list (see each writer for how those two columns are populated)
    — this function only builds the per-data-column CASE expression.

    Parameters
    ----------
    columns : Iterable[str]
        The data columns this writer can populate (NOT including
        `ticker`/`fiscal_year`/`quarter`, the conflict key, or
        `fundamentals_source`/`fundamentals_source_priority` themselves —
        those two are always overwritten by whichever write actually wins,
        handled separately by the caller).

    Returns
    -------
    str
        Comma-joined `col = CASE ... END` fragments, ready to interpolate
        into an `ON CONFLICT (...) DO UPDATE SET {clause}` clause.
    """
    clauses: List[str] = []
    for col in columns:
        clauses.append(
            f"{col} = CASE "
            f"WHEN excluded.{col} IS NULL THEN fundamentals.{col} "
            f"WHEN fundamentals.{col} IS NULL THEN excluded.{col} "
            f"WHEN excluded.fundamentals_source_priority >= "
            f"COALESCE(fundamentals.fundamentals_source_priority, 0) "
            f"THEN excluded.{col} "
            f"ELSE fundamentals.{col} END"
        )
    # Provenance columns themselves: record whichever source actually won
    # this write's priority comparison (same CASE test, reused so the
    # stored priority always reflects the winning source, not just the
    # most recent writer to run).
    clauses.append(
        "fundamentals_source = CASE "
        "WHEN excluded.fundamentals_source_priority >= "
        "COALESCE(fundamentals.fundamentals_source_priority, 0) "
        "THEN excluded.fundamentals_source ELSE fundamentals.fundamentals_source END"
    )
    clauses.append(
        "fundamentals_source_priority = CASE "
        "WHEN excluded.fundamentals_source_priority >= "
        "COALESCE(fundamentals.fundamentals_source_priority, 0) "
        "THEN excluded.fundamentals_source_priority ELSE fundamentals.fundamentals_source_priority END"
    )
    return ", ".join(clauses)
