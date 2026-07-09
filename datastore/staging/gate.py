"""
datastore/staging/gate.py

Phase: A25 (Write-Audit-Publish Architecture)
Specs: SPEC-QUALITY-002 (flag, don't silently drop/write)
Owner: Platform / DataStore
Consumers: scripts/insert_fno_files.py, ingestion/backfill_runner.py (pilot),
    datastore/staging/publish.py

Why this exists
----------------
Scrapers write straight into production DuckDB tables today — a bad parse,
a NaN, or a stale/duplicate source response becomes production data
instantly, with no checkpoint between "HTTP response landed" and "this is
trusted enough to train on" (FeatureBacklog.md A25). This module is the
middle stage: raw batches land here first, get validated, and only rows
that pass move on to datastore/staging/publish.py. Rejected rows are never
silently dropped — they land in `staging.rejected_rows` with a reason,
per this project's established "flag don't silently drop" discipline (see
features/fundamental_quality_gate.py for the same convention applied to
fundamentals).

A20 (Data Integrity Checker, not yet built) is expected to plug its own
checks in here as additional `validators` — this module is deliberately a
thin, generic list-of-validators runner rather than anything
bhavcopy/fno-specific, so A20 doesn't need to build its own separate gate.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Callable, List, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

# A validator takes the current candidate DataFrame and returns
# (passed_df, rejected_df). rejected_df must have the SAME columns as
# passed_df plus a "reason" column (str) explaining the rejection.
Validator = Callable[[pd.DataFrame], Tuple[pd.DataFrame, pd.DataFrame]]

_REJECTED_ROWS_TABLE = "staging.rejected_rows"

_CREATE_REJECTED_ROWS = f"""
    CREATE TABLE IF NOT EXISTS {_REJECTED_ROWS_TABLE} (
        source_table VARCHAR NOT NULL,
        reason VARCHAR NOT NULL,
        row_json VARCHAR NOT NULL,
        staged_at TIMESTAMP NOT NULL DEFAULT current_timestamp
    )
"""


@dataclass
class StageResult:
    table_name: str
    staged_rows: int
    rejected_rows: int
    reasons: List[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        """True if at least one row was staged. An all-rejected batch is
        NOT staged/published — see stage_dataframe's docstring."""
        return self.staged_rows > 0


def _ensure_staging_schema(conn) -> None:
    conn.execute("CREATE SCHEMA IF NOT EXISTS staging")
    conn.execute(_CREATE_REJECTED_ROWS)


def _record_rejected(conn, table_name: str, rejected_df: pd.DataFrame) -> None:
    if rejected_df.empty:
        return
    reason_col = rejected_df["reason"]
    row_cols = [c for c in rejected_df.columns if c != "reason"]
    rows = [
        (
            table_name,
            str(reason),
            json.dumps(row.to_dict(), default=str),
        )
        for reason, (_, row) in zip(reason_col, rejected_df[row_cols].iterrows())
    ]
    conn.executemany(
        f"INSERT INTO {_REJECTED_ROWS_TABLE} (source_table, reason, row_json) VALUES (?, ?, ?)",
        rows,
    )


def stage_dataframe(
    conn,
    table_name: str,
    df: pd.DataFrame,
    validators: List[Validator],
) -> StageResult:
    """
    Land `df` into `staging.<table_name>` after running it through
    `validators` in order. Each validator may reject rows; rejected rows
    are recorded in staging.rejected_rows (visible, never silently
    dropped) and excluded from what gets staged. Rows that pass every
    validator are written to `staging.<table_name>` via CREATE OR REPLACE
    TABLE, ready for datastore/staging/publish.py::publish_table to
    atomically promote.

    An empty (0-row) `df`, or a batch where every row is rejected,
    produces StageResult.ok == False — callers must not call
    publish_table in that case (nothing to publish, or nothing trustworthy
    to publish).
    """
    _ensure_staging_schema(conn)

    candidate = df
    all_reasons: List[str] = []
    for validator in validators:
        passed, rejected = validator(candidate)
        if not rejected.empty:
            _record_rejected(conn, table_name, rejected)
            all_reasons.extend(rejected["reason"].tolist())
            logger.warning(
                "staging gate: %s rejected %d/%d rows for %s",
                validator.__name__ if hasattr(validator, "__name__") else validator,
                len(rejected),
                len(candidate),
                table_name,
            )
        candidate = passed

    conn.register("_stage_batch", candidate)
    try:
        conn.execute(f"CREATE OR REPLACE TABLE staging.{table_name} AS SELECT * FROM _stage_batch")
    finally:
        conn.unregister("_stage_batch")

    return StageResult(
        table_name=table_name,
        staged_rows=len(candidate),
        rejected_rows=len(df) - len(candidate),
        reasons=all_reasons,
    )


def stage_via_sql(
    conn,
    table_name: str,
    new_batch_df: pd.DataFrame,
    merge_select_sql: str,
    merge_params: list,
    validators: List[Validator],
) -> StageResult:
    """
    Large-table variant of stage_dataframe: validates only the incremental
    `new_batch_df` (small — one date/ticker/month's worth of new rows) in
    Python/pandas, then performs the merge with the FULL production table
    entirely inside DuckDB via `merge_select_sql` — never pulling the
    existing production table into a pandas DataFrame.

    Why this exists
    ----------------
    stage_dataframe requires the caller to already have the full merged
    table as a pandas DataFrame. For a 100M+ row table (e.g. fno_data),
    round-tripping the whole production table through
    `conn.execute(...).df()` + `pd.concat` multiplies its memory footprint
    several times over (pandas object overhead, an intermediate copy for
    the concat, plus the original) — confirmed live: staging fno_data this
    way pushed the process to 8GB+ RSS and into swap. DuckDB's own
    columnar engine can perform the identical UNION ALL / filter directly
    against the on-disk table without ever materializing it in Python.

    Parameters
    ----------
    conn : duckdb.DuckDBPyConnection
    table_name : str
    new_batch_df : pd.DataFrame
        Only the NEW rows this batch is contributing — validated against
        `validators` the same way stage_dataframe would. Existing
        production rows are assumed already-validated (they passed this
        same gate whenever they were originally staged) and are not
        re-validated on every publish.
    merge_select_sql : str
        A SELECT statement (may reference the production table by name
        and a registered "_stage_new_batch" view for the validated new
        rows) producing the full merged table content, e.g.:
        "SELECT * FROM fno_data WHERE trade_date NOT IN (?, ?) "
        "UNION ALL SELECT * FROM _stage_new_batch"
    merge_params : list
        Positional parameters for `merge_select_sql`'s `?` placeholders.
    validators : List[Validator]
        Applied to `new_batch_df` only, same semantics as stage_dataframe.

    Returns
    -------
    StageResult
        `staged_rows`/`rejected_rows` describe the new-batch validation
        only (not the merged table's total row count — call
        `publish_table`'s return value for that).
    """
    _ensure_staging_schema(conn)

    candidate = new_batch_df
    all_reasons: List[str] = []
    for validator in validators:
        passed, rejected = validator(candidate)
        if not rejected.empty:
            _record_rejected(conn, table_name, rejected)
            all_reasons.extend(rejected["reason"].tolist())
            logger.warning(
                "staging gate: %s rejected %d/%d rows for %s",
                validator.__name__ if hasattr(validator, "__name__") else validator,
                len(rejected),
                len(candidate),
                table_name,
            )
        candidate = passed

    if candidate.empty and not new_batch_df.empty:
        # Every new row was rejected — nothing new to merge in, but the
        # existing production table content is still valid; caller
        # decides whether "no new rows, but republish unchanged existing
        # content" is worth doing (usually not — see call sites).
        return StageResult(
            table_name=table_name, staged_rows=0, rejected_rows=len(new_batch_df), reasons=all_reasons,
        )

    conn.register("_stage_new_batch", candidate)
    try:
        conn.execute(
            f"CREATE OR REPLACE TABLE staging.{table_name} AS {merge_select_sql}",
            merge_params,
        )
    finally:
        conn.unregister("_stage_new_batch")

    return StageResult(
        table_name=table_name,
        staged_rows=len(candidate),
        rejected_rows=len(new_batch_df) - len(candidate),
        reasons=all_reasons,
    )


def drop_staging_table(conn, table_name: str) -> None:
    """Staging tables are transient — drop after a successful publish so
    steady-state disk cost stays ~zero (A25 storage-budget design)."""
    conn.execute(f"DROP TABLE IF EXISTS staging.{table_name}")


def null_check_validator(required_columns: List[str]) -> Validator:
    """
    Build a validator rejecting any row with a null/NaN in one of
    `required_columns`. Generic stand-in for A20's fuller null/NaN sweep —
    see this module's docstring.
    """

    def _validate(candidate: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        if candidate.empty:
            return candidate, candidate.assign(reason=pd.Series(dtype=str))
        present_cols = [c for c in required_columns if c in candidate.columns]
        bad_mask = candidate[present_cols].isnull().any(axis=1) if present_cols else pd.Series(
            False, index=candidate.index
        )
        rejected = candidate.loc[bad_mask].copy()
        if not rejected.empty:
            rejected["reason"] = rejected[present_cols].apply(
                lambda row: "null/NaN in: " + ", ".join(c for c in present_cols if pd.isnull(row[c])),
                axis=1,
            )
        passed = candidate.loc[~bad_mask]
        return passed, rejected

    _validate.__name__ = f"null_check_validator({required_columns})"
    return _validate
