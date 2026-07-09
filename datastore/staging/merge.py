"""
datastore/staging/merge.py

Phase: A25 (Write-Audit-Publish Architecture) — full rollout
Owner: Platform / DataStore
Consumers: scripts/backfill_fundamentals_trendlyne.py,
    scripts/backfill_fundamentals_nse_xbrl.py,
    ingestion/scrapers/amfi_holdings.py, ingestion/scrapers/corporate_actions.py

Why this exists
----------------
The A25 pilot (fno_data, ohlcv_adjusted) staged a batch that fully
replaced or additively unioned with production content — no row-level
merge was needed because those tables are written per-date (a whole
date's rows are either all-new or a clean replace of that date).

The remaining rollout sources write with real per-row UPSERT semantics
that datastore/staging/publish.py's whole-table CREATE OR REPLACE cannot
reproduce by simply unioning DataFrames — each source's existing SQL
encodes a specific conflict-resolution policy:

- trendlyne (scripts/backfill_fundamentals_trendlyne.py): existing value
  wins on conflict (COALESCE(fundamentals.col, excluded.col)) for most
  columns — never overwrites an already-populated value — EXCEPT
  quality_flag/quality_flag_reason, where the new value always wins.
- nse_xbrl (scripts/backfill_fundamentals_nse_xbrl.py): new value wins on
  conflict (COALESCE(excluded.col, fundamentals.col)) for every column.
- amfi_holdings.py::sync_duckdb_table: whole-month replace (DELETE WHERE
  month = ? then INSERT) — a partition-level replace, not a row-level
  merge.
- corporate_actions.py::upsert_corporate_actions: INSERT ... ON CONFLICT
  DO NOTHING — existing rows are never touched; only genuinely new
  (ticker, ex_date, action_type) combinations are added.

These three merge policies are implemented here in pandas so that a
staged batch's `stage_dataframe`/`publish_table` swap reproduces the same
conflict-resolution behavior as the original per-row SQL, not a naive
last-write-wins replace.
"""

from __future__ import annotations

from typing import List, Optional

import pandas as pd


def coalesce_merge(
    existing_df: pd.DataFrame,
    new_df: pd.DataFrame,
    key_cols: List[str],
    new_wins: bool,
    force_new_wins_cols: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Row-level COALESCE-style upsert merge, matching
    `... ON CONFLICT (key_cols) DO UPDATE SET col = COALESCE(a, b)` SQL.

    For keys present in both `existing_df` and `new_df`, per-column value
    is taken from whichever side "wins" (new_df if `new_wins`, else
    existing_df) when that side's value is non-null, falling back to the
    other side otherwise — same semantics as SQL COALESCE(winner, loser).
    Keys present in only one side pass through unchanged.

    `force_new_wins_cols`: columns where new_df always wins regardless of
    the overall `new_wins` policy (e.g. trendlyne's quality_flag/
    quality_flag_reason, which take the freshly-computed value even
    though every other column prefers the existing one).
    """
    if existing_df.empty:
        return new_df.reset_index(drop=True)
    if new_df.empty:
        return existing_df.reset_index(drop=True)

    existing_indexed = existing_df.set_index(key_cols)
    new_indexed = new_df.set_index(key_cols)

    if new_wins:
        merged = new_indexed.combine_first(existing_indexed)
    else:
        merged = existing_indexed.combine_first(new_indexed)

    if force_new_wins_cols:
        overlap_keys = new_indexed.index.intersection(merged.index)
        for col in force_new_wins_cols:
            if col in new_indexed.columns:
                new_vals = new_indexed.loc[overlap_keys, col]
                non_null = new_vals[new_vals.notna()]
                merged.loc[non_null.index, col] = non_null

    return merged.reset_index()


def partition_replace_merge(
    existing_df: pd.DataFrame,
    new_df: pd.DataFrame,
    partition_col: str,
    partition_values: List,
) -> pd.DataFrame:
    """
    Whole-partition replace merge, matching
    `DELETE FROM t WHERE partition_col IN (...)` + `INSERT`: every row in
    `existing_df` whose `partition_col` value is in `partition_values` is
    dropped, then `new_df` is appended. Rows for any other partition
    value are left untouched. `partition_values` must be passed
    explicitly (not inferred from new_df) so an empty `new_df` for a
    partition still correctly clears it — mirroring
    amfi_holdings.py::sync_duckdb_table's "DELETE first, INSERT only if
    non-empty" behavior.
    """
    if existing_df.empty:
        return new_df.reset_index(drop=True)
    partition_series = pd.Series(partition_values, dtype=existing_df[partition_col].dtype)
    remaining = existing_df[~existing_df[partition_col].isin(partition_series)]
    return pd.concat([remaining, new_df], ignore_index=True)


def insert_ignore_merge(
    existing_df: pd.DataFrame,
    new_df: pd.DataFrame,
    key_cols: List[str],
) -> pd.DataFrame:
    """
    Insert-or-ignore merge, matching `INSERT ... ON CONFLICT (key_cols)
    DO NOTHING`: existing rows are never modified; new_df rows whose key
    already exists in existing_df are dropped (existing wins untouched),
    only genuinely new keys are appended.
    """
    if existing_df.empty:
        return new_df.drop_duplicates(subset=key_cols).reset_index(drop=True)
    if new_df.empty:
        return existing_df.reset_index(drop=True)

    existing_keys = existing_df.set_index(key_cols).index
    new_indexed = new_df.set_index(key_cols)
    genuinely_new = new_indexed.loc[~new_indexed.index.isin(existing_keys)]
    genuinely_new = genuinely_new[~genuinely_new.index.duplicated(keep="first")]

    return pd.concat([existing_df, genuinely_new.reset_index()], ignore_index=True)
