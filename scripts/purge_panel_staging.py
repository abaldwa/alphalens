#!/usr/bin/env python3
"""
scripts/purge_panel_staging.py

Clears stale rows out of feature_panel_staging.duckdb.

Why this matters (2026-08-12)
-----------------------------
This is the ONLY cache in the feature pipeline that can go stale across runs.
Everything else is either in-memory and per-process (the bulk-OHLCV window
cache in datastore/client.py) or already regenerated. Rebooting the machine
does NOT clear this file — it is on disk.

`stage_batch_panels` keys rows by run_id and, by design, SKIPS any ticker that
already has rows staged under the same run_id ("resume after a crash without
recomputing"). That resume behaviour is correct within one run, but it means
rows staged BEFORE the Fyers-primary OHLCV backfill were computed from the old,
wrong prices — and would be silently reused if a run_id were ever repeated.

The file had grown to ~18.9 GB because completed runs' rows are never dropped
after they are consumed. Staged rows have no value once their backfill run has
finished writing its parquets, so anything not belonging to an in-flight run is
pure waste.

Usage:
    python scripts/purge_panel_staging.py                     # dry run
    python scripts/purge_panel_staging.py --apply             # delete ALL rows
    python scripts/purge_panel_staging.py --apply --keep RUN  # spare one run_id
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import duckdb  # noqa: E402

from config.settings import FEATURE_PANEL_STAGING_DB_PATH as DB  # noqa: E402

TABLE = "feature_panel_staging"


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--apply", action="store_true", help="actually delete (default: dry run)")
    p.add_argument("--keep", action="append", default=[],
                   help="run_id to preserve (repeatable) — use for an in-flight run")
    args = p.parse_args()

    size_gb = DB.stat().st_size / 1e9 if DB.exists() else 0.0
    print(f"database: {DB}  ({size_gb:.1f} GB)\n")

    try:
        conn = duckdb.connect(str(DB), read_only=not args.apply)
    except duckdb.IOException as exc:
        raise SystemExit(
            f"cannot open {DB}: {exc}\n"
            "DuckDB is single-writer and blocks even read_only connections — "
            "another feature_backfill run is holding it. Wait for it to finish."
        )

    rows = conn.execute(
        f"SELECT run_id, COUNT(*) FROM {TABLE} GROUP BY 1 ORDER BY 2 DESC"
    ).fetchall()

    keep = set(args.keep)
    total = kept = 0
    print(f"  {'run_id':<52}{'rows':>14}  action")
    for run_id, n in rows:
        total += n
        action = "KEEP" if run_id in keep else "delete"
        if run_id in keep:
            kept += n
        print(f"  {str(run_id):<52}{n:>14,}  {action}")
    print(f"\n  {'TOTAL':<52}{total:>14,}")
    print(f"  {'to delete':<52}{total - kept:>14,}")

    if not args.apply:
        print("\nDRY RUN — nothing deleted. Re-run with --apply.")
        return

    if keep:
        ph = ",".join("?" for _ in keep)
        conn.execute(f"DELETE FROM {TABLE} WHERE run_id NOT IN ({ph})", list(keep))
    else:
        # Faster than DELETE and reclaims cleanly; the table is recreated (and
        # column-reconciled) by _ensure_staging_table on the next run.
        conn.execute(f"DROP TABLE IF EXISTS {TABLE}")

    conn.execute("CHECKPOINT")
    conn.close()

    # DuckDB does not return freed pages to the filesystem in place; a rewrite
    # is what actually shrinks the file.
    #
    # [BUG FIX 2026-08-12] This used to compact with
    #     CREATE TABLE compact.t AS SELECT * FROM t
    # which silently DROPS the PRIMARY KEY (and every index) — CTAS copies data
    # and column types only. stage_batch_panels' INSERT ... ON CONFLICT
    # (run_id, ticker, date) then fails with "the specified columns as conflict
    # target are not referenced by a UNIQUE/PRIMARY KEY CONSTRAINT or INDEX",
    # which feature_backfill.py catches and turns into a silent fallback to the
    # per-date path. Rebuild the schema properly instead of copying it.
    print("\nCompacting (VACUUM-equivalent rewrite)...")
    tmp = DB.with_suffix(".compact.duckdb")
    if tmp.exists():
        tmp.unlink()

    from features.panel_staging import _STAGED_FEATURE_COLUMNS, _TABLE_NAME

    cols_ddl = ",\n            ".join(f'"{c}" DOUBLE' for c in _STAGED_FEATURE_COLUMNS)
    src = duckdb.connect(str(DB))
    src.execute(f"ATTACH '{tmp}' AS compact")
    src.execute(
        f"""
        CREATE TABLE compact.{_TABLE_NAME} (
            run_id VARCHAR NOT NULL,
            ticker VARCHAR NOT NULL,
            date VARCHAR NOT NULL,
            {cols_ddl},
            PRIMARY KEY (run_id, ticker, date)
        )
        """
    )
    src.execute(f"INSERT INTO compact.{_TABLE_NAME} SELECT * FROM {_TABLE_NAME}")
    src.execute(
        f"CREATE INDEX idx_{_TABLE_NAME}_run_date ON compact.{_TABLE_NAME} (run_id, date)"
    )
    src.execute("DETACH compact")
    src.close()
    tmp.replace(DB)

    # Prove the constraint survived — the whole point of the fix above.
    chk = duckdb.connect(str(DB), read_only=True)
    pk = [r[1] for r in chk.execute(f"PRAGMA table_info('{_TABLE_NAME}')").fetchall() if r[5]]
    chk.close()
    if not pk:
        raise SystemExit("PRIMARY KEY missing after compaction — refusing to report success")
    print(f"  primary key preserved: {pk}")

    print(f"done — {DB} is now {DB.stat().st_size / 1e9:.2f} GB")


if __name__ == "__main__":
    main()
