"""
datastore/staging/snapshot.py

Phase: A25 (Write-Audit-Publish Architecture)
Specs: SPEC-SYS-005 (Storage Budgets)
Owner: Platform / DataStore
Consumers: ingestion/scheduler/daily_pipeline.py (step_publish_and_snapshot),
    scripts/restore_snapshot.py

Why this exists
----------------
Atomic publish (datastore/staging/publish.py) means a bad batch can still
replace good production data instantly if it slipped past the staging
gate. N=7 daily rollback snapshots (FeatureBacklog.md A25) give a way
back. To fit the project's 15GB storage budget, snapshots are
incremental: a table's parquet export is only re-written when its content
actually changed since the prior snapshot; unchanged tables are
hard-linked to the prior day's file (near-zero marginal cost) instead of
re-copied. A hard link is safe to prune independently — deleting an older
snapshot directory only drops that directory's own link to the underlying
inode, the newer snapshot's link (and the file's content) is unaffected.
"""

from __future__ import annotations

import hashlib
import logging
import os
import shutil
from datetime import date as date_type
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger(__name__)

_HASH_CHUNK_SIZE = 1024 * 1024


def _sha256_of_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(_HASH_CHUNK_SIZE), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _find_previous_snapshot_dir(snapshot_dir: Path, before: str) -> Optional[Path]:
    candidates = sorted(
        (d for d in snapshot_dir.iterdir() if d.is_dir() and d.name < before),
        reverse=True,
    )
    return candidates[0] if candidates else None


def take_snapshot(conn, tables: List[str], snapshot_dir: Path, snapshot_date: Optional[str] = None) -> Path:
    """
    Export each of `tables` from `conn` to
    snapshot_dir/<snapshot_date>/<table>.parquet. If a table's content is
    byte-identical to its export in the most recent prior snapshot
    directory, the prior file is hard-linked instead of re-exported (A25's
    incremental-snapshot storage-budget design).

    Returns the snapshot directory created.
    """
    snapshot_date = snapshot_date or date_type.today().isoformat()
    out_dir = snapshot_dir / snapshot_date
    out_dir.mkdir(parents=True, exist_ok=True)
    prev_dir = _find_previous_snapshot_dir(snapshot_dir, before=snapshot_date)

    for table_name in tables:
        final_path = out_dir / f"{table_name}.parquet"
        tmp_path = out_dir / f"{table_name}.parquet.tmp"
        conn.execute(f"COPY {table_name} TO '{tmp_path}' (FORMAT PARQUET)")

        prev_path = prev_dir / f"{table_name}.parquet" if prev_dir else None
        if prev_path and prev_path.exists() and _sha256_of_file(tmp_path) == _sha256_of_file(prev_path):
            tmp_path.unlink()
            os.link(prev_path, final_path)
            logger.info("take_snapshot: %s unchanged since %s, hard-linked", table_name, prev_dir.name)
        else:
            tmp_path.rename(final_path)
            logger.info("take_snapshot: %s exported fresh to %s", table_name, final_path)

    return out_dir


def prune_snapshots(snapshot_dir: Path, keep_n: int) -> List[Path]:
    """
    Delete all but the `keep_n` most recent snapshot directories under
    snapshot_dir (sorted by directory name, which is the ISO snapshot
    date). Returns the list of directories removed.
    """
    if not snapshot_dir.exists():
        return []
    all_dirs = sorted((d for d in snapshot_dir.iterdir() if d.is_dir()), reverse=True)
    to_remove = all_dirs[keep_n:]
    for d in to_remove:
        shutil.rmtree(d)
        logger.info("prune_snapshots: removed %s", d)
    return to_remove


def list_snapshot_dates(snapshot_dir: Path) -> List[str]:
    if not snapshot_dir.exists():
        return []
    return sorted(d.name for d in snapshot_dir.iterdir() if d.is_dir())


def restore_snapshot(
    conn,
    snapshot_dir: Path,
    snapshot_date: str,
    tables: Optional[List[str]] = None,
) -> List[str]:
    """
    Atomically restore one or more tables from snapshot_dir/<snapshot_date>
    back into production, via the same CREATE OR REPLACE TABLE ... AS
    SELECT primitive as datastore/staging/publish.py::publish_table (same
    single-atomic-statement, single-writer-connection discipline — see
    that module's docstring). Caller is responsible for holding
    publish.publish_run_lock() around this call and for taking a
    pre-restore safety snapshot first (scripts/restore_snapshot.py does
    both).

    Restores every table with a .parquet file in the snapshot directory if
    `tables` is None, else only the named tables.

    Raises FileNotFoundError if snapshot_date doesn't exist, listing the
    dates that do.
    """
    target_dir = snapshot_dir / snapshot_date
    if not target_dir.exists():
        available = list_snapshot_dates(snapshot_dir)
        raise FileNotFoundError(
            f"No snapshot for date '{snapshot_date}'. Available dates: {available}"
        )

    available_tables = [p.stem for p in target_dir.glob("*.parquet")]
    restore_list = tables if tables is not None else available_tables
    missing = [t for t in restore_list if t not in available_tables]
    if missing:
        raise FileNotFoundError(
            f"No snapshot parquet for tables {missing} on '{snapshot_date}'. "
            f"Available: {available_tables}"
        )

    restored = []
    for table_name in restore_list:
        parquet_path = target_dir / f"{table_name}.parquet"
        conn.execute(
            f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet('{parquet_path}')"
        )
        row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        logger.info("restore_snapshot: %s restored from %s (%d rows)", table_name, snapshot_date, row_count)
        restored.append(table_name)

    return restored
