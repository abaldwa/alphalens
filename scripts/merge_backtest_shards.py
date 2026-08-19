"""
scripts/merge_backtest_shards.py

Owner: Platform / Backtest
Run: PYTHONPATH=$PWD .venv/bin/python -m scripts.merge_backtest_shards \
        --shard-db datastore/backtest_store/shard1.duckdb [--shard-db ...] [--dry-run]

Folds a sweep shard's backtest store back into the shared one.

WHY SHARDS HAVE THEIR OWN STORE
-------------------------------
DuckDB permits ONE writer process per file. Two concurrent
run_sweep_inprocess shards against the shared store do not merely contend for
the write lock -- the second fails outright:

    IO Error: Could not set lock on file ".../backtest.duckdb":
    Conflicting lock is held ... (PID ...)

and with DUCKDB_WRITE_LOCK_RETRY_ATTEMPTS raised it fails SLOWLY: measured
2026-08-19 at 1,384s and 1,496s per job before giving up, having done the
whole simulation first. `defer_db_writes` does not help -- it shortens the
write to a tail, but a tail every ~20s in one process keeps the file locked
against the other almost continuously.

So each shard sets ALPHALENS_BACKTEST_DUCKDB_PATH to its own file and this
script merges afterwards. Parallelism without contention.

MERGE SEMANTICS
---------------
`backtest_runs`, `backtest_trades`, `backtest_feature_log`,
`strategy_signals`, `backtest_exit_decisions` are append-only fact tables
keyed by run_id. A run_id is a uuid generated per run, so shards cannot
collide; rows are inserted, and any run_id already present in the target is
SKIPPED rather than duplicated (re-running a merge must be idempotent).

`strategy_catalog` is NOT append-only -- it is a per-strategy rollup carrying
n_runs, first_run_at, last_run_at and latest_run_id. Blind-inserting it would
either duplicate the primary key or, worse, silently report a strategy as
having run half as often as it did. It is merged properly: counts summed,
first/last timestamps taken as min/max, and latest_run_id taken from whichever
side has the later last_run_at.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Any, List

import duckdb

from config.settings import BACKTEST_DUCKDB_PATH

logger = logging.getLogger(__name__)

#: Append-only, keyed by run_id. Order does not matter -- none reference each
#: other, they all reference backtest_runs.
FACT_TABLES = (
    "backtest_runs",
    "backtest_trades",
    "backtest_feature_log",
    "strategy_signals",
    "backtest_exit_decisions",
)


def _scalar(conn: duckdb.DuckDBPyConnection, sql: str) -> Any:
    """The single value of a one-row query. See _shared_columns for why this
    module refuses to index a possibly-None fetchone()."""
    row = conn.execute(sql).fetchone()
    if row is None:
        raise RuntimeError(f"expected one row, got none: {sql}")
    return row[0]


def _columns(conn: duckdb.DuckDBPyConnection, table: str, catalog: str) -> List[str]:
    return [
        r[0]
        for r in conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = ? AND table_catalog = ? ORDER BY ordinal_position",
            [table, catalog],
        ).fetchall()
    ]


def _shared_columns(conn: duckdb.DuckDBPyConnection, table: str, target_catalog: str) -> List[str]:
    """Columns to copy, NAMED -- never positional.

    THE BUG THIS EXISTS TO KILL (2026-08-19). The insert used
    `SELECT * FROM shard.{table}`, which matches columns BY POSITION. The two
    stores were created by different versions of the schema, so
    `backtest_runs` held the same 30 columns in a DIFFERENT ORDER -- diverging
    at index 21, where the target has `live_eligible` (BOOL) and the shard has
    `regime_breakdown_json` (VARCHAR). DuckDB refused to convert the JSON to a
    BOOL and the merge aborted, which is the only reason it was caught: had
    the mismatched pair been type-compatible, 630 runs would have been written
    with silently transposed values.

    A column present on one side only is SKIPPED rather than guessed at, and
    reported, so a genuine schema drift is visible instead of quietly dropped.
    """
    target = _columns(conn, table, target_catalog)
    shard = set(_columns(conn, table, "shard"))
    shared = [c for c in target if c in shard]
    missing = [c for c in target if c not in shard]
    if missing:
        logger.warning(
            "  %-24s shard lacks %d column(s), inserted as NULL: %s",
            table, len(missing), ", ".join(missing),
        )
    return shared


def _table_exists(conn: duckdb.DuckDBPyConnection, name: str, catalog: str) -> bool:
    """Whether `catalog` holds a table called `name`.

    Scoped by catalog deliberately. Once the shard is ATTACHed, an unscoped
    information_schema lookup answers "does EITHER database have it", which
    reported the shard's missing backtest_trades as present and then failed
    on the count. A shard legitimately lacks a table the target has (a
    momentum-only shard writes no exit_decisions), so absence on either side
    is a skip, not an error.
    """
    row = conn.execute(
        "SELECT count(*) FROM information_schema.tables "
        "WHERE table_name = ? AND table_catalog = ?",
        [name, catalog],
    ).fetchone()
    return bool(row[0]) if row else False


def merge_shard(target: Path, shard: Path, dry_run: bool) -> None:
    conn = duckdb.connect(str(target), read_only=dry_run)
    try:
        # ATTACH takes no prepared parameters (DuckDB parser rejects "?"),
        # so the path is inlined. Quotes are escaped rather than trusted --
        # the path comes from argv, not a user-facing surface, but a literal
        # is a literal.
        target_catalog = Path(target).stem
        shard_literal = str(shard).replace("'", "''")
        conn.execute(f"ATTACH '{shard_literal}' AS shard (READ_ONLY)")
        try:
            for table in FACT_TABLES:
                if not _table_exists(conn, table, target_catalog):
                    logger.info("  %-24s target has no such table, skipped", table)
                    continue
                if not _table_exists(conn, table, "shard"):
                    logger.info("  %-24s shard has no such table, skipped", table)
                    continue
                n_shard = _scalar(conn, f"SELECT count(*) FROM shard.{table}")
                if not n_shard:
                    continue
                # run_id already present = already merged. Idempotent by design.
                new_rows = _scalar(
                    conn, f"SELECT count(*) FROM shard.{table} s "
                    f"WHERE NOT EXISTS (SELECT 1 FROM {table} t WHERE t.run_id = s.run_id)"
                )
                logger.info("  %-24s %d in shard, %d new", table, n_shard, new_rows)
                if not dry_run and new_rows:
                    cols = _shared_columns(conn, table, target_catalog)
                    col_list = ", ".join(f'"{c}"' for c in cols)
                    conn.execute(
                        f"INSERT INTO {table} ({col_list}) "
                        f"SELECT {col_list} FROM shard.{table} s "
                        f"WHERE NOT EXISTS (SELECT 1 FROM {table} t WHERE t.run_id = s.run_id)"
                    )

            # strategy_catalog: a rollup, not a fact table. See module docstring.
            if not _table_exists(conn, "strategy_catalog", "shard"):
                logger.info("  %-24s shard has no such table, skipped", "strategy_catalog")
                return
            n_cat = _scalar(conn, "SELECT count(*) FROM shard.strategy_catalog")
            logger.info("  %-24s %d in shard (rollup merge)", "strategy_catalog", n_cat)
            if not dry_run and n_cat:
                conn.execute(
                    """
                    INSERT INTO strategy_catalog
                    SELECT s.* FROM shard.strategy_catalog s
                    WHERE NOT EXISTS (
                        SELECT 1 FROM strategy_catalog t WHERE t.strategy_key = s.strategy_key
                    )
                    """
                )
                conn.execute(
                    """
                    UPDATE strategy_catalog AS t
                    SET n_runs       = t.n_runs + s.n_runs,
                        first_run_at = least(t.first_run_at, s.first_run_at),
                        last_run_at  = greatest(t.last_run_at, s.last_run_at),
                        latest_run_id = CASE
                            WHEN s.last_run_at > t.last_run_at THEN s.latest_run_id
                            ELSE t.latest_run_id END
                    FROM shard.strategy_catalog AS s
                    WHERE t.strategy_key = s.strategy_key
                    """
                )
        finally:
            conn.execute("DETACH shard")
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--shard-db", type=Path, action="append", required=True)
    parser.add_argument("--target", type=Path, default=Path(BACKTEST_DUCKDB_PATH))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    shards: List[Path] = args.shard_db
    for shard in shards:
        if not shard.exists():
            raise SystemExit(f"shard db not found: {shard}")
        logger.info("merging %s -> %s%s", shard, args.target, " [dry-run]" if args.dry_run else "")
        merge_shard(args.target, shard, args.dry_run)
    logger.info("done")


if __name__ == "__main__":
    main()
