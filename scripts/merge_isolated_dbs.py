#!/usr/bin/env python3
"""Merge all isolated backtest_proc_*.duckdb files into main backtest.duckdb."""

import duckdb
import sys
from pathlib import Path

ISOLATED_DIR = Path("datastore/backtest_store/temp_dbs")
MAIN_DB = Path("datastore/backtest_store/backtest.duckdb")

def checkpoint_and_merge() -> bool:
    """Checkpoint all isolated DBs and merge into main."""
    isolated_dbs = sorted(ISOLATED_DIR.glob("backtest_proc_*.duckdb"))

    if not isolated_dbs:
        print("❌ No isolated DB files found")
        return False

    print(f"Found {len(isolated_dbs)} isolated DBs")

    # Checkpoint each isolated DB
    for db_path in isolated_dbs:
        print(f"Checkpointing {db_path.name}...")
        db = duckdb.connect(str(db_path))
        db.execute("CHECKPOINT")
        db.close()

    # Create/connect to main DB
    main_db = duckdb.connect(str(MAIN_DB))

    # Attach and copy from each isolated DB
    for i, db_path in enumerate(isolated_dbs):
        alias = f"isolated_{i}"
        print(f"Attaching {db_path.name} as {alias}...")
        main_db.execute(f"ATTACH '{db_path}' AS {alias}")

        # Copy data from each table
        for table in ["backtest_runs", "backtest_feature_log", "strategy_catalog"]:
            print(f"  Copying {alias}.{table}...")
            try:
                main_db.execute(f"""
                    INSERT INTO {table}
                    SELECT * FROM {alias}.{table}
                """)
            except Exception as e:
                print(f"    ⚠️  {e}")

        main_db.execute(f"DETACH {alias}")

    main_db.close()
    print("✅ Merge complete")
    return True

if __name__ == "__main__":
    success = checkpoint_and_merge()
    sys.exit(0 if success else 1)
