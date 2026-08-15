#!/usr/bin/env python3
"""
Pre-start checks for the daily scheduler (S1.2).

Checks performed:
 - Detect common backtest writer processes (simple pgrep-based check).
 - Attempt to open BACKTEST_DUCKDB_PATH in read-only mode.

Exit code 0 means safe to start scheduler; non-zero indicates a condition
that should be resolved first.
"""
import subprocess
import sys

import duckdb

from config.settings import BACKTEST_DUCKDB_PATH


def check_backtest_processes() -> bool:
    """Return True if suspect backtest writer processes are running."""
    patterns = [
        "run_phase1_backtest.py",
        "run_phase2_backtest.py",
        "run_iterative_backtest.py",
        "run_strategy_queue.py",
        "run_batch_backtest.py",
    ]
    for p in patterns:
        try:
            out = subprocess.check_output(["pgrep", "-f", p], stderr=subprocess.DEVNULL)
            if out.strip():
                print(f"Found backtest process matching '{p}': {out.decode().strip()}")
                return True
        except subprocess.CalledProcessError:
            # pgrep returned no matches
            continue
        except FileNotFoundError:
            # pgrep not available on this system — skip process checks
            print("pgrep not available; skipping process checks")
            return False
    return False


def check_backtest_db_readable() -> bool:
    """Attempt to open the BACKTEST_DUCKDB_PATH read-only and run a trivial query."""
    try:
        conn = duckdb.connect(str(BACKTEST_DUCKDB_PATH), read_only=True)
        try:
            conn.execute("SELECT 1").fetchall()
        finally:
            conn.close()
        return True
    except Exception as exc:
        print(f"Failed to open BACKTEST_DUCKDB_PATH read-only: {exc}")
        return False


def main() -> int:
    print("S1.2 pre-start checks: checking for backtest writers and DB readability...")

    procs = check_backtest_processes()
    if procs:
        print("Backtest writer processes appear to be running — pause them before starting the scheduler.")
        return 3

    db_ok = check_backtest_db_readable()
    if not db_ok:
        print("Backtest DB not readable in read-only mode — investigate locks before starting the scheduler.")
        return 4

    print("Pre-start checks passed: no backtest writers found and BACKTEST DB is readable.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
