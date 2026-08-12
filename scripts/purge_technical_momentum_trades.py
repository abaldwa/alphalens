#!/usr/bin/env python3
"""
scripts/purge_technical_momentum_trades.py

Drops all Technical and Momentum backtest trade data ahead of the 2007-start
regeneration (user instruction, 2026-08-12).

Why
---
Every technical run currently in the store was executed on 2026-08-11, i.e.
BEFORE the Fyers-primary backfill corrected ~1.5M ticker-days of 2007-2016
OHLCV. Those runs priced trades off adjustment factors that manufactured
impossible returns (BAJFINANCE entering at Rs 0.04 against a real Rs 3.16),
and because the engine reinvests a lump capital base, a single corrupted early
trade multiplies into every later trade in the same run. The numbers are not
salvageable by re-cutting or re-costing them; they have to be regenerated.

Momentum rows are purged for the same reason and because the user is
regenerating that channel too.

Scope
-----
Deletes, for channel in {technical, momentum}:
  * backtest_trades          (the trade book itself)
  * backtest_runs            (the run headers)
  * backtest_exit_decisions  (has its own channel column)
  * backtest_feature_log     (keyed by run_id only -> resolved via the runs
                              being deleted, captured BEFORE they are removed)

backtest_trades_enriched is a VIEW over backtest_trades and needs no action.
Fundamental and ML runs are left completely untouched.

Usage:
    python scripts/purge_technical_momentum_trades.py            # dry run
    python scripts/purge_technical_momentum_trades.py --apply
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import duckdb  # noqa: E402

from config.settings import BACKTEST_DUCKDB_PATH  # noqa: E402

CHANNELS = ("technical", "momentum")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--apply", action="store_true", help="actually delete (default: dry run)")
    args = p.parse_args()

    conn = duckdb.connect(str(BACKTEST_DUCKDB_PATH), read_only=not args.apply)
    ph = ",".join("?" for _ in CHANNELS)

    # Resolve the affected run_ids FIRST — backtest_feature_log has no channel
    # column, so once backtest_runs is deleted there is no way to identify its
    # orphaned rows.
    run_ids = [
        r[0] for r in conn.execute(
            f"SELECT run_id FROM backtest_runs WHERE channel IN ({ph})", list(CHANNELS)
        ).fetchall()
    ]

    counts = {
        "backtest_trades": conn.execute(
            f"SELECT COUNT(*) FROM backtest_trades WHERE channel IN ({ph})", list(CHANNELS)
        ).fetchone()[0],
        "backtest_runs": len(run_ids),
        "backtest_exit_decisions": conn.execute(
            f"SELECT COUNT(*) FROM backtest_exit_decisions WHERE channel IN ({ph})", list(CHANNELS)
        ).fetchone()[0],
    }
    if run_ids:
        rph = ",".join("?" for _ in run_ids)
        counts["backtest_feature_log"] = conn.execute(
            f"SELECT COUNT(*) FROM backtest_feature_log WHERE run_id IN ({rph})", run_ids
        ).fetchone()[0]
    else:
        counts["backtest_feature_log"] = 0

    print(f"database: {BACKTEST_DUCKDB_PATH}")
    print(f"channels: {', '.join(CHANNELS)}\n")
    for t, n in counts.items():
        print(f"  {t:<28} {n:>12,} rows")
    print()

    # Show what survives, so an over-broad delete is visible before it happens.
    print("  retained by channel:")
    for ch, n in conn.execute(
        f"SELECT channel, COUNT(*) FROM backtest_runs WHERE channel NOT IN ({ph}) GROUP BY 1 ORDER BY 2 DESC",
        list(CHANNELS),
    ).fetchall():
        print(f"    {ch:<26} {n:>12,} runs")
    print()

    if not args.apply:
        print("DRY RUN — nothing deleted. Re-run with --apply to execute.")
        return

    conn.execute("BEGIN TRANSACTION")
    try:
        if run_ids:
            rph = ",".join("?" for _ in run_ids)
            conn.execute(f"DELETE FROM backtest_feature_log WHERE run_id IN ({rph})", run_ids)
        conn.execute(f"DELETE FROM backtest_exit_decisions WHERE channel IN ({ph})", list(CHANNELS))
        conn.execute(f"DELETE FROM backtest_trades WHERE channel IN ({ph})", list(CHANNELS))
        conn.execute(f"DELETE FROM backtest_runs WHERE channel IN ({ph})", list(CHANNELS))
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    # Reclaim the space rather than leaving it in the WAL.
    conn.execute("CHECKPOINT")

    print("DELETED. Verifying:")
    for t, where in (
        ("backtest_trades", f"channel IN ({ph})"),
        ("backtest_runs", f"channel IN ({ph})"),
        ("backtest_exit_decisions", f"channel IN ({ph})"),
    ):
        n = conn.execute(f"SELECT COUNT(*) FROM {t} WHERE {where}", list(CHANNELS)).fetchone()[0]
        print(f"  {t:<28} {n:>12,} rows remaining (expect 0)")
    conn.close()


if __name__ == "__main__":
    main()
