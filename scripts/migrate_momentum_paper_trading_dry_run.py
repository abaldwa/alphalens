"""
scripts/migrate_momentum_paper_trading_dry_run.py

Phase: Unified Backtest & Paper Trading Umbrella, open item #2
("Migrate Momentum's manual momentum_trades journal into the unified
paper-trading schema" — BacktestUmbrellaPlan.md Phase 5)
Owner: Platform / Backtest

DRY RUN ONLY. This script reads the real `momentum_trades` table
(read-only DuckDB connection, `persist=False` so it never holds a lock
against the live scheduler/API — see datastore/api/db.py's module
docstring) and reports how each row WOULD map onto the unified
paper-trading schema (backtest/paper_trading/approval_queue.py +
live_runner.py). It writes nothing: no pending-action files, no
execution files, no portfolio state, no DB rows. Real production data
belongs to a real migration decision, not a speculative automated one —
see BacktestUmbrellaPlan.md Phase 5's note that this migration is
"correctly out of scope for an implementation pass done without the
table's owner present."

What this reports, per strategy_id:
  - n_trades, n_open (no sale_date), n_closed
  - what the mapped PendingAction stream would look like (buy on
    purchase_date, sell on sale_date), i.e. how many distinct trading
    days would retroactively count toward Gate 7 if migrated
  - a REQUIRED-DECISION flag: momentum_trades has no notion of a
    strategy's initial_capital / horizon_bucket (the unified schema's
    StrategyPortfolio needs both) — this script never invents one, it
    only reports the real qty*price cash flow shape so a human can pick
    real values before any actual migration is written.
  - any rows this script could NOT map cleanly (e.g. missing
    purchase_price) — printed explicitly, never silently skipped.

Usage
-----
  python -m scripts.migrate_momentum_paper_trading_dry_run
"""

import argparse
import json
import logging
from collections import defaultdict
from typing import Any, Dict, List

from config.settings import DUCKDB_PATH
from datastore.api.db import get_duckdb_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_COLUMNS = [
    "id", "strategy_id", "ticker", "purchase_date", "qty", "purchase_price",
    "sale_date", "sell_price", "entry_rank", "exit_rank", "suggestion_id",
    "purchase_rationale", "sell_rationale", "journal_entry",
]


def _fetch_real_momentum_trades() -> List[Dict[str, Any]]:
    """Real rows from the live momentum_trades table — read-only,
    persist=False (never locks out the scheduler/API — SPEC-SCHED-013)."""
    # read_only=True only applies to a real file — an in-memory :memory: DB
    # (DUCKDB_PATH=None, used by this module's tests) cannot be opened
    # read-only at all (DuckDB CatalogException), so only request it when
    # there's a real file to protect from a write lock.
    with get_duckdb_connection(DUCKDB_PATH, read_only=DUCKDB_PATH is not None, persist=False) as conn:
        exists = conn.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = 'momentum_trades'"
        ).fetchone()[0]
        if not exists:
            return []
        rows = conn.execute(f"SELECT {', '.join(_COLUMNS)} FROM momentum_trades ORDER BY strategy_id, purchase_date").fetchall()
    return [dict(zip(_COLUMNS, r)) for r in rows]


def _map_trade_to_paper_actions(trade: Dict[str, Any]) -> Dict[str, Any]:
    """Describe (never construct) the PendingAction(s) this row would
    become: a 'buy' on purchase_date, and a 'sell' on sale_date if closed."""
    problems = []
    if trade["purchase_price"] is None:
        problems.append("purchase_price is NULL — cannot derive a mapped buy fill price")
    if trade["qty"] is None or trade["qty"] <= 0:
        problems.append("qty is NULL/non-positive")

    mapped_actions = []
    if not problems:
        mapped_actions.append(
            {
                "as_of_date": str(trade["purchase_date"]), "ticker": trade["ticker"], "action": "buy",
                "executed_price": trade["purchase_price"], "executed_quantity": trade["qty"],
            }
        )
        if trade["sale_date"] is not None:
            if trade["sell_price"] is None:
                problems.append("sale_date present but sell_price is NULL — cannot derive a mapped sell fill price")
            else:
                mapped_actions.append(
                    {
                        "as_of_date": str(trade["sale_date"]), "ticker": trade["ticker"], "action": "sell",
                        "executed_price": trade["sell_price"], "executed_quantity": trade["qty"],
                    }
                )

    return {"trade_id": trade["id"], "mapped_actions": mapped_actions, "problems": problems}


def run_dry_run() -> Dict[str, Any]:
    trades = _fetch_real_momentum_trades()
    if not trades:
        logger.info("No real rows in momentum_trades (table missing or empty) — nothing to report.")
        return {"strategies": {}, "n_trades_total": 0}

    by_strategy: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for t in trades:
        by_strategy[t["strategy_id"]].append(t)

    report: Dict[str, Any] = {"strategies": {}, "n_trades_total": len(trades)}
    for strategy_id, strategy_trades in by_strategy.items():
        mapped = [_map_trade_to_paper_actions(t) for t in strategy_trades]
        n_open = sum(1 for t in strategy_trades if t["sale_date"] is None)
        n_closed = len(strategy_trades) - n_open
        trading_days = {a["as_of_date"] for m in mapped for a in m["mapped_actions"]}
        unmappable = [m for m in mapped if m["problems"]]
        total_deployed = sum(
            (t["qty"] or 0) * (t["purchase_price"] or 0) for t in strategy_trades if t["purchase_price"] is not None
        )

        report["strategies"][strategy_id] = {
            "n_trades": len(strategy_trades),
            "n_open": n_open,
            "n_closed": n_closed,
            "distinct_trading_days_that_would_count_toward_gate7": len(trading_days),
            "n_unmappable_rows": len(unmappable),
            "unmappable_trade_ids_and_reasons": [
                {"trade_id": m["trade_id"], "problems": m["problems"]} for m in unmappable
            ],
            "total_capital_deployed_across_all_buys_inr": total_deployed,
            "REQUIRED_HUMAN_DECISION": (
                "momentum_trades has no initial_capital/horizon_bucket concept — the destination "
                "StrategyPortfolio requires both before any real accept() can run. This dry run does "
                "not choose values; a real migration needs an explicit initial_capital (e.g. the "
                "capital actually deployed historically, or a fresh round number) and horizon_bucket "
                "(likely HorizonBucket.D21 or CUSTOM given Momentum's monthly-rebalance cadence) "
                "picked by a human before backtest.paper_trading.live_runner.save_portfolio_state() "
                "is ever called for this strategy_id."
            ),
        }

    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="print the full report as JSON instead of a summary")
    args = parser.parse_args()

    report = run_dry_run()

    if args.json:
        print(json.dumps(report, indent=2, default=str))
        return

    print("\n" + "=" * 78)
    print("  Momentum -> Unified Paper Trading migration DRY RUN (no writes)")
    print("=" * 78)
    print(f"  Real rows read from momentum_trades: {report['n_trades_total']}")
    for strategy_id, s in report["strategies"].items():
        print(f"\n  Strategy: {strategy_id}")
        print(f"    trades: {s['n_trades']} (open={s['n_open']}, closed={s['n_closed']})")
        print(f"    distinct trading days that would count toward Gate 7: {s['distinct_trading_days_that_would_count_toward_gate7']}")
        print(f"    total capital deployed across all buys: Rs.{s['total_capital_deployed_across_all_buys_inr']:,.0f}")
        if s["n_unmappable_rows"]:
            print(f"    UNMAPPABLE ROWS: {s['n_unmappable_rows']} — {s['unmappable_trade_ids_and_reasons']}")
        print(f"    REQUIRED HUMAN DECISION: {s['REQUIRED_HUMAN_DECISION']}")
    print("\n  No files or DB rows were written by this dry run.")
    print("=" * 78)


if __name__ == "__main__":
    main()
