"""
FrameworkResultsDBWriter — persists a BacktestResult into the new
framework_backtest_runs / framework_backtest_trades tables (see
results/db_schema.py for the schema and the versioning rationale).

Distinct from results/writer.py (ResultsWriter), which writes the same
BacktestResult to a JSON file under results/runs/ — that path stays for
quick file-based inspection/diffing; this path is what makes framework
results queryable alongside the legacy backtest_runs table for real
comparison and reporting.

SAFETY: writes to datastore/backtest_store/backtest.duckdb, the SAME
shared, production-adjacent store the legacy engine uses (single-writer
DuckDB — see CLAUDE.md's concurrency section). Call this only for a real,
intentional persist — never from a test or a parity-check run (those use
an isolated temp DB, see scripts/parity_check.py).
"""

import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import duckdb

from momentum_framework.backtesting.result import BacktestResult
from momentum_framework.results.db_schema import SCHEMA_SQL

PROD_BACKTEST_DB_PATH = Path("/home/amit/projects/AlphaLens/datastore/backtest_store/backtest.duckdb")


def _build_round_trips(trades: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Pairs Portfolio.trade_log's buy/sell events into round-trip rows —
    Portfolio holds at most one open Position per ticker at a time (see
    backtesting/portfolio.py), so within one ticker's chronologically
    ordered events, buys and sells alternate strictly and pairing is
    unambiguous: each buy opens a trade, the next sell (if any) for that
    same ticker closes it. A buy with no following sell is still open at
    the end of the backtest and gets sale_date/sale_price/pnl = None.
    """
    by_ticker: Dict[str, List[Dict[str, Any]]] = {}
    for t in trades:
        by_ticker.setdefault(t["ticker"], []).append(t)

    round_trips = []
    for ticker, events in by_ticker.items():
        events = sorted(events, key=lambda e: e["date"])
        open_buy = None
        for event in events:
            if event["action"] == "buy":
                open_buy = event
            elif event["action"] == "sell" and open_buy is not None:
                buy_date = open_buy["date"]
                sale_date = event["date"]
                holding_days = (
                    (date.fromisoformat(sale_date) - date.fromisoformat(buy_date)).days
                    if isinstance(buy_date, str) and isinstance(sale_date, str) else None
                )
                round_trips.append({
                    "ticker": ticker,
                    "qty": open_buy["shares"],
                    "buy_date": buy_date,
                    "buy_price": open_buy["price"],
                    "sale_date": sale_date,
                    "sale_price": event["price"],
                    "pnl_inr": event.get("pnl"),
                    "pnl_pct": (event["price"] / open_buy["price"] - 1.0) if open_buy["price"] else None,
                    "holding_days": holding_days,
                })
                open_buy = None
        if open_buy is not None:  # still open at end of backtest
            round_trips.append({
                "ticker": ticker,
                "qty": open_buy["shares"],
                "buy_date": open_buy["date"],
                "buy_price": open_buy["price"],
                "sale_date": None,
                "sale_price": None,
                "pnl_inr": None,
                "pnl_pct": None,
                "holding_days": None,
            })
    return round_trips


class FrameworkResultsDBWriter:
    def __init__(self, db_path: Path = PROD_BACKTEST_DB_PATH):
        self.db_path = db_path

    def write(
        self,
        result: BacktestResult,
        engine: str = "native",
        universe_cache_used: bool = True,
        parity_checked: bool = False,
    ) -> None:
        conn = duckdb.connect(str(self.db_path), read_only=False)
        try:
            conn.execute(SCHEMA_SQL)

            config = result.config
            conn.execute(
                """
                INSERT INTO framework_backtest_runs (
                    run_id, strategy_id, strategy_code, band_id, engine,
                    source_commit, source_commit_dirty, framework_version,
                    start_date, end_date, config_json, metrics_json,
                    trade_count, integrity_passed, integrity_detail_json,
                    data_gaps_json, universe_cache_used, parity_checked,
                    run_executed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (run_id) DO NOTHING
                """,
                [
                    result.run_id,
                    result.strategy_id,
                    config.get("strategy_code"),
                    config.get("band_id"),
                    engine,
                    result.source_commit or "unknown",
                    bool(result.integrity_detail.get("source_commit_dirty", False)),
                    result.framework_version,
                    config.get("start_date"),
                    config.get("end_date"),
                    json.dumps(config, default=str),
                    json.dumps(result.metrics, default=str),
                    result.trade_count,
                    result.integrity_passed,
                    json.dumps(result.integrity_detail, default=str),
                    json.dumps(result.data_gaps, default=str),
                    universe_cache_used,
                    parity_checked,
                    datetime.now(timezone.utc),
                ],
            )

            if result.trades:
                round_trips = _build_round_trips(result.trades)
                rows = [
                    (result.run_id, rt["ticker"], rt["qty"], rt["buy_date"], rt["buy_price"],
                     rt["sale_date"], rt["sale_price"], rt["pnl_inr"], rt["pnl_pct"], rt["holding_days"])
                    for rt in round_trips
                ]
                conn.executemany(
                    """INSERT INTO framework_backtest_trades
                       (run_id, ticker, qty, buy_date, buy_price, sale_date, sale_price,
                        pnl_inr, pnl_pct, holding_days)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    rows,
                )
            conn.commit()
        finally:
            conn.close()
