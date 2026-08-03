"""
backtest/export_trade_book.py

Owner: Platform / Backtest
Consumers: backtest/run_orchestrator_backtest.py (calls this after every
run), operator CLI (`python -m backtest.export_trade_book --run-id ...`)

Builds one enriched trade-book CSV per run: ticker, market-cap tier at
buy, buy/sell date & price, entry/exit REASON with the actual indicator
values that triggered it, days held, P&L. User request: "Trade Book in
.csv format containing... reason for entry with actual value of the
indicators, reason for the exit with actual value of the indicators."

Pure post-processing — reads three things that already exist, joins them,
writes a CSV. Does not touch backtest/core/engine.py's simulation loop:
  1. trade_log_{run_id}.csv (backtest/core/engine.py::_write_trade_log) —
     ticker/qty/buy_date/buy_price/sale_date/sale_price/stock_rank/
     pnl_inr/pnl_pct/exit_reason (exit_reason is engine.py's own short
     code, e.g. "stop_loss"/"target"/"exit_model_urgent"/"forced_close" —
     NOT the same as this module's entry/exit "reason" columns below,
     which additionally carry the real indicator values).
  2. backtest_feature_log (run_id, ticker, as_of_date, feature_vector_json,
     decision_taken) — the full feature vector considered on the buy date
     and (if present) the sell date, joined by (run_id, ticker, date).
  3. strategy_catalog — only to resolve a human-readable descriptor for
     the output filename; not required for correctness.

A ticker/date with no matching backtest_feature_log row (adapter didn't
log a feature vector that day, e.g. a forced_close/corporate-action exit
with no adapter re-evaluation) gets an empty reason/indicator column —
never fabricated.
"""

import argparse
import csv
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from backtest.momentum_backtest import trade_cagr as _trade_cagr
from backtest.momentum_metrics import return_population_zscores
from backtest.trade_book_html import render_trade_book_html
from config.settings import BACKTEST_DUCKDB_PATH
from datastore.api.db import get_duckdb_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

REPORTS_DIR = Path(__file__).resolve().parent / "reports"
# 2026-08-01: HTML trade books for the Technical channel specifically (user
# request) — a sibling of backtest/reports/momentum/html/ (that one is
# still CSV-only/convert-on-demand, per explicit choice not to touch it).
HTML_OUT_DIR = REPORTS_DIR / "technical" / "html"

TRADE_LOG_COLUMNS = [
    "ticker", "qty", "buy_date", "buy_price", "sale_date", "sale_price", "stock_rank",
    "pnl_inr", "pnl_pct", "exit_reason",
]


def _market_cap_tier(stock_rank: str) -> str:
    """Human-readable tier label from stock_rank — same rank bands as the
    Momentum sweep (WIDE_BANDS in scripts/run_momentum_experimentation.py
    + RANK_BANDS in features/momentum_universe.py), for consistency across
    every channel's trade book. Blank rank -> blank tier (never guessed)."""
    if not stock_rank:
        return ""
    rank = int(stock_rank)
    bounds = [
        ("Nifty50", 1, 50), ("Nifty51-100", 51, 100), ("Nifty100-150", 101, 150),
        ("Nifty150-200", 151, 200), ("Nifty200-250", 201, 250), ("Nifty251-500", 251, 500),
        ("Nifty501-800", 501, 800),
    ]
    for label, lo, hi in bounds:
        if lo <= rank <= hi:
            return label
    return "Nifty800+"


def _load_feature_row(conn, run_id: str, ticker: str, as_of_date: str) -> Optional[Dict[str, Any]]:
    if not as_of_date:
        return None
    row = conn.execute(
        "SELECT feature_vector_json, decision_taken FROM backtest_feature_log "
        "WHERE run_id = ? AND ticker = ? AND as_of_date = ?",
        [run_id, ticker, as_of_date],
    ).fetchone()
    if row is None:
        return None
    feature_vector_json, decision_taken = row
    return {"decision_taken": decision_taken, "feature_vector": json.loads(feature_vector_json)}


_CSV_COLUMNS = [
    "run_id", "ticker", "market_cap_tier_at_buy", "buy_date", "buy_price",
    "entry_reason", "entry_indicator_values",
    "sell_date", "sell_price", "exit_reason", "exit_indicator_values",
    "days_held", "pnl_inr", "pnl_pct",
    "trade_cagr", "return_zscore", "entry_signal_score", "entry_signal_zscore",
]
# HTML columns are a curated subset of _CSV_COLUMNS — the two indicator-
# value JSON blobs are omitted from the human-scanning HTML view (they're
# still in the CSV) since they're long and not usefully sortable/scannable.
_HTML_COLUMNS = [c for c in _CSV_COLUMNS if c not in ("entry_indicator_values", "exit_indicator_values")]


def _write(conn, run_id: str, trades, out_path: Path, write_html: bool = False) -> None:
    # Two passes: first collect every row's realized return_pct and entry
    # signal score so return_zscore/entry_signal_zscore can be computed
    # against the whole run's population (same population-stats approach
    # Momentum's trade_quality_metrics/entry_signal_zscores use), then
    # write once every row's values are known.
    enriched: List[Dict[str, Any]] = []
    for t in trades:
        entry = _load_feature_row(conn, run_id, t["ticker"], t["buy_date"])
        exit_ = _load_feature_row(conn, run_id, t["ticker"], t["sale_date"])

        buy_date, sale_date = t["buy_date"], t.get("sale_date") or ""
        buy_price = float(t["buy_price"]) if t["buy_price"] not in (None, "") else None
        sell_price = float(t["sale_price"]) if t.get("sale_price") not in (None, "") else None
        days_held = None
        if buy_date and sale_date:
            from datetime import date as date_type
            days_held = (date_type.fromisoformat(sale_date) - date_type.fromisoformat(buy_date)).days

        pnl_pct = t.get("pnl_pct")
        return_pct = float(pnl_pct) * 100 if pnl_pct not in (None, "") else None
        entry_fv = entry["feature_vector"] if entry else None
        entry_score = entry_fv.get("score") if entry_fv else None

        enriched.append({
            "run_id": run_id, "ticker": t["ticker"],
            "market_cap_tier_at_buy": _market_cap_tier(t.get("stock_rank", "")),
            "buy_date": buy_date, "buy_price": buy_price,
            "entry_reason": entry["decision_taken"] if entry else "",
            "entry_indicator_values": json.dumps(entry["feature_vector"]) if entry else "",
            "sell_date": sale_date, "sell_price": sell_price,
            "exit_reason": t.get("exit_reason", ""),
            "exit_indicator_values": json.dumps(exit_["feature_vector"]) if exit_ else "",
            "days_held": days_held,
            "pnl_inr": t.get("pnl_inr"), "pnl_pct": pnl_pct,
            "trade_cagr": _trade_cagr(
                buy_price, sell_price, days_held,
                ticker=t["ticker"], buy_date=buy_date, sell_date=sale_date, run_id=run_id,
            ) if buy_price else None,
            "_return_pct": return_pct,
            "_entry_score": float(entry_score) if entry_score is not None else None,
        })

    return_z = return_population_zscores([r["_return_pct"] for r in enriched])
    signal_z = return_population_zscores([r["_entry_score"] for r in enriched])
    for row, rz, sz in zip(enriched, return_z["zscores"], signal_z["zscores"]):
        row["return_zscore"] = rz
        row["entry_signal_score"] = row.pop("_entry_score")
        row["entry_signal_zscore"] = sz
        row.pop("_return_pct")

    with open(out_path, "w", newline="") as out_fh:
        writer = csv.writer(out_fh)
        writer.writerow(_CSV_COLUMNS)
        for row in enriched:
            writer.writerow([row[c] for c in _CSV_COLUMNS])

    if write_html:
        html_path = HTML_OUT_DIR / f"trade_book_{run_id}.html"
        render_trade_book_html(
            enriched, _HTML_COLUMNS, title=f"Trade Book — {run_id}", out_path=html_path,
            subtitle=f"Technical channel run {run_id}",
        )
        logger.info(f"Trade book HTML written to {html_path}")


def export_trade_book(
    run_id: str, trade_log_path: Path, out_path: Optional[Path] = None, conn: Any = None,
    write_html: bool = False,
) -> Path:
    """
    Parameters
    ----------
    conn : an already-open DuckDB connection to BACKTEST_DUCKDB_PATH, reused
        as-is (no new connection opened) — pass the caller's own connection
        when calling from inside an already-open `with get_duckdb_connection(...)`
        block (e.g. run_orchestrator_backtest.py) to avoid DuckDB's
        single-read-write-connection-per-file limit. If None (standalone
        CLI use), opens and closes its own read-only connection.
    write_html : also render a sortable/filterable HTML page (2026-08-01
        user request) to HTML_OUT_DIR — off by default so every existing
        caller's behavior (CSV-only) is unaffected; the new Technical
        sweep scripts (scripts/run_technical_*.py) pass True.
    """
    out_path = out_path or REPORTS_DIR / f"trade_book_{run_id}.csv"
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    with open(trade_log_path, newline="") as fh:
        trades = list(csv.DictReader(fh))

    if conn is not None:
        _write(conn, run_id, trades, out_path, write_html=write_html)
    else:
        with get_duckdb_connection(BACKTEST_DUCKDB_PATH, read_only=True, persist=False) as owned_conn:
            _write(owned_conn, run_id, trades, out_path, write_html=write_html)

    logger.info(f"Trade book written to {out_path} ({len(trades)} trades)")
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Build an enriched trade-book CSV for one backtest run")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--trade-log-path", required=True)
    parser.add_argument("--write-html", action="store_true", help="Also render a sortable HTML trade book (backtest/reports/technical/html/)")
    args = parser.parse_args()
    export_trade_book(args.run_id, Path(args.trade_log_path), write_html=args.write_html)


if __name__ == "__main__":
    main()
