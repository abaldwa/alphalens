"""
scripts/build_momentum_yoy_report.py

Phase: FeatureBacklog.md ML38 — momentum strategy YoY report
Owner: Platform / Backtest
Consumers: scripts that build the YoY HTML page

2026-07-18 user request: a year-on-year (April 1 - March 31, Indian FY)
breakdown for every variant in scripts/run_momentum_grid2.py's 432-variant
grid: Starting Capital (Cash+Stocks on hand), Ending Capital (Cash+M2M
value of stocks, net of capital-gains tax), Return for the year, Churn,
Average Holding Period, and Nifty Midcap 150 / Nifty Smallcap 250 returns
for the same FY (from scripts/build_momentum_benchmark_db.py's local
benchmark DuckDB — real data 2023-07 onward only, blank for earlier FYs
per 2026-07-18 user decision).

Simplifications (stated, not silent):
  - "Ending Capital" at an FY boundary = the equity curve's mark-to-market
    total_value at the closest engine snapshot on/before that FY-end date,
    minus CUMULATIVE capital-gains tax on every transaction closed on or
    before that date (backtest/momentum_tax.py rates). This is a
    running post-tax NAV, not a per-year tax settlement — real Indian tax
    is filed and paid annually per FY's own realized gains, but this
    module doesn't attempt inter-year tax-lot bookkeeping (matches
    backtest/momentum_tax.py's own stated simplifications: no loss
    set-off, no LTCG exemption threshold).
  - The engine's equity_curve only has one point per rebalance (not
    daily), so FY-boundary values are the nearest available snapshot
    on/before Apr 1 / Mar 31 — not the literal calendar-day value.
  - "Churn" for a year = count of transactions whose sell_date falls in
    that FY (positions closed during the year, including grace-period
    forced sells). "Average Holding Period" = mean holding_days of those
    same closed transactions. Positions still open at FY end are not
    counted in either metric for that year (consistent with how
    backtest/momentum_metrics.py's churn_factor already treats churn as a
    realized-transaction count, not open-position turnover).

Writes DuckDB temp tables (backtest/reports/momentum/momentum_yoy.duckdb,
table `yoy_report`) for further analysis, and a compact JSON for the
standalone YoY HTML page.
"""

import argparse
import json
import logging
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional

import duckdb
import pandas as pd

from backtest.core.tax import compute_transaction_tax

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

REPORTS_DIR = Path(__file__).resolve().parent.parent / "backtest" / "reports" / "momentum"
YOY_DB = REPORTS_DIR / "momentum_yoy.duckdb"


def _fy_boundaries(start_date: str, end_date: str) -> List[Dict]:
    """[{fy_label, fy_start, fy_end}] for every Apr1-Mar31 FY overlapping
    [start_date, end_date], clipped to the backtest's actual date range."""
    sd = date.fromisoformat(start_date)
    ed = date.fromisoformat(end_date)
    fy_start_year = sd.year if sd.month >= 4 else sd.year - 1
    fys = []
    y = fy_start_year
    while date(y, 4, 1) <= ed:
        fy_start = max(date(y, 4, 1), sd)
        fy_end = min(date(y + 1, 3, 31), ed)
        if fy_start <= fy_end:
            fys.append({
                "fy_label": f"FY{y % 100:02d}-{(y + 1) % 100:02d}",
                "fy_start": fy_start.isoformat(),
                "fy_end": fy_end.isoformat(),
            })
        y += 1
    return fys


def _nearest_value_on_or_before(equity_curve: List[Dict], target: str) -> Optional[float]:
    best = None
    for pt in equity_curve:
        if pt["date"] <= target:
            best = pt["total_value"]
        else:
            break
    return best


def _cumulative_tax_through(transactions: List[Dict], through_date: str) -> float:
    total = 0.0
    for t in transactions:
        if t["status"] == "closed" and t["sell_date"] is not None and t["sell_date"] <= through_date:
            total += compute_transaction_tax(t)
        elif t["status"] == "open" and t["sell_date"] is not None and t["sell_date"] <= through_date:
            # open positions are marked-to-market with a sell_date/sell_price
            # by the engine at the final backtest date only; treat the same
            # way post_tax_ending_value() does for a single point-in-time NAV.
            total += compute_transaction_tax(t)
    return total


def _benchmark_return(conn, index_name: str, fy_start: str, fy_end: str) -> Optional[float]:
    row_start = conn.execute(
        "SELECT close FROM benchmark_index WHERE index_name=? AND date<=? ORDER BY date DESC LIMIT 1",
        [index_name, fy_start],
    ).fetchone()
    row_end = conn.execute(
        "SELECT close FROM benchmark_index WHERE index_name=? AND date<=? ORDER BY date DESC LIMIT 1",
        [index_name, fy_end],
    ).fetchone()
    if not row_start or not row_end:
        return None
    start_close, end_close = row_start[0], row_end[0]
    if start_close <= 0:
        return None
    return (end_close / start_close) - 1.0


def build_yoy(report: Dict, bench_conn) -> List[Dict]:
    rows = []
    for v in report["variants"]:
        variant_id = (
            f"b{v['band_id']}_{v['rank_start']}-{v['rank_end']}_lb{v['lookback_months']}mo_"
            f"{v['rebalance_period']}_top{v['top_n']}_g{v['grace_cycles']}"
        )
        equity_curve = v["equity_curve"]
        transactions = v["transactions"]
        fys = _fy_boundaries(v["start_date"], v["end_date"])

        prev_post_tax_value = v["starting_capital"]
        for fy in fys:
            raw_start_value = _nearest_value_on_or_before(equity_curve, fy["fy_start"])
            raw_end_value = _nearest_value_on_or_before(equity_curve, fy["fy_end"])
            if raw_end_value is None:
                continue
            tax_through_end = _cumulative_tax_through(transactions, fy["fy_end"])
            post_tax_end_value = raw_end_value - tax_through_end
            starting_value = prev_post_tax_value if raw_start_value is not None else prev_post_tax_value
            fy_return = (post_tax_end_value / starting_value - 1.0) if starting_value > 0 else None

            closed_this_fy = [
                t for t in transactions
                if t["status"] == "closed" and t["sell_date"] is not None
                and fy["fy_start"] <= t["sell_date"] <= fy["fy_end"]
            ]
            churn = len(closed_this_fy)
            avg_holding_days = (
                sum(t["holding_days"] for t in closed_this_fy) / len(closed_this_fy)
                if closed_this_fy else None
            )

            midcap_ret = _benchmark_return(bench_conn, "nifty_midcap_150", fy["fy_start"], fy["fy_end"])
            smallcap_ret = _benchmark_return(bench_conn, "nifty_smallcap_250", fy["fy_start"], fy["fy_end"])

            rows.append({
                "variant_id": variant_id,
                "band_id": v["band_id"], "rank_start": v["rank_start"], "rank_end": v["rank_end"],
                "lookback_months": v["lookback_months"], "rebalance_period": v["rebalance_period"],
                "top_n": v["top_n"], "grace_cycles": v["grace_cycles"],
                "fy_label": fy["fy_label"], "fy_start": fy["fy_start"], "fy_end": fy["fy_end"],
                "starting_capital": starting_value,
                "ending_capital": post_tax_end_value,
                "return_pct": fy_return * 100.0 if fy_return is not None else None,
                "churn": churn,
                "avg_holding_days": avg_holding_days,
                "nifty_midcap_150_return_pct": midcap_ret * 100.0 if midcap_ret is not None else None,
                "nifty_smallcap_250_return_pct": smallcap_ret * 100.0 if smallcap_ret is not None else None,
            })
            prev_post_tax_value = post_tax_end_value
    return rows


def main():
    parser = argparse.ArgumentParser(description="Build ML38 grid2 year-on-year (Apr-Mar) report")
    parser.add_argument("--grid2-report", required=True, help="Path to momentum_grid2_*.json")
    args = parser.parse_args()

    report = json.loads(Path(args.grid2_report).read_text())
    logger.info("Loaded %d variants from %s", len(report["variants"]), args.grid2_report)

    bench_conn = duckdb.connect(str(YOY_DB))
    rows = build_yoy(report, bench_conn)
    logger.info("Built %d (variant, FY) rows", len(rows))

    df = pd.DataFrame(rows)
    bench_conn.execute("DROP TABLE IF EXISTS yoy_report")
    bench_conn.register("df_view", df)
    bench_conn.execute("CREATE TABLE yoy_report AS SELECT * FROM df_view")
    bench_conn.close()
    logger.info("Wrote yoy_report table to %s (%d rows)", YOY_DB, len(rows))

    out_json = REPORTS_DIR / "momentum_yoy_report.json"
    out_json.write_text(json.dumps(rows, default=str))
    logger.info("Wrote %s", out_json)


if __name__ == "__main__":
    main()
