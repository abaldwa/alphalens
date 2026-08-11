#!/usr/bin/env python3
"""
scripts/prorate_trades_from_date.py

Re-cuts an ALREADY-COMPLETED backtest's trade book from a later start date, to
get a directional read without waiting for a re-run.

[2026-08-11] Written because the 2007-2026 TA sweep's first 10 years sit on
legacy OHLCV (Fyers only covers 2017+), where 518 broken corporate-action
adjustments across 434 tickers fabricate returns.

WHAT THIS CAN AND CANNOT TELL YOU
---------------------------------
VALID here - these are per-trade and independent of position sizing:
    trade count, win rate, median/mean pnl_pct, holding days, exit reasons

NOT VALID, and deliberately not computed:
    CAGR, final capital, rupee P&L, Sharpe, Sortino, max drawdown, taxes

The reason is compounding. A lump-capital backtest reinvests, so every position
opened after 2017 was sized from capital the 2007-2016 segment had already
inflated - one fake 187x gain in 2010 scales every later trade. The qty and
pnl_inr on those rows are therefore wrong, even though the rows fall inside the
clean window. Only pnl_pct (a pure price ratio) survives.

Nor is this equivalent to re-running from 2017-04-01: that changes which
positions are open at the boundary, how much cash is free at each rebalance,
and therefore which candidates get taken at all. Treat the output as a ranking
hint, not a result. The re-run is the answer.

Trades are filtered on buy_date, so anything ENTERED at a legacy-corrupted
price is excluded even if it closed inside the clean window.

Usage:
    python scripts/prorate_trades_from_date.py --from-date 2017-04-01
    python scripts/prorate_trades_from_date.py --from-date 2017-04-01 --min-trades 30
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import duckdb  # noqa: E402

from config.settings import BACKTEST_DUCKDB_PATH  # noqa: E402

_SQL = """
SELECT template_name,
       COUNT(*)                                                AS trades,
       ROUND(100.0 * AVG(CASE WHEN pnl_inr > 0 THEN 1.0 ELSE 0.0 END), 1) AS win_rate,
       ROUND(100.0 * MEDIAN(pnl_pct), 2)                       AS median_pct,
       ROUND(100.0 * AVG(pnl_pct), 2)                          AS mean_pct,
       ROUND(AVG(holding_days))                                AS avg_hold_d,
       SUM(CASE WHEN buy_price < 5 THEN 1 ELSE 0 END)          AS sub_rs5_entries
FROM backtest_trades
WHERE buy_date >= ?
GROUP BY 1
HAVING COUNT(*) >= ?
ORDER BY mean_pct DESC
"""


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--from-date", required=True, help="YYYY-MM-DD; trades ENTERED on/after this")
    p.add_argument("--min-trades", type=int, default=30)
    p.add_argument("--limit", type=int, default=20)
    p.add_argument("--db-path", default=str(BACKTEST_DUCKDB_PATH))
    args = p.parse_args()

    conn = duckdb.connect(args.db_path, read_only=True)
    try:
        df = conn.execute(_SQL, [args.from_date, args.min_trades]).fetchdf()
    finally:
        conn.close()

    print(f"Trades entered on/after {args.from_date} — equal-weight, sizing-independent.")
    print("Rupee P&L / CAGR / Sharpe deliberately omitted: those depend on position")
    print("sizes inherited from the contaminated pre-2017 capital base.\n")
    print(df.head(args.limit).to_string(index=False))


if __name__ == "__main__":
    main()
