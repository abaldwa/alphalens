"""
scripts/run_momentum_downtrend_filter_comparison.py

Phase: FeatureBacklog.md ML38 — momentum strategy implementation
Owner: Platform / Backtest
Consumers: operator CLI

2026-07-15 user request: for comparison purposes, rerun the full ML38
300-variant grid (5 market-cap rank bands x 4 lookbacks x 5 rebalance
cadences x 3 portfolio sizes) with an added short-term reversal filter —
a ticker is excluded from this rebalance's picks if its trailing 20-
trading-day return is a >=5% drop (downtrend_filter_pct=0.05 on
MomentumBacktester), even if its main-lookback momentum still ranks it
in the top-N. The idea: skip names whose longer-lookback momentum score
looks good but that have already started reversing hard recently.
Summary metrics only (no per-trade ledger, no equity curve) — this is a
sensitivity comparison against the existing no-filter baseline in
backtest/reports/momentum/, not a new primary dataset.
"""

import argparse
import json
import logging
from datetime import date
from pathlib import Path
from typing import Dict, List

from backtest.momentum_backtest import MomentumBacktester
from backtest.momentum_metrics import cagr, churn_factor
from backtest.momentum_tax import post_tax_ending_value
from config.settings import DUCKDB_PATH
from config.timezone import now_ist
from datastore.api.db import get_duckdb_connection
from features.momentum_signal import LOOKBACK_MONTHS, lookback_trading_days, load_price_panel
from features.momentum_universe import RANK_BANDS, all_yearly_full_rankings, yearly_band_universes_from_rankings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

STARTING_CAPITAL = 1_000_000.0
INVESTABLE_PCT = 0.8
TOP_N_OPTIONS = [10, 15, 20]
REBALANCE_PERIODS = {"weekly": 5, "biweekly": 10, "monthly": 21, "bimonthly": 42, "quarterly": 63}
DOWNTREND_FILTER_PCT = 0.05
DOWNTREND_LOOKBACK_DAYS = 20

REPORTS_DIR = Path(__file__).resolve().parent.parent / "backtest" / "reports" / "momentum"


def _union_tickers(yearly_rankings) -> List[str]:
    tickers = set()
    for ranked in yearly_rankings.values():
        if not ranked.empty:
            tickers.update(ranked["ticker"].tolist())
    return sorted(tickers)


def run(years_back: int, grace_cycles: int) -> Dict:
    end_date = now_ist().date()
    start_date = date(end_date.year - years_back, end_date.month, end_date.day)

    with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
        yearly_rankings = all_yearly_full_rankings(
            conn, start_date.isoformat(), end_date.isoformat(), include_delisted=True,
        )  # 2026-07-20 survivorship-bias fix — BacktestUmbrellaPlan.md Gap #1
        candidate_tickers = _union_tickers(yearly_rankings)
        price_panel = load_price_panel(conn, candidate_tickers, start_date.isoformat(), end_date.isoformat())

    variants = []
    for band_id, rank_start, rank_end in RANK_BANDS:
        yearly_universes = yearly_band_universes_from_rankings(yearly_rankings, rank_start, rank_end)
        for lookback_months in LOOKBACK_MONTHS:
            lookback_days = lookback_trading_days(lookback_months)
            for rebalance_name, rebalance_days in REBALANCE_PERIODS.items():
                for top_n in TOP_N_OPTIONS:
                    logger.info(
                        "grace=%d band=%d lookback=%dmo rebalance=%s top_n=%d",
                        grace_cycles, band_id, lookback_months, rebalance_name, top_n,
                    )
                    engine = MomentumBacktester(
                        price_panel=price_panel,
                        yearly_universes=yearly_universes,
                        lookback_days=lookback_days,
                        rebalance_every_n_trading_days=rebalance_days,
                        starting_capital=STARTING_CAPITAL,
                        investable_pct=INVESTABLE_PCT,
                        top_n=top_n,
                        grace_cycles=grace_cycles,
                        downtrend_filter_pct=DOWNTREND_FILTER_PCT,
                        downtrend_lookback_days=DOWNTREND_LOOKBACK_DAYS,
                    )
                    result = engine.run()
                    churn = churn_factor(result.rebalance_events)
                    post_tax_value = post_tax_ending_value(result.ending_value, result.transactions)
                    closed = [t for t in result.transactions if t["status"] == "closed"]
                    win_rate = (
                        sum(1 for t in closed if t["sell_price"] > t["buy_price"]) / len(closed)
                        if closed else None
                    )
                    variants.append({
                        "downtrend_filter_pct": DOWNTREND_FILTER_PCT,
                        "grace_cycles": grace_cycles,
                        "band_id": band_id,
                        "rank_start": rank_start,
                        "rank_end": rank_end,
                        "lookback_months": lookback_months,
                        "rebalance_period": rebalance_name,
                        "top_n": top_n,
                        "cagr": cagr(result.starting_capital, result.ending_value, result.start_date, result.end_date),
                        "post_tax_cagr": cagr(result.starting_capital, post_tax_value, result.start_date, result.end_date),
                        "churn_avg_transactions_per_year": churn["avg_transactions_per_year"],
                        "win_rate": win_rate,
                        "ending_value": result.ending_value,
                        "n_trades": len(result.transactions),
                    })

    return {"generated_at": now_ist().isoformat(), "variants": variants}


def main():
    parser = argparse.ArgumentParser(description="ML38 short-term-downtrend-filter comparison (5% / 20d)")
    parser.add_argument("--years-back", type=int, default=10)
    parser.add_argument("--grace-cycles", type=int, default=2)
    args = parser.parse_args()

    report = run(years_back=args.years_back, grace_cycles=args.grace_cycles)

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = (
        REPORTS_DIR
        / f"momentum_downtrend_filter_comparison_grace{args.grace_cycles}_{now_ist().strftime('%Y%m%d_%H%M%S')}.json"
    )
    with open(out_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    logger.info("Wrote report to %s", out_path)


if __name__ == "__main__":
    main()
