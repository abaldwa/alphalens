"""
scripts/run_momentum_grid2.py

Phase: FeatureBacklog.md ML38 — momentum strategy implementation
Owner: Platform / Backtest
Consumers: operator CLI, scripts that build the momentum dashboard + YoY report

2026-07-18 user request: a second ML38 grid, distinct rank bands from the
original 5-band grid in scripts/run_momentum_experimentation.py —
100-150, 151-200, 201-250, 200-400 — crossed with 6/9/12-month lookbacks,
10/15/20-stock portfolios, weekly/fortnightly/monthly rebalancing, and
(new axis) grace_cycles in {0, 2, 5, 10}. 4 x 3 x 3 x 3 x 4 = 432 variants,
each with a full per-trade transaction ledger + equity curve (needed for
the separate year-on-year report, not just summary metrics).

Bands here are NOT added to features/momentum_universe.py's RANK_BANDS —
that constant is used elsewhere (dashboards, other scripts) and adding to
it would change unrelated behavior. yearly_band_universes_from_rankings()
takes rank_start/rank_end directly, so ad-hoc bands are passed straight
through without touching the shared constant.
"""

import argparse
import json
import logging
from datetime import date
from pathlib import Path
from typing import Dict, List

from backtest.momentum_orchestrator_runner import run_momentum_orchestrated
from backtest.core.metrics import cagr, churn_factor, total_return, xirr
from backtest.core.tax import compute_total_tax
from backtest.core.tax import post_tax_ending_value_from_dicts as post_tax_ending_value
from config.settings import DUCKDB_PATH
from config.timezone import now_ist
from datastore.api.db import get_duckdb_connection
from features.momentum_signal import lookback_trading_days, load_price_panel
from features.momentum_universe import all_yearly_full_rankings, yearly_band_universes_from_rankings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

STARTING_CAPITAL = 1_000_000.0
INVESTABLE_PCT = 0.8
SIP_MONTHLY_AMOUNT = 50_000.0

# (band_id, rank_start, rank_end) — ad-hoc bands for this grid only.
BANDS2 = [
    (101, 100, 150),
    (102, 151, 200),
    (103, 201, 250),
    (104, 200, 400),
]
LOOKBACK_MONTHS2 = [6, 9, 12]
REBALANCE_PERIODS2 = {"weekly": 5, "fortnightly": 10, "monthly": 21}
TOP_N_OPTIONS2 = [10, 15, 20]
# [H4, 2026-08-18] grace_cycles no longer exists on MomentumAdapter --
# deprecated by the 2026-08-18 user decision (§19: pure-play momentum is a
# plain rank rotation). This axis is collapsed to one placeholder value
# (band/lookback/rebalance/top_n stay real, independent axes) rather than
# silently sweeping a knob that no longer changes anything.
GRACE_CYCLES2 = [None]

REPORTS_DIR = Path(__file__).resolve().parent.parent / "backtest" / "reports" / "momentum"


def _union_tickers(yearly_rankings) -> List[str]:
    tickers = set()
    for ranked in yearly_rankings.values():
        if not ranked.empty:
            tickers.update(ranked["ticker"].tolist())
    return sorted(tickers)


def _summarize(result, top_n: int, grace_cycles: int) -> Dict:
    churn = churn_factor(result.rebalance_events)
    txns = result.transactions
    total_invested = sum(t["buy_price"] * t["qty"] for t in txns)
    total_sell_value = sum(t["sell_price"] * t["qty"] for t in txns if t["sell_price"] is not None)

    closed = [t for t in txns if t["status"] == "closed"]
    win_rate = (sum(1 for t in closed if t["sell_price"] > t["buy_price"]) / len(closed)) if closed else None

    total_tax = compute_total_tax(txns)
    post_tax_value = post_tax_ending_value(result.ending_value, txns)
    post_tax_cagr = cagr(result.starting_capital, post_tax_value, result.start_date, result.end_date)

    return {
        "top_n": top_n,
        "grace_cycles": grace_cycles,
        "starting_capital": result.starting_capital,
        "ending_value": result.ending_value,
        "start_date": result.start_date,
        "end_date": result.end_date,
        "total_return": total_return(result.starting_capital, result.ending_value),
        "cagr": cagr(result.starting_capital, result.ending_value, result.start_date, result.end_date),
        "churn_avg_transactions_per_year": churn["avg_transactions_per_year"],
        "n_rebalances": len(result.rebalance_events),
        "total_invested": total_invested,
        "total_sell_value": total_sell_value,
        "win_rate": win_rate,
        "n_closed_trades": len(closed),
        "n_open_trades": len(txns) - len(closed),
        "total_tax": total_tax,
        "post_tax_ending_value": post_tax_value,
        "post_tax_cagr": post_tax_cagr,
        "equity_curve": result.equity_curve,
        "transactions": txns,
    }


def _sip_summary(price_panel, yearly_universes, lookback_days, rebalance_days, top_n, grace_cycles) -> Dict:
    result = run_momentum_orchestrated(
        price_panel=price_panel,
        yearly_universes=yearly_universes,
        lookback_days=lookback_days,
        rebalance_every_n_trading_days=rebalance_days,
        starting_capital=STARTING_CAPITAL,
        investable_pct=INVESTABLE_PCT,
        top_n=top_n,
        sip_amount=SIP_MONTHLY_AMOUNT,
    )
    cash_flows = [(cf["date"], cf["amount"]) for cf in result.cash_flows]
    cash_flows.append((result.end_date, result.ending_value))
    sip_xirr = xirr(cash_flows)
    return {
        "sip_total_contributed": result.total_contributed,
        "sip_ending_value": result.ending_value,
        "sip_xirr": sip_xirr,
    }


def run_grid2(years_back: int = 10) -> Dict:
    end_date = now_ist().date()
    start_date = date(end_date.year - years_back, end_date.month, end_date.day)

    with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
        logger.info("Computing yearly full market-cap rankings (top 400) %s..%s", start_date, end_date)
        yearly_rankings = all_yearly_full_rankings(
            conn, start_date.isoformat(), end_date.isoformat(), max_rank=400, include_delisted=True,
        )  # 2026-07-20 survivorship-bias fix — BacktestUmbrellaPlan.md Gap #1
        if not yearly_rankings:
            raise RuntimeError("No real ohlcv_adjusted rows found in the requested date range — cannot run.")

        candidate_tickers = _union_tickers(yearly_rankings)
        logger.info("Loading price panel for %d candidate tickers over %s..%s", len(candidate_tickers), start_date, end_date)
        price_panel = load_price_panel(conn, candidate_tickers, start_date.isoformat(), end_date.isoformat())
        if price_panel.empty:
            raise RuntimeError("Price panel came back empty for the candidate ticker set — cannot run.")

    variants = []
    total = len(BANDS2) * len(LOOKBACK_MONTHS2) * len(REBALANCE_PERIODS2) * len(TOP_N_OPTIONS2) * len(GRACE_CYCLES2)
    done = 0
    for band_id, rank_start, rank_end in BANDS2:
        yearly_universes = yearly_band_universes_from_rankings(yearly_rankings, rank_start, rank_end)
        for lookback_months in LOOKBACK_MONTHS2:
            lookback_days = lookback_trading_days(lookback_months)
            for rebalance_name, rebalance_days in REBALANCE_PERIODS2.items():
                for top_n in TOP_N_OPTIONS2:
                    for grace_cycles in GRACE_CYCLES2:
                        done += 1
                        logger.info(
                            "[%d/%d] band=%d (rank %d-%d) lookback=%dmo rebalance=%s top_n=%d grace=%s",
                            done, total, band_id, rank_start, rank_end, lookback_months, rebalance_name, top_n, grace_cycles,
                        )
                        result = run_momentum_orchestrated(
                            price_panel=price_panel,
                            yearly_universes=yearly_universes,
                            lookback_days=lookback_days,
                            rebalance_every_n_trading_days=rebalance_days,
                            starting_capital=STARTING_CAPITAL,
                            investable_pct=INVESTABLE_PCT,
                            top_n=top_n,
                        )
                        summary = _summarize(result, top_n, grace_cycles)
                        sip = _sip_summary(price_panel, yearly_universes, lookback_days, rebalance_days, top_n, grace_cycles)
                        variants.append({
                            "band_id": band_id,
                            "rank_start": rank_start,
                            "rank_end": rank_end,
                            "lookback_months": lookback_months,
                            "rebalance_period": rebalance_name,
                            **summary,
                            **sip,
                        })

    return {"generated_at": now_ist().isoformat(), "variants": variants}


def main():
    parser = argparse.ArgumentParser(description="Run ML38 momentum strategy grid #2 (100-150/151-200/201-250/200-400 bands)")
    parser.add_argument("--years-back", type=int, default=10)
    args = parser.parse_args()

    report = run_grid2(years_back=args.years_back)

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = REPORTS_DIR / f"momentum_grid2_{now_ist().strftime('%Y%m%d_%H%M%S')}.json"
    with open(out_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    logger.info("Wrote report to %s (%d variants)", out_path, len(report["variants"]))


if __name__ == "__main__":
    main()
