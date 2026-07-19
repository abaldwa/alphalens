"""
scripts/run_momentum_refinement_v2.py

Phase: FeatureBacklog.md ML38 — momentum strategy refinement
Owner: Platform / Backtest
Consumers: operator CLI (`python3 -m scripts.run_momentum_refinement_v2`)

2026-07-14 user follow-up to the first refinement sweep
(scripts/run_momentum_refinement.py): that 480-variant sweep found its
best CAGR at 11 stocks / 6mo lookback / 45-calendar-day rebalance
(29.83% CAGR). The user asked to dig further in the immediate
neighborhood: rebalance 45 +/- 5 calendar days, and 9-13 stocks. Lookback
is fixed at 6 months (it dominated every top result in the first sweep,
so this pass doesn't re-sweep it). Fixes the rank band at 100-150 and
sweeps:
  - top_n: 9..13 stocks
  - lookback: 6 months only
  - grace: same 3 engine-native settings as the first sweep (0/1/2
    rebalance cycles held before force-sell)
  - rebalance: 40, 45, 50 calendar days, converted to trading days at
    the codebase's existing 21-trading-days-per-month convention.

5 x 1 x 3 x 3 = 45 variants, single run each (no SIP pass, same as the
first refinement sweep).
"""

import argparse
import json
import logging
from datetime import date
from pathlib import Path

from backtest.momentum_backtest import MomentumBacktester
from backtest.momentum_metrics import cagr, churn_factor, total_return
from backtest.momentum_tax import compute_total_tax, post_tax_ending_value
from config.settings import DUCKDB_PATH
from config.timezone import now_ist
from datastore.api.db import get_duckdb_connection
from features.momentum_signal import lookback_trading_days, load_price_panel
from features.momentum_universe import all_yearly_full_rankings, yearly_band_universes_from_rankings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

STARTING_CAPITAL = 1_000_000.0
INVESTABLE_PCT = 0.8

BAND_ID, RANK_START, RANK_END = 3, 100, 150

TOP_N_OPTIONS = [9, 10, 11, 12, 13]
LOOKBACK_MONTHS_OPTIONS = [6]

# grace_cycles: engine-native unit (rebalance cycles a dropped-out name is
# held before force-sell). See module docstring for the month->cycle note.
GRACE_OPTIONS = {"sell_immediately": 0, "sell_after_1_rebalance": 1, "sell_after_2_rebalances": 2}

# calendar days -> trading days at 21 trading days / 30 calendar days.
REBALANCE_CALENDAR_DAYS = [40, 45, 50]


def _calendar_days_to_trading_days(calendar_days: int) -> int:
    return max(1, round(calendar_days * 21 / 30))


def _summarize(result, top_n: int, grace_label: str, rebalance_calendar_days: int) -> dict:
    churn = churn_factor(result.rebalance_events)
    txns = result.transactions
    total_invested = sum(t["buy_price"] * t["qty"] for t in txns)
    total_sell_value = sum(t["sell_price"] * t["qty"] for t in txns if t["sell_price"] is not None)
    txn_total_return = (total_sell_value - total_invested) / total_invested if total_invested > 0 else 0.0

    closed = [t for t in txns if t["status"] == "closed"]
    win_rate = (sum(1 for t in closed if t["sell_price"] > t["buy_price"]) / len(closed)) if closed else None

    total_tax = compute_total_tax(txns)
    post_tax_value = post_tax_ending_value(result.ending_value, txns)
    post_tax_cagr = cagr(result.starting_capital, post_tax_value, result.start_date, result.end_date)

    return {
        "top_n": top_n,
        "grace_label": grace_label,
        "rebalance_calendar_days": rebalance_calendar_days,
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
        "txn_total_return": txn_total_return,
        "win_rate": win_rate,
        "n_closed_trades": len(closed),
        "n_open_trades": len(txns) - len(closed),
        "total_tax": total_tax,
        "post_tax_ending_value": post_tax_value,
        "post_tax_cagr": post_tax_cagr,
    }


def run_refinement(years_back: int = 10) -> dict:
    end_date = now_ist().date()
    start_date = date(end_date.year - years_back, end_date.month, end_date.day)

    with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
        logger.info("Computing yearly full market-cap rankings (top 200) %s..%s", start_date, end_date)
        yearly_rankings = all_yearly_full_rankings(conn, start_date.isoformat(), end_date.isoformat())
        if not yearly_rankings:
            raise RuntimeError("No real ohlcv_adjusted rows found in the requested date range — cannot run.")

        yearly_universes = yearly_band_universes_from_rankings(yearly_rankings, RANK_START, RANK_END)
        candidate_tickers = sorted({t for u in yearly_universes.values() for t in u})
        logger.info("Loading price panel for %d candidate tickers over %s..%s", len(candidate_tickers), start_date, end_date)
        price_panel = load_price_panel(conn, candidate_tickers, start_date.isoformat(), end_date.isoformat())
        if price_panel.empty:
            raise RuntimeError("Price panel came back empty for the candidate ticker set — cannot run.")

    variants = []
    total = len(TOP_N_OPTIONS) * len(LOOKBACK_MONTHS_OPTIONS) * len(GRACE_OPTIONS) * len(REBALANCE_CALENDAR_DAYS)
    i = 0
    for lookback_months in LOOKBACK_MONTHS_OPTIONS:
        lookback_days = lookback_trading_days(lookback_months)
        for rebalance_calendar_days in REBALANCE_CALENDAR_DAYS:
            rebalance_trading_days = _calendar_days_to_trading_days(rebalance_calendar_days)
            for grace_label, grace_cycles in GRACE_OPTIONS.items():
                for top_n in TOP_N_OPTIONS:
                    i += 1
                    logger.info(
                        "[%d/%d] lookback=%dmo rebalance=%dcd(%dtd) grace=%s top_n=%d",
                        i, total, lookback_months, rebalance_calendar_days, rebalance_trading_days, grace_label, top_n,
                    )
                    engine = MomentumBacktester(
                        price_panel=price_panel,
                        yearly_universes=yearly_universes,
                        lookback_days=lookback_days,
                        rebalance_every_n_trading_days=rebalance_trading_days,
                        starting_capital=STARTING_CAPITAL,
                        investable_pct=INVESTABLE_PCT,
                        top_n=top_n,
                        grace_cycles=grace_cycles,
                    )
                    result = engine.run()
                    summary = _summarize(result, top_n, grace_label, rebalance_calendar_days)
                    variants.append({
                        "band_id": BAND_ID,
                        "rank_start": RANK_START,
                        "rank_end": RANK_END,
                        "lookback_months": lookback_months,
                        "rebalance_trading_days": rebalance_trading_days,
                        **summary,
                    })

    return {"generated_at": now_ist().isoformat(), "variants": variants}


REPORTS_DIR = Path(__file__).resolve().parent.parent / "backtest" / "reports" / "momentum"


def main():
    parser = argparse.ArgumentParser(description="ML38 Band3 (rank 100-150) refinement sweep")
    parser.add_argument("--years-back", type=int, default=10)
    args = parser.parse_args()

    report = run_refinement(years_back=args.years_back)

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = REPORTS_DIR / f"momentum_refinement_v2_{now_ist().strftime('%Y%m%d_%H%M%S')}.json"
    with open(out_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    logger.info("Wrote report to %s", out_path)

    summary = sorted(report["variants"], key=lambda v: v["cagr"], reverse=True)
    print("\nTop 15 variants by CAGR:")
    print(f"{'top_n':<7}{'lookback':<10}{'reb_cd':<8}{'grace':<24}{'CAGR':>9}{'PostTaxCAGR':>13}{'WinRate':>9}{'AvgChurn/yr':>14}")
    for v in summary[:15]:
        wr = f"{v['win_rate']*100:.1f}%" if v["win_rate"] is not None else "n/a"
        print(
            f"{v['top_n']:<7}{v['lookback_months']:<10}{v['rebalance_calendar_days']:<8}{v['grace_label']:<24}"
            f"{v['cagr']*100:>8.2f}%{v['post_tax_cagr']*100:>12.2f}%{wr:>9}{v['churn_avg_transactions_per_year']:>14.1f}"
        )


if __name__ == "__main__":
    main()
