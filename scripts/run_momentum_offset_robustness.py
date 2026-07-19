"""
scripts/run_momentum_offset_robustness.py

Phase: FeatureBacklog.md ML38 — momentum strategy refinement
Owner: Platform / Backtest
Consumers: operator CLI (`python3 -m scripts.run_momentum_offset_robustness`)

2026-07-14 user follow-up: the refinement sweep found a sharp CAGR spike
at rebalance=45 calendar days (top_n=9: 40cd->26.6%, 45cd->34.0%,
50cd->18.2%) — a cliff shape rather than a smooth optimum, which is the
classic signature of a rebalance schedule that happens to align with a
handful of lucky/unlucky price moves in this one 10-year window rather
than a genuine, date-independent momentum effect.

This script tests that directly: for each of 3 candidate variants, hold
every parameter fixed and shift ONLY which trading day the rebalance
schedule starts counting from (rebalance_offset_days = 0..10, i.e. the
whole schedule slides by 1 trading day at a time for 10 steps — see
MomentumBacktester.rebalance_offset_days). If CAGR stays roughly stable
across offsets, the edge is real; if it swings wildly, it's a hindsight
artifact of this specific offset lining up with specific dates.

Candidates (2026-07-14 user request):
  - "sweep winner": top_n=9, 6mo lookback, 45cd rebalance (32 trading
    days), grace=sell_after_1_rebalance (1 cycle) — the new best found by
    the refinement sweep.
  - "v1 sweep best": top_n=11, 6mo lookback, 45cd rebalance (32 trading
    days), grace=sell_after_2_rebalances (2 cycles) — the first
    refinement sweep's best, also requested to be kept under
    consideration.
  - "original best": top_n=15, 6mo lookback, monthly rebalance (21
    trading days), grace=2 cycles (GRACE_CYCLES default) — the original
    240-variant grid's best (23.59% CAGR), used as a stability baseline
    since it wasn't found via this offset-sensitive sweep.
"""

import argparse
import json
import logging
from datetime import date
from pathlib import Path

from backtest.momentum_backtest import MomentumBacktester
from backtest.momentum_metrics import cagr
from backtest.momentum_tax import post_tax_ending_value
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

OFFSETS = list(range(0, 11))  # 0..10 trading days

CANDIDATES = [
    {"label": "sweep_winner_9stk_45cd", "top_n": 9, "lookback_months": 6, "rebalance_trading_days": 32, "grace_cycles": 1},
    {"label": "v1_best_11stk_45cd", "top_n": 11, "lookback_months": 6, "rebalance_trading_days": 32, "grace_cycles": 2},
    {"label": "original_best_15stk_monthly", "top_n": 15, "lookback_months": 6, "rebalance_trading_days": 21, "grace_cycles": 2},
]


def _run_one(price_panel, yearly_universes, candidate, offset_days):
    lookback_days = lookback_trading_days(candidate["lookback_months"])
    engine = MomentumBacktester(
        price_panel=price_panel,
        yearly_universes=yearly_universes,
        lookback_days=lookback_days,
        rebalance_every_n_trading_days=candidate["rebalance_trading_days"],
        starting_capital=STARTING_CAPITAL,
        investable_pct=INVESTABLE_PCT,
        top_n=candidate["top_n"],
        grace_cycles=candidate["grace_cycles"],
        rebalance_offset_days=offset_days,
    )
    result = engine.run()
    post_tax_value = post_tax_ending_value(result.ending_value, result.transactions)
    return {
        "label": candidate["label"],
        "top_n": candidate["top_n"],
        "lookback_months": candidate["lookback_months"],
        "rebalance_trading_days": candidate["rebalance_trading_days"],
        "grace_cycles": candidate["grace_cycles"],
        "offset_days": offset_days,
        "cagr": cagr(result.starting_capital, result.ending_value, result.start_date, result.end_date),
        "post_tax_cagr": cagr(result.starting_capital, post_tax_value, result.start_date, result.end_date),
        "ending_value": result.ending_value,
        "n_rebalances": len(result.rebalance_events),
    }


def run_offset_robustness(years_back: int = 10) -> dict:
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

    runs = []
    total = len(CANDIDATES) * len(OFFSETS)
    i = 0
    for candidate in CANDIDATES:
        for offset_days in OFFSETS:
            i += 1
            logger.info("[%d/%d] %s offset=%d", i, total, candidate["label"], offset_days)
            runs.append(_run_one(price_panel, yearly_universes, candidate, offset_days))

    return {"generated_at": now_ist().isoformat(), "band_id": BAND_ID, "rank_start": RANK_START, "rank_end": RANK_END, "runs": runs}


REPORTS_DIR = Path(__file__).resolve().parent.parent / "backtest" / "reports" / "momentum"


def main():
    parser = argparse.ArgumentParser(description="ML38 rebalance-offset robustness check")
    parser.add_argument("--years-back", type=int, default=10)
    args = parser.parse_args()

    report = run_offset_robustness(years_back=args.years_back)

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = REPORTS_DIR / f"momentum_offset_robustness_{now_ist().strftime('%Y%m%d_%H%M%S')}.json"
    with open(out_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    logger.info("Wrote report to %s", out_path)

    print(f"\n{'label':<32}{'offset':<8}{'CAGR':>9}{'PostTaxCAGR':>13}")
    for r in report["runs"]:
        print(f"{r['label']:<32}{r['offset_days']:<8}{r['cagr']*100:>8.2f}%{r['post_tax_cagr']*100:>12.2f}%")

    print("\nPer-candidate CAGR spread across offsets 0-10:")
    for candidate in CANDIDATES:
        label = candidate["label"]
        cagrs = [r["cagr"] for r in report["runs"] if r["label"] == label]
        print(f"{label:<32} min={min(cagrs)*100:6.2f}%  max={max(cagrs)*100:6.2f}%  spread={(max(cagrs)-min(cagrs))*100:6.2f}pp  offset0={cagrs[0]*100:6.2f}%")


if __name__ == "__main__":
    main()
