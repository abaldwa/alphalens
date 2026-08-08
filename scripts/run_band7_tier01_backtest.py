"""
scripts/run_band7_tier01_backtest.py

LIMITED backtest (2026-08-08, Tier 0 + Tier 1 review) — Band 7 (rank
501-800) only, the "lottery" band that produced the +139/+146/+114% FYs in
the winning variant `max_defensive_b7_501-800_lb6mo_bimonthly_top10`.

Compares the baseline engine (with the Tier 0 fixes baked in: adtv_cr
passthrough to _one_leg_cost + post-tax scoring) against four Tier 0+Tier 1
variants that add the two "ride winners, cut losers" mechanisms:

  - exit_rank       (15, 20): hold a name as long as its momentum rank is
                    <= exit_rank, not just within the top_n buy set.
  - trailing_stop_pct (0.25, 0.30): daily max-drawdown-from-peak stop,
                    cut a crashed name between rebalances.

Everything reuses the exact helper functions and config constants from
scripts/run_momentum_dynamic_report.py so the comparison is apples-to-
apples with the full sweep, but only Band 7 x the single winning
configuration (lb6mo / bimonthly / top10 / max_defensive) is run —
1 baseline + 4 variants = 5 backtests, each with the same 10-year window.

Usage:
    python scripts/run_band7_tier01_backtest.py [--years-back 10]
"""

import argparse
import logging
import sys
from datetime import date
from pathlib import Path
from typing import Dict, List

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from backtest.momentum_backtest import MomentumBacktester
from backtest.momentum_metrics import cagr, churn_factor, sharpe_sortino_calmar, win_rate
from backtest.momentum_tax import post_tax_ending_value
from config.settings import DUCKDB_PATH
from config.timezone import now_ist
from datastore.api.db import get_duckdb_connection
from features.momentum_signal import build_momentum_panel, load_price_panel, load_volume_panel, lookback_trading_days
from features.momentum_universe import all_yearly_full_rankings, yearly_band_universes_from_rankings
from scripts.run_momentum_dynamic_report import (
    GRACE_CYCLES,
    INVESTABLE_PCT,
    REBALANCE_PERIODS,
    STARTING_CAPITAL,
    _build_market_cap_panel,
    _build_strategies,
    _load_beta_map,
    _load_quality_scores,
    _load_regime_series,
    _load_static_shares_outstanding,
    _union_tickers,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# The single winning configuration from the full sweep (post-tax-scored):
# strategy / band / lookback / rebalance / top_n.
STRATEGY = "max_defensive"
BAND_ID, RANK_START, RANK_END = 7, 501, 800
LOOKBACK_MONTHS = 6
REBALANCE_NAME = "bimonthly"
TOP_N = 10

# Tier 1 sweep grid.
EXIT_RANKS = [15, 20]
TRAILING_STOPS = [0.25, 0.30]


def _fy_buckets(equity_curve: List[Dict]) -> Dict[str, List[Dict]]:
    """Group an equity curve (list of {"date","total_value"}) into Indian
    fiscal years (Apr 1 .. Mar 31) so each variant's YoY return profile can
    be shown — the shape that exposed the +139/+146/+114% lottery years."""
    buckets: Dict[str, List[Dict]] = {}
    for pt in equity_curve:
        d = pd.Timestamp(pt["date"])
        fy_year = d.year if d.month >= 4 else d.year - 1
        fy = f"FY{fy_year}-{str(fy_year + 1)[-2:]}"
        buckets.setdefault(fy, []).append(pt)
    return buckets


def _fy_cagrs(equity_curve: List[Dict]) -> Dict[str, float]:
    """Per-fiscal-year CAGR from first to last equity point in each FY."""
    out: Dict[str, float] = {}
    for fy, pts in _fy_buckets(equity_curve).items():
        if len(pts) < 2:
            continue
        first, last = pts[0], pts[-1]
        c = cagr(first["total_value"], last["total_value"], first["date"], last["date"])
        out[fy] = c
    return out


def _run_variant(kwargs: Dict, price_panel, yearly_universes: Dict, lookback_days: int,
                 rebalance_days: int, momentum_panel, **extra):
    engine = MomentumBacktester(
        price_panel=price_panel,
        yearly_universes=yearly_universes,
        lookback_days=lookback_days,
        rebalance_every_n_trading_days=rebalance_days,
        grace_cycles=GRACE_CYCLES,
        momentum_panel=momentum_panel,
        **kwargs,
        **extra,
    )
    return engine.run()


def _summarise(result, label: str, start_date: str, end_date: str) -> Dict:
    churn = churn_factor(result.rebalance_events)
    post_tax_value = post_tax_ending_value(result.ending_value, result.transactions)
    post_tax_cagr = cagr(result.starting_capital, post_tax_value, start_date, end_date)
    raw_cagr = cagr(result.starting_capital, result.ending_value, start_date, end_date)
    ratios = sharpe_sortino_calmar(result.equity_curve, raw_cagr)
    closed = [t for t in result.transactions if t["status"] == "closed"]
    avg_days_held = (sum(t["holding_days"] for t in closed) / len(closed)) if closed else None
    return {
        "label": label,
        "post_tax_cagr": post_tax_cagr,
        "cagr": raw_cagr,
        "sharpe": ratios["sharpe"],
        "sortino": ratios["sortino"],
        "max_drawdown": ratios["max_drawdown"],
        "win_rate": win_rate(result.transactions),
        "avg_days_held": avg_days_held,
        "churn_yr": churn["avg_transactions_per_year"],
        "n_closed": len(closed),
        "n_open": len(result.transactions) - len(closed),
        "value_10L": result.ending_value,
        "post_tax_value_10L": post_tax_value,
        "fy_cagrs": _fy_cagrs(result.equity_curve),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--years-back", type=int, default=10)
    args = ap.parse_args()

    end_date = now_ist().date()
    start_date = date(end_date.year - args.years_back, end_date.month, end_date.day)

    with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
        yearly_rankings = all_yearly_full_rankings(
            conn, start_date.isoformat(), end_date.isoformat(), max_rank=800, include_delisted=True,
        )
        candidate_tickers = _union_tickers(yearly_rankings)
        logger.info("Loading price/volume panels for %d candidate tickers", len(candidate_tickers))
        price_panel = load_price_panel(conn, candidate_tickers, start_date.isoformat(), end_date.isoformat())
        volume_panel = load_volume_panel(conn, candidate_tickers, start_date.isoformat(), end_date.isoformat())
        shares_map = _load_static_shares_outstanding(conn, candidate_tickers)
        beta_map = _load_beta_map(conn, candidate_tickers)

    market_cap_panel = _build_market_cap_panel(price_panel, shares_map)
    regime_series = _load_regime_series()
    quality_scores = _load_quality_scores(candidate_tickers)
    strategies = _build_strategies(volume_panel, market_cap_panel, beta_map, regime_series, quality_scores)

    strategy_kwargs = strategies[STRATEGY]
    yearly_universes = yearly_band_universes_from_rankings(yearly_rankings, RANK_START, RANK_END)
    lookback_days = lookback_trading_days(LOOKBACK_MONTHS)
    rebalance_days = REBALANCE_PERIODS[REBALANCE_NAME]
    momentum_panel = build_momentum_panel(price_panel, lookback_days)

    logger.info("Band %d (%d-%d) %s lb%dmo %s top%d | %s..%s",
                BAND_ID, RANK_START, RANK_END, STRATEGY, LOOKBACK_MONTHS,
                REBALANCE_NAME, TOP_N, start_date.isoformat(), end_date.isoformat())

    runs: List[Dict] = []

    baseline = _run_variant(
        strategy_kwargs, price_panel, yearly_universes, lookback_days, rebalance_days,
        momentum_panel, starting_capital=STARTING_CAPITAL, investable_pct=INVESTABLE_PCT,
        top_n=TOP_N,
    )
    runs.append(_summarise(baseline, "baseline", start_date.isoformat(), end_date.isoformat()))

    for exit_rank in EXIT_RANKS:
        for stop in TRAILING_STOPS:
            r = _run_variant(
                strategy_kwargs, price_panel, yearly_universes, lookback_days, rebalance_days,
                momentum_panel, starting_capital=STARTING_CAPITAL, investable_pct=INVESTABLE_PCT,
                top_n=TOP_N, exit_rank=exit_rank, trailing_stop_pct=stop,
            )
            runs.append(_summarise(
                r, f"exit_rank={exit_rank}, stop={stop}", start_date.isoformat(), end_date.isoformat(),
            ))

    # Summary table.
    print("\n=== Band 7 (%d-%d) %s lb%dmo %s top%d | post-tax comparison ===\n"
          % (RANK_START, RANK_END, STRATEGY, LOOKBACK_MONTHS, REBALANCE_NAME, TOP_N))
    hdr = (f"{'variant':<28}{'post-tax CAGR':>14}{'CAGR':>9}{'Sharpe':>8}{'Sortino':>8}"
           f"{'MaxDD':>8}{'WinRate':>9}{'avgDays':>8}{'churn/yr':>9}{'10L value':>12}")
    print(hdr)
    print("-" * len(hdr))
    for r in runs:
        print(f"{r['label']:<28}"
              f"{r['post_tax_cagr']:>13.1%}{r['cagr']:>9.1%}"
              f"{r['sharpe']:>8.2f}{r['sortino']:>8.2f}"
              f"{r['max_drawdown']:>8.1%}{r['win_rate']:>8.1%}{r['avg_days_held']:>8.0f}"
              f"{r['churn_yr']:>9.1f}{r['value_10L']:>12,.0f}")

    # YoY (per-FY CAGR) profile.
    fys = sorted(runs[0]["fy_cagrs"].keys())
    print("\n=== Per-FY CAGR profile (pre-tax, from equity curve) ===\n")
    print(f"{'variant':<28}" + "".join(f"{fy:>10}" for fy in fys))
    print("-" * (28 + 10 * len(fys)))
    for r in runs:
        row = "".join(f"{r['fy_cagrs'].get(fy, float('nan')):>10.0%}" for fy in fys)
        print(f"{r['label']:<28}{row}")


if __name__ == "__main__":
    main()
