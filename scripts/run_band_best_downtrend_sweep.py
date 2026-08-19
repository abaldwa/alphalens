"""
scripts/run_band_best_downtrend_sweep.py

Quick sweep: add downtrend_filter_pct to each band's current best variant
and measure impact on negative FYs vs overall CAGR.
"""

import logging
import sys
from typing import Any, Dict, List
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

from backtest.momentum_orchestrator_runner import run_momentum_orchestrated
from backtest.core.metrics import cagr, sharpe_sortino_calmar
from backtest.core.tax import post_tax_ending_value_from_dicts as post_tax_ending_value
from config.settings import DUCKDB_PATH
from config.timezone import now_ist
from datastore.api.db import get_duckdb_connection
from features.momentum_signal import build_momentum_panel, load_price_panel, load_volume_panel, lookback_trading_days
from features.momentum_universe import all_yearly_full_rankings, yearly_band_universes_from_rankings
from scripts.run_momentum_dynamic_report import (
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

# [2026-08-19] STALE BAND NUMBERING -- deliberately NOT renumbered.
# RANK_BANDS was renumbered to a contiguous 1-7 on this date, but the configs
# below were selected by the 2026-08-05 sweep, which ran against the OLD
# tables: ids 8/6/7 there meant ranks 201-250 / 251-500 / 501-800. Only
# 501-800 survives unchanged (it is band 7 today); 201-250 and 251-500 were
# replaced by the 201-300 / 301-500 partition, so there is no id that means
# the same universe those two rows were measured on. Renumbering the keys
# would silently attach an empirical finding to a universe it was never
# measured against. The inline `_bands` literal below is kept for the same
# reason: it is what these results correspond to. Re-run the band sweep on
# the current RANK_BANDS before trusting these for bands 5-7.
# Current band-best configs (from the 2026-08-05 report)
BAND_BEST: Dict[int, Dict[str, Any]] = {
    1: dict(strategy="max_defensive", lookback_months=9, rebalance="bimonthly", top_n=15),
    2: dict(strategy="all_risk", lookback_months=3, rebalance="quarterly", top_n=15),
    3: dict(strategy="balanced", lookback_months=6, rebalance="bimonthly", top_n=10),
    4: dict(strategy="all_risk", lookback_months=3, rebalance="quarterly", top_n=15),
    6: dict(strategy="risk_managed", lookback_months=9, rebalance="quarterly", top_n=20),
    7: dict(strategy="max_defensive", lookback_months=6, rebalance="bimonthly", top_n=10),
    8: dict(strategy="balanced", lookback_months=9, rebalance="quarterly", top_n=10),
}

# Downtrend filter grid to test
DOWN_CANDIDATES = [0.10, 0.15, 0.20]
DOWN_LOOKBACK = 20


def _fy_cagrs(equity_curve: Any) -> Dict[str, float]:
    buckets: Dict[str, List[Any]] = {}
    for pt in equity_curve:
        d = pd.Timestamp(pt["date"])
        fy_year = d.year if d.month >= 4 else d.year - 1
        fy = f"FY{fy_year}-{str(fy_year + 1)[-2:]}"
        buckets.setdefault(fy, []).append(pt)
    out = {}
    for fy, pts in buckets.items():
        if len(pts) >= 2:
            first, last = pts[0], pts[-1]
            out[fy] = cagr(first["total_value"], last["total_value"], first["date"], last["date"])
    return out


def _run_variant(
    kwargs: Any, price_panel: Any, yearly_universes: Any, lookback_days: Any,
    rebalance_days: Any, momentum_panel: Any, **extra: Any,
) -> Any:
    # [H4, 2026-08-18] grace_cycles/momentum_panel no longer exist on
    # MomentumAdapter -- deprecated by §19 (grace_cycles) / always
    # recomputed from price_panel (momentum_panel override). momentum_panel
    # param kept for call-site compatibility but ignored.
    return run_momentum_orchestrated(
        price_panel=price_panel,
        yearly_universes=yearly_universes,
        lookback_days=lookback_days,
        rebalance_every_n_trading_days=rebalance_days,
        **kwargs,
        **extra,
    )


def main() -> None:
    end_date = now_ist().date()
    start_date = date(end_date.year - 10, end_date.month, end_date.day)

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

    results: List[Dict[str, Any]] = []

    for band_id, cfg in BAND_BEST.items():
        strategy_kwargs = strategies[cfg["strategy"]]
        # The OLD band table, kept deliberately -- see the STALE BAND
        # NUMBERING note at the top of this module. These configs were
        # selected against these ranges; RANK_BANDS today means something
        # else for ids 6 and 8.
        _bands = [
            (1, 1, 50), (2, 51, 100), (3, 101, 150), (4, 151, 200),
            (8, 201, 250), (6, 251, 500), (7, 501, 800),
        ]
        _rank_start, _rank_end = next(b for b in _bands if b[0] == band_id)[1:3]
        yearly_universes = yearly_band_universes_from_rankings(
            yearly_rankings, _rank_start, _rank_end
        )
        lookback_days = lookback_trading_days(cfg["lookback_months"])
        rebalance_days = REBALANCE_PERIODS[cfg["rebalance"]]
        momentum_panel = build_momentum_panel(price_panel, lookback_days)

        # Baseline (no downtrend filter)
        base = _run_variant(
            strategy_kwargs, price_panel, yearly_universes, lookback_days, rebalance_days,
            momentum_panel, starting_capital=STARTING_CAPITAL, investable_pct=INVESTABLE_PCT,
            top_n=cfg["top_n"],
        )
        base_fy = _fy_cagrs(base.equity_curve)
        base_cagr = cagr(base.starting_capital, base.ending_value, base.start_date, base.end_date)
        base_post_tax = cagr(base.starting_capital, post_tax_ending_value(base.ending_value, base.transactions), base.start_date, base.end_date)
        base_ratios = sharpe_sortino_calmar(base.equity_curve, base_cagr)

        neg_fys_base = {fy: r for fy, r in base_fy.items() if r < 0}

        print(f"\n=== Band {band_id} {cfg['strategy']} {cfg['lookback_months']}mo {cfg['rebalance']} top{cfg['top_n']} ===")
        print(f"  Baseline: CAGR={base_cagr:.1%} post_tax={base_post_tax:.1%} Sharpe={base_ratios['sharpe']:.2f} MaxDD={base_ratios['max_drawdown']:.1%}")
        print(f"  Negative FYs: {', '.join(f'{fy} {r:.1%}' for fy, r in sorted(neg_fys_base.items())) or 'none'}")

        # Test each downtrend filter
        for down_pct in DOWN_CANDIDATES:
            test = _run_variant(
                strategy_kwargs, price_panel, yearly_universes, lookback_days, rebalance_days,
                momentum_panel, starting_capital=STARTING_CAPITAL, investable_pct=INVESTABLE_PCT,
                top_n=cfg["top_n"], downtrend_filter_pct=down_pct, downtrend_lookback_days=DOWN_LOOKBACK,
            )
            test_fy = _fy_cagrs(test.equity_curve)
            test_cagr = cagr(test.starting_capital, test.ending_value, test.start_date, test.end_date)
            test_post_tax = cagr(test.starting_capital, post_tax_ending_value(test.ending_value, test.transactions), test.start_date, test.end_date)
            test_ratios = sharpe_sortino_calmar(test.equity_curve, test_cagr)

            neg_fys_test = {fy: r for fy, r in test_fy.items() if r < 0}
            cagr_delta = test_cagr - base_cagr
            post_tax_delta = test_post_tax - base_post_tax
            neg_delta = {fy: test_fy.get(fy, 0) - base_fy.get(fy, 0) for fy in set(base_fy) | set(test_fy) if base_fy.get(fy, 0) < 0 or test_fy.get(fy, 0) < 0}

            print(f"  downtrend={down_pct:.0%}: CAGR={test_cagr:.1%} ({cagr_delta:+.1%}) post_tax={test_post_tax:.1%} ({post_tax_delta:+.1%}) Sharpe={test_ratios['sharpe']:.2f} MaxDD={test_ratios['max_drawdown']:.1%}")
            print(f"    Neg FYs: {', '.join(f'{fy} {r:.1%}' for fy, r in sorted(neg_fys_test.items())) or 'none'}")
            if neg_delta:
                improved = sum(1 for d in neg_delta.values() if d > 0)
                worsened = sum(1 for d in neg_delta.values() if d < 0)
                print(f"    Neg FY change: {improved} improved, {worsened} worsened")

            results.append({
                "band": band_id,
                "strategy": cfg["strategy"],
                "config": f"{cfg['lookback_months']}mo_{cfg['rebalance']}_top{cfg['top_n']}",
                "downtrend": down_pct,
                "cagr": test_cagr,
                "post_tax_cagr": test_post_tax,
                "sharpe": test_ratios["sharpe"],
                "max_dd": test_ratios["max_drawdown"],
                "neg_fys": neg_fys_test,
            })

    # Summary table
    print("\n\n=== SUMMARY: Downtrend Filter Impact on Negative FYs ===")
    print(f"{'Band':<5}{'Strategy':<15}{'Config':<20}{'Down%':<7}{'CAGR':>8}{'ΔCAGR':>8}{'PostTax':>8}{'ΔPostTax':>9}{'Sharpe':>7}{'MaxDD':>7}{'NegFYs'}")
    print("-" * 110)
    for r in results:
        # `ret`, not `r` -- the old name shadowed the row being formatted.
        neg_fys: Dict[str, float] = r["neg_fys"]
        neg_str = ", ".join(f"{fy[:6]}{ret:.0%}" for fy, ret in sorted(neg_fys.items()))
        print(f"{r['band']:<5}{r['strategy']:<15}{r['config']:<20}{r['downtrend']:<7.0%}"
              f"{r['cagr']:>7.1%}{r['cagr']-r['cagr']:>8}{r['post_tax_cagr']:>7.1%}"
              f"{r['post_tax_cagr']-r['post_tax_cagr']:>9}{r['sharpe']:>7.2f}"
              f"{r['max_dd']:>6.1%}  {neg_str}")


if __name__ == "__main__":
    main()
