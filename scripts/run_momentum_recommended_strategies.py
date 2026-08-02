"""
scripts/run_momentum_recommended_strategies.py

Phase: FeatureBacklog.md ML38 — momentum strategy robustness overlays (composite follow-up)
Owner: Platform / Backtest
Consumers: operator CLI (`python3 -m scripts.run_momentum_recommended_strategies`),
    datastore/api/routers/momentum.py's /recommended_strategies endpoints

2026-07-29 user request: scripts/run_momentum_filter_overlays.py tested each of
its 7 MomentumBacktester filters ONE AT A TIME against the baseline. This
script instead runs COMPOSITE strategies (several filters combined in one
MomentumBacktester call) for the 5 rank bands the user asked to compare —
100-150, 150-200, 201-250, 251-500, 501-800 — with multiple risk/reward
variants per band (not one prescribed answer) so the user can pick a
tradeoff rather than have one picked for them:

  - Balanced (all 5 bands): liquidity_floor + quality_gated + adtv_capped_sizing
    + circuit_lock_proxy. These 4 showed near-zero-to-positive CAGR cost
    across every band in the individual-filter overlay sweep — cheap
    execution-realism guardrails, not alpha-costing choices.
  - Risk-Managed (201-250, 251-500, 501-800 only): Balanced + regime_conditional
    (disables buys in a high-vol regime) — the one filter that showed a small
    CAGR *gain* with reduced churn in 251-500 in the individual sweep.
  - Max-Defensive (201-250, 251-500, 501-800 only): Risk-Managed +
    size_beta_orthogonalized — neutralizes the size/beta tilt that grows
    with rank depth, at the largest CAGR cost of any non-downtrend filter.

`downtrend_filter` is deliberately excluded from every composite: it was the
single most expensive filter in every band in the individual sweep, with no
drawdown/crash-protection evidence in this codebase to justify the cost (no
daily equity curve, no volatility metric was computed until this same
change). It remains available standalone via run_momentum_filter_overlays.py
for reference, not bundled into a "recommended" strategy.

2026-07-29 user request (liquidity_floor no-op fix, THIS RUN ONLY): the
production MIN_ADTV_CR is 0.0 under the active full_nse UNIVERSE_PROFILE
(config/settings.py) — an intentional profile default for the live system,
not a bug, but it makes liquidity_floor meaningless for this comparison.
This script hardcodes RECOMMENDED_MIN_ADTV_CR = 0.1 (the phase_3 profile's
threshold) as a LOCAL override passed directly into MomentumBacktester,
never touching config/settings.py or UNIVERSE_PROFILE.

Sharpe/Sortino/Calmar: computed exactly like run_momentum_filter_overlays.py
and run_momentum_experimentation.py, via backtest.momentum_metrics.
sharpe_sortino_calmar(equity_curve, cagr) — frequency-aware, no fresh
backtest needed beyond what this script already runs.

Year-on-year: scripts/build_momentum_yoy_report.py's build_yoy(report, conn)
is reused as-is (not reimplemented) — it operates on any {"variants": [...]}
report dict where each variant has equity_curve/transactions/starting_capital/
start_date/end_date/band_id/rank_start/rank_end/lookback_months/
rebalance_period/top_n/grace_cycles, which every variant below provides.

Same 4-lookback x 5-rebalance x 3-top_n grid (60 configs) per strategy
variant as the other two sweep scripts, for direct comparability. SIP is
skipped (matches run_momentum_filter_overlays.py's precedent) to keep the
(5 balanced + 3*2 risk-managed/max-defensive = 11) x 60 = 660 backtests
tractable.
"""

import argparse
import json
import logging
from datetime import date
from pathlib import Path
from typing import Dict, List

import duckdb

from backtest.momentum_backtest import MomentumBacktester
from backtest.momentum_metrics import cagr, churn_factor, sharpe_sortino_calmar, trade_quality_metrics
from backtest.momentum_tax import post_tax_ending_value
from config.settings import DUCKDB_PATH, MAX_ORDER_VS_ADTV
from config.timezone import now_ist
from datastore.api.db import get_duckdb_connection
from features.momentum_signal import LOOKBACK_MONTHS, lookback_trading_days, load_price_panel, load_volume_panel
from features.momentum_universe import all_yearly_full_rankings, yearly_band_universes_from_rankings
from features.regime_signal import HIGH_VOL
from scripts.build_momentum_yoy_report import build_yoy
from scripts.run_momentum_filter_overlays import (
    CIRCUIT_BAND_PCT,
    QUALITY_GATE,
    _build_market_cap_panel,
    _load_beta_map,
    _load_quality_scores,
    _load_regime_series,
    _load_static_shares_outstanding,
    _union_tickers,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# 2026-07-29: same 5 bands the user named for this comparison. 201-250 is
# the new band (see run_momentum_experimentation.py / run_momentum_filter_
# overlays.py's WIDE_BANDS) — kept local here too, not added to the shared
# RANK_BANDS (that constant also drives the 5 live paper-trading strategies).
BANDS = [(3, 100, 150), (4, 150, 200), (8, 201, 250), (6, 251, 500), (7, 501, 800)]

# Bands whose earlier per-filter results were largely cost-free (100-150,
# 150-200) only get the Balanced variant. The deeper/untested bands get all
# 3, so the risk/reward tradeoff is a real choice.
BALANCED_ONLY_BAND_IDS = {3, 4}

STARTING_CAPITAL = 1_000_000.0
INVESTABLE_PCT = 0.8
GRACE_CYCLES = 2
TOP_N_OPTIONS = [10, 15, 20]
REBALANCE_PERIODS = {"weekly": 5, "biweekly": 10, "monthly": 21, "bimonthly": 42, "quarterly": 63}

RECOMMENDED_MIN_ADTV_CR = 0.1  # phase_3 profile threshold, THIS RUN ONLY — see module docstring

REPORTS_DIR = Path(__file__).resolve().parent.parent / "backtest" / "reports" / "momentum"
MOMENTUM_YOY_DB = REPORTS_DIR / "momentum_yoy.duckdb"


def _run_variant(
    kwargs: Dict, price_panel, yearly_universes: Dict,
    lookback_days: int, rebalance_days: int, top_n: int,
):
    engine = MomentumBacktester(
        price_panel=price_panel,
        yearly_universes=yearly_universes,
        lookback_days=lookback_days,
        rebalance_every_n_trading_days=rebalance_days,
        starting_capital=STARTING_CAPITAL,
        investable_pct=INVESTABLE_PCT,
        top_n=top_n,
        grace_cycles=GRACE_CYCLES,
        **kwargs,
    )
    return engine.run()


def _build_strategies(
    volume_panel, market_cap_panel, beta_map, regime_series, quality_scores,
) -> Dict[str, Dict]:
    balanced = {
        "volume_panel": volume_panel,
        "min_adtv_cr": RECOMMENDED_MIN_ADTV_CR,
        "max_pct_of_adtv": MAX_ORDER_VS_ADTV,
        "circuit_band_pct": CIRCUIT_BAND_PCT,
        "quality_scores": quality_scores,
        "quality_gate": QUALITY_GATE,
    }
    risk_managed = dict(balanced, regime_series=regime_series, disable_in_regimes={HIGH_VOL})
    max_defensive = dict(
        risk_managed,
        orthogonalize_vs_size_beta=True,
        market_cap_panel=market_cap_panel,
        beta_map=beta_map,
    )
    return {"balanced": balanced, "risk_managed": risk_managed, "max_defensive": max_defensive}


def run_recommended_strategies(years_back: int = 10) -> Dict:
    end_date = now_ist().date()
    start_date = date(end_date.year - years_back, end_date.month, end_date.day)

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
    logger.info(
        "Auxiliary data ready: shares_outstanding=%d tickers, beta_map=%d tickers, "
        "market_cap_panel cols=%d, regime_series=%s, quality_scores=%d tickers",
        len(shares_map), len(beta_map), market_cap_panel.shape[1],
        "none" if regime_series is None else f"{len(regime_series)} rows",
        len(quality_scores),
    )

    strategies = _build_strategies(volume_panel, market_cap_panel, beta_map, regime_series, quality_scores)

    variants: List[Dict] = []
    for band_id, rank_start, rank_end in BANDS:
        strategy_names = ["balanced"] if band_id in BALANCED_ONLY_BAND_IDS else list(strategies.keys())
        yearly_universes = yearly_band_universes_from_rankings(yearly_rankings, rank_start, rank_end)
        for strategy_name in strategy_names:
            kwargs = strategies[strategy_name]
            for lookback_months in LOOKBACK_MONTHS:
                lookback_days = lookback_trading_days(lookback_months)
                for rebalance_name, rebalance_days in REBALANCE_PERIODS.items():
                    for top_n in TOP_N_OPTIONS:
                        logger.info(
                            "strategy=%s band=%d lookback=%dmo rebalance=%s top_n=%d",
                            strategy_name, band_id, lookback_months, rebalance_name, top_n,
                        )
                        result = _run_variant(
                            kwargs, price_panel, yearly_universes, lookback_days, rebalance_days, top_n,
                        )
                        churn = churn_factor(result.rebalance_events)
                        post_tax_value = post_tax_ending_value(result.ending_value, result.transactions)
                        closed = [t for t in result.transactions if t["status"] == "closed"]
                        win_rate = (
                            sum(1 for t in closed if t["sell_price"] is not None and t["sell_price"] > t["buy_price"])
                            / len(closed) if closed else None
                        )
                        avg_days_held = (
                            sum(t["holding_days"] for t in closed) / len(closed) if closed else None
                        )
                        trade_quality = trade_quality_metrics(result.transactions)
                        variant_cagr = cagr(
                            result.starting_capital, result.ending_value, result.start_date, result.end_date
                        )
                        post_tax_cagr = cagr(
                            result.starting_capital, post_tax_value, result.start_date, result.end_date
                        )
                        ratios = sharpe_sortino_calmar(result.equity_curve, variant_cagr)
                        variants.append({
                            "strategy": strategy_name,
                            "band_id": band_id, "rank_start": rank_start, "rank_end": rank_end,
                            "lookback_months": lookback_months, "rebalance_period": rebalance_name,
                            "top_n": top_n, "grace_cycles": GRACE_CYCLES,
                            "starting_capital": result.starting_capital,
                            "ending_value": result.ending_value,
                            "start_date": result.start_date, "end_date": result.end_date,
                            "cagr": variant_cagr,
                            "post_tax_cagr": post_tax_cagr,
                            "sharpe": ratios["sharpe"],
                            "sortino": ratios["sortino"],
                            "calmar": ratios["calmar"],
                            "churn_avg_transactions_per_year": churn["avg_transactions_per_year"],
                            "win_rate": win_rate,
                            "n_closed_trades": len(closed),
                            "n_open_trades": len(result.transactions) - len(closed),
                            "avg_days_held": avg_days_held,
                            "total_trades": trade_quality["total_trades"],
                            "avg_trade_duration_days": trade_quality["avg_trade_duration_days"],
                            "n_outlier_trades": trade_quality["n_outlier_trades"],
                            "max_abs_return_zscore": trade_quality["max_abs_return_zscore"],
                            "equity_curve": result.equity_curve,
                            "transactions": result.transactions,
                        })

    report = {
        "generated_at": now_ist().isoformat(),
        "strategies": {
            "balanced": {
                "min_adtv_cr": RECOMMENDED_MIN_ADTV_CR, "max_pct_of_adtv": MAX_ORDER_VS_ADTV,
                "circuit_band_pct": CIRCUIT_BAND_PCT, "quality_gate": QUALITY_GATE,
            },
            "risk_managed": {"adds": ["regime_conditional (disable_in_regimes=high_vol)"]},
            "max_defensive": {"adds": ["regime_conditional", "size_beta_orthogonalized"]},
        },
        "bands": [{"band_id": b, "rank_start": s, "rank_end": e} for b, s, e in BANDS],
        "variants": variants,
    }

    bench_conn = duckdb.connect(str(MOMENTUM_YOY_DB))
    try:
        yoy_rows = build_yoy(report, bench_conn)
    finally:
        bench_conn.close()
    logger.info("Built %d (variant, FY) YoY rows", len(yoy_rows))

    # equity_curve/transactions kept per-variant for YoY/downstream use, but
    # dropped from the persisted variants list itself (same as run_momentum_
    # filter_overlays.py) to keep the report file a manageable size — the
    # detail lives in yoy_rows and the summary metrics above.
    for v in variants:
        v.pop("equity_curve", None)
        v.pop("transactions", None)

    report["yoy"] = yoy_rows
    return report


def main():
    parser = argparse.ArgumentParser(description="ML38 momentum recommended-strategy composite sweep")
    parser.add_argument("--years-back", type=int, default=10)
    args = parser.parse_args()

    report = run_recommended_strategies(years_back=args.years_back)

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = REPORTS_DIR / f"momentum_recommended_strategies_{now_ist().strftime('%Y%m%d_%H%M%S')}.json"
    with open(out_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    logger.info("Wrote report to %s (%d variants, %d yoy rows)", out_path, len(report["variants"]), len(report["yoy"]))


if __name__ == "__main__":
    main()
