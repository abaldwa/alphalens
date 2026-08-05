"""
scripts/run_momentum_dynamic_report.py

Phase: FeatureBacklog.md ML38 — momentum strategy dynamic report
Owner: Platform / Backtest
Consumers: operator CLI (`python3 -m scripts.run_momentum_dynamic_report`),
    datastore/api/routers/momentum.py's /dynamic_report endpoints

2026-07-30 user request: replace scripts/run_momentum_recommended_strategies.py's
3-category, 5-band composite sweep with one consolidated report covering:

  - 7 rank bands: 1-50, 51-100, 101-150, 151-200, 201-250, 251-500, 501-800
    (RANK_BANDS band_id 1/2 as-is; band 3/4 relabeled here to 101-150/
    151-200 to close the rank-101/151 overlap the raw RANK_BANDS tuples
    have with bands 2/3 — a purely cosmetic label fix, the underlying
    yearly_band_universes_from_rankings() call is exact-rank-inclusive
    either way).
  - 4 filter-preset categories per band: All Risk (baseline, zero filters),
    Balanced, Risk-Managed, Max-Defensive — see _build_strategies() for the
    exact filter combination each one applies. All 4 now run for every
    band (the prior BALANCED_ONLY_BAND_IDS restriction for 101-150/151-200
    is dropped per this request — full coverage, not a band-dependent
    subset).
  - Same 4-lookback x 5-rebalance x 3-top_n grid (60 configs) per (band,
    category) as every other sweep script, for direct comparability.
  - TWO backtest passes per variant: a lump-sum pass (starting_capital=10L,
    no SIP — "Value of 10 Lakhs") and a SIP pass (starting_capital=10K,
    sip_amount=10K/month — "Value of 10K Sip", "Monthly SIP CAGR" via XIRR).
    7 bands x 4 categories x 60 configs x 2 passes = 3,360 backtests.

New metrics vs. run_momentum_recommended_strategies.py: max_drawdown (now
returned by backtest.momentum_metrics.sharpe_sortino_calmar), win_rate
(extracted to backtest.momentum_metrics.win_rate), total_signals (new
MomentumBacktestResult field — sum of |target_set| across every rebalance,
i.e. post-filter buy signals generated, whether or not cash allowed the buy
to execute), total_trades (n_closed_trades + n_open_trades).

Judgment / scoring (2026-07-30 user request for a reproducible "best
strategy" call, not a black-box editorial pick):

    score = 0.30*z(sharpe) + 0.25*z(sortino) + 0.25*z(cagr)
            - 0.20*z(abs(max_drawdown))

z-scored within each (band_id, category) group of 60 lump-sum variants —
comparing across bands/categories on raw score would conflate "this band is
just structurally choppier" with "this variant is worse", so every z-score
is relative to its own band+category cohort. The top-scoring variant in
each (band_id, category) group is flagged is_recommended=True — the
"judgemental call on the best strategy in each category" per band. The
single highest RAW score across the whole report (comparing best-in-cohort
picks to each other, which is fair since each is already a per-cohort
winner) is flagged is_most_important=True.

Trade books: each variant's lump-sum-pass transactions are written to
backtest/reports/momentum/dynamic/<variant_id>.csv (see _export_trade_csv),
so the page can offer a per-variant trade-book download link without a new
DB table — same "pure post-processing of an existing in-memory result"
pattern as backtest/export_trade_book.py, just CSV columns drawn straight
from MomentumBacktestResult.transactions instead of a DB-backed trade_log.

Year-on-year: scripts/build_momentum_yoy_report.py's build_yoy(report, conn)
is reused as-is, exactly like run_momentum_recommended_strategies.py did.
"""

import argparse
import csv
import json
import logging
import multiprocessing
from datetime import date
from pathlib import Path
from typing import Dict, List

import duckdb
import numpy as np

from backtest.momentum_backtest import MomentumBacktester
from backtest.momentum_metrics import cagr, churn_factor, sharpe_sortino_calmar, win_rate, xirr
from backtest.momentum_tax import post_tax_ending_value
from config.settings import DUCKDB_PATH, MAX_ORDER_VS_ADTV
from config.timezone import now_ist
from datastore.api.db import get_duckdb_connection
from features.momentum_signal import (
    LOOKBACK_MONTHS,
    build_momentum_panel,
    lookback_trading_days,
    load_price_panel,
    load_volume_panel,
)
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

# 2026-07-30: exactly the 7 bands the user named. Labels below relabel
# RANK_BANDS' band_id 3/4 (100-150/150-200) to the non-overlapping
# 101-150/151-200 the user asked for; band_id 5 (100-200 mixed) is dropped
# — not part of this sweep.
BANDS = [
    (1, 1, 50), (2, 51, 100), (3, 101, 150), (4, 151, 200),
    (8, 201, 250), (6, 251, 500), (7, 501, 800),
]

STARTING_CAPITAL = 1_000_000.0
SIP_STARTING_CAPITAL = 10_000.0
SIP_AMOUNT = 10_000.0
INVESTABLE_PCT = 0.8
GRACE_CYCLES = 2
TOP_N_OPTIONS = [10, 15, 20]
REBALANCE_PERIODS = {"weekly": 5, "biweekly": 10, "monthly": 21, "bimonthly": 42, "quarterly": 63}

RECOMMENDED_MIN_ADTV_CR = 0.1  # phase_3 profile threshold override — see run_momentum_recommended_strategies.py precedent

REPORTS_DIR = Path(__file__).resolve().parent.parent / "backtest" / "reports" / "momentum"
DYNAMIC_TRADES_DIR = REPORTS_DIR / "dynamic"
MOMENTUM_YOY_DB = REPORTS_DIR / "momentum_yoy.duckdb"

# ---------------------------------------------------------------------------
# Parallel execution: fork workers inherit these globals via CoW (no pickling
# of large panels).  Set once in the parent BEFORE Pool creation.
# ---------------------------------------------------------------------------
_G: Dict = {}


def _compute_variant(args):
    """Run one (lump + SIP) pair for a single config — runs in a forked worker,
    inherits shared panels from _G via CoW (zero-copy until modified)."""
    band_id, rank_start, rank_end, strategy_name, lookback_months, rebalance_name, top_n = args
    try:
        lookback_days = lookback_trading_days(lookback_months)
        rebalance_days = REBALANCE_PERIODS[rebalance_name]
        yearly_universes = _G["yearly_universes"][band_id]
        strategy_kwargs = _G["strategies"][strategy_name]
        price_panel = _G["price_panel"]
        momentum_panel = _G["momentum_panels"][lookback_days]

        result = _run_variant(
            strategy_kwargs, price_panel, yearly_universes, lookback_days, rebalance_days,
            starting_capital=STARTING_CAPITAL, investable_pct=INVESTABLE_PCT,
            top_n=top_n, momentum_panel=momentum_panel,
        )
        sip_result = _run_variant(
            strategy_kwargs, price_panel, yearly_universes, lookback_days, rebalance_days,
            starting_capital=SIP_STARTING_CAPITAL, investable_pct=INVESTABLE_PCT,
            top_n=top_n, sip_amount=SIP_AMOUNT, momentum_panel=momentum_panel,
        )

        churn = churn_factor(result.rebalance_events)
        post_tax_value = post_tax_ending_value(result.ending_value, result.transactions)
        closed = [t for t in result.transactions if t["status"] == "closed"]
        avg_days_held = (
            sum(t["holding_days"] for t in closed) / len(closed) if closed else None
        )
        variant_cagr = cagr(result.starting_capital, result.ending_value, result.start_date, result.end_date)
        post_tax_cagr = cagr(result.starting_capital, post_tax_value, result.start_date, result.end_date)
        ratios = sharpe_sortino_calmar(result.equity_curve, variant_cagr)

        sip_cash_flows = [(cf["date"], cf["amount"]) for cf in sip_result.cash_flows]
        sip_cash_flows.append((sip_result.end_date, sip_result.ending_value))
        sip_cagr = xirr(sip_cash_flows) if len(sip_cash_flows) >= 2 else None

        variant_id = (
            f"{strategy_name}_b{band_id}_{rank_start}-{rank_end}_"
            f"lb{lookback_months}mo_{rebalance_name}_top{top_n}"
        )
        trade_book_file = _export_trade_csv(variant_id, result.transactions)

        return {
            "variant_id": variant_id,
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
            "max_drawdown": ratios["max_drawdown"],
            "churn_avg_transactions_per_year": churn["avg_transactions_per_year"],
            "win_rate": win_rate(result.transactions),
            "total_signals": result.total_signals,
            "n_closed_trades": len(closed),
            "n_open_trades": len(result.transactions) - len(closed),
            "total_trades": len(result.transactions),
            "avg_days_held": avg_days_held,
            "value_10L": result.ending_value,
            "value_10k_sip": sip_result.ending_value,
            "sip_cagr": sip_cagr,
            "trade_book_file": trade_book_file,
            "equity_curve": result.equity_curve,
            "transactions": result.transactions,
        }
    except Exception as exc:
        logger.error("variant failed: band=%d strategy=%s lb=%d rebalance=%s top_n=%d: %s",
                      band_id, strategy_name, lookback_months, rebalance_name, top_n, exc,
                      exc_info=True)
        return None

SCORE_WEIGHTS = {"sharpe": 0.30, "sortino": 0.25, "cagr": 0.25, "abs_max_drawdown": -0.20}


def _run_variant(kwargs: Dict, price_panel, yearly_universes: Dict, lookback_days: int, rebalance_days: int, **extra):
    engine = MomentumBacktester(
        price_panel=price_panel,
        yearly_universes=yearly_universes,
        lookback_days=lookback_days,
        rebalance_every_n_trading_days=rebalance_days,
        grace_cycles=GRACE_CYCLES,
        **kwargs,
        **extra,
    )
    return engine.run()


def _build_strategies(volume_panel, market_cap_panel, beta_map, regime_series, quality_scores) -> Dict[str, Dict]:
    all_risk: Dict = {}
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
        risk_managed, orthogonalize_vs_size_beta=True, market_cap_panel=market_cap_panel, beta_map=beta_map,
    )
    return {
        "all_risk": all_risk,
        "balanced": balanced,
        "risk_managed": risk_managed,
        "max_defensive": max_defensive,
    }


def _export_trade_csv(variant_id: str, transactions: List[Dict]) -> str:
    DYNAMIC_TRADES_DIR.mkdir(parents=True, exist_ok=True)
    out_path = DYNAMIC_TRADES_DIR / f"{variant_id}.csv"
    fields = [
        "ticker", "status", "buy_date", "buy_price", "buy_momentum_rank",
        "sell_date", "sell_price", "sell_momentum_rank", "holding_days",
    ]
    with open(out_path, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for t in transactions:
            writer.writerow(t)
    return out_path.name


def _zscore(values: List[float]) -> List[float]:
    arr = np.array(values, dtype=float)
    mask = np.isfinite(arr)
    if mask.sum() < 2 or arr[mask].std() < 1e-9:
        return [0.0] * len(arr)
    mean, std = arr[mask].mean(), arr[mask].std()
    return [float((v - mean) / std) if np.isfinite(v) else 0.0 for v in arr]


def _score_cohort(variants: List[Dict]) -> None:
    """Mutates each variant dict in-place: adds 'score' and, on exactly one
    variant per (band_id, category) cohort, 'is_recommended': True."""
    sharpe_z = _zscore([v["sharpe"] if v["sharpe"] is not None else np.nan for v in variants])
    sortino_z = _zscore([v["sortino"] if v["sortino"] is not None else np.nan for v in variants])
    cagr_z = _zscore([v["cagr"] if v["cagr"] is not None else np.nan for v in variants])
    mdd_z = _zscore([abs(v["max_drawdown"]) if v["max_drawdown"] is not None else np.nan for v in variants])

    for i, v in enumerate(variants):
        v["score"] = (
            SCORE_WEIGHTS["sharpe"] * sharpe_z[i]
            + SCORE_WEIGHTS["sortino"] * sortino_z[i]
            + SCORE_WEIGHTS["cagr"] * cagr_z[i]
            + SCORE_WEIGHTS["abs_max_drawdown"] * mdd_z[i]
        )
        v["is_recommended"] = False

    best_idx = max(range(len(variants)), key=lambda i: variants[i]["score"])
    variants[best_idx]["is_recommended"] = True


def run_dynamic_report(years_back: int = 10, workers: int = 8) -> Dict:
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

    strategies = _build_strategies(volume_panel, market_cap_panel, beta_map, regime_series, quality_scores)

    # Precompute momentum panels ONCE per lookback, reused across all 4
    # categories and both lump+SIP passes. Previously every one of the 3,360
    # backtests recomputed trailing_momentum_from_panel() at each rebalance;
    # now it is a single vectorised rolling pass per lookback + an O(1) .loc.
    logger.info("Precomputing %d momentum panels (signal reuse across strategies)", len(LOOKBACK_MONTHS))
    momentum_panels = {
        lookback_trading_days(lb): build_momentum_panel(price_panel, lookback_trading_days(lb))
        for lb in LOOKBACK_MONTHS
    }

    # Each band's yearly universes computed once, shared read-only by workers.
    yearly_universes_per_band = {
        band_id: yearly_band_universes_from_rankings(yearly_rankings, rank_start, rank_end)
        for band_id, rank_start, rank_end in BANDS
    }

    # Flat task list — one (lump + SIP) pair per config.
    tasks = [
        (band_id, rank_start, rank_end, strategy_name, lookback_months, rebalance_name, top_n)
        for band_id, rank_start, rank_end in BANDS
        for strategy_name in strategies
        for lookback_months in LOOKBACK_MONTHS
        for rebalance_name in REBALANCE_PERIODS
        for top_n in TOP_N_OPTIONS
    ]
    logger.info("Dispatching %d configs across %d workers", len(tasks), workers)

    # Shared context inherited by fork workers via CoW (zero-copy; set before Pool).
    _G["price_panel"] = price_panel
    _G["momentum_panels"] = momentum_panels
    _G["strategies"] = strategies
    _G["yearly_universes"] = yearly_universes_per_band

    ctx = multiprocessing.get_context("fork")
    with ctx.Pool(processes=max(1, workers)) as pool:
        raw_variants = pool.map(_compute_variant, tasks, chunksize=4)

    raw_variants = [v for v in raw_variants if v is not None]
    logger.info("Completed %d variants (excluded %d failed)",
                len(raw_variants), len(tasks) - len(raw_variants))

    # Re-group into (band_id, category) cohorts so _score_cohort picks exactly
    # one winner per cohort, matching the pre-parallel output.
    cohort_map: Dict[tuple, List[Dict]] = {}
    for v in raw_variants:
        cohort_map.setdefault((v["band_id"], v["strategy"]), []).append(v)

    variants: List[Dict] = []
    for (band_id, strategy_name) in sorted(cohort_map):
        cohort = cohort_map[(band_id, strategy_name)]
        _score_cohort(cohort)
        variants.extend(cohort)

    most_important_idx = max(
        (i for i, v in enumerate(variants) if v["is_recommended"]),
        key=lambda i: variants[i]["score"],
        default=None,
    )
    for v in variants:
        v["is_most_important"] = False
        v["is_band_most_important"] = False
        v["top_cagr_rank"] = None
    if most_important_idx is not None:
        variants[most_important_idx]["is_most_important"] = True

    # Per-universe judgment calls (2026-07-30 user request): within each
    # band, (a) the single best pick among that band's 4 category winners
    # ("Most Important" for THIS universe, as distinct from the one overall
    # is_most_important pick above), and (b) the top-2 variants by raw CAGR
    # across ALL 240 configs in that band (any category/lookback/rebalance/
    # top_n) — a pure-return comparison point alongside the risk-adjusted
    # score-based picks.
    by_band: Dict[int, List[Dict]] = {}
    for v in variants:
        by_band.setdefault(v["band_id"], []).append(v)

    for band_variants in by_band.values():
        recommended_in_band = [v for v in band_variants if v["is_recommended"]]
        if recommended_in_band:
            max(recommended_in_band, key=lambda v: v["score"])["is_band_most_important"] = True

        with_cagr = [v for v in band_variants if v["cagr"] is not None]
        top2_cagr = sorted(with_cagr, key=lambda v: v["cagr"], reverse=True)[:2]
        for rank, v in enumerate(top2_cagr, start=1):
            v["top_cagr_rank"] = rank

    report = {
        "generated_at": now_ist().isoformat(),
        "score_formula": (
            "0.30*z(sharpe) + 0.25*z(sortino) + 0.25*z(cagr) - 0.20*z(abs(max_drawdown)), "
            "z-scored within each (band_id, category) cohort of 60 variants"
        ),
        "strategies": {
            "all_risk": {"filters": []},
            "balanced": {
                "filters": ["liquidity_floor", "quality_gated", "adtv_capped_sizing", "circuit_lock_proxy"],
                "min_adtv_cr": RECOMMENDED_MIN_ADTV_CR, "max_pct_of_adtv": MAX_ORDER_VS_ADTV,
                "circuit_band_pct": CIRCUIT_BAND_PCT, "quality_gate": QUALITY_GATE,
            },
            "risk_managed": {"filters": ["balanced", "regime_conditional (disable_in_regimes=high_vol)"]},
            "max_defensive": {"filters": ["risk_managed", "size_beta_orthogonalized"]},
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

    for v in variants:
        v.pop("equity_curve", None)
        v.pop("transactions", None)

    report["yoy"] = yoy_rows
    return report


def main():
    parser = argparse.ArgumentParser(description="ML38 momentum dynamic report (7 bands x 4 categories x 60 configs x 2 passes)")
    parser.add_argument("--years-back", type=int, default=10)
    parser.add_argument("--workers", type=int, default=8,
                        help="number of parallel fork workers for the backtest grid")
    args = parser.parse_args()

    report = run_dynamic_report(years_back=args.years_back, workers=args.workers)

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = REPORTS_DIR / f"momentum_dynamic_report_{now_ist().strftime('%Y%m%d_%H%M%S')}.json"
    with open(out_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    logger.info("Wrote report to %s (%d variants, %d yoy rows)", out_path, len(report["variants"]), len(report["yoy"]))


if __name__ == "__main__":
    main()
