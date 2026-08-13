"""
scripts/run_technical_recommended_strategies.py

Phase: Technical Analysis Momentum-parity backtest reporting (2026-08-01)
Owner: Platform / Backtest
Consumers: operator CLI
    (`python3 -m scripts.run_technical_recommended_strategies`),
    datastore/api/routers/technical_backtest.py's /recommended_strategies(/trigger)

Composite entry-filter strategies (Balanced / Risk-Managed / Max-Defensive
— exact same 3-tier risk/reward pattern as
scripts/run_momentum_recommended_strategies.py) across every screener
template, PLUS a curated set of cross-style "combo" strategies
(TechnicalComboAdapter — 2026-08-01 "combination of strategies" request),
PLUS a per-variant signal-failure breakdown (losing trades with their
entry-condition snapshot, read back from each job's own enriched
trade_book_{run_id}.csv — backtest/export_trade_book.py's 2026-08-01
entry_signal_score/entry_indicator_values columns).

Fixed exit_variant="baseline" and max_hold_days=63 (~1 quarter) across
every variant here — this script isolates FILTER/COMBO choice, not
exit-timeframe choice (that's scripts/run_technical_experimentation.py's
job). Capital fixed at TECHNICAL_INITIAL_CAPITAL (Rs 1 Lakh).

2026-08-01 user instruction: do NOT run the backtest yet.
"""

import argparse
import csv
import json
import logging
from datetime import date, timedelta
from typing import Dict, List, Optional

from backtest.run_strategy_queue import run_queue
from config.timezone import now_ist
from scripts.run_technical_experimentation import (
    DEFAULT_MAX_WORKERS,
    QUEUE_DEFS_DIR,
    REPORTS_DIR,
    SCREENER_CACHE_DIR,
    TECHNICAL_INITIAL_CAPITAL,
    _load_job_report,
)
from systems.technical_analysis.screener.templates import TEMPLATES

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

FIXED_EXIT_VARIANT = "baseline"
FIXED_MAX_HOLD_DAYS = 63
TOP_N_OPTIONS = [10, 15, 20]

# Composite filter tiers — identical risk/reward escalation pattern to
# Momentum's Balanced/Risk-Managed/Max-Defensive (scripts/
# run_momentum_recommended_strategies.py), translated to Technical's own
# entry-filter kwargs (backtest/adapters/technical_adapter.py).
_BALANCED_FIELDS = {"min_adtv_cr": 0.1, "quality_gate_min_f_score": 4, "circuit_band_pct": 0.19}
# NOTE: downtrend_lookback_days is Momentum-only (backtest/momentum_backtest.py),
# NOT a valid Technical orchestrator flag — do not include here.
_RISK_MANAGED_FIELDS = {**_BALANCED_FIELDS, "downtrend_filter_pct": 0.05}
_MAX_DEFENSIVE_FIELDS = {**_RISK_MANAGED_FIELDS, "disable_buys_in_regime": "bear"}

COMPOSITE_STRATEGIES: Dict[str, Dict] = {
    "balanced": _BALANCED_FIELDS,
    "risk_managed": _RISK_MANAGED_FIELDS,
    "max_defensive": _MAX_DEFENSIVE_FIELDS,
}

# Curated cross-style template combos (TEMPLATE_STYLE-diverse pairs) — a
# starting set for evaluation, not exhaustive; adjust once real numbers
# come back. Each combo also gets all 3 COMPOSITE_STRATEGIES filter tiers
# applied on top (same filters threaded to every sub-adapter).
COMBO_TEMPLATES: List[List[str]] = [
    ["A1", "C1"],   # Trend Following + (time series) Momentum
    # C3 was dropped 2026-08-13 as a definitional duplicate of C1, so this combo
    # would now pair B2 with a template that no longer exists. Repointed to C2
    # (cross-sectional momentum) rather than C1: this combo's purpose is a
    # Trend-Following + Momentum pair distinct from ["A1", "C1"] above, and
    # reusing C1 here would have made the two combos differ only in their
    # trend leg.
    ["B2", "C2"],   # Trend Following (IBD base breakout) + Cross-Sectional Momentum
    ["A4", "C5"],   # Mean Reversion + 52-week-high proximity (Trend Following)
]


def build_jobs(start_date: date, end_date: date, quick: bool = False) -> List[Dict]:
    template_names = [t.name for t in TEMPLATES[:5]] if quick else [t.name for t in TEMPLATES]
    top_n_options = [TOP_N_OPTIONS[0]] if quick else TOP_N_OPTIONS
    combos = COMBO_TEMPLATES[:1] if quick else COMBO_TEMPLATES

    jobs: List[Dict] = []

    def _base_job(top_n: int) -> Dict:
        return {
            "kind": "orchestrator", "channel": "technical",
            "start_date": start_date.isoformat(), "end_date": end_date.isoformat(),
            "universe_spec": "curated", "max_tickers": 800, "min_history_days": 60,
            "top_n": top_n, "exit_variant": FIXED_EXIT_VARIANT, "max_hold_days": FIXED_MAX_HOLD_DAYS,
            "initial_capital": TECHNICAL_INITIAL_CAPITAL,
            "defer_db_writes": True,
            "precomputed_matches_dir": str(SCREENER_CACHE_DIR),
            # 2026-08-06: prefetch_feature_parquets DISABLED — eagerly
            # loading every day's feature Parquet for a 10-year/800-ticker
            # window blew to ~8.5 GB RSS (far beyond the ~1.5 GB estimate
            # in run_orchestrator_backtest.py's docstring), triggering swap
            # thrash and risking systemd-oomd kill. The lazy single-slot
            # cache is slower but memory-safe. Re-enable only for short
            # windows (<2 years) or small universes (<200 tickers) once
            # a chunked-prefetch implementation caps peak memory. OHLCV
            # doesn't need a flag here: every job shares one date range,
            # so run_strategy_queue.py's _maybe_prewarm_ohlcv()
            # auto-injects ohlcv_snapshot_dir (FeatureBacklog A73).
            "prefetch_feature_parquets": False,
        }

    for template_name in template_names:
        for top_n in top_n_options:
            for strategy_name, extra_fields in COMPOSITE_STRATEGIES.items():
                job = {**_base_job(top_n), "template_name": template_name, **extra_fields}
                job["_variant_kind"] = "single"
                job["_strategy_name"] = strategy_name
                job["_template_name"] = template_name
                jobs.append(job)

    for combo in combos:
        for top_n in top_n_options:
            for strategy_name, extra_fields in COMPOSITE_STRATEGIES.items():
                job = {**_base_job(top_n), "combo_templates": ",".join(combo), **extra_fields}
                job["_variant_kind"] = "combo"
                job["_strategy_name"] = strategy_name
                job["_template_name"] = "+".join(combo)
                jobs.append(job)

    return jobs


def _strip_bookkeeping(job: Dict) -> Dict:
    return {k: v for k, v in job.items() if not k.startswith("_")}


def _signal_failure_breakdown_from_trade_book(run_id: Optional[str]) -> Dict:
    """Reads back this job's own enriched trade_book_{run_id}.csv
    (backtest/export_trade_book.py, 2026-08-01 entry_signal_score/
    entry_indicator_values columns) and computes the losing-trades
    breakdown — same concept as backtest/technical_reporting.py's
    signal_failure_breakdown(), operating on CSV rows instead of live
    Trade objects since a queue job's trades aren't in this process."""
    empty = {"n_losing_trades": 0, "n_winning_trades": 0, "losing_trades": [],
             "mean_matched_conditions_ratio_losers": None, "mean_matched_conditions_ratio_winners": None}
    if not run_id:
        return empty
    path = REPORTS_DIR / f"trade_book_{run_id}.csv"
    if not path.exists():
        return empty

    losers, winners = [], []
    loser_ratios, winner_ratios = [], []
    with open(path, newline="") as fh:
        for row in csv.DictReader(fh):
            pnl_pct = row.get("pnl_pct")
            if pnl_pct in (None, ""):
                continue
            pnl_pct = float(pnl_pct)
            fv_raw = row.get("entry_indicator_values") or ""
            ratio = None
            try:
                fv = json.loads(fv_raw) if fv_raw else None
                if fv and fv.get("matched") and fv.get("total_conditions"):
                    ratio = fv.get("matched_conditions", 0) / fv["total_conditions"]
            except (json.JSONDecodeError, KeyError, ZeroDivisionError, TypeError):
                ratio = None

            if pnl_pct < 0:
                losers.append(row)
                if ratio is not None:
                    loser_ratios.append(ratio)
            else:
                winners.append(row)
                if ratio is not None:
                    winner_ratios.append(ratio)

    return {
        "n_losing_trades": len(losers),
        "n_winning_trades": len(winners),
        "losing_trades": [
            {
                "ticker": r["ticker"], "buy_date": r["buy_date"], "sell_date": r["sell_date"],
                "pnl_pct": float(r["pnl_pct"]), "entry_signal_score": r.get("entry_signal_score"),
            }
            for r in losers
        ],
        "mean_matched_conditions_ratio_losers": (sum(loser_ratios) / len(loser_ratios)) if loser_ratios else None,
        "mean_matched_conditions_ratio_winners": (sum(winner_ratios) / len(winner_ratios)) if winner_ratios else None,
    }


def aggregate_report(jobs: List[Dict], report_suffix: str, include_signal_failures: bool = True) -> Dict:
    variants = []
    for i, job in enumerate(jobs):
        report = _load_job_report(report_suffix, i)
        if report is None:
            continue
        metrics = report.get("metrics", {})
        run_id = report.get("run", {}).get("run_id")
        variant = {
            "strategy": job["_strategy_name"], "variant_kind": job["_variant_kind"],
            "template": job["_template_name"], "top_n": job["top_n"],
            "run_id": run_id,
            "cagr": metrics.get("cagr"),
            "sharpe": metrics.get("sharpe"),
            "sortino": metrics.get("sortino"),
            "calmar": metrics.get("calmar"),
            "win_rate": metrics.get("win_rate"),
            "n_trades": metrics.get("n_trades"),
            "total_trades": metrics.get("total_trades"),
            "avg_trade_duration_days": metrics.get("avg_trade_duration_days"),
            "n_outlier_trades": metrics.get("n_outlier_trades"),
            "max_abs_return_zscore": metrics.get("max_abs_return_zscore"),
        }
        if include_signal_failures:
            variant["signal_failures"] = _signal_failure_breakdown_from_trade_book(run_id)
        variants.append(variant)

    return {
        "generated_at": now_ist().isoformat(),
        "initial_capital": TECHNICAL_INITIAL_CAPITAL,
        "strategies": {
            "balanced": {"filters": _BALANCED_FIELDS},
            "risk_managed": {"adds": ["downtrend_filter_pct"]},
            "max_defensive": {"adds": ["disable_buys_in_regime=bear"]},
        },
        "combo_templates": COMBO_TEMPLATES,
        "n_jobs_total": len(jobs),
        "n_jobs_reported": len(variants),
        "variants": variants,
    }


def run_recommended_strategies(
    years_back: int = 10, quick: bool = False, end_date: Optional[date] = None,
    report_suffix: Optional[str] = None, max_workers: int = DEFAULT_MAX_WORKERS,
) -> Dict:
    end = end_date or now_ist().date()
    start = end - timedelta(days=365 * years_back)
    jobs = build_jobs(start, end, quick=quick)

    suffix = report_suffix or f"technical_recommended_strategies_{now_ist().strftime('%Y%m%d_%H%M%S')}"
    QUEUE_DEFS_DIR.mkdir(parents=True, exist_ok=True)
    queue_def_path = QUEUE_DEFS_DIR / f"{suffix}.json"
    with open(queue_def_path, "w") as fh:
        json.dump({"jobs": [_strip_bookkeeping(j) for j in jobs]}, fh, indent=2)
    logger.info(f"run_technical_recommended_strategies: wrote {len(jobs)}-job queue def to {queue_def_path}")

    run_queue(
        [_strip_bookkeeping(j) for j in jobs], report_suffix=suffix, stop_on_failure=False,
        max_workers=max_workers,
    )

    report = aggregate_report(jobs, suffix)
    report_path = REPORTS_DIR / "technical" / f"technical_recommended_strategies_{now_ist().strftime('%Y%m%d_%H%M%S')}.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with open(report_path, "w") as fh:
        json.dump(report, fh, indent=2, default=str)
    logger.info(f"run_technical_recommended_strategies: wrote report to {report_path}")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Technical channel recommended composite/combo strategies (Momentum-parity)")
    parser.add_argument("--years-back", type=int, default=10)
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--report-suffix", default=None)
    parser.add_argument("--max-workers", type=int, default=DEFAULT_MAX_WORKERS)
    args = parser.parse_args()
    run_recommended_strategies(
        years_back=args.years_back, quick=args.quick, report_suffix=args.report_suffix,
        max_workers=args.max_workers,
    )


if __name__ == "__main__":
    main()
