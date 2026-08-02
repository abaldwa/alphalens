"""
scripts/run_technical_filter_overlays.py

Phase: Technical Analysis Momentum-parity backtest reporting (2026-08-01)
Owner: Platform / Backtest
Consumers: operator CLI (`python3 -m scripts.run_technical_filter_overlays`),
    datastore/api/routers/technical_backtest.py's /filter_overlays(/trigger)

Runs each of the 5 new TechnicalAdapter entry-side filters (min_adtv_cr,
quality_gate, downtrend_filter_pct, circuit_band_pct, disable_buys_in_regime
— backtest/adapters/technical_adapter.py's 2026-08-01 Momentum-parity
additions) INDIVIDUALLY against the same TEMPLATES x top_n grid, at a fixed
exit_variant="baseline" and max_hold_days (isolating each filter's own
effect rather than conflating it with exit-timeframe/exit-variant choice —
mirrors scripts/run_momentum_filter_overlays.py's "7 filters run
individually" structure, 5 filters here since size_beta_orthogonalized has
no Technical analogue, see the approved plan).

2026-08-01 user instruction: do NOT run the backtest yet.
"""

import argparse
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
    _job_descriptor,
    _load_job_report,
    _quick_templates,
)
from systems.technical_analysis.screener.templates import TEMPLATES

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

FIXED_EXIT_VARIANT = "baseline"
FIXED_MAX_HOLD_DAYS = 63  # ~1 quarter — a reasonable middle value; isolates each filter's own effect
TOP_N_OPTIONS = [10, 15, 20]

# filter_name -> extra job fields (threaded into run_orchestrator_backtest.py's
# CLI via run_strategy_queue.py's job-dict-to-flags mechanism). None (the
# "baseline" reference row) applies no extra filter — same reference-row
# convention as Momentum's overlay sweep.
FILTERS: Dict[str, Dict] = {
    "baseline": {},
    "liquidity_floor": {"min_adtv_cr": 0.1},  # phase_3 threshold, same as the Momentum analysis run
    "quality_gated": {"quality_gate_min_f_score": 4},
    "downtrend_filter": {"downtrend_filter_pct": 0.05, "downtrend_lookback_days": 20},
    "circuit_lock_proxy": {"circuit_band_pct": 0.19},
    "regime_conditional": {"disable_buys_in_regime": "bear"},
}


def build_jobs(start_date: date, end_date: date, quick: bool = False) -> List[Dict]:
    template_names = _quick_templates() if quick else [t.name for t in TEMPLATES]
    top_n_options = [TOP_N_OPTIONS[0]] if quick else TOP_N_OPTIONS

    jobs: List[Dict] = []
    for template_name in template_names:
        for top_n in top_n_options:
            for filter_name, extra_fields in FILTERS.items():
                job = {
                    "kind": "orchestrator", "channel": "technical",
                    "start_date": start_date.isoformat(), "end_date": end_date.isoformat(),
                    "universe_spec": "curated", "max_tickers": 800, "min_history_days": 60,
                    "template_name": template_name, "top_n": top_n,
                    "exit_variant": FIXED_EXIT_VARIANT, "max_hold_days": FIXED_MAX_HOLD_DAYS,
                    "initial_capital": TECHNICAL_INITIAL_CAPITAL,
                    "defer_db_writes": True,
                    "precomputed_matches_dir": str(SCREENER_CACHE_DIR),
                    **extra_fields,
                }
                job["_filter_name"] = filter_name  # queue-only bookkeeping, stripped before dispatch below
                jobs.append(job)
    return jobs


def _strip_bookkeeping(job: Dict) -> Dict:
    return {k: v for k, v in job.items() if not k.startswith("_")}


def aggregate_report(jobs: List[Dict], report_suffix: str) -> Dict:
    variants = []
    for i, job in enumerate(jobs):
        report = _load_job_report(report_suffix, i)
        if report is None:
            continue
        metrics = report.get("metrics", {})
        variants.append({
            "filter": job["_filter_name"],
            **_job_descriptor(_strip_bookkeeping(job)),
            "run_id": report.get("run", {}).get("run_id"),
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
        })
    return {
        "generated_at": now_ist().isoformat(),
        "filters": list(FILTERS.keys()),
        "filter_params": FILTERS,
        "n_jobs_total": len(jobs),
        "n_jobs_reported": len(variants),
        "variants": variants,
    }


def run_filter_overlays(
    years_back: int = 10, quick: bool = False, end_date: Optional[date] = None,
    report_suffix: Optional[str] = None, max_workers: int = DEFAULT_MAX_WORKERS,
) -> Dict:
    end = end_date or now_ist().date()
    start = end - timedelta(days=365 * years_back)
    jobs = build_jobs(start, end, quick=quick)

    suffix = report_suffix or f"technical_filter_overlays_{now_ist().strftime('%Y%m%d_%H%M%S')}"
    QUEUE_DEFS_DIR.mkdir(parents=True, exist_ok=True)
    queue_def_path = QUEUE_DEFS_DIR / f"{suffix}.json"
    with open(queue_def_path, "w") as fh:
        json.dump({"jobs": [_strip_bookkeeping(j) for j in jobs]}, fh, indent=2)
    logger.info(f"run_technical_filter_overlays: wrote {len(jobs)}-job queue def to {queue_def_path}")

    run_queue(
        [_strip_bookkeeping(j) for j in jobs], report_suffix=suffix, stop_on_failure=False,
        max_workers=max_workers,
    )

    report = aggregate_report(jobs, suffix)
    report_path = REPORTS_DIR / "technical" / f"technical_filter_overlays_{now_ist().strftime('%Y%m%d_%H%M%S')}.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with open(report_path, "w") as fh:
        json.dump(report, fh, indent=2, default=str)
    logger.info(f"run_technical_filter_overlays: wrote report to {report_path}")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Technical channel entry-filter overlay sweep (Momentum-parity)")
    parser.add_argument("--years-back", type=int, default=10)
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--report-suffix", default=None)
    parser.add_argument("--max-workers", type=int, default=DEFAULT_MAX_WORKERS)
    args = parser.parse_args()
    run_filter_overlays(
        years_back=args.years_back, quick=args.quick, report_suffix=args.report_suffix,
        max_workers=args.max_workers,
    )


if __name__ == "__main__":
    main()
