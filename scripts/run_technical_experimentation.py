"""
scripts/run_technical_experimentation.py

Phase: Technical Analysis Momentum-parity backtest reporting (2026-08-01)
Owner: Platform / Backtest
Consumers: operator CLI (`python3 -m scripts.run_technical_experimentation`),
    datastore/api/routers/technical_backtest.py's /experimentation(/trigger)

Baseline sweep for the Technical channel, modeled directly on
scripts/run_momentum_experimentation.py: instead of Momentum's
band x lookback x rebalance x top_n grid (run in-process against
MomentumBacktester), this sweeps
TEMPLATES (42) x EXIT_POLICY_VARIANTS (7) x max_hold_days x top_n against
the shared BacktestOrchestrator via backtest/run_orchestrator_backtest.py —
one real subprocess per config, driven through backtest/run_strategy_queue.py
(the same queue runner that already safely handled 400+-job grids this
session) rather than an in-process loop, since each config here is a full
orchestrator run (real OHLCV fetch, DuckDB writes, corporate-action
handling) — much heavier per-config than Momentum's lightweight in-memory
backtest, and NOT safe to run concurrently in-process the way Momentum's
loop is (backtest.core.batch_common.exclusive_backtest_lock already
enforces strictly-sequential real backtests).

Full grid: 42 templates x 7 exit variants x 4 max_hold_days x 3 top_n =
3,528 configs. --quick reduces this for iteration (5 templates, 1
max_hold_days, 1 top_n = 35 configs). Capital fixed at
TECHNICAL_INITIAL_CAPITAL (Rs 1 Lakh per strategy, 2026-08-01 user
decision) — a script constant, not touching config/settings.py's global
default.

2026-08-01 user instruction: do NOT run the backtest yet — this script is
written and unit-testable (job-list construction, report aggregation) but
not invoked as part of this change.
"""

import argparse
import json
import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Optional

from backtest.core.engine import EXIT_POLICY_VARIANTS
from backtest.run_strategy_queue import run_queue
from config.timezone import now_ist
from systems.technical_analysis.screener.templates import TEMPLATES

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

REPORTS_DIR = Path(__file__).resolve().parent.parent / "backtest" / "reports"
QUEUE_DEFS_DIR = REPORTS_DIR / "queue_defs"

TECHNICAL_INITIAL_CAPITAL = 100_000.0  # Rs 1 Lakh — 2026-08-01 user decision, script-local only

# Trading-day horizons to test the day-count exit barrier at (~1/3/6/12
# months) — the "optimum time for each strategy" isn't known, so this is a
# genuine sweep axis, same granularity as Momentum's LOOKBACK_MONTHS.
MAX_HOLD_DAYS_OPTIONS = [21, 63, 126, 252]
TOP_N_OPTIONS = [10, 15, 20]

# "condition"/"unconstrained" don't honor max_hold_days (see
# backtest/core/engine.py::build_exit_model_for_variant's docstring) — swept
# once each (max_hold_days omitted) rather than once per hold-days value,
# to avoid 4 literally-identical duplicate runs per template.
_VARIANTS_IGNORING_HOLD_DAYS = {"condition", "unconstrained"}


def _quick_templates() -> List[str]:
    return [t.name for t in TEMPLATES[:5]]


def build_jobs(
    start_date: date, end_date: date, quick: bool = False,
    universe_spec: str = "curated", max_tickers: int = 800, min_history_days: int = 60,
) -> List[Dict]:
    template_names = _quick_templates() if quick else [t.name for t in TEMPLATES]
    hold_days_options = [MAX_HOLD_DAYS_OPTIONS[1]] if quick else MAX_HOLD_DAYS_OPTIONS  # 63d only in quick mode
    top_n_options = [TOP_N_OPTIONS[0]] if quick else TOP_N_OPTIONS

    jobs: List[Dict] = []
    for template_name in template_names:
        for variant in EXIT_POLICY_VARIANTS:
            hold_days_grid = [None] if variant in _VARIANTS_IGNORING_HOLD_DAYS else hold_days_options
            for max_hold_days in hold_days_grid:
                for top_n in top_n_options:
                    job = {
                        "kind": "orchestrator", "channel": "technical",
                        "start_date": start_date.isoformat(), "end_date": end_date.isoformat(),
                        "universe_spec": universe_spec, "max_tickers": max_tickers,
                        "min_history_days": min_history_days,
                        "template_name": template_name, "top_n": top_n,
                        "exit_variant": variant,
                        "initial_capital": TECHNICAL_INITIAL_CAPITAL,
                    }
                    if max_hold_days is not None:
                        job["max_hold_days"] = max_hold_days
                    jobs.append(job)
    return jobs


def _job_descriptor(job: Dict) -> Dict:
    """The sweep-axis values a report row needs, independent of whatever
    ended up in the job's actual saved run.config (belt-and-suspenders —
    read back from the job dict we built, not assumed to match)."""
    return {
        "template_name": job["template_name"], "top_n": job["top_n"],
        "exit_variant": job["exit_variant"], "max_hold_days": job.get("max_hold_days"),
    }


def _load_job_report(report_suffix: str, job_index: int) -> Optional[Dict]:
    path = REPORTS_DIR / f"orchestrator_{report_suffix}_job{job_index}.json"
    if not path.exists():
        logger.warning(f"run_technical_experimentation: no report file for job[{job_index}] at {path}")
        return None
    with open(path) as fh:
        return json.load(fh)


def aggregate_report(jobs: List[Dict], report_suffix: str) -> Dict:
    """Reads back every job's own orchestrator_{report_suffix}_job{N}.json
    (written by run_orchestrator_backtest.py) and flattens them into one
    variants list, same shape as momentum_experimentation_*.json's
    (cagr/sharpe/sortino/calmar/total_trades/avg_trade_duration_days/
    n_outlier_trades/max_abs_return_zscore per variant) so the same
    frontend DataTable pattern works unmodified for Technical."""
    variants = []
    for i, job in enumerate(jobs):
        report = _load_job_report(report_suffix, i)
        if report is None:
            continue
        metrics = report.get("metrics", {})
        variants.append({
            **_job_descriptor(job),
            "run_id": report.get("run", {}).get("run_id"),
            "cagr": metrics.get("cagr"),
            "cagr_trading_day_legacy": metrics.get("cagr_trading_day_legacy"),
            "xirr": metrics.get("xirr"),
            "sharpe": metrics.get("sharpe"),
            "sortino": metrics.get("sortino"),
            "calmar": metrics.get("calmar"),
            "win_rate": metrics.get("win_rate"),
            "max_drawdown": metrics.get("max_drawdown"),
            "n_trades": metrics.get("n_trades"),
            "total_trades": metrics.get("total_trades"),
            "avg_trade_duration_days": metrics.get("avg_trade_duration_days"),
            "n_outlier_trades": metrics.get("n_outlier_trades"),
            "max_abs_return_zscore": metrics.get("max_abs_return_zscore"),
            "final_capital": metrics.get("final_capital"),
            "turnover_ratio": metrics.get("turnover_ratio"),
        })
    return {
        "generated_at": now_ist().isoformat(),
        "initial_capital": TECHNICAL_INITIAL_CAPITAL,
        "n_jobs_total": len(jobs),
        "n_jobs_reported": len(variants),
        "variants": variants,
    }


def run_experimentation(
    years_back: int = 10, quick: bool = False, end_date: Optional[date] = None,
    report_suffix: Optional[str] = None,
) -> Dict:
    end = end_date or now_ist().date()
    start = end - timedelta(days=365 * years_back)
    jobs = build_jobs(start, end, quick=quick)

    suffix = report_suffix or f"technical_experimentation_{now_ist().strftime('%Y%m%d_%H%M%S')}"
    QUEUE_DEFS_DIR.mkdir(parents=True, exist_ok=True)
    queue_def_path = QUEUE_DEFS_DIR / f"{suffix}.json"
    with open(queue_def_path, "w") as fh:
        json.dump({"jobs": jobs}, fh, indent=2)
    logger.info(f"run_technical_experimentation: wrote {len(jobs)}-job queue def to {queue_def_path}")

    run_queue(jobs, report_suffix=suffix, stop_on_failure=False)

    report = aggregate_report(jobs, suffix)
    report_path = REPORTS_DIR / "technical" / f"technical_experimentation_{now_ist().strftime('%Y%m%d_%H%M%S')}.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with open(report_path, "w") as fh:
        json.dump(report, fh, indent=2, default=str)
    logger.info(f"run_technical_experimentation: wrote report to {report_path}")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Technical channel baseline sweep (Momentum-parity)")
    parser.add_argument("--years-back", type=int, default=10)
    parser.add_argument("--quick", action="store_true", help="Reduced grid (5 templates, 1 max_hold_days, 1 top_n) for iteration")
    parser.add_argument("--report-suffix", default=None)
    args = parser.parse_args()
    run_experimentation(years_back=args.years_back, quick=args.quick, report_suffix=args.report_suffix)


if __name__ == "__main__":
    main()
